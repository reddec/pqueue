package pqueue

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/reddec/pqueue/internal"
	"go.etcd.io/bbolt"
)

const (
	DefaultBucket     = "messages"
	DefaultStorageDir = "queue-data"
	DefaultInlineSize = 8192
)

var (
	ErrEmpty = errors.New("queue is empty")
)

// Default is alias to Open with all defaults. dir/data as storage dir and dir/index.db
// as metadata storage.  Queue must be closed to avoid resource leak.
func Default(dir string) (*ClosableQueue, error) {
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0766); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	indexFile := filepath.Join(dir, "index.db")
	return Open(indexFile, Config{
		StorageDir: storageDir,
	})
}

// Open queue and allocate resources. Queue must be closed to avoid resource leak.
func Open(indexFile string, config Config) (*ClosableQueue, error) {
	db, err := bbolt.Open(indexFile, 0666, bbolt.DefaultOptions)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	return &ClosableQueue{
		Queue: New(db, config),
	}, nil
}

type Config struct {
	StorageDir string // directory to store files, default is DefaultStorageDir
	Bucket     string // bucket name in bbolt db, default is DefaultBucket
	InlineSize int    // payload bigger then the size will be stored outside of queue, default is DefaultInlineSize
}

// New queue using pre-allocated BBoltDB.
func New(db *bbolt.DB, config Config) *Queue {
	if config.Bucket == "" {
		config.Bucket = DefaultBucket
	}
	if config.StorageDir == "" {
		config.StorageDir = DefaultStorageDir
	}
	if config.InlineSize <= 0 {
		config.InlineSize = DefaultInlineSize
	}

	return &Queue{
		bucket:       []byte(config.Bucket),
		db:           db,
		storageDir:   config.StorageDir,
		messageReady: make(chan struct{}, 1),
		inlineSize:   config.InlineSize,
	}
}

// Stats of queue operation.
type Stats struct {
	Added    int64 // total amount of successfully added items to the queue (can only grow)
	Returned int64 // total amount of successfully committed items with discard=false (can only grow)
	Removed  int64 // total amount of successfully committed items with discard=true (can only grow)
	Size     int64 // size of the queue
	Locked   int64 // number of locked messages
}

type ClosableQueue struct {
	*Queue
}

// Close internal database.
func (cq *ClosableQueue) Close() error {
	return cq.db.Close()
}

// Queue is designed to be universal and process messages with payload bigger than RAM by storing
// data as separated file and keeping in queue only reference to that file. Small messages will
// be inlined in the queue.
//
// Important: to re-use queue it's required to define same storage location as before, otherwise
// links to payload files will be broken.
type Queue struct {
	inlineSize   int
	stats        Stats
	storageDir   string
	db           *bbolt.DB
	bucket       []byte
	locked       internal.IntSet
	messageReady chan struct{}
}

// Put item to queue.
// If data stream is smaller or equal to inline size it will be stored in queue metadata,
// otherwise it will be stored as linked file.
//
// Properties (could be nil) always stored in queue. Do not put too much information into properties because
// queue item should be read in-memory before processing.
//
// Returns unique ID of the message.
func (bq *Queue) Put(data io.Reader, properties map[string][]byte) (uint64, error) {
	var targetFile string

	metadata, tempFile, err := bq.storeStream(data, properties)
	if err != nil {
		return 0, fmt.Errorf("store stream: %w", err)
	}
	var assignedID uint64
	// save
	err = bq.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bq.bucket)
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}

		id, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("allocate next id: %w", err)
		}

		if tempFile != nil {
			// 'commit' payload file by renaming temp file to ID-based filename. This supposed to be atomic operation.
			targetFile = bq.linkedFile(id)
			err = os.Rename(tempFile.Name(), targetFile)

			if err != nil {
				targetFile = "" // not moved
				return fmt.Errorf("rename temp file to queue file: %w", err)
			}

			tempFile = nil
		}

		var k [8]byte
		binary.BigEndian.PutUint64(k[:], id)
		assignedID = id
		return b.Put(k[:], metadata)
	})
	if err == nil {
		atomic.AddInt64(&bq.stats.Added, 1)
		bq.notifyReady()
		return assignedID, nil
	}
	if tempFile != nil {
		_ = os.Remove(tempFile.Name())
	}
	if targetFile != "" {
		// cleanup mess
		_ = os.Remove(targetFile)
	}
	return 0, err
}

// Try getting message from the queue or return ErrEmpty. Returned message MUST be committed.
func (bq *Queue) Try() (message *Message, err error) {
	err = bq.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bq.bucket)
		if b == nil {
			return ErrEmpty
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := binary.BigEndian.Uint64(k)
			if !bq.locked.Add(id) || len(v) == 0 {
				continue // already locked or broken
			}

			var meta internal.Metadata
			_, err := meta.UnmarshalMsg(v)
			if err != nil {
				return fmt.Errorf("broken message %d record: %w", id, err)
			}

			switch meta.PackageType {
			case internal.InlineData:
				// message data stored as next bytes
				message = &Message{
					id:    id,
					queue: bq,
					meta:  meta,
				}
			case internal.LinkedData:
				// message data stored as linked file
				fallthrough
			default:
				message = &Message{
					id:    id,
					meta:  meta,
					queue: bq,
				}
			}
			return nil
		}
		return ErrEmpty
	})
	return
}

// Get message from the queue or block till message will be available or context canceled.
// Returned message MUST be committed.
func (bq *Queue) Get(ctx context.Context) (message *Message, err error) {
	for {
		message, err = bq.Try()
		if err == nil {
			break
		}
		if !errors.Is(err, ErrEmpty) {
			return
		}

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-bq.messageReady:
		}
	}
	bq.notifyReady()
	return
}

// Clear queue items and linked files. This may take a time in case of big queue.
// Running clear with opened linked files may cause platform-depended behaviour.
func (bq *Queue) Clear() error {
	return bq.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bq.bucket)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := binary.BigEndian.Uint64(k)
			if err := c.Delete(); err != nil {
				return fmt.Errorf("remove record %d: %w", id, err)
			}
			bq.locked.Remove(id)
			var meta internal.Metadata
			if _, err := meta.UnmarshalMsg(v); err == nil && meta.PackageType == internal.LinkedData {
				if err := os.Remove(bq.linkedFile(id)); err != nil {
					return fmt.Errorf("remove linked file for record %d: %w", id, err)
				}
			}
		}
		return nil
	})
}

// Stats snapshot. It requires view transaction to the kv database, so it is not free in terms of performance.
func (bq *Queue) Stats() Stats {
	var size int64
	_ = bq.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bq.bucket)
		if b != nil {
			size = int64(b.Stats().KeyN)
		}
		return nil
	})
	cp := bq.stats
	cp.Size = size
	cp.Locked = int64(bq.locked.Size())
	return cp
}

// commit single message by ID and release lock. Discard also removes message.
// in case discard=false, message will be re-saved with new attempt value.
func (bq *Queue) commit(id uint64, kind internal.PackagingType, discard bool) (err error) {
	if !discard {
		err = bq.requeue(id)
	} else {
		err = bq.discard(id, kind)
	}
	if err != nil {
		return err
	}
	if discard {
		atomic.AddInt64(&bq.stats.Removed, 1)
	} else {
		atomic.AddInt64(&bq.stats.Returned, 1)
	}
	return nil
}

// requeue message back to queue and increment attempt number.
func (bq *Queue) requeue(id uint64) error {
	defer bq.notifyReady()
	defer bq.locked.Remove(id)
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)

	// we have to increment attempts number
	return bq.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bq.bucket)
		if b == nil {
			return nil
		}
		v := b.Get(k[:])
		if v == nil {
			// we have ID but no metadata...
			// something strange but OK, let's ignore it
			return nil
		}
		var meta internal.Metadata
		_, err := meta.UnmarshalMsg(v)
		if err != nil {
			return fmt.Errorf("parse metadata for message %d: %w", id, err)
		}
		meta.Attempts++
		v, err = meta.MarshalMsg(nil)
		if err != nil {
			return fmt.Errorf("marshal metadata for message %d: %w", id, err)
		}
		return b.Put(k[:], v)
	})
}

// discard message from queue and remove linked files if needed.
func (bq *Queue) discard(id uint64, kind internal.PackagingType) error {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)

	err := bq.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bq.bucket)
		if b == nil {
			return nil
		}
		return b.Delete(k[:])
	})

	if err != nil {
		return fmt.Errorf("remove entry from queue: %w", err)
	}
	// it's now safe to unlock entry because we removed it from the queue.
	// no need to notify - we are not releasing message.
	bq.locked.Remove(id)

	if kind == internal.LinkedData {
		if err := os.Remove(bq.linkedFile(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove linked file: %w", err)
		}
	}
	return nil
}

func (bq *Queue) storeStream(stream io.Reader, properties map[string][]byte) ([]byte, *os.File, error) {
	var meta internal.Metadata
	meta.Properties = properties
	meta.PackageType = internal.InlineData // by default

	var buffer = make([]byte, 1+bq.inlineSize) // +1 for trigger
	// pre-fetch piece of stream 1 byte bigger then inline size
	n, err := readBuffer(stream, buffer)
	if err != nil {
		return nil, nil, fmt.Errorf("pre-fetch stream: %w", err)
	}
	if n <= bq.inlineSize {
		// content will be stored in the queue
		meta.InlineData = buffer[:n]
		meta.Size = int64(n)
		data, err := meta.MarshalMsg(nil) // this sub-optimal place 'cause we are using re-allocating buffer
		return data, nil, err
	}
	// content should be stored in file
	meta.PackageType = internal.LinkedData

	f, written, err := bq.saveLinkedData(io.MultiReader(bytes.NewReader(buffer), stream))
	if err != nil {
		return nil, nil, fmt.Errorf("save linked data: %w", err)
	}
	// add information about size
	meta.Size = written
	// re-encode metadata
	data, err := meta.MarshalMsg(nil)
	return data, f, err
}

func (bq *Queue) saveLinkedData(data io.Reader) (*os.File, int64, error) {
	tempFile, err := ioutil.TempFile(bq.storageDir, "")
	if err != nil {
		return nil, 0, fmt.Errorf("create temp file: %w", err)
	}
	written, err := io.Copy(tempFile, data)
	if err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, 0, fmt.Errorf("copy data to file: %w", err)
	}
	err = tempFile.Close()
	if err != nil {
		_ = os.Remove(tempFile.Name())
		return nil, written, fmt.Errorf("close file: %w", err)
	}
	return tempFile, written, nil
}

func (bq *Queue) linkedFile(id uint64) string {
	return filepath.Join(bq.storageDir, strconv.FormatUint(id, 10))
}

func (bq *Queue) notifyReady() {
	select {
	case bq.messageReady <- struct{}{}:
	default:
	}
}

type Message struct {
	id       uint64
	meta     internal.Metadata
	complete bool
	queue    *Queue
	lock     sync.Mutex
}

// Attempt number started from 1. Increased every commit with discard=false.
func (m *Message) Attempt() int64 {
	return m.meta.Attempts + 1
}

// Size of content in bytes.
func (m *Message) Size() int64 {
	return m.meta.Size
}

// Properties as user provided for Put. Any modifications will not be visible for next Get/Try operation (ie: not saved).
func (m *Message) Properties() map[string][]byte {
	return m.meta.Properties
}

// Get property and interpret it as string. Sugar for Properties().
func (m *Message) Get(propertyName string) string {
	return string(m.meta.Properties[propertyName])
}

// ID of message. Unique within queue.
func (m *Message) ID() uint64 {
	return m.id
}

// Open stream of payload. Can be invoked several times to get multiple parallel streams.
// All streams must be closed individually.
func (m *Message) Open() (io.ReadSeekCloser, error) {
	switch m.meta.PackageType {
	case internal.InlineData:
		return &readSeekNopCloser{ReadSeeker: bytes.NewReader(m.meta.InlineData)}, nil
	case internal.LinkedData:
		return os.Open(m.queue.linkedFile(m.id))
	default:
		return nil, fmt.Errorf("can not create stream for message type %d", m.meta.PackageType)
	}
}

// Bytes of payload. Danger for big payload!
func (m *Message) Bytes() ([]byte, error) {
	if m.meta.PackageType == internal.InlineData {
		return m.meta.InlineData, nil
	}
	if m.meta.PackageType == internal.LinkedData {
		return ioutil.ReadFile(m.queue.linkedFile(m.id))
	}
	return nil, fmt.Errorf("can not detect message type %d", m.meta.PackageType)
}

// Commit message from the queue.
//
// If discard flag set, message will be completely removed from the queue, otherwise message will be released and
// available for next Get operation.
func (m *Message) Commit(discard bool) error {
	m.lock.Lock()
	if m.complete {
		m.lock.Unlock()
		return nil
	}
	m.complete = true
	m.lock.Unlock()
	return m.queue.commit(m.id, m.meta.PackageType, discard)
}

func readBuffer(reader io.Reader, buffer []byte) (int, error) {
	done := false
	n := len(buffer)
	total := 0
	for total < n && !done {
		v, err := reader.Read(buffer[total:])
		if errors.Is(err, io.EOF) {
			done = true
		} else if err != nil {
			return total, err
		}
		total += v
	}
	return total, nil
}

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (rsnc *readSeekNopCloser) Close() error {
	return nil
}
