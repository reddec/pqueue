package pqueue_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/reddec/pqueue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	q, err := pqueue.Open(filepath.Join(tmpDir, "index.db"), pqueue.Config{
		StorageDir: tmpDir,
	})
	require.NoError(t, err)

	defer q.Close()

	id, err := q.Put(bytes.NewBufferString("some data"))
	require.NoError(t, err)
	require.NoFileExists(t, fmt.Sprint(tmpDir, "/", id)) // should be inlined

	msg, err := q.Try()
	require.NoError(t, err)
	require.Equal(t, id, msg.ID())
	data, err := ioutil.ReadAll(msg)
	require.NoError(t, err)
	assert.Equal(t, "some data", string(data))
	assert.Equal(t, int64(9), msg.Size())

	err = msg.Commit(true)
	require.NoError(t, err)

	_, err = q.Try()
	assert.ErrorIs(t, err, pqueue.ErrEmpty)

	t.Run("linked file created, works and removed", func(t *testing.T) {
		bigPayload := make([]byte, pqueue.DefaultInlineSize+1)
		_, err := io.ReadFull(rand.Reader, bigPayload)
		require.NoError(t, err)

		id, err = q.Put(bytes.NewReader(bigPayload))
		require.NoError(t, err)

		require.FileExists(t, fmt.Sprint(tmpDir, "/", id))

		m, err := q.Try()
		require.NoError(t, err)
		require.Equal(t, id, m.ID())
		require.Equal(t, int64(len(bigPayload)), m.Size())
		loadedPayload, err := io.ReadAll(m)
		require.NoError(t, err)
		require.Equal(t, bigPayload, loadedPayload)
		err = m.Commit(true)
		require.NoError(t, err)

		require.NoFileExists(t, fmt.Sprint(tmpDir, "/", id))
	})

	t.Run("parallel work supported", func(t *testing.T) {
		var writers sync.WaitGroup
		var readers sync.WaitGroup

		var sum uint32

		for i := 0; i < 5; i++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				msg, err := q.Get(context.Background())
				defer msg.Commit(true)
				require.NoError(t, err)
				data, err := ioutil.ReadAll(msg)
				require.NoError(t, err)
				v := binary.BigEndian.Uint32(data)
				atomic.AddUint32(&sum, v)
			}()
		}

		for i := 0; i < 5; i++ {
			writers.Add(1)
			go func(i int) {
				defer writers.Done()
				var v [4]byte
				binary.BigEndian.PutUint32(v[:], uint32(i))
				_, err := q.Put(bytes.NewReader(v[:]))
				require.NoError(t, err)
			}(i)
		}

		writers.Wait()
		readers.Wait()

		var expected int
		for i := 0; i < 5; i++ {
			expected += i
		}

		require.Equal(t, uint32(expected), sum)
	})
}

func ExampleDefault() {
	// error handling omitted for convenience
	q, _ := pqueue.Default("./data")
	id, _ := q.Put(bytes.NewBufferString("hello world"))
	fmt.Println("id:", id)

	msg, _ := q.Get(context.TODO())
	defer msg.Commit(true)

	fmt.Println("got id:", msg.ID())
	data, _ := ioutil.ReadAll(msg)
	fmt.Println(string(data))
	//output:
	//id: 1
	//got id: 1
	//hello world
}