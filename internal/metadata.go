package internal

// PackagingType defines how to store payload
type PackagingType byte

const (
	InlineData PackagingType = 0 // content stored in next bytes
	LinkedData PackagingType = 1 // content stored in attached file
)

const CurrentVersion = 1

//go:generate msgp
//msgp:tuple Metadata
//msgp:shim PackagingType as:byte

// Metadata of message, stored directly in queue.
type Metadata struct {
	// Protocol version
	Version uint16
	// Payload storing type
	PackageType PackagingType
	// Payload size in bytes
	Size int64
	// Number of attempts. 0 means fresh messaged. Should grow every re-queue operation (except abnormal shutdown).
	Attempts int64
	// inline data, valid ony in case package type = inline
	InlineData []byte
}
