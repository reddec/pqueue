package internal

// PackagingType defines how to store payload
type PackagingType byte

const (
	InlineData PackagingType = 0 // content stored in next bytes
	LinkedData PackagingType = 1 // content stored in attached file
)

//go:generate msgp
//msgp:shim PackagingType as:byte

// Metadata of message, stored directly in queue.
type Metadata struct {
	// Payload storing type
	PackageType PackagingType `msg:"t,omitempty"`
	// Payload size in bytes
	Size int64 `msg:"s,omitempty"`
	// Number of attempts. 0 means fresh messaged. Should grow every re-queue operation (except abnormal shutdown).
	Attempts int64 `msg:"a,omitempty"`
	// inline data, valid ony in case package type = inline
	InlineData []byte `msg:"d,omitempty"`
	// user-defined properties
	Properties map[string][]byte `msg:"p,omitempty"`
}
