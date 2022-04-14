//go:generate mockgen -package mocks -destination mocks/interface.go -source=interface.go
//go:generate mockgen -package mocks -destination mocks/sdk_s3api.go github.com/aws/aws-sdk-go/service/s3/s3iface S3API
package s3

import (
	"errors"
	"io"
)

var ErrKeyNotFound = errors.New("key not found")

type BasicClient interface {
	Lister
	Getter
	Putter
	BufferPutter
	Deleter
}

type Client interface {
	BasicClient
	Mover
}

type Lister interface {
	List(key string) (keys []string, err error)
}

type Getter interface {
	// Get returns ErrKeyNotFound if the given key doesn't exist.
	Get(key string) (data []byte, err error)
}

type Putter interface {
	Put(key string, data []byte) (err error)
}

// BufferPutter can be used to put a file to S3 since File implements Read and Seek.
type BufferPutter interface {
	BufferPut(key string, buf io.ReadSeeker) (err error)
}

type Deleter interface {
	Delete(key string) error
}

type Mover interface {
	// Move returns ErrKeyNotFound if the src key doesn't exist.
	Move(src, dst string) error
}
