package allocator

type Allocator[T any] interface {
	Create() (*T, error)
	Delete(*T) error
}
