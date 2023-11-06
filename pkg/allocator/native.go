package allocator

// NativeAllocator uses Go's built-in memory management
type NativeAllocator[T any] struct{}

func (a *NativeAllocator[T]) Create() (*T, error) {
	return new(T), nil
}
func (a *NativeAllocator[T]) Delete(t *T) error {
	// noop
	return nil
}

// Enforce that NativeAllocator[T] implements Allocator[T]
var _ Allocator[int] = (*NativeAllocator[int])(nil)
