package queue

import "sync"

type Queue[T any] struct {
	head *node[T]
	tail *node[T]
	len  int

	mu *sync.Mutex
}

type node[T any] struct {
	value T
	next  *node[T]
}

func New[T any]() *Queue[T] {
	return &Queue[T]{
		mu: &sync.Mutex{},
	}
}

func (q *Queue[T]) Push(value T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	n := &node[T]{}
	n.value = value

	if q.head == nil {
		q.head = n
		q.tail = n
	} else {
		q.tail.next = n
		q.tail = n
	}

	q.len++
}

func (q *Queue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var t T

	if q.head == nil {
		return t, false
	}

	n := q.head
	q.head = n.next
	if q.head == nil {
		q.tail = nil
	}

	q.len--
	return n.value, true
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.len
}

func (q *Queue[T]) Empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.len == 0
}
