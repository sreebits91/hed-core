package queue

import (
	"sync"
	"hed-core/pkg/engine"
)

type RingQueue struct {
	mu     sync.Mutex
	items  []*engine.TxPayload
	head   int
	tail   int
	size   int
	closed bool
}

func NewRingQueue(capacity int) *RingQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &RingQueue{items: make([]*engine.TxPayload, capacity)}
}

func (q *RingQueue) Push(tx *engine.TxPayload) bool {
	if tx == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.size == len(q.items) {
		return false
	}
	q.items[q.tail] = tx
	q.tail = (q.tail + 1) % len(q.items)
	q.size++
	return true
}

func (q *RingQueue) Pop() (*engine.TxPayload, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		return nil, false
	}
	tx := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return tx, true
}

func (q *RingQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *RingQueue) Capacity() int { return len(q.items) }

func (q *RingQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
}
