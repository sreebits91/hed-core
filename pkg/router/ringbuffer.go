package router

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	ErrBufferFull  = errors.New("ring buffer full")
	ErrBufferEmpty = errors.New("ring buffer empty")
)

// TransactionPayload represents a single raw transaction payload in hed-core
type TransactionPayload struct {
	TxID      string
	Namespace string
	Payload   []byte
	Key       string
}

// slot wraps the data with CPU cache-line padding (64 bytes) to prevent false sharing
type slot struct {
	tx   TransactionPayload
	_pad [64 - (unsafeSizeOfTxPayload % 64)]byte
}

const unsafeSizeOfTxPayload = 64 // Approximated cache boundary alignment

// RingBuffer is a lock-free, bounded MPMC (Multi-Producer Multi-Consumer) ring buffer
type RingBuffer struct {
	_pad0 [64]byte
	head  atomic.Uint64 // Write sequence counter
	_pad1 [64]byte
	tail  atomic.Uint64 // Read sequence counter
	_pad2 [64]byte
	mask  uint64
	size  uint64
	slots []slot
}

// NewRingBuffer initializes a RingBuffer. Size MUST be a power of 2 (e.g., 1024, 65536)
func NewRingBuffer(capacity uint64) *RingBuffer {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of 2")
	}
	return &RingBuffer{
		size:  capacity,
		mask:  capacity - 1,
		slots: make([]slot, capacity),
	}
}

// Push Enqueues a transaction using atomic Compare-And-Swap (CAS)
func (rb *RingBuffer) Push(tx TransactionPayload) error {
	for {
		head := rb.head.Load()
		tail := rb.tail.Load()

		if head-tail >= rb.size {
			return ErrBufferFull
		}

		if rb.head.CompareAndSwap(head, head+1) {
			idx := head & rb.mask
			rb.slots[idx].tx = tx
			return nil
		}
		// Yield CPU thread to avoid spinning burn under high worker contention
		runtime.Gosched()
	}
}

// PopBatch Dequeues up to `maxBatch` transactions atomically without locking
func (rb *RingBuffer) PopBatch(maxBatch uint64) ([]TransactionPayload, error) {
	for {
		tail := rb.tail.Load()
		head := rb.head.Load()

		if tail >= head {
			return nil, ErrBufferEmpty
		}

		available := head - tail
		n := maxBatch
		if available < n {
			n = available
		}

		if rb.tail.CompareAndSwap(tail, tail+n) {
			batch := make([]TransactionPayload, n)
			for i := uint64(0); i < n; i++ {
				idx := (tail + i) & rb.mask
				batch[i] = rb.slots[idx].tx
			}
			return batch, nil
		}
		runtime.Gosched()
	}
}

// Length returns current unconsumed items count
func (rb *RingBuffer) Length() uint64 {
	head := rb.head.Load()
	tail := rb.tail.Load()
	if head >= tail {
		return head - tail
	}
	return 0
}
