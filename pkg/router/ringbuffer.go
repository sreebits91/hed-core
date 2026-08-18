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

// TransactionPayload represents a single raw transaction payload in hed-core.
type TransactionPayload struct {
	TxID      string
	Namespace string
	Payload   []byte
	Key       string
}

// slot is a sequence-numbered cell. The sequence is the ownership protocol:
// producers publish a fully written payload by advancing it to pos+1, while
// consumers return ownership by advancing it to pos+size.
type slot struct {
	seq atomic.Uint64
	tx  TransactionPayload
}

// RingBuffer is a bounded lock-free MPMC queue using per-slot sequence
// numbers. Unlike a head/tail-only ring, a consumer can never observe a slot
// before its producer has published the payload.
type RingBuffer struct {
	head  atomic.Uint64
	tail  atomic.Uint64
	mask  uint64
	size  uint64
	slots []slot
}

// NewRingBuffer initializes a bounded queue. Capacity must be a power of two.
func NewRingBuffer(capacity uint64) *RingBuffer {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of 2")
	}

	rb := &RingBuffer{
		size:  capacity,
		mask:  capacity - 1,
		slots: make([]slot, capacity),
	}
	for i := uint64(0); i < capacity; i++ {
		rb.slots[i].seq.Store(i)
	}
	return rb
}

// Push enqueues a transaction. It returns ErrBufferFull when no slot is
// immediately available rather than blocking indefinitely.
func (rb *RingBuffer) Push(tx TransactionPayload) error {
	for spins := 0; ; spins++ {
		pos := rb.head.Load()
		s := &rb.slots[pos&rb.mask]
		seq := s.seq.Load()
		dif := int64(seq - pos)

		if dif == 0 {
			if rb.head.CompareAndSwap(pos, pos+1) {
				s.tx = tx
				s.seq.Store(pos + 1) // publish only after payload is complete
				return nil
			}
		} else if dif < 0 {
			return ErrBufferFull
		}

		if spins&7 == 7 {
			runtime.Gosched()
		}
	}
}

// PopBatch dequeues up to maxBatch transactions. Each consumer first claims a
// published slot and only then reads its payload, preventing stale/unwritten
// data under MPMC contention.
func (rb *RingBuffer) PopBatch(maxBatch uint64) ([]TransactionPayload, error) {
	if maxBatch == 0 {
		return nil, ErrBufferEmpty
	}

	batch := make([]TransactionPayload, 0, maxBatch)
	for len(batch) < int(maxBatch) {
		tail := rb.tail.Load()
		s := &rb.slots[tail&rb.mask]
		seq := s.seq.Load()
		dif := int64(seq - (tail + 1))

		if dif == 0 {
			if !rb.tail.CompareAndSwap(tail, tail+1) {
				runtime.Gosched()
				continue
			}

			batch = append(batch, s.tx)
			// Mark this cell available to the producer that owns the next
			// generation of this position.
			s.seq.Store(tail + rb.size)
			continue
		}

		if dif < 0 {
			break
		}
		runtime.Gosched()
	}

	if len(batch) == 0 {
		return nil, ErrBufferEmpty
	}
	return batch, nil
}

// Length is an approximate instantaneous occupancy. Under concurrent MPMC
// access it is a diagnostic value, not a synchronization primitive.
func (rb *RingBuffer) Length() uint64 {
	head := rb.head.Load()
	tail := rb.tail.Load()
	if head < tail {
		return 0
	}
	length := head - tail
	if length > rb.size {
		return rb.size
	}
	return length
}
