package batch

import (
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/queue"
)

type Batcher struct {
	queue queue.TransactionQueue
	size  int
	timeout time.Duration
}

func New(q queue.TransactionQueue, size int, timeout time.Duration) *Batcher {
	if size < 1 { size = 1 }
	if timeout <= 0 { timeout = time.Millisecond }
	return &Batcher{queue: q, size: size, timeout: timeout}
}

func (b *Batcher) Collect() []*engine.TxPayload {
	batch := make([]*engine.TxPayload, 0, b.size)
	deadline := time.Now().Add(b.timeout)
	for len(batch) < b.size {
		tx, ok := b.queue.Pop()
		if ok {
			batch = append(batch, tx)
			continue
		}
		if len(batch) > 0 && time.Now().After(deadline) { break }
		time.Sleep(time.Microsecond)
	}
	return batch
}
