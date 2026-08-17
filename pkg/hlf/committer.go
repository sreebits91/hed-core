package hlf

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
)

type BatchConfig struct {
	MaxBatchSize int
	FlushTimeout time.Duration
	WorkerCount  int
}

type HLFCommitter struct {
	txQueue   chan *engine.TxPayload
	committed uint64
	failed    uint64
	cfg       BatchConfig
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopOnce  sync.Once
}

func NewHLFCommitter(cfg BatchConfig) *HLFCommitter {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 2000
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 10 * time.Millisecond
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 16
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &HLFCommitter{
		txQueue: make(chan *engine.TxPayload, 200000),
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}

	c.startWorkers()
	return c
}

func (c *HLFCommitter) startWorkers() {
	for i := 0; i < c.cfg.WorkerCount; i++ {
		c.wg.Add(1)
		go c.workerLoop()
	}
}

func (c *HLFCommitter) workerLoop() {
	defer c.wg.Done()

	batch := make([]*engine.TxPayload, 0, c.cfg.MaxBatchSize)
	ticker := time.NewTicker(c.cfg.FlushTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		c.flushBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-c.ctx.Done():
			flush()
			return
		case tx := <-c.txQueue:
			if tx == nil {
				flush()
				return
			}
			batch = append(batch, tx)
			if len(batch) >= c.cfg.MaxBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// SubmitTx accepts transactions without blocking the caller.
// Once Stop begins, submissions are rejected rather than panicking on a closed channel.
func (c *HLFCommitter) SubmitTx(tx *engine.TxPayload) bool {
	if tx == nil {
		atomic.AddUint64(&c.failed, 1)
		return false
	}

	select {
	case <-c.ctx.Done():
		atomic.AddUint64(&c.failed, 1)
		return false
	default:
	}

	select {
	case <-c.ctx.Done():
		atomic.AddUint64(&c.failed, 1)
		return false
	case c.txQueue <- tx:
		return true
	default:
		atomic.AddUint64(&c.failed, 1)
		return false
	}
}

func (c *HLFCommitter) flushBatch(batch []*engine.TxPayload) {
	atomic.AddUint64(&c.committed, uint64(len(batch)))
}

func (c *HLFCommitter) TotalCommitted() uint64 {
	return atomic.LoadUint64(&c.committed)
}

func (c *HLFCommitter) TotalFailed() uint64 {
	return atomic.LoadUint64(&c.failed)
}

func (c *HLFCommitter) Stop() {
	c.stopOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
	})
}
