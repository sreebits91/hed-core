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

type CommitSink interface {
	Commit(context.Context, []*engine.TxPayload) error
}

type HLFCommitter struct {
	txQueue chan *engine.TxPayload
	committed uint64
	failed    uint64
	cfg       BatchConfig
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stateMu   sync.Mutex
	sinkMu    sync.RWMutex
	sink      CommitSink
	stopping  bool
}

func NewHLFCommitter(cfg BatchConfig) *HLFCommitter {
	return NewHLFCommitterWithSink(cfg, nil)
}

func NewHLFCommitterWithSink(cfg BatchConfig, sink CommitSink) *HLFCommitter {
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
		sink:    sink,
	}
	c.startWorkers()
	return c
}

// SetSink should normally be called before submissions begin. It is protected
// so that a concurrent read of the sink cannot race with configuration.
func (c *HLFCommitter) SetSink(sink CommitSink) {
	c.sinkMu.Lock()
	c.sink = sink
	c.sinkMu.Unlock()
}

func (c *HLFCommitter) currentSink() CommitSink {
	c.sinkMu.RLock()
	defer c.sinkMu.RUnlock()
	return c.sink
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
		case tx, ok := <-c.txQueue:
			if !ok {
				flush()
				return
			}
			if tx == nil {
				atomic.AddUint64(&c.failed, 1)
				continue
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

func (c *HLFCommitter) SubmitTx(tx *engine.TxPayload) bool {
	if tx == nil {
		atomic.AddUint64(&c.failed, 1)
		return false
	}

	// Serialise the stopping transition with sends. This lets Stop close the
	// queue safely without a concurrent sender ever writing to a closed channel.
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.stopping {
		atomic.AddUint64(&c.failed, 1)
		return false
	}

	select {
	case c.txQueue <- tx:
		return true
	default:
		atomic.AddUint64(&c.failed, 1)
		return false
	}
}

func (c *HLFCommitter) flushBatch(batch []*engine.TxPayload) {
	sink := c.currentSink()
	if sink != nil {
		if err := sink.Commit(c.ctx, batch); err != nil {
			atomic.AddUint64(&c.failed, uint64(len(batch)))
			return
		}
	}
	atomic.AddUint64(&c.committed, uint64(len(batch)))
}

func (c *HLFCommitter) TotalCommitted() uint64 {
	return atomic.LoadUint64(&c.committed)
}

func (c *HLFCommitter) TotalFailed() uint64 {
	return atomic.LoadUint64(&c.failed)
}

// Stop prevents new submissions, drains all accepted transactions, waits for
// every worker to flush its final batch, and only then cancels the context.
// This gives accepted transactions a deterministic completion boundary.
func (c *HLFCommitter) Stop() {
	c.stopOnce.Do(func() {
		c.stateMu.Lock()
		c.stopping = true
		close(c.txQueue)
		c.stateMu.Unlock()

		c.wg.Wait()
		c.cancel()
	})
}
