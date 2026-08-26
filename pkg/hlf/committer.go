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
	QueueSize    int
}

type HLFCommitter struct {
	txQueue   chan *engine.TxPayload
	committed uint64
	failed    uint64
	dropped   uint64
	cfg       BatchConfig
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopped   atomic.Bool
}

func NewHLFCommitter(cfg BatchConfig) *HLFCommitter {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 2000
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 2 * time.Millisecond
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 500000
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &HLFCommitter{
		txQueue: make(chan *engine.TxPayload, cfg.QueueSize),
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	c.wg.Add(1)
	go c.workerLoop()
	return c
}

func (c *HLFCommitter) workerLoop() {
	defer c.wg.Done()

	batch := make([]*engine.TxPayload, 0, c.cfg.MaxBatchSize)
	timer := time.NewTimer(c.cfg.FlushTimeout)
	defer timer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			for {
				select {
				case tx := <-c.txQueue:
					if tx != nil {
						batch = append(batch, tx)
						if len(batch) >= c.cfg.MaxBatchSize {
							c.flushBatch(batch)
							batch = batch[:0]
						}
					}
				default:
					if len(batch) > 0 {
						c.flushBatch(batch)
					}
					return
				}
			}

		case tx := <-c.txQueue:
			if tx == nil {
				continue
			}
			batch = append(batch, tx)
			if len(batch) >= c.cfg.MaxBatchSize {
				c.flushBatch(batch)
				batch = batch[:0]
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.cfg.FlushTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				c.flushBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(c.cfg.FlushTimeout)
		}
	}
}

func (c *HLFCommitter) SubmitTx(tx *engine.TxPayload) bool {
	if tx == nil || c.stopped.Load() {
		atomic.AddUint64(&c.failed, 1)
		return false
	}

	select {
	case c.txQueue <- tx:
		return true
	default:
		atomic.AddUint64(&c.failed, 1)
		atomic.AddUint64(&c.dropped, 1)
		return false
	}
}

func (c *HLFCommitter) flushBatch(batch []*engine.TxPayload) {
	atomic.AddUint64(&c.committed, uint64(len(batch)))
}

func (c *HLFCommitter) TotalCommitted() uint64 { return atomic.LoadUint64(&c.committed) }
func (c *HLFCommitter) TotalFailed() uint64    { return atomic.LoadUint64(&c.failed) }
func (c *HLFCommitter) TotalDropped() uint64   { return atomic.LoadUint64(&c.dropped) }
func (c *HLFCommitter) QueueDepth() int        { return len(c.txQueue) }
func (c *HLFCommitter) QueueCapacity() int     { return cap(c.txQueue) }

func (c *HLFCommitter) Stop() {
	c.stopOnce.Do(func() {
		c.stopped.Store(true)
		c.cancel()
		c.wg.Wait()
	})
}
