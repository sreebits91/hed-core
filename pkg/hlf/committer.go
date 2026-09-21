package hlf

import (
	"context"
	"hash/fnv"
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
	Partitions   int
}

type BatchCommitFunc func(context.Context, []*engine.TxPayload) error

type HLFCommitter struct {
	txQueues  []chan *engine.TxPayload
	committed uint64
	failed    uint64
	dropped   uint64
	cfg       BatchConfig
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopped   atomic.Bool
	commitFn  BatchCommitFunc
	commitMu  sync.RWMutex
}

func NewHLFCommitter(cfg BatchConfig) *HLFCommitter {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 2000
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 2 * time.Millisecond
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 32
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 500000
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 8
	}
	if cfg.Partitions > cfg.WorkerCount {
		cfg.Partitions = cfg.WorkerCount
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &HLFCommitter{
		txQueues: make([]chan *engine.TxPayload, cfg.Partitions),
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
	}

	queueSize := cfg.QueueSize / cfg.Partitions
	if queueSize < 1 {
		queueSize = 1
	}
	for i := range c.txQueues {
		c.txQueues[i] = make(chan *engine.TxPayload, queueSize)
	}
	c.startWorkers()
	return c
}

func (c *HLFCommitter) startWorkers() {
	for i := 0; i < c.cfg.WorkerCount; i++ {
		partition := i % len(c.txQueues)
		c.wg.Add(1)
		go c.workerLoop(c.txQueues[partition])
	}
}

func (c *HLFCommitter) workerLoop(queue <-chan *engine.TxPayload) {
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
			// Drain this partition before exiting so Stop provides a deterministic
			// handoff boundary rather than abandoning already accepted work.
			for {
				select {
				case tx := <-queue:
					if tx != nil {
						batch = append(batch, tx)
						if len(batch) >= c.cfg.MaxBatchSize {
							flush()
						}
					}
				default:
					flush()
					return
				}
			}
		case tx := <-queue:
			if tx == nil {
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

// SubmitTx performs a bounded, non-blocking enqueue. Transactions are routed
// deterministically to partitions so producers do not contend on one global
// queue and the same transaction key remains on the same queue.
func (c *HLFCommitter) SubmitTx(tx *engine.TxPayload) bool {
	if tx == nil || c.stopped.Load() {
		atomic.AddUint64(&c.failed, 1)
		return false
	}

	partition := c.partitionFor(tx)
	select {
	case <-c.ctx.Done():
		atomic.AddUint64(&c.failed, 1)
		return false
	case c.txQueues[partition] <- tx:
		return true
	default:
		atomic.AddUint64(&c.failed, 1)
		atomic.AddUint64(&c.dropped, 1)
		return false
	}
}

func (c *HLFCommitter) partitionFor(tx *engine.TxPayload) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tx.TxUUID))
	return int(h.Sum32() % uint32(len(c.txQueues)))
}

func (c *HLFCommitter) flushBatch(batch []*engine.TxPayload) {
	if len(batch) == 0 { return }
	c.commitMu.RLock()
	fn := c.commitFn
	c.commitMu.RUnlock()
	if fn != nil {
		if err := fn(c.ctx, batch); err != nil {
			atomic.AddUint64(&c.failed, uint64(len(batch)))
			return
		}
	}
	atomic.AddUint64(&c.committed, uint64(len(batch)))
}

// SetCommitFunc installs the real Fabric Gateway commit boundary. A nil
// callback keeps the committer useful for deterministic load tests.
func (c *HLFCommitter) SetCommitFunc(fn BatchCommitFunc) { c.commitMu.Lock(); c.commitFn = fn; c.commitMu.Unlock() }

func (c *HLFCommitter) TotalCommitted() uint64 {
	return atomic.LoadUint64(&c.committed)
}

func (c *HLFCommitter) TotalFailed() uint64 {
	return atomic.LoadUint64(&c.failed)
}

func (c *HLFCommitter) TotalDropped() uint64 {
	return atomic.LoadUint64(&c.dropped)
}

func (c *HLFCommitter) QueueDepth() int {
	total := 0
	for _, q := range c.txQueues {
		total += len(q)
	}
	return total
}

func (c *HLFCommitter) QueueCapacity() int {
	total := 0
	for _, q := range c.txQueues {
		total += cap(q)
	}
	return total
}

func (c *HLFCommitter) Stop() {
	c.stopOnce.Do(func() {
		c.stopped.Store(true)
		c.cancel()
		c.wg.Wait()
	})
}
