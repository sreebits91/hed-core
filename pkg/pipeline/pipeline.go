package pipeline

import (
	"errors"
	"sync"
	"sync/atomic"

	"hed-core/pkg/batch"
	"hed-core/pkg/commit"
	"hed-core/pkg/dedup"
	"hed-core/pkg/engine"
	"hed-core/pkg/metrics"
	"hed-core/pkg/partition"
	"hed-core/pkg/queue"
)

type Config struct {
	Partitions int
	QueueSize int
	BatchSize int
}

type Pipeline struct {
	queues []queue.TransactionQueue
	router *partition.Router
	backend commit.Backend
	dedup dedup.Store
	metrics metrics.Counters
	stopOnce sync.Once
	stopped atomic.Bool
	wg sync.WaitGroup
}

func New(cfg Config, backend commit.Backend) *Pipeline {
	if cfg.Partitions < 1 { cfg.Partitions = 1 }
	if cfg.QueueSize < 1 { cfg.QueueSize = 10000 }
	if cfg.BatchSize < 1 { cfg.BatchSize = 1000 }
	if backend == nil { backend = commit.NoopBackend{} }
	queues := make([]queue.TransactionQueue, cfg.Partitions)
	p := &Pipeline{queues: queues, backend: backend}
	for i := range queues { queues[i] = queue.NewRingQueue(cfg.QueueSize) }
	p.router = partition.New(queues)
	p.wg.Add(len(queues))
	for _, q := range queues {
		go p.runPartition(q, cfg.BatchSize)
	}
	return p
}

func (p *Pipeline) runPartition(q queue.TransactionQueue, batchSize int) {
	defer p.wg.Done()
	b := batch.New(q, batchSize, 2_000_000)
	for !p.stopped.Load() {
		batch := b.Collect()
		if len(batch) == 0 { continue }
		if err := p.backend.Commit(batch); err != nil {
			p.metrics.Failed.Add(uint64(len(batch)))
			continue
		}
		p.metrics.Committed.Add(uint64(len(batch)))
	}
}

func (p *Pipeline) Submit(tx *engine.TxPayload) error {
	if tx == nil { return errors.New("nil transaction") }
	if p.stopped.Load() { return errors.New("pipeline stopped") }
	p.metrics.Ingress.Add(1)
	if !p.dedup.Accept(tx.TxUUID) { return errors.New("duplicate transaction") }
	if !p.router.Route(tx) {
		p.metrics.Dropped.Add(1)
		return errors.New("pipeline queue full")
	}
	p.metrics.Accepted.Add(1)
	return nil
}

func (p *Pipeline) Metrics() map[string]uint64 { return p.metrics.Snapshot() }

func (p *Pipeline) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		for _, q := range p.queues { q.Close() }
		p.wg.Wait()
	})
}
