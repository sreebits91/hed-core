package delta

import (
	"fmt"
	"sync"
	"sync/atomic"

	"hed-core/pkg/plugin"
)

const numShards = 256

type shard struct {
	sync.Mutex
	items map[string]*int64
}

type DeltaEngine struct {
	db      *plugin.KeyDBEngine
	shards  [numShards]*shard
	txCount uint64
}

func New(db *plugin.KeyDBEngine) *DeltaEngine {
	e := &DeltaEngine{db: db}
	for i := 0; i < numShards; i++ {
		e.shards[i] = &shard{items: make(map[string]*int64, 1024)}
	}
	return e
}

func (d *DeltaEngine) getShard(key string) *shard {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return d.shards[hash%numShards]
}

// ApplyDelta applies the update while holding the shard lock so a concurrent
// flush cannot detach the key between pointer creation and the atomic update.
func (d *DeltaEngine) ApplyDelta(channelID, key string, deltaValue int64) {
	if deltaValue == 0 {
		return
	}
	s := d.getShard(key)
	s.Lock()
	ptr, exists := s.items[key]
	if !exists {
		ptr = new(int64)
		s.items[key] = ptr
	}
	atomic.AddInt64(ptr, deltaValue)
	s.Unlock()
	atomic.AddUint64(&d.txCount, 1)
}

// FlushToDB snapshots each shard. If persistence fails, the snapshot is
// merged back into the live maps so updates are not silently lost.
func (d *DeltaEngine) FlushToDB(channelID string) error {
	if d.db == nil {
		return fmt.Errorf("delta engine has no storage backend")
	}

	batch := make(map[string]int64, 4096)
	for i := 0; i < numShards; i++ {
		s := d.shards[i]
		s.Lock()
		snapshot := s.items
		s.items = make(map[string]*int64, len(snapshot))
		s.Unlock()

		for key, deltaPtr := range snapshot {
			if deltaVal := atomic.LoadInt64(deltaPtr); deltaVal != 0 {
				batch[key] += deltaVal
			}
		}
	}

	if len(batch) == 0 {
		return nil
	}
	if err := d.db.BatchWriteDeltas(channelID, batch); err != nil {
		for i := 0; i < numShards; i++ {
			// Requeue by key. Concurrent writes are merged rather than overwritten.
			s := d.shards[i]
			s.Lock()
			for key, value := range batch {
				if d.getShard(key) != s {
					continue
				}
				ptr := s.items[key]
				if ptr == nil {
					ptr = new(int64)
					s.items[key] = ptr
				}
				atomic.AddInt64(ptr, value)
			}
			s.Unlock()
		}
		return fmt.Errorf("persisting delta batch: %w", err)
	}
	return nil
}

func (d *DeltaEngine) GetTxCount() uint64 { return atomic.LoadUint64(&d.txCount) }

func (d *DeltaEngine) ResetTxCount() { atomic.StoreUint64(&d.txCount, 0) }
