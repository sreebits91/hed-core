package delta

import (
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"hed-core/pkg/plugin"
)

// 512 shards keeps the hot path highly parallel while remaining cache-friendly.
// The power-of-two count lets shard selection use a mask instead of modulo.
const numShards = 512
const shardMask = numShards - 1

type shard struct {
	sync.RWMutex
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
		e.shards[i] = &shard{items: make(map[string]*int64, 256)}
	}
	return e
}

func (d *DeltaEngine) getShard(key string) *shard {
	return d.shards[xxhash.Sum64String(key)&shardMask]
}

// ApplyDelta updates the in-memory delta atomically. The shard read lock is
// held through the atomic update so FlushToDB cannot swap a map while an
// in-flight transaction is still mutating an entry from that map.
func (d *DeltaEngine) ApplyDelta(channelID, key string, deltaValue int64) {
	if deltaValue == 0 {
		return
	}

	s := d.getShard(key)
	s.RLock()
	ptr, exists := s.items[key]
	if !exists {
		s.RUnlock()
		s.Lock()
		ptr, exists = s.items[key]
		if !exists {
			ptr = new(int64)
			s.items[key] = ptr
		}
		s.Unlock()
		s.RLock()
	}
	atomic.AddInt64(ptr, deltaValue)
	s.RUnlock()
	atomic.AddUint64(&d.txCount, 1)
}

// FlushToDB double-buffers each shard. A write lock ensures all in-flight
// updates to the old map have completed before the snapshot is processed.
func (d *DeltaEngine) FlushToDB(channelID string) error {
	batch := make(map[string]int64, 4096)

	for i := 0; i < numShards; i++ {
		s := d.shards[i]
		s.Lock()
		snapshot := s.items
		s.items = make(map[string]*int64, len(snapshot))
		s.Unlock()

		for keyStr, deltaPtr := range snapshot {
			deltaVal := atomic.LoadInt64(deltaPtr)
			if deltaVal != 0 {
				batch[keyStr] = deltaVal
			}
		}
	}

	if len(batch) == 0 {
		return nil
	}
	return d.db.BatchWrite(channelID, batch)
}

func (d *DeltaEngine) GetTxCount() uint64 {
	return atomic.LoadUint64(&d.txCount)
}

func (d *DeltaEngine) ResetTxCount() {
	atomic.StoreUint64(&d.txCount, 0)
}
