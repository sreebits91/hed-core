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
	db      plugin.StateEngine
	shards  [numShards]*shard
	txCount uint64
}

func New(db plugin.StateEngine) *DeltaEngine {
	e := &DeltaEngine{db: db}
	for i := 0; i < numShards; i++ {
		e.shards[i] = &shard{items: make(map[string]*int64, 1024)}
	}
	return e
}

func (d *DeltaEngine) getShard(channelID, key string) *shard {
	var hash uint32 = 2166136261
	for _, value := range []byte(channelID + "\x00" + key) {
		hash ^= uint32(value)
		hash *= 16777619
	}
	return d.shards[hash%numShards]
}

// ApplyDelta executes microsecond-scale relative updates in RAM.
func (d *DeltaEngine) ApplyDelta(channelID, key string, deltaValue int64) {
	if deltaValue == 0 {
		return
	}

	s := d.getShard(channelID, key)
	compositeKey := channelID + "\x00" + key

	s.Lock()
	ptr, exists := s.items[compositeKey]
	if !exists {
		ptr = new(int64)
		s.items[compositeKey] = ptr
	}
	s.Unlock()

	atomic.AddInt64(ptr, deltaValue)
	atomic.AddUint64(&d.txCount, 1)
}

// FlushToDB durably applies a snapshot. Failed writes are restored into the
// active shard maps so a transient storage failure cannot silently lose deltas.
func (d *DeltaEngine) FlushToDB(channelID string) error {
	if d.db == nil {
		return fmt.Errorf("delta storage engine is nil")
	}

	batch := make(map[string]int64, 4096)
	for i := 0; i < numShards; i++ {
		s := d.shards[i]
		s.Lock()
		snapshot := s.items
		s.items = make(map[string]*int64, len(snapshot))
		s.Unlock()

		for compositeKey, deltaPtr := range snapshot {
			parts := splitCompositeKey(compositeKey)
			if parts.channel != channelID {
				// Preserve deltas belonging to other channels in the active map.
				s.Lock()
				s.items[compositeKey] = deltaPtr
				s.Unlock()
				continue
			}
			if deltaVal := atomic.LoadInt64(deltaPtr); deltaVal != 0 {
				batch[parts.key] += deltaVal
			}
		}
	}

	if len(batch) == 0 {
		return nil
	}

	if err := d.db.BatchWrite(channelID, batch); err != nil {
		// Requeue the failed aggregate. Do not overwrite newer deltas: merge them
		// into the current active value using atomic addition.
		for key, deltaVal := range batch {
			s := d.getShard(channelID, key)
			compositeKey := channelID + "\x00" + key
			s.Lock()
			ptr, exists := s.items[compositeKey]
			if !exists {
				ptr = new(int64)
				s.items[compositeKey] = ptr
			}
			s.Unlock()
			atomic.AddInt64(ptr, deltaVal)
		}
		return fmt.Errorf("persist delta batch: %w", err)
	}

	return nil
}

type compositeParts struct {
	channel string
	key     string
}

func splitCompositeKey(value string) compositeParts {
	for i := 0; i < len(value); i++ {
		if value[i] == 0 {
			return compositeParts{channel: value[:i], key: value[i+1:]}
		}
	}
	return compositeParts{key: value}
}

func (d *DeltaEngine) GetTxCount() uint64 {
	return atomic.LoadUint64(&d.txCount)
}

func (d *DeltaEngine) ResetTxCount() {
	atomic.StoreUint64(&d.txCount, 0)
}
