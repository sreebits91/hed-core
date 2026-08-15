package delta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"hed-core/pkg/plugin"
)

const numShards = 256

type shard struct {
	sync.Mutex
	items map[string]*int64
}

type pendingBatch struct {
	requestID string
	channelID string
	updates   map[string]int64
}

type DeltaEngine struct {
	db         plugin.StateEngine
	shards     [numShards]*shard
	txCount    uint64
	pendingMu  sync.Mutex
	pending    []pendingBatch
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

func (d *DeltaEngine) ApplyDelta(channelID, key string, deltaValue int64) {
	if deltaValue == 0 {
		return
	}
	s := d.getShard(channelID, key)
	compositeKey := channelID + "\x00" + key
	s.Lock()
	ptr, ok := s.items[compositeKey]
	if !ok {
		ptr = new(int64)
		s.items[compositeKey] = ptr
	}
	s.Unlock()
	atomic.AddInt64(ptr, deltaValue)
	atomic.AddUint64(&d.txCount, 1)
}

// FlushToDB persists pending failed requests first, then snapshots current RAM
// deltas. Each logical batch receives a stable request ID and uses the backend's
// idempotent write contract, preventing a retry from double-applying mutations.
func (d *DeltaEngine) FlushToDB(channelID string) error {
	if d.db == nil {
		return fmt.Errorf("delta storage engine is nil")
	}

	d.pendingMu.Lock()
	pending := append([]pendingBatch(nil), d.pending...)
	d.pendingMu.Unlock()
	for _, p := range pending {
		if err := d.db.BatchWriteWithID(p.requestID, p.channelID, p.updates); err != nil {
			return fmt.Errorf("retry pending delta %s: %w", p.requestID, err)
		}
		d.removePending(p.requestID)
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

	requestID := batchID(channelID, batch)
	if err := d.db.BatchWriteWithID(requestID, channelID, batch); err != nil {
		d.pendingMu.Lock()
		d.pending = append(d.pending, pendingBatch{
			requestID: requestID,
			channelID: channelID,
			updates:   cloneBatch(batch),
		})
		d.pendingMu.Unlock()
		return fmt.Errorf("persist delta batch %s: %w", requestID, err)
	}
	return nil
}

func (d *DeltaEngine) removePending(requestID string) {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	for i := range d.pending {
		if d.pending[i].requestID == requestID {
			d.pending = append(d.pending[:i], d.pending[i+1:]...)
			return
		}
	}
}

func cloneBatch(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func batchID(channelID string, batch map[string]int64) string {
	keys := make([]string, 0, len(batch))
	for k := range batch {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	h.Write([]byte(channelID))
	for _, k := range keys {
		fmt.Fprintf(h, "\x00%s=%d", k, batch[k])
	}
	return "delta-" + hex.EncodeToString(h.Sum(nil))
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
