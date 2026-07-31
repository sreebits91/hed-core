package delta

import (
	"sync"
	"sync/atomic"

	"hed-core/pkg/plugin"
)

type DeltaEngine struct {
	db          plugin.StateEngine
	deltaBuffer sync.Map
	txCount     uint64
}

func New(db plugin.StateEngine) *DeltaEngine {
	return &DeltaEngine{db: db}
}

func (d *DeltaEngine) ApplyDelta(channelID, key string, deltaValue int64) {
	fullKey := channelID + ":" + key
	for {
		val, _ := d.deltaBuffer.LoadOrStore(fullKey, new(int64))
		ptr := val.(*int64)
		atomic.AddInt64(ptr, deltaValue)
		atomic.AddUint64(&d.txCount, 1)
		break
	}
}

func (d *DeltaEngine) FlushToDB(channelID string) error {
	batch := make(map[string][]byte)
	d.deltaBuffer.Range(func(key, val interface{}) bool {
		kStr := key.(string)
		deltaPtr := val.(*int64)
		delta := atomic.SwapInt64(deltaPtr, 0)
		batch[kStr] = []byte(string(rune(delta)))
		return true
	})
	return d.db.BatchWrite(channelID, batch)
}

func (d *DeltaEngine) GetTxCount() uint64 {
	return atomic.LoadUint64(&d.txCount)
}

func (d *DeltaEngine) ResetTxCount() {
	atomic.StoreUint64(&d.txCount, 0)
}
