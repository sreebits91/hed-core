package yugabyte

import (
	"sync"
	"time"

	"hed-core/pkg/plugin"
)

type YugabyteEngine struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func New() plugin.StateEngine {
	return &YugabyteEngine{store: make(map[string][]byte)}
}

func (y *YugabyteEngine) Name() string { return "YugabyteDB (Distributed SQL)" }

func (y *YugabyteEngine) Init(cfg map[string]string) error { return nil }

func (y *YugabyteEngine) GetState(channelID, key string) ([]byte, error) {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return y.store[channelID+":"+key], nil
}

func (y *YugabyteEngine) PutState(channelID, key string, value []byte) error {
	// Simulate distributed SQL network & consensus overhead
	time.Sleep(10 * time.Microsecond)
	y.mu.Lock()
	y.store[channelID+":"+key] = value
	y.mu.Unlock()
	return nil
}

func (y *YugabyteEngine) BatchWrite(channelID string, updates map[string][]byte) error {
	time.Sleep(50 * time.Microsecond)
	y.mu.Lock()
	defer y.mu.Unlock()
	for k, v := range updates {
		y.store[channelID+":"+k] = v
	}
	return nil
}

func (y *YugabyteEngine) Close() error { return nil }
