package memory

import (
	"sync"

	"hed-core/pkg/plugin"
)

type MemoryEngine struct {
	mu    sync.RWMutex
	store map[string][]byte
}

func New() plugin.StateEngine {
	return &MemoryEngine{store: make(map[string][]byte)}
}

func (m *MemoryEngine) Name() string { return "In-Memory RAM (KeyDB)" }

func (m *MemoryEngine) Init(cfg map[string]string) error { return nil }

func (m *MemoryEngine) GetState(channelID, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store[channelID+":"+key], nil
}

func (m *MemoryEngine) PutState(channelID, key string, value []byte) error {
	m.mu.Lock()
	m.store[channelID+":"+key] = value
	m.mu.Unlock()
	return nil
}

func (m *MemoryEngine) BatchWrite(channelID string, updates map[string][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range updates {
		m.store[channelID+":"+k] = v
	}
	return nil
}

func (m *MemoryEngine) Close() error { return nil }
