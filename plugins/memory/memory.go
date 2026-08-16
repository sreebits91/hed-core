package memory

import (
	"fmt"
	"sync"
	"time"

	"hed-core/pkg/plugin"
)

type MemoryEngine struct {
	mu        sync.RWMutex
	store     map[string][]byte
	processed map[string]time.Time
}

func New() plugin.StateEngine {
	return &MemoryEngine{
		store:     make(map[string][]byte),
		processed: make(map[string]time.Time),
	}
}

func (m *MemoryEngine) Name() string { return "In-Memory RAM (KeyDB)" }

func (m *MemoryEngine) Init(cfg map[string]string) error { return nil }

func (m *MemoryEngine) GetState(channelID, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.store[channelID+":"+key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), value...), nil
}

func (m *MemoryEngine) PutState(channelID, key string, value []byte) error {
	m.mu.Lock()
	m.store[channelID+":"+key] = append([]byte(nil), value...)
	m.mu.Unlock()
	return nil
}

func (m *MemoryEngine) BatchWrite(channelID string, updates map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, delta := range updates {
		if delta == 0 {
			continue
		}
		fullKey := channelID + ":" + key
		current := int64(0)
		if value, ok := m.store[fullKey]; ok {
			_, _ = fmt.Sscanf(string(value), "%d", &current)
		}
		m.store[fullKey] = []byte(fmt.Sprintf("%d", current+delta))
	}
	return nil
}

func (m *MemoryEngine) BatchWriteWithID(requestID, channelID string, updates map[string]int64) error {
	if requestID == "" {
		return fmt.Errorf("request ID is required for idempotent write")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.processed[requestID]; exists {
		return nil
	}
	for key, delta := range updates {
		if delta == 0 {
			continue
		}
		fullKey := channelID + ":" + key
		current := int64(0)
		if value, ok := m.store[fullKey]; ok {
			_, _ = fmt.Sscanf(string(value), "%d", &current)
		}
		m.store[fullKey] = []byte(fmt.Sprintf("%d", current+delta))
	}
	m.processed[requestID] = time.Now()
	return nil
}

func (m *MemoryEngine) Close() error { return nil }
