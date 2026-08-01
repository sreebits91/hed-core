package plugin

import (
	"fmt"
	"hash/fnv"
	"sync"
)

const ShardCount = 32

type DBShard struct {
	mu    sync.RWMutex
	store map[string]map[string][]byte
}

type KeyDBEngine struct {
	name   string
	shards [ShardCount]*DBShard
}

func NewKeyDBEngine() *KeyDBEngine {
	engine := &KeyDBEngine{
		name: "KeyDB",
	}
	for i := 0; i < ShardCount; i++ {
		engine.shards[i] = &DBShard{
			store: make(map[string]map[string][]byte),
		}
	}
	return engine
}

func (k *KeyDBEngine) getShard(namespace string) *DBShard {
	h := fnv.New32a()
	h.Write([]byte(namespace))
	return k.shards[h.Sum32()%ShardCount]
}

func (k *KeyDBEngine) Name() string {
	return k.name
}

func (k *KeyDBEngine) Init(config map[string]string) error {
	_ = config
	return nil
}

func (k *KeyDBEngine) GetState(channelID string, key string) ([]byte, error) {
	shard := k.getShard(channelID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if ns, ok := shard.store[channelID]; ok {
		if val, exists := ns[key]; exists {
			return val, nil
		}
	}
	return nil, fmt.Errorf("key %s not found", key)
}

func (k *KeyDBEngine) PutState(channelID string, key string, value []byte) error {
	shard := k.getShard(channelID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.store[channelID]; !exists {
		shard.store[channelID] = make(map[string][]byte)
	}
	shard.store[channelID][key] = value
	return nil
}

func (k *KeyDBEngine) Get(key string) ([]byte, error) {
	shard := k.shards[0] // Default shard
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if ns, ok := shard.store["default"]; ok {
		if val, exists := ns[key]; exists {
			return val, nil
		}
	}
	return nil, fmt.Errorf("key %s not found", key)
}

func (k *KeyDBEngine) Put(key string, val []byte) error {
	shard := k.shards[0]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.store["default"]; !exists {
		shard.store["default"] = make(map[string][]byte)
	}
	shard.store["default"][key] = val
	return nil
}

func (k *KeyDBEngine) Delete(key string) error {
	shard := k.shards[0]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if ns, ok := shard.store["default"]; ok {
		delete(ns, key)
	}
	return nil
}

func (k *KeyDBEngine) BatchWrite(namespace string, batch map[string][]byte) error {
	shard := k.getShard(namespace)

	shard.mu.Lock()
	if _, exists := shard.store[namespace]; !exists {
		shard.store[namespace] = make(map[string][]byte, len(batch))
	}

	ns := shard.store[namespace]
	for key, val := range batch {
		if val == nil {
			delete(ns, key)
		} else {
			ns[key] = val
		}
	}
	shard.mu.Unlock()
	return nil
}

// Close satisfies the plugin.StateEngine interface requirements
func (k *KeyDBEngine) Close() error {
	for i := 0; i < ShardCount; i++ {
		k.shards[i].mu.Lock()
		k.shards[i].store = nil
		k.shards[i].mu.Unlock()
	}
	return nil
}
