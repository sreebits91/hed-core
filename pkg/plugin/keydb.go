package plugin

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type DBShard struct {
	mu    sync.RWMutex
	store map[string]map[string][]byte
}

type KeyDBEngine struct {
	name       string
	shardCount uint32
	shards     []*DBShard
}

func NewKeyDBEngine(name string, shardCount uint32) *KeyDBEngine {
	if name == "" {
		name = "KeyDB"
	}
	if shardCount == 0 {
		shardCount = 32
	}

	engine := &KeyDBEngine{
		name:       name,
		shardCount: shardCount,
		shards:     make([]*DBShard, shardCount),
	}
	for i := uint32(0); i < shardCount; i++ {
		engine.shards[i] = &DBShard{
			store: make(map[string]map[string][]byte),
		}
	}
	return engine
}

// Init fulfills plugin.StateEngine requirement
func (k *KeyDBEngine) Init(config map[string]string) error {
	return nil
}

func (k *KeyDBEngine) getShard(key string) *DBShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return k.shards[h.Sum32()%k.shardCount]
}

func (k *KeyDBEngine) Name() string {
	return k.name
}

// GetState fulfills namespace-aware read
func (k *KeyDBEngine) GetState(namespace string, key string) ([]byte, error) {
	shard := k.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if ns, ok := shard.store[namespace]; ok {
		if val, exists := ns[key]; exists {
			return val, nil
		}
	}
	return nil, fmt.Errorf("key %s in namespace %s not found", key, namespace)
}

// PutState fulfills namespace-aware write
func (k *KeyDBEngine) PutState(namespace string, key string, value []byte) error {
	shard := k.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.store[namespace]; !exists {
		shard.store[namespace] = make(map[string][]byte)
	}
	shard.store[namespace][key] = value
	return nil
}

func (k *KeyDBEngine) Get(key string) ([]byte, error) {
	return k.GetState("default", key)
}

func (k *KeyDBEngine) Put(key string, val []byte) error {
	return k.PutState("default", key, val)
}

func (k *KeyDBEngine) Delete(key string) error {
	shard := k.getShard(key)
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

func (k *KeyDBEngine) Close() error {
	for i := uint32(0); i < k.shardCount; i++ {
		k.shards[i].mu.Lock()
		k.shards[i].store = nil
		k.shards[i].mu.Unlock()
	}
	return nil
}
