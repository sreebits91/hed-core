package plugin

import (
	"fmt"
	"sync"
)

type KeyDBEngine struct {
	name  string
	store map[string]map[string][]byte
	mu    sync.RWMutex
}

func NewKeyDBEngine() *KeyDBEngine {
	return &KeyDBEngine{
		name:  "KeyDB",
		store: make(map[string]map[string][]byte),
	}
}

func (k *KeyDBEngine) Name() string {
	return k.name
}

func (k *KeyDBEngine) Init(config map[string]string) error {
	return nil
}

func (k *KeyDBEngine) GetState(channelID, key string) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	namespace := channelID
	if namespace == "" {
		namespace = "default"
	}

	if ns, ok := k.store[namespace]; ok {
		if val, exists := ns[key]; exists {
			return val, nil
		}
	}
	return nil, fmt.Errorf("key %s not found", key)
}

func (k *KeyDBEngine) PutState(channelID, key string, val []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	namespace := channelID
	if namespace == "" {
		namespace = "default"
	}
	if _, exists := k.store[namespace]; !exists {
		k.store[namespace] = make(map[string][]byte)
	}
	k.store[namespace][key] = val
	return nil
}

func (k *KeyDBEngine) Get(key string) ([]byte, error) {
	return k.GetState("default", key)
}

func (k *KeyDBEngine) Put(key string, val []byte) error {
	return k.PutState("default", key, val)
}

func (k *KeyDBEngine) Delete(key string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if ns, ok := k.store["default"]; ok {
		delete(ns, key)
	}
	return nil
}

// BatchWrite handles namespaced batch operations matching plugin.StateEngine.
func (k *KeyDBEngine) BatchWrite(namespace string, batch map[string][]byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, exists := k.store[namespace]; !exists {
		k.store[namespace] = make(map[string][]byte)
	}

	for key, val := range batch {
		if val == nil {
			delete(k.store[namespace], key)
		} else {
			k.store[namespace][key] = val
		}
	}
	return nil
}

func (k *KeyDBEngine) Close() error {
	return nil
}
