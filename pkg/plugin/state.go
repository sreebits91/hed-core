package plugin

import "fmt"

// StateEngine is the core plug-in / plug-out contract for database engines
type StateEngine interface {
	Name() string
	Init(config map[string]string) error
	GetState(channelID string, key string) ([]byte, error)
	PutState(channelID string, key string, value []byte) error
	BatchWrite(channelID string, updates map[string][]byte) error
	Close() error
}

// Registry handles runtime registration and dynamic switching of database engines
type Registry struct {
	plugins map[string]StateEngine
}

func NewRegistry() *Registry {
	return &Registry{plugins: make(map[string]StateEngine)}
}

func (r *Registry) Register(engine StateEngine) {
	r.plugins[engine.Name()] = engine
}

func (r *Registry) Get(name string) (StateEngine, error) {
	engine, exists := r.plugins[name]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", name)
	}
	return engine, nil
}

// List returns the registered plugin names.
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	return names
}
