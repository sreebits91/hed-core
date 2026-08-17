package plugin

import (
	"sync"
	"testing"
)

type testEngine struct{ name string }
func (e *testEngine) Name() string { return e.name }
func (e *testEngine) Init(map[string]string) error { return nil }
func (e *testEngine) GetState(string, string) ([]byte, error) { return nil, nil }
func (e *testEngine) PutState(string, string, []byte) error { return nil }
func (e *testEngine) BatchWrite(string, map[string][]byte) error { return nil }
func (e *testEngine) Close() error { return nil }

func TestRegistryConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "engine-" + string(rune('a'+i%26))
			r.Register(&testEngine{name: name})
			_, _ = r.Get(name)
			_ = r.List()
		}(i)
	}
	wg.Wait()
	if len(r.List()) == 0 {
		t.Fatal("expected registered engines")
	}
}
