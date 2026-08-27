package benchmark

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
)

func TestProfilingCapture(t *testing.T) {
	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(1000)
	result := Run256WorkerBenchmark(t.Context(), newTestEngine(), 10000, 256)
	if result.TotalTx != 10000 { t.Fatalf("completed=%d want=10000", result.TotalTx) }
	path := os.Getenv("HED_GOROUTINE_PROFILE")
	if path == "" { t.Fatal("HED_GOROUTINE_PROFILE is required") }
	f, err := os.Create(path); if err != nil { t.Fatal(err) }
	defer f.Close()
	if err := pprof.Lookup("goroutine").WriteTo(f, 0); err != nil { t.Fatal(err) }
}

func newTestEngine() *delta.DeltaEngine { return delta.New(nil) }
