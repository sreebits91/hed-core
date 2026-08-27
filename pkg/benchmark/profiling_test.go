package benchmark

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"hed-core/pkg/delta"
)

func TestProfilingCapture(t *testing.T) {
	path := os.Getenv("HED_GOROUTINE_PROFILE")
	if path == "" {
		t.Skip("profiling capture is opt-in; set HED_GOROUTINE_PROFILE to enable it")
	}

	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(1000)
	result := Run256WorkerBenchmark(t.Context(), delta.New(nil), 10000, 256)
	if result.TotalTx != 10000 {
		t.Fatalf("completed=%d want=10000", result.TotalTx)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.Lookup("goroutine").WriteTo(f, 0); err != nil {
		t.Fatal(err)
	}
}
