package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
)

type result struct {
	TargetTPS       int     `json:"target_tps"`
	DurationSeconds float64 `json:"duration_seconds"`
	Workers         int     `json:"workers"`
	BatchSize       int     `json:"batch_size"`
	FlushMS         int     `json:"flush_ms"`
	Accepted        uint64  `json:"accepted"`
	Committed       uint64  `json:"committed"`
	Rejected        uint64  `json:"rejected"`
	ActualTPS       float64 `json:"actual_tps"`
	P50Us           int64   `json:"p50_us"`
	P95Us           int64   `json:"p95_us"`
	P99Us           int64   `json:"p99_us"`
	ErrorRate       float64 `json:"error_rate"`
}

func percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func main() {
	target := flag.Int("tps", 100000, "target submission rate")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	workers := flag.Int("workers", 64, "producer goroutines")
	batch := flag.Int("batch", 2000, "committer batch size")
	flush := flag.Duration("flush", 10*time.Millisecond, "committer flush interval")
	jsonOut := flag.String("json", "", "optional JSON output file")
	flag.Parse()

	if *target <= 0 || *workers <= 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "tps, workers and duration must be positive")
		os.Exit(2)
	}

	committer := hlf.NewHLFCommitter(hlf.BatchConfig{MaxBatchSize: *batch, FlushTimeout: *flush, WorkerCount: *workers})
	defer committer.Stop()

	latencies := make([]int64, 0, *target*int(duration.Seconds()))
	var latMu sync.Mutex
	var accepted uint64
	var rejected uint64

	start := time.Now()
	deadline := start.Add(*duration)
	var wg sync.WaitGroup
	wg.Add(*workers)
	for worker := 0; worker < *workers; worker++ {
		go func(workerID int) {
			defer wg.Done()
			for seq := workerID; ; seq += *workers {
				now := time.Now()
				if now.After(deadline) {
					return
				}
				elapsed := now.Sub(start)
				targetAt := time.Duration(float64(seq) * float64(time.Second) / float64(*target))
				if targetAt > elapsed {
					time.Sleep(targetAt - elapsed)
					now = time.Now()
					if now.After(deadline) {
						return
					}
				}

				tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: fmt.Sprintf("acc-%d", seq%100000), Amount: 1}
				txStart := time.Now()
				if committer.SubmitTx(tx) {
					atomic.AddUint64(&accepted, 1)
					lat := time.Since(txStart).Microseconds()
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				} else {
					atomic.AddUint64(&rejected, 1)
				}
			}
		}(worker)
	}
	wg.Wait()

	committer.Stop()
	elapsed := time.Since(start)
	acceptedCount := atomic.LoadUint64(&accepted)
	rejectedCount := atomic.LoadUint64(&rejected)
	committed := committer.TotalCommitted()
	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50, p95, p99 := percentile(latencies, .50), percentile(latencies, .95), percentile(latencies, .99)
	latMu.Unlock()

	total := acceptedCount + rejectedCount
	errorRate := 0.0
	if total > 0 {
		errorRate = float64(rejectedCount) / float64(total)
	}
	out := result{TargetTPS: *target, DurationSeconds: elapsed.Seconds(), Workers: *workers, BatchSize: *batch, FlushMS: int(*flush / time.Millisecond), Accepted: acceptedCount, Committed: committed, Rejected: rejectedCount, ActualTPS: float64(committed) / elapsed.Seconds(), P50Us: p50, P95Us: p95, P99Us: p99, ErrorRate: errorRate}

	encoded, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(encoded))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, append(encoded, '\n'), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write JSON: %v\n", err)
			os.Exit(1)
		}
	}
}
