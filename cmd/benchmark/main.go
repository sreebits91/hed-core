package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
)

type runtimeStats struct {
	NumGC        uint32 `json:"num_gc"`
	TotalAllocMB uint64 `json:"total_alloc_mb"`
	HeapAllocMB  uint64 `json:"heap_alloc_mb"`
	Goroutines   int    `json:"goroutines"`
	GOMAXPROCS   int    `json:"gomaxprocs"`
}

type stageResult struct {
	TargetTPS       int          `json:"target_tps"`
	DurationSeconds float64      `json:"duration_seconds"`
	Accepted        uint64       `json:"accepted"`
	Committed       uint64       `json:"committed"`
	Rejected        uint64       `json:"rejected"`
	ActualTPS       float64      `json:"actual_tps"`
	P50Us           int64        `json:"submit_p50_us"`
	P95Us           int64        `json:"submit_p95_us"`
	P99Us           int64        `json:"submit_p99_us"`
	Sampled         uint64       `json:"latency_samples"`
	ErrorRate       float64      `json:"error_rate"`
	Saturated       bool         `json:"saturated"`
	Runtime         runtimeStats `json:"runtime"`
}

type benchmarkResult struct {
	Mode             string        `json:"mode"`
	Workers          int           `json:"workers"`
	BatchSize        int           `json:"batch_size"`
	FlushMS          int           `json:"flush_ms"`
	LatencySampleRate int           `json:"latency_sample_rate"`
	StartTPS         int           `json:"start_tps"`
	MaxTPS           int           `json:"max_tps"`
	GrowthFactor     float64       `json:"growth_factor"`
	StageDuration    float64       `json:"stage_duration_seconds"`
	Stages           []stageResult `json:"stages"`
	PeakSustainable  int           `json:"peak_sustainable_tps"`
	StopReason       string        `json:"stop_reason"`
}

func readRuntimeStats() runtimeStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return runtimeStats{NumGC: m.NumGC, TotalAllocMB: m.TotalAlloc / 1024 / 1024, HeapAllocMB: m.HeapAlloc / 1024 / 1024, Goroutines: runtime.NumGoroutine(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
}

func percentile(values []int64, p float64) int64 {
	if len(values) == 0 { return 0 }
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func runStage(target int, duration time.Duration, workers, batch int, flush time.Duration, maxError float64, maxP99 time.Duration, sampleRate int) stageResult {
	committer := hlf.NewHLFCommitter(hlf.BatchConfig{MaxBatchSize: batch, FlushTimeout: flush, WorkerCount: workers})
	before := readRuntimeStats()
	latencies := make([][]int64, workers)
	var accepted, rejected uint64
	start := time.Now()
	deadline := start.Add(duration)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(workerID int) {
			defer wg.Done()
			local := make([]int64, 0, 1024)
			for seq := workerID; ; seq += workers {
				now := time.Now()
				if now.After(deadline) { break }
				targetAt := time.Duration(float64(seq) * float64(time.Second) / float64(target))
				if wait := targetAt - now.Sub(start); wait > 0 { time.Sleep(wait) }
				if time.Now().After(deadline) { break }
				tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: fmt.Sprintf("acc-%d", seq), Amount: 1}
				txStart := time.Now()
				if committer.SubmitTx(tx) {
					atomic.AddUint64(&accepted, 1)
					if sampleRate == 1 || seq%sampleRate == 0 { local = append(local, time.Since(txStart).Microseconds()) }
				} else {
					atomic.AddUint64(&rejected, 1)
				}
			}
			latencies[workerID] = local
		}(worker)
	}
	wg.Wait()
	committer.Stop()
	elapsed := time.Since(start)
	after := readRuntimeStats()

	acceptedCount := atomic.LoadUint64(&accepted)
	rejectedCount := atomic.LoadUint64(&rejected)
	committed := committer.TotalCommitted()
	all := make([]int64, 0)
	for _, local := range latencies { all = append(all, local...) }
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p50, p95, p99 := percentile(all, .50), percentile(all, .95), percentile(all, .99)

	total := acceptedCount + rejectedCount
	errorRate := 0.0
	if total > 0 { errorRate = float64(rejectedCount) / float64(total) }
	actualTPS := float64(committed) / elapsed.Seconds()
	saturated := errorRate > maxError || (maxP99 > 0 && time.Duration(p99)*time.Microsecond > maxP99) || actualTPS < float64(target)*0.90

	return stageResult{
		TargetTPS: target, DurationSeconds: elapsed.Seconds(), Accepted: acceptedCount, Committed: committed, Rejected: rejectedCount,
		ActualTPS: actualTPS, P50Us: p50, P95Us: p95, P99Us: p99, Sampled: uint64(len(all)), ErrorRate: errorRate, Saturated: saturated,
		Runtime: runtimeStats{NumGC: after.NumGC - before.NumGC, TotalAllocMB: after.TotalAllocMB - before.TotalAllocMB, HeapAllocMB: after.HeapAllocMB, Goroutines: after.Goroutines, GOMAXPROCS: after.GOMAXPROCS},
	}
}

func main() {
	startTPS := flag.Int("start-tps", 0, "initial offered rate; 0 derives it from worker count")
	maxTPS := flag.Int("max-tps", 0, "maximum offered rate; 0 derives it from the ramp")
	growth := flag.Float64("growth", 1.5, "multiplicative increase between stages")
	stages := flag.Int("stages", 12, "maximum number of ramp stages")
	stageDuration := flag.Duration("stage-duration", 3*time.Second, "duration of each ramp stage")
	workers := flag.Int("workers", 64, "producer goroutines")
	batch := flag.Int("batch", 2000, "committer batch size")
	flush := flag.Duration("flush", 10*time.Millisecond, "committer flush interval")
	maxError := flag.Float64("max-error-rate", 0.01, "error rate at which a stage is saturated")
	maxP99 := flag.Duration("max-p99", 0, "optional p99 submit latency saturation threshold; 0 disables it")
	sampleRate := flag.Int("latency-sample-rate", 10, "record one latency sample every N accepted transactions")
	jsonOut := flag.String("json", "", "optional JSON output file")
	flag.Parse()

	if *workers <= 0 || *growth <= 1 || *stages <= 0 || *stageDuration <= 0 || *batch <= 0 || *startTPS < 0 || *maxTPS < 0 || *maxError < 0 || *maxError > 1 || *sampleRate <= 0 {
		fmt.Fprintln(os.Stderr, "invalid benchmark configuration")
		os.Exit(2)
	}
	if *startTPS == 0 { *startTPS = *workers * 100 }
	if *maxTPS == 0 { *maxTPS = int(float64(*startTPS) * math.Pow(*growth, float64(*stages-1))) }
	if *maxTPS < *startTPS { fmt.Fprintln(os.Stderr, "max-tps must be >= start-tps"); os.Exit(2) }

	out := benchmarkResult{Mode: "adaptive-ramp", Workers: *workers, BatchSize: *batch, FlushMS: int(*flush / time.Millisecond), LatencySampleRate: *sampleRate, StartTPS: *startTPS, MaxTPS: *maxTPS, GrowthFactor: *growth, StageDuration: stageDuration.Seconds()}
	target, lastGood := *startTPS, 0
	for stage := 0; stage < *stages && target <= *maxTPS; stage++ {
		result := runStage(target, *stageDuration, *workers, *batch, *flush, *maxError, *maxP99, *sampleRate)
		out.Stages = append(out.Stages, result)
		fmt.Printf("stage=%d target=%d actual=%.0f committed=%d submit_p99=%dus samples=%d errors=%.2f%% saturated=%t gc=%d alloc=%dMB goroutines=%d\n", stage+1, result.TargetTPS, result.ActualTPS, result.Committed, result.P99Us, result.Sampled, result.ErrorRate*100, result.Saturated, result.Runtime.NumGC, result.Runtime.TotalAllocMB, result.Runtime.Goroutines)
		if result.Saturated { out.StopReason = "saturation threshold reached"; break }
		lastGood = result.TargetTPS
		next := int(math.Ceil(float64(target) * *growth)); if next <= target { next = target + 1 }; target = next
	}
	if out.StopReason == "" { out.StopReason = "ramp limit reached" }
	out.PeakSustainable = lastGood
	encoded, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(encoded))
	if *jsonOut != "" { if err := os.WriteFile(*jsonOut, append(encoded, '\n'), 0644); err != nil { fmt.Fprintf(os.Stderr, "write JSON: %v\n", err); os.Exit(1) } }
}
