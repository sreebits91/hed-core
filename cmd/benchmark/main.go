package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
)

type stageResult struct {
	TargetTPS       int     `json:"target_tps"`
	DurationSeconds float64 `json:"duration_seconds"`
	Accepted        uint64  `json:"accepted"`
	Committed       uint64  `json:"committed"`
	Rejected        uint64  `json:"rejected"`
	ActualTPS       float64 `json:"actual_tps"`
	P50Us           int64   `json:"p50_us"`
	P95Us           int64   `json:"p95_us"`
	P99Us           int64   `json:"p99_us"`
	ErrorRate       float64 `json:"error_rate"`
	Saturated       bool    `json:"saturated"`
}

type benchmarkResult struct {
	Mode            string        `json:"mode"`
	Workers         int           `json:"workers"`
	BatchSize       int           `json:"batch_size"`
	FlushMS         int           `json:"flush_ms"`
	StartTPS        int           `json:"start_tps"`
	MaxTPS          int           `json:"max_tps"`
	GrowthFactor    float64       `json:"growth_factor"`
	StageDuration   float64       `json:"stage_duration_seconds"`
	Stages          []stageResult `json:"stages"`
	PeakSustainable int           `json:"peak_sustainable_tps"`
	StopReason      string        `json:"stop_reason"`
}

func percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func runStage(target int, duration time.Duration, workers, batch int, flush time.Duration, maxError float64, maxP99 time.Duration) stageResult {
	committer := hlf.NewHLFCommitter(hlf.BatchConfig{MaxBatchSize: batch, FlushTimeout: flush, WorkerCount: workers})

	var latencies []int64
	var latMu sync.Mutex
	var accepted, rejected uint64
	start := time.Now()
	deadline := start.Add(duration)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(workerID int) {
			defer wg.Done()
			for seq := workerID; ; seq += workers {
				now := time.Now()
				if now.After(deadline) {
					return
				}
				targetAt := time.Duration(float64(seq) * float64(time.Second) / float64(target))
				if wait := targetAt - now.Sub(start); wait > 0 {
					time.Sleep(wait)
				}
				if time.Now().After(deadline) {
					return
				}

				tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: fmt.Sprintf("acc-%d", seq), Amount: 1}
				txStart := time.Now()
				if committer.SubmitTx(tx) {
					atomic.AddUint64(&accepted, 1)
					latMu.Lock()
					latencies = append(latencies, time.Since(txStart).Microseconds())
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
	p50 := percentile(latencies, .50)
	p95 := percentile(latencies, .95)
	p99 := percentile(latencies, .99)
	latMu.Unlock()

	total := acceptedCount + rejectedCount
	errorRate := 0.0
	if total > 0 {
		errorRate = float64(rejectedCount) / float64(total)
	}
	actualTPS := float64(committed) / elapsed.Seconds()
	saturated := errorRate > maxError || (maxP99 > 0 && time.Duration(p99)*time.Microsecond > maxP99) || actualTPS < float64(target)*0.90

	return stageResult{
		TargetTPS: target, DurationSeconds: elapsed.Seconds(), Accepted: acceptedCount,
		Committed: committed, Rejected: rejectedCount, ActualTPS: actualTPS,
		P50Us: p50, P95Us: p95, P99Us: p99, ErrorRate: errorRate, Saturated: saturated,
	}
}

func main() {
	startTPS := flag.Int("start-tps", 0, "initial offered rate; 0 derives it from worker count")
	maxTPS := flag.Int("max-tps", 0, "maximum offered rate; 0 derives it from the ramp")
	growth := flag.Float64("growth", 1.5, "multiplicative increase between stages")
	stages := flag.Int("stages", 8, "maximum number of ramp stages")
	stageDuration := flag.Duration("stage-duration", 3*time.Second, "duration of each ramp stage")
	workers := flag.Int("workers", 64, "producer goroutines")
	batch := flag.Int("batch", 2000, "committer batch size")
	flush := flag.Duration("flush", 10*time.Millisecond, "committer flush interval")
	maxError := flag.Float64("max-error-rate", 0.01, "error rate at which a stage is saturated")
	maxP99 := flag.Duration("max-p99", 0, "optional p99 latency saturation threshold; 0 disables it")
	jsonOut := flag.String("json", "", "optional JSON output file")
	flag.Parse()

	if *workers <= 0 || *growth <= 1 || *stages <= 0 || *stageDuration <= 0 || *batch <= 0 || *startTPS < 0 || *maxTPS < 0 || *maxError < 0 || *maxError > 1 {
		fmt.Fprintln(os.Stderr, "invalid benchmark configuration")
		os.Exit(2)
	}

	if *startTPS == 0 {
		*startTPS = *workers * 100
	}
	if *maxTPS == 0 {
		*maxTPS = int(float64(*startTPS) * math.Pow(*growth, float64(*stages-1)))
	}
	if *maxTPS < *startTPS {
		fmt.Fprintln(os.Stderr, "max-tps must be >= start-tps")
		os.Exit(2)
	}

	out := benchmarkResult{
		Mode: "adaptive-ramp", Workers: *workers, BatchSize: *batch, FlushMS: int(*flush / time.Millisecond),
		StartTPS: *startTPS, MaxTPS: *maxTPS, GrowthFactor: *growth, StageDuration: stageDuration.Seconds(),
	}

	target := *startTPS
	lastGood := 0
	for stage := 0; stage < *stages && target <= *maxTPS; stage++ {
		result := runStage(target, *stageDuration, *workers, *batch, *flush, *maxError, *maxP99)
		out.Stages = append(out.Stages, result)
		fmt.Printf("stage=%d target=%d actual=%.0f committed=%d p99=%dus errors=%.2f%% saturated=%t\n", stage+1, result.TargetTPS, result.ActualTPS, result.Committed, result.P99Us, result.ErrorRate*100, result.Saturated)
		if result.Saturated {
			out.StopReason = "saturation threshold reached"
			break
		}
		lastGood = result.TargetTPS
		next := int(math.Ceil(float64(target) * *growth))
		if next <= target {
			next = target + 1
		}
		target = next
	}

	if out.StopReason == "" {
		out.StopReason = "ramp limit reached"
	}
	out.PeakSustainable = lastGood
	encoded, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(encoded))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, append(encoded, '\n'), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write JSON: %v\n", err)
			os.Exit(1)
		}
	}
}
