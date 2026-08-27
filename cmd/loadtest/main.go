package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"

	"hed-core/pkg/benchmark"
	"hed-core/pkg/delta"
)

func main() {
	total := flag.Int("tx", 100000, "transactions")
	workers := flag.Int("workers", 256, "workers")
	flag.Parse()
	if *total <= 0 || *workers <= 0 { panic("tx and workers must be positive") }
	runtime.GOMAXPROCS(runtime.NumCPU())
	result := benchmark.Run256WorkerBenchmark(context.Background(), delta.New(nil), *total, *workers)
	fmt.Printf("transactions=%d duration=%s workers=%d tps=%.2f\n", result.TotalTx, result.Duration, result.WorkerCount, result.TPS)
	if int(result.TotalTx) != *total { panic(fmt.Sprintf("completed %d of %d transactions", result.TotalTx, *total)) }
}
