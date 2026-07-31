package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"hed-core/pkg/benchmark"
	"hed-core/pkg/delta"
	"hed-core/pkg/plugin"
	"hed-core/plugins/memory"
	"hed-core/plugins/yugabyte"
)

func main() {
	registry := plugin.NewRegistry()
	registry.Register(memory.New())
	registry.Register(yugabyte.New())

	activeDBName := "In-Memory RAM (KeyDB)"
	activeDB, _ := registry.Get(activeDBName)

	workers := 64
	channels := 16

	for {
		clearTerminal()
		fmt.Println("==================================================================")
		fmt.Println("     HYPERENGINE-DRUNIX (HED): HIGH-THROUGHPUT BENCHMARK SUITE    ")
		fmt.Println("==================================================================")
		fmt.Printf(" [Active Storage Plugin] : %s\n", activeDBName)
		fmt.Printf(" [Parallel Workers]      : %d threads\n", workers)
		fmt.Printf(" [Sharded Sub-Channels]  : %d channels\n", channels)
		fmt.Println("------------------------------------------------------------------")
		fmt.Println(" Options:")
		fmt.Println("  1. Execute Real-Time Throughput Benchmark")
		fmt.Println("  2. Plug-Out / Plug-In Database Driver")
		fmt.Println("  3. Tune Thread Workers & Shards")
		fmt.Println("  4. Exit")
		fmt.Println("==================================================================")
		fmt.Print("Select option [1-4]: ")

		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			runBenchmarkUI(activeDB, workers, channels)
		case "2":
			fmt.Println("\nAvailable Storage Engine Plugins:")
			fmt.Println("  1. In-Memory RAM (KeyDB) -> Targeted for 100,000 - 400,000+ TPS")
			fmt.Println("  2. YugabyteDB (Distributed SQL) -> Simulated Enterprise SQL")
			fmt.Print("Select DB Engine: ")
			var dbChoice string
			fmt.Scanln(&dbChoice)
			if dbChoice == "2" {
				activeDBName = "YugabyteDB (Distributed SQL)"
			} else {
				activeDBName = "In-Memory RAM (KeyDB)"
			}
			activeDB, _ = registry.Get(activeDBName)
			fmt.Printf("Successfully loaded plugin %s!\n", activeDBName)
			time.Sleep(1 * time.Second)
		case "3":
			fmt.Print("Enter Worker Threads (e.g., 32, 64, 128): ")
			fmt.Scanln(&workers)
			fmt.Print("Enter Parallel Channels (e.g., 8, 16, 32): ")
			fmt.Scanln(&channels)
		case "4":
			fmt.Println("Exiting HED Suite.")
			os.Exit(0)
		}
	}
}

func runBenchmarkUI(db plugin.StateEngine, workers, channels int) {
	deltaEngine := delta.New(db)
	runner := benchmark.NewRunner(deltaEngine, workers, channels)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metricsChan := make(chan benchmark.Metrics)
	go runner.Run(ctx, metricsChan)

	clearTerminal()
	fmt.Println("==================================================================")
	fmt.Printf(" RUNNING HIGH-SPEED BENCHMARK | DB: %s\n", db.Name())
	fmt.Println("==================================================================")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n------------------------------------------------------------------")
			fmt.Println(" Benchmark Test Execution Completed.")
			fmt.Println(" Press ENTER to return to main menu...")
			var b string
			fmt.Scanln(&b)
			return
		case m := <-metricsChan:
			fmt.Printf("\r [TPS]: %s | [Total Txs]: %d | [Workers]: %d | [Shards]: %d",
				renderBar(m.TPS), m.TotalTxs, m.ActiveWorkerNum, m.ActiveChannels)
		}
	}
}

func renderBar(tps float64) string {
	bars := int(tps / 5000)
	if bars > 30 {
		bars = 30
	}
	barStr := strings.Repeat("█", bars)
	return fmt.Sprintf("%8.2f TPS [%-30s]", tps, barStr)
}

func clearTerminal() {
	fmt.Print("\033[H\033[2J")
}
