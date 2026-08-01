package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"hed-core/pkg/dashboard"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

func getEnvInt(key string, defaultVal int) int {
	if valStr := os.Getenv(key); valStr != "" {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return defaultVal
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	shardCount := uint32(getEnvInt("KEYDB_SHARDS", 64))
	channelsCount := getEnvInt("HLF_CHANNELS", 32)
	workersCount := getEnvInt("HLF_WORKERS", 128)
	batchSize := getEnvInt("HLF_BATCH_SIZE", 500)
	queueCap := getEnvInt("HLF_QUEUE_CAPACITY", 50000)

	// 1. Initialize Registry & KeyDB Engine
	reg := plugin.NewRegistry()
	keydbEngine := plugin.NewKeyDBEngine("KeyDB", shardCount)
	reg.Register(keydbEngine)

	defaultDB, err := reg.Get("KeyDB")
	if err != nil {
		log.Fatalf("Failed to load KeyDB plugin: %v", err)
	}

	// 2. Initialize Committer
	committer := hlf.NewHLFCommitter(hlf.CommitterConfig{
		Channels:        channelsCount,
		Workers:         workersCount,
		BatchSize:       batchSize,
		QueueCapacity:   queueCap,
		FlushIntervalMs: 2 * time.Millisecond,
	}, defaultDB)

	// 3. Start Dashboard Server (Pass 4 arguments including committer)
	hlfNet := &hlf.Network{}
	hlfSrv := dashboard.NewHLFServer(hlfNet)
	srv := dashboard.NewServer(reg, defaultDB, hlfSrv, committer)

	defer committer.Stop()

	listenAddr := fmt.Sprintf(":%s", port)
	fmt.Printf("Starting HED Core Engine [Shards: %d | Channels: %d | Workers: %d] on http://localhost%s\n",
		shardCount, channelsCount, workersCount, listenAddr)

	if err := srv.Start(listenAddr); err != nil {
		log.Fatalf("Dashboard server failed: %v", err)
	}
}
