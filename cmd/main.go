package main

import (
	"fmt"
	"log"

	"hed-core/pkg/dashboard"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

func main() {
	// 1. Initialize Registry & 32-way sharded KeyDB Engine
	reg := plugin.NewRegistry()
	keydbEngine := plugin.NewKeyDBEngine()
	reg.Register(keydbEngine)

	defaultDB, err := reg.Get("KeyDB")
	if err != nil {
		log.Fatalf("Failed to load KeyDB plugin: %v", err)
	}

	// 2. Initialize Parallel Committer (32 Channels, 128 Workers, 500 Batch)
	committer := hlf.NewHLFCommitter(hlf.CommitterConfig{
		Channels:  32,
		Workers:   128,
		BatchSize: 500,
	}, defaultDB)

	// 3. Initialize Servers
	hlfNet := &hlf.Network{}
	hlfSrv := dashboard.NewHLFServer(hlfNet)
	hlfSrv.BeginLifecycleSimulation()
	srv := dashboard.NewServer(reg, defaultDB, hlfSrv, committer)

	defer committer.Stop()

	port := ":8080"
	fmt.Printf("Starting HED Core Engine on http://localhost%s\n", port)
	if err := srv.Start(port); err != nil {
		log.Fatalf("Dashboard server failed: %v", err)
	}
}
