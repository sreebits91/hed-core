package main

import (
	"fmt"
	"log"

	"hed-core/pkg/dashboard"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

func main() {
	// 1. Initialize registry
	reg := plugin.NewRegistry()

	// 2. Instantiate and register KeyDB
	keydbEngine := plugin.NewKeyDBEngine()
	reg.Register(keydbEngine)

	// 3. Retrieve KeyDB from registry as default
	defaultDB, err := reg.Get("KeyDB")
	if err != nil {
		log.Fatalf("Failed to load KeyDB plugin: %v", err)
	}

	// 4. Initialize HLF Server & Dashboard Server
	hlfNet := &hlf.Network{}
	hlfSrv := dashboard.NewHLFServer(hlfNet)
	srv := dashboard.NewServer(reg, defaultDB, hlfSrv)

	// 5. Start Dashboard
	port := ":8080"
	fmt.Printf("Starting HED Core Dashboard on http://localhost%s\n", port)
	if err := srv.Start(port); err != nil {
		log.Fatalf("Dashboard server failed: %v", err)
	}
}