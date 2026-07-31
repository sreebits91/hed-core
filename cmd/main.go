package main

import (
	"fmt"
	"log"

	"hed-core/pkg/dashboard"
	"hed-core/pkg/plugin"
	"hed-core/plugins/memory"
	"hed-core/plugins/yugabyte"
)

func main() {
	registry := plugin.NewRegistry()
	registry.Register(memory.New())
	registry.Register(yugabyte.New())

	defaultDB, _ := registry.Get("In-Memory RAM (KeyDB)")

	server := dashboard.NewServer(registry, defaultDB)

	fmt.Println("==================================================================")
	fmt.Println("     HYPERENGINE-DRUNIX (HED): WEB DASHBOARD SERVER               ")
	fmt.Println("==================================================================")

	if err := server.Start(":8080"); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
