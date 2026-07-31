package main

import (
	"log"
	"fmt"

	"hed-core/pkg/dashboard"
	"hed-core/pkg/hlf"
)

func main() {
	server := dashboard.NewHLFServer(hlf.DefaultFabricVersion)

	fmt.Println("==================================================================")
	fmt.Println("   HYPERLEDGER FABRIC (HLF) DEPLOYMENT PIPELINE & DASHBOARD       ")
	fmt.Println("==================================================================")
	fmt.Printf(" [Target HLF Version] : v%s\n", hlf.DefaultFabricVersion)
	fmt.Println(" [Dashboard Web URL]  : http://localhost:8080")
	fmt.Println("==================================================================")

	if err := server.Start(":8080"); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
