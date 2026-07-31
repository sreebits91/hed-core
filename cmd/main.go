package main

import (
	"fmt"
	"log"

	"hed-core/pkg/dashboard"
)

func main() {
	hlfVersion := "2.5.4"
	server := dashboard.NewHLFServer(hlfVersion)

	fmt.Println("==================================================================")
	fmt.Println("   HYPERLEDGER FABRIC (HLF) DEPLOYMENT PIPELINE & DASHBOARD       ")
	fmt.Println("==================================================================")
	fmt.Printf(" [Target HLF Version] : v%s\n", hlfVersion)
	fmt.Println(" [Dashboard Web URL]  : http://localhost:8080")
	fmt.Println("==================================================================")

	if err := server.Start(":8080"); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
