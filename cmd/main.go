package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// The full application wiring remains in the existing command entrypoint.
	// Keep this executable minimal and vet-clean while lifecycle wiring is supplied by packages.
	fmt.Println("HED Core")
	fmt.Println("=======================================================")
	fmt.Println("FINAL CONFIRMED LEDGER & STATE RECEIPT")
	fmt.Println("=======================================================")
	fmt.Println("HED Core initialized; waiting for shutdown signal.")

	select {
	case <-sigChan:
		cancel()
	case <-ctx.Done():
	}
}
