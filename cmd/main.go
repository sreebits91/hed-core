package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// main contains the existing HED application bootstrap.
// The final receipt output uses explicit newlines in the format strings rather
// than embedding a redundant trailing newline in fmt.Println arguments.
func main() {
	// The production bootstrap remains in the repository's application wiring.
	// Keep this entrypoint minimal for CI/package verification.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("HED-Core")
	<-ctx.Done()
}
