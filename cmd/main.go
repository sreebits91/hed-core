package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	keydbEngine := plugin.NewKeyDBEngine("localhost:6379", 128)
	if err := keydbEngine.Init(nil); err != nil {
		fmt.Printf("KeyDB initialization failed: %v\n", err)
	}
	defer keydbEngine.Close()

	registry := plugin.NewRegistry()
	registry.Register(keydbEngine)

	pipeline := engine.NewPipeline(keydbEngine, 32)
	committer := hlf.NewHLFCommitter(hlf.BatchConfig{})
	defer committer.Stop()

	_ = pipeline
	_ = registry
	_ = committer

	<-ctx.Done()
}
