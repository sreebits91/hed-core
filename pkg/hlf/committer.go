package hlf

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/plugin"
)

type CommitterConfig struct {
	Channels  int
	Workers   int
	BatchSize int
}

type HLFCommitter struct {
	config     CommitterConfig
	engine     plugin.StateEngine
	txCount    uint64
	channels   []chan map[string][]byte
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewHLFCommitter(cfg CommitterConfig, engine plugin.StateEngine) *HLFCommitter {
	ctx, cancel := context.WithCancel(context.Background())
	c := &HLFCommitter{
		config:     cfg,
		engine:     engine,
		channels:   make([]chan map[string][]byte, cfg.Channels),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	for i := 0; i < cfg.Channels; i++ {
		c.channels[i] = make(chan map[string][]byte, 200000)
	}

	for i := 0; i < cfg.Channels; i++ {
		for w := 0; w < max(1, cfg.Workers/cfg.Channels); w++ {
			c.wg.Add(1)
			go c.startChannelWorker(i)
		}
	}

	return c
}

func (c *HLFCommitter) SubmitTx(channelID int, key string, value []byte) {
	chIdx := channelID % c.config.Channels
	batch := map[string][]byte{key: value}

	select {
	case c.channels[chIdx] <- batch:
		atomic.AddUint64(&c.txCount, 1)
	default:
		// queue is full; keep pushing by retrying briefly
		for i := 0; i < 10; i++ {
			select {
			case c.channels[chIdx] <- batch:
				atomic.AddUint64(&c.txCount, 1)
				return
			default:
				time.Sleep(time.Microsecond)
			}
		}
	}
}

func (c *HLFCommitter) startChannelWorker(channelID int) {
	defer c.wg.Done()
	channelName := fmt.Sprintf("channel-%d", channelID+1)
	queue := c.channels[channelID]

	pendingBatch := make(map[string][]byte, c.config.BatchSize)
	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			if len(pendingBatch) > 0 {
				_ = c.engine.BatchWrite(channelName, pendingBatch)
			}
			return

		case item, ok := <-queue:
			if !ok {
				return
			}
			for k, v := range item {
				pendingBatch[k] = v
			}

			if len(pendingBatch) >= c.config.BatchSize {
				_ = c.engine.BatchWrite(channelName, pendingBatch)
				pendingBatch = make(map[string][]byte, c.config.BatchSize)
			}

		case <-ticker.C:
			if len(pendingBatch) > 0 {
				_ = c.engine.BatchWrite(channelName, pendingBatch)
				pendingBatch = make(map[string][]byte, c.config.BatchSize)
			}
		}
	}
}

func (c *HLFCommitter) TotalCommitted() uint64 {
	return atomic.LoadUint64(&c.txCount)
}

func (c *HLFCommitter) Stop() {
	c.cancelFunc()
	c.wg.Wait()
}
