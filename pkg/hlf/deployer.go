package hlf

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

type StageStatus string

const (
	StatusPending    StageStatus = "pending"
	StatusInProgress StageStatus = "in_progress"
	StatusCompleted  StageStatus = "completed"
	StatusFailed     StageStatus = "failed"
)

type DeploymentStage struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Status      StageStatus `json:"status"`
	Duration    string      `json:"duration"`
	Color       string      `json:"color"`
}

type Deployer struct {
	opts       DeployOptions
	listeners  map[chan string]bool
	mu         sync.Mutex
	stages     []*DeploymentStage
	logHistory []string
}

func NewDeployer(opts DeployOptions) *Deployer {
	return &Deployer{
		opts:      opts,
		listeners: make(map[chan string]bool),
		stages: []*DeploymentStage{
			{ID: "stage_1", Name: "Pre-requisites Check", Description: "Verify Docker & Go versions", Status: StatusPending, Color: "#38bdf8"},
			{ID: "stage_2", Name: "Binary Bootstrap", Description: "Download Fabric binaries and images", Status: StatusPending, Color: "#818cf8"},
			{ID: "stage_3", Name: "Network Teardown & Spin-up", Description: "Clean stale ledgers and launch Docker containers", Status: StatusPending, Color: "#fbbf24"},
			{ID: "stage_4", Name: "Channel Creation & Join", Description: "Create channel and join peers", Status: StatusPending, Color: "#f472b6"},
			{ID: "stage_5", Name: "Chaincode Deployment", Description: "Install & commit HED Smart Contracts", Status: StatusPending, Color: "#34d399"},
		},
		logHistory: make([]string, 0),
	}
}

func (d *Deployer) RegisterListener(ch chan string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.listeners[ch] = true

	for _, pastLog := range d.logHistory {
		jsonPayload := fmt.Sprintf(`{"stages": %s, "log": %q}`, d.serializeStages(), pastLog)
		ch <- fmt.Sprintf("data: %s\n\n", jsonPayload)
	}
}

func (d *Deployer) UnregisterListener(ch chan string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.listeners, ch)
}

func (d *Deployer) broadcast(logLine string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.logHistory = append(d.logHistory, logLine)
	jsonPayload := fmt.Sprintf(`{"stages": %s, "log": %q}`, d.serializeStages(), logLine)

	for ch := range d.listeners {
		select {
		case ch <- fmt.Sprintf("data: %s\n\n", jsonPayload):
		default:
		}
	}
}

func (d *Deployer) serializeStages() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, s := range d.stages {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(`{"id":%q,"name":%q,"description":%q,"status":%q,"duration":%q,"color":%q}`,
			s.ID, s.Name, s.Description, s.Status, s.Duration, s.Color))
	}
	buf.WriteString("]")
	return buf.String()
}

// RunDeployment preserves the original API while applying the configured
// deployment timeout. Call RunDeploymentContext when the caller owns a context.
func (d *Deployer) RunDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), CommandTimeout)
	defer cancel()
	if err := d.RunDeploymentContext(ctx); err != nil {
		d.broadcast(fmt.Sprintf("❌ Deployment failed: %v", err))
	}
}

// RunDeploymentContext executes the Fabric lifecycle sequentially and fails fast.
// A failed stage is returned to the caller and later stages are not executed.
func (d *Deployer) RunDeploymentContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	absPath, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolve working directory: %w", err)
	}
	binPath := absPath + "/fabric-samples/bin"
	configPath := absPath + "/fabric-samples/config"
	envVars := append(os.Environ(),
		"PATH="+os.Getenv("PATH")+":"+binPath,
		"FABRIC_CFG_PATH="+configPath,
	)

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 0, func(ctx context.Context) error {
		return d.runCmdContext(ctx, "docker", "version")
	}); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 1, func(ctx context.Context) error {
		if err := os.MkdirAll(FabricSamplesDir, 0755); err != nil {
			return fmt.Errorf("failed to create fabric-samples dir: %w", err)
		}

		dlScript := "if [ ! -f fabric-samples/install-fabric.sh ]; then curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh -o fabric-samples/install-fabric.sh && chmod +x fabric-samples/install-fabric.sh; fi"
		dlCmd := exec.CommandContext(ctx, "sh", "-c", dlScript)
		if out, err := dlCmd.CombinedOutput(); err != nil {
			d.broadcast(string(out))
			return fmt.Errorf("failed to fetch install-fabric.sh: %w", err)
		}

		cmd := exec.CommandContext(ctx, "./install-fabric.sh", "docker", "binary", "-f", d.opts.FabricVersion)
		cmd.Dir = FabricSamplesDir
		return d.streamCmdOutput(cmd)
	}); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 2, func(ctx context.Context) error {
		cleanCmd := exec.CommandContext(ctx, "./network.sh", "down")
		cleanCmd.Dir = TestNetworkDir
		cleanCmd.Env = envVars
		// Teardown is best-effort, but cancellation is never ignored.
		if err := d.streamCmdOutput(cleanCmd); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d.broadcast(fmt.Sprintf("⚠️ network teardown notice: %v", err))
		}

		upCmd := exec.CommandContext(ctx, "./network.sh", "up", "-ca")
		upCmd.Dir = TestNetworkDir
		upCmd.Env = envVars
		return d.streamCmdOutput(upCmd)
	}); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 3, func(ctx context.Context) error {
		channels := d.channels()
		for _, channelID := range channels {
			cmd := exec.CommandContext(ctx, "./network.sh", "createChannel", "-c", channelID)
			cmd.Dir = TestNetworkDir
			cmd.Env = envVars
			if err := d.streamCmdOutput(cmd); err != nil {
				return fmt.Errorf("create channel %q: %w", channelID, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return d.executeStage(ctx, 4, func(ctx context.Context) error {
		ccDir := FabricSamplesDir + "/asset-transfer-basic/chaincode-go"
		targetVer := d.opts.TargetGoVer
		if targetVer == "" {
			targetVer = TargetGoVersion
		}

		fixScript := fmt.Sprintf(`
			if [ -f "go.mod" ]; then
				sed -i 's/go 1.25/%s/g' go.mod || true
				sed -i 's/go 1.24/%s/g' go.mod || true
			fi
		`, targetVer, targetVer)

		fixCmd := exec.CommandContext(ctx, "sh", "-c", fixScript)
		fixCmd.Dir = ccDir
		if err := fixCmd.Run(); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}

		vendorCmd := exec.CommandContext(ctx, "sh", "-c", "go mod tidy && go mod vendor")
		vendorCmd.Dir = ccDir
		if out, err := vendorCmd.CombinedOutput(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("prepare chaincode dependencies: %w: %s", err, string(out))
		}

		for _, channelID := range d.channels() {
			cmd := exec.CommandContext(ctx, "./network.sh", "deployCC",
				"-c", channelID,
				"-ccn", d.opts.ChaincodeName,
				"-ccp", DefaultChaincodePath,
				"-ccl", DefaultChaincodeLang,
			)
			cmd.Dir = TestNetworkDir
			cmd.Env = envVars
			if err := d.streamCmdOutput(cmd); err != nil {
				return fmt.Errorf("deploy chaincode on channel %q: %w", channelID, err)
			}
		}
		return nil
	})
}

func (d *Deployer) channels() []string {
	channels := d.opts.Channels
	if len(channels) == 0 && d.opts.ChannelID != "" {
		channels = []string{d.opts.ChannelID}
	}
	if len(channels) == 0 {
		channels = []string{DefaultChannelID}
	}
	return channels
}

func (d *Deployer) executeStage(ctx context.Context, idx int, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := d.stages[idx]
	s.Status = StatusInProgress
	start := time.Now()
	d.broadcast(fmt.Sprintf("=== Starting Stage: %s ===", s.Name))

	err := fn(ctx)
	s.Duration = time.Since(start).Round(time.Millisecond).String()
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		s.Status = StatusFailed
		d.broadcast(fmt.Sprintf("❌ Error in %s: %v", s.Name, err))
		return fmt.Errorf("%s: %w", s.Name, err)
	}

	s.Status = StatusCompleted
	d.broadcast(fmt.Sprintf("✅ Completed Stage: %s in %s", s.Name, s.Duration))
	return nil
}

func (d *Deployer) runCmdContext(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		d.broadcast(string(out))
	}
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (d *Deployer) streamCmdOutput(cmd *exec.Cmd) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return err
	}

	buf := make([]byte, 1024)
	for {
		n, readErr := stdout.Read(buf)
		if n > 0 {
			d.broadcast(string(buf[:n]))
		}
		if readErr != nil {
			break
		}
	}

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}
