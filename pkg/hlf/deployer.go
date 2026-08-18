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
	deployMu   sync.Mutex
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
		select {
		case ch <- fmt.Sprintf("data: %s\n\n", jsonPayload):
		default:
			return
		}
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
	message := fmt.Sprintf("data: %s\n\n", jsonPayload)
	for ch := range d.listeners {
		select {
		case ch <- message:
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

func (d *Deployer) RunDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), CommandTimeout)
	defer cancel()
	if err := d.RunDeploymentContext(ctx); err != nil {
		d.broadcast(fmt.Sprintf("❌ Deployment failed: %v", err))
	}
}

// RunDeploymentContext runs a single deployment at a time and stops at the
// first failed stage. The caller controls cancellation and the deadline.
func (d *Deployer) RunDeploymentContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	d.deployMu.Lock()
	defer d.deployMu.Unlock()

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

	if err := d.executeStage(ctx, 0, func(ctx context.Context) error {
		return d.runCmdContext(ctx, "docker", "version")
	}); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 1, func(ctx context.Context) error {
		if err := os.MkdirAll(FabricSamplesDir, 0755); err != nil {
			return fmt.Errorf("failed to create fabric-samples dir: %w", err)
		}

		// Pin the bootstrap script to the configured Fabric release instead of
		// executing a moving script from the repository's main branch.
		installURL := fmt.Sprintf("https://raw.githubusercontent.com/hyperledger/fabric/v%s/scripts/install-fabric.sh", d.opts.FabricVersion)
		dlScript := fmt.Sprintf("if [ ! -f fabric-samples/install-fabric.sh ]; then curl -fsSL %q -o fabric-samples/install-fabric.sh && chmod +x fabric-samples/install-fabric.sh; fi", installURL)
		if err := d.runShellContext(ctx, dlScript); err != nil {
			return fmt.Errorf("failed to fetch pinned install-fabric.sh: %w", err)
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
		if err := d.streamCmdOutput(cleanCmd); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}

		upCmd := exec.CommandContext(ctx, "./network.sh", "up", "-ca")
		upCmd.Dir = TestNetworkDir
		upCmd.Env = envVars
		return d.streamCmdOutput(upCmd)
	}); err != nil {
		return err
	}

	if err := d.executeStage(ctx, 3, func(ctx context.Context) error {
		for _, channelID := range d.channels() {
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
		d.setStageStatus(idx, StatusFailed, "0s")
		return err
	}

	d.setStageStatus(idx, StatusInProgress, "")
	start := time.Now()
	d.broadcast(fmt.Sprintf("=== Starting Stage: %s ===", d.stageName(idx)))

	err := fn(ctx)
	duration := time.Since(start).Round(time.Millisecond).String()
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		d.setStageStatus(idx, StatusFailed, duration)
		d.broadcast(fmt.Sprintf("❌ Error in %s: %v", d.stageName(idx), err))
		return fmt.Errorf("%s: %w", d.stageName(idx), err)
	}

	d.setStageStatus(idx, StatusCompleted, duration)
	d.broadcast(fmt.Sprintf("✅ Completed Stage: %s in %s", d.stageName(idx), duration))
	return nil
}

func (d *Deployer) stageName(idx int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if idx < 0 || idx >= len(d.stages) {
		return "unknown stage"
	}
	return d.stages[idx].Name
}

func (d *Deployer) setStageStatus(idx int, status StageStatus, duration string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if idx < 0 || idx >= len(d.stages) {
		return
	}
	d.stages[idx].Status = status
	if duration != "" {
		d.stages[idx].Duration = duration
	}
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

func (d *Deployer) runShellContext(ctx context.Context, script string) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", script)
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
	return cmd.Wait()
}
