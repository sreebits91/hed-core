package hlf

import (
	"bytes"
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

	// Replay past terminal logs to newly connected dashboard clients
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

func (d *Deployer) RunDeployment() {
	absPath, _ := os.Getwd()
	binPath := absPath + "/fabric-samples/bin"
	configPath := absPath + "/fabric-samples/config"
	envVars := append(os.Environ(),
		"PATH="+os.Getenv("PATH")+":"+binPath,
		"FABRIC_CFG_PATH="+configPath,
	)

	// Stage 1: Check System Prereqs
	d.executeStage(0, func() error {
		return d.runCmd("docker", "version")
	})

	// Stage 2: Download Binaries & Images (Self-Healing Download)
	d.executeStage(1, func() error {
		if err := os.MkdirAll(FabricSamplesDir, 0755); err != nil {
			return fmt.Errorf("failed to create fabric-samples dir: %w", err)
		}

		dlScript := "if [ ! -f fabric-samples/install-fabric.sh ]; then curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh -o fabric-samples/install-fabric.sh && chmod +x fabric-samples/install-fabric.sh; fi"
		dlCmd := exec.Command("sh", "-c", dlScript)
		if out, err := dlCmd.CombinedOutput(); err != nil {
			d.broadcast(string(out))
			return fmt.Errorf("failed to fetch install-fabric.sh: %w", err)
		}

		cmd := exec.Command("./install-fabric.sh", "docker", "binary", "-f", d.opts.FabricVersion)
		cmd.Dir = FabricSamplesDir
		return d.streamCmdOutput(cmd)
	})

	// Stage 3: Teardown stale state & Launch fresh peers + generate MSP crypto
	d.executeStage(2, func() error {
		cleanCmd := exec.Command("./network.sh", "down")
		cleanCmd.Dir = TestNetworkDir
		cleanCmd.Env = envVars
		_ = d.streamCmdOutput(cleanCmd)

		upCmd := exec.Command("./network.sh", "up", "-ca")
		upCmd.Dir = TestNetworkDir
		upCmd.Env = envVars
		return d.streamCmdOutput(upCmd)
	})

	// Stage 4: Create channel and join peers
	d.executeStage(3, func() error {
		cmd := exec.Command("./network.sh", "createChannel", "-c", d.opts.ChannelID)
		cmd.Dir = TestNetworkDir
		cmd.Env = envVars
		return d.streamCmdOutput(cmd)
	})

	// Stage 5: Deploy Chaincode (Self-Healing go.mod & Vendoring)
	d.executeStage(4, func() error {
		ccDir := FabricSamplesDir + "/asset-transfer-basic/chaincode-go"

		targetVer := d.opts.TargetGoVer
		if targetVer == "" {
			targetVer = TargetGoVersion
		}

		// 1. Self-Healing: Rewrites incompatible go versions to target (e.g. 1.22)
		fixScript := fmt.Sprintf(`
			if [ -f "go.mod" ]; then
				sed -i 's/go 1.25/%s/g' go.mod || true
				sed -i 's/go 1.24/%s/g' go.mod || true
			fi
		`, targetVer, targetVer)

		fixCmd := exec.Command("sh", "-c", fixScript)
		fixCmd.Dir = ccDir
		_ = fixCmd.Run()

		// 2. Local module vendoring
		vendorCmd := exec.Command("sh", "-c", "go mod tidy && go mod vendor")
		vendorCmd.Dir = ccDir
		if out, err := vendorCmd.CombinedOutput(); err != nil {
			d.broadcast(fmt.Sprintf("⚠️ Go vendor notice: %s", string(out)))
		}

		// 3. Chaincode lifecycle commit
		cmd := exec.Command("./network.sh", "deployCC",
			"-c", d.opts.ChannelID,
			"-ccn", d.opts.ChaincodeName,
			"-ccp", DefaultChaincodePath,
			"-ccl", DefaultChaincodeLang,
		)
		cmd.Dir = TestNetworkDir
		cmd.Env = envVars
		return d.streamCmdOutput(cmd)
	})
}

func (d *Deployer) executeStage(idx int, fn func() error) {
	s := d.stages[idx]
	s.Status = StatusInProgress
	start := time.Now()
	d.broadcast(fmt.Sprintf("=== Starting Stage: %s ===", s.Name))

	err := fn()

	s.Duration = time.Since(start).Round(time.Millisecond).String()
	if err != nil {
		s.Status = StatusFailed
		d.broadcast(fmt.Sprintf("❌ Error in %s: %v", s.Name, err))
	} else {
		s.Status = StatusCompleted
		d.broadcast(fmt.Sprintf("✅ Completed Stage: %s in %s", s.Name, s.Duration))
	}
}

func (d *Deployer) runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	d.broadcast(string(out))
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
		n, err := stdout.Read(buf)
		if n > 0 {
			d.broadcast(string(buf[:n]))
		}
		if err != nil {
			break
		}
	}

	return cmd.Wait()
}
