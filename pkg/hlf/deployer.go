package hlf

import (
	"bytes"
	"fmt"
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
	version   string
	listeners map[chan string]bool
	mu        sync.Mutex
	stages    []*DeploymentStage
}

func NewDeployer(version string) *Deployer {
	return &Deployer{
		version:   version,
		listeners: make(map[chan string]bool),
		stages: []*DeploymentStage{
			{ID: "stage_1", Name: "Pre-requisites Check", Description: "Verify Docker & Go versions", Status: StatusPending, Color: "#38bdf8"},
			{ID: "stage_2", Name: "Binary Bootstrap", Description: "Download Fabric binaries and images", Status: StatusPending, Color: "#818cf8"},
			{ID: "stage_3", Name: "Network Teardown & Spin-up", Description: "Clean stale ledgers and launch Docker containers", Status: StatusPending, Color: "#fbbf24"},
			{ID: "stage_4", Name: "Channel Creation & Join", Description: "Create mychannel and join peers", Status: StatusPending, Color: "#f472b6"},
			{ID: "stage_5", Name: "Chaincode Deployment", Description: "Install & commit HED Smart Contracts", Status: StatusPending, Color: "#34d399"},
		},
	}
}

func (d *Deployer) RegisterListener(ch chan string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.listeners[ch] = true
}

func (d *Deployer) UnregisterListener(ch chan string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.listeners, ch)
}

func (d *Deployer) broadcast(logLine string) {
	d.mu.Lock()
	defer d.mu.Unlock()

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
	// Stage 1: Check Prereqs
	d.executeStage(0, func() error {
		return d.runCmd("docker", "version")
	})

	// Stage 2: Download Binaries
	d.executeStage(1, func() error {
		cmd := exec.Command("./install-fabric.sh", "docker", "binary", "-f", d.version)
		cmd.Dir = "fabric-samples"
		return d.streamCmdOutput(cmd)
	})

	// Stage 3: Teardown existing stale network & Spin up fresh peers
	d.executeStage(2, func() error {
		// Teardown stale networks and volumes first
		cleanCmd := exec.Command("./network.sh", "down")
		cleanCmd.Dir = "fabric-samples/test-network"
		_ = d.streamCmdOutput(cleanCmd)

		// Start fresh network
		upCmd := exec.Command("./network.sh", "up", "-ca")
		upCmd.Dir = "fabric-samples/test-network"
		return d.streamCmdOutput(upCmd)
	})

	// Stage 4: Create channel and join peers
	d.executeStage(3, func() error {
		cmd := exec.Command("./network.sh", "createChannel", "-c", "mychannel")
		cmd.Dir = "fabric-samples/test-network"
		return d.streamCmdOutput(cmd)
	})

	// Stage 5: Deploy Chaincode
	d.executeStage(4, func() error {
		cmd := exec.Command("./network.sh", "deployCC", "-ccn", "basic", "-ccp", "../asset-transfer-basic/chaincode-go", "-ccl", "go")
		cmd.Dir = "fabric-samples/test-network"
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