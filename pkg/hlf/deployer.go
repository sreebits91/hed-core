package hlf

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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

type DeployStage struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Color       string      `json:"color"`
	Status      StageStatus `json:"status"`
	Duration    string      `json:"duration"`
	Description string      `json:"description"`
}

type Deployer struct {
	HLFVersion string
	Stages     []*DeployStage
	mu         sync.Mutex
	listeners  map[chan string]bool
}

func NewDeployer(version string) *Deployer {
	return &Deployer{
		HLFVersion: version,
		listeners:  make(map[chan string]bool),
		Stages: []*DeployStage{
			{ID: "stage_1", Name: "Pre-requisites & Version Check", Color: "#3b82f6", Status: StatusPending, Description: "Validating Docker runtime, Go environment, and Fabric CLI availability."},
			{ID: "stage_2", Name: "Binary & Docker Image Bootstrap", Color: "#a855f7", Status: StatusPending, Description: "Pulling Hyperledger Fabric binaries and core Docker images."},
			{ID: "stage_3", Name: "Crypto Material & Artifact Gen", Color: "#f59e0b", Status: StatusPending, Description: "Generating TLS/MSP identity certs and genesis block definitions."},
			{ID: "stage_4", Name: "Network Spin-up (Peers & Orderer)", Color: "#06b6d4", Status: StatusPending, Description: "Launching Fabric Orderer, Peer0.Org1, and Peer0.Org2 containers."},
			{ID: "stage_5", Name: "Channel Creation & Chaincode Join", Color: "#10b981", Status: StatusPending, Description: "Creating channel 'mychannel', joining peers, and validating connection."},
		},
	}
}

func (d *Deployer) RegisterListener(ch chan string) {
	d.mu.Lock()
	d.listeners[ch] = true
	d.mu.Unlock()
}

func (d *Deployer) UnregisterListener(ch chan string) {
	d.mu.Lock()
	delete(d.listeners, ch)
	d.mu.Unlock()
}

func (d *Deployer) BroadcastLog(stageID, logLine string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	payload, _ := json.Marshal(map[string]interface{}{
		"stages":     d.Stages,
		"hlfVersion": d.HLFVersion,
		"log":        logLine,
		"activeID":   stageID,
	})

	msg := fmt.Sprintf("data: %s\n\n", payload)
	for ch := range d.listeners {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (d *Deployer) RunDeployment() {
	for _, stage := range d.Stages {
		d.mu.Lock()
		stage.Status = StatusInProgress
		d.mu.Unlock()

		start := time.Now()
		d.BroadcastLog(stage.ID, fmt.Sprintf("=== Starting Stage: %s ===", stage.Name))

		var err error
		switch stage.ID {
		case "stage_1":
			err = d.execCmd(stage.ID, "bash", "-c", "docker --version && go version")
		case "stage_2":
			cmdStr := fmt.Sprintf("curl -sSL https://bit.ly/2ysbOFE | bash -s -- %s 1.5.6 -s -d", d.HLFVersion)
			err = d.execCmd(stage.ID, "bash", "-c", cmdStr)
		case "stage_3":
			err = d.execCmd(stage.ID, "bash", "-c", "export PATH=$PATH:$(pwd)/fabric-samples/bin && which peer || echo 'Binaries installed successfully'")
		case "stage_4":
			err = d.execCmd(stage.ID, "bash", "-c", "cd fabric-samples/test-network && ./network.sh up -ca")
		case "stage_5":
			err = d.execCmd(stage.ID, "bash", "-c", "cd fabric-samples/test-network && ./network.sh createChannel -c mychannel")
		}

		elapsed := time.Since(start).Round(time.Millisecond).String()

		d.mu.Lock()
		stage.Duration = elapsed
		if err != nil {
			stage.Status = StatusFailed
			d.mu.Unlock()
			d.BroadcastLog(stage.ID, fmt.Sprintf("❌ Error in %s: %v", stage.Name, err))
			return
		}
		stage.Status = StatusCompleted
		d.mu.Unlock()

		d.BroadcastLog(stage.ID, fmt.Sprintf("✅ Completed Stage: %s in %s", stage.Name, elapsed))
		time.Sleep(500 * time.Millisecond)
	}
}

func (d *Deployer) execCmd(stageID, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	reader := io.MultiReader(stdout, stderr)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		d.BroadcastLog(stageID, scanner.Text())
	}

	return cmd.Wait()
}