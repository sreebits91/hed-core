package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"hed-core/pkg/hlf"
)

type PeerInfo struct {
	Name     string `json:"name"`
	Org      string `json:"org"`
	MSP      string `json:"msp"`
	Endpoint string `json:"endpoint"`
	Status   string `json:"status"`
}

type PhaseStatus struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"` // COMPLETED, RUNNING, PENDING
}

type HLFServer struct {
	network         *hlf.Network
	mu              sync.RWMutex
	peerCount       int
	orgCount        int
	channelName     string
	channels        []string
	contractVersion string
	lastInstalledAt string
	installed       bool
	phases          []PhaseStatus
	logs            []map[string]string
	txLogs          []map[string]string
	deployer        *hlf.Deployer
}

func NewHLFServer(net *hlf.Network) *HLFServer {
	s := &HLFServer{
		network:         net,
		peerCount:       4,
		orgCount:        2,
		channelName:     hlf.DefaultChannelID,
		channels:        []string{hlf.DefaultChannelID},
		contractVersion: "hed-cc v1.0",
		lastInstalledAt: "",
		installed:       false,
		deployer:        hlf.NewDeployer(hlf.DefaultOptions()),
	}
	s.resetPhases()
	return s
}

func (s *HLFServer) resetPhases() {
	s.phases = []PhaseStatus{
		{ID: "P1", Name: "1. Binaries & Docker Images", Description: "Downloading fabric-samples & hyperledger binaries", Status: "PENDING"},
		{ID: "P2", Name: "2. Crypto Material Generation", Description: "Generating MSP certificates via cryptogen", Status: "PENDING"},
		{ID: "P3", Name: "3. Genesis Block & Channels", Description: "Configuring Raft orderer & channel artifacts", Status: "PENDING"},
		{ID: "P4", Name: "4. Peer Container Launch", Description: "Starting peer and orderer Docker containers", Status: "PENDING"},
		{ID: "P5", Name: "5. Smart Contract Deployment", Description: "Packaging, installing & committing chaincode", Status: "PENDING"},
	}
}

func (s *HLFServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/hlf/telemetry", s.handleTelemetry)
	mux.HandleFunc("/api/hlf/install", s.handleInstall)
	mux.HandleFunc("/api/hlf/deploy", s.handleDeploy)
	mux.HandleFunc("/api/hlf/logs", s.handleLogs)
}

func (s *HLFServer) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]PeerInfo, 0)
	if s.installed {
		for i := 1; i <= s.peerCount; i++ {
			orgID := ((i - 1) % s.orgCount) + 1
			peers = append(peers, PeerInfo{
				Name:     "peer0.org" + string(rune('0'+orgID)) + ".example.com",
				Org:      "Org" + string(rune('0'+orgID)),
				MSP:      "Org" + string(rune('0'+orgID)) + "MSP",
				Endpoint: "grpc://localhost:" + string(rune('7'+i-1)) + "051",
				Status:   "CONNECTED",
			})
		}
	}

	channels := []string{}
	if s.installed {
		if len(s.channels) > 0 {
			channels = append(channels, s.channels...)
		} else if s.channelName != "" {
			channels = append(channels, s.channelName)
		}
	}

	resp := map[string]interface{}{
		"installed":         s.installed,
		"peer_count":        s.peerCount,
		"org_count":         s.orgCount,
		"channel_name":      s.channelName,
		"contract_version":  s.contractVersion,
		"last_installed_at": s.lastInstalledAt,
		"peers":             peers,
		"channels":          channels,
		"phases":            s.phases,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *HLFServer) handleInstall(w http.ResponseWriter, r *http.Request) {
	if r != nil && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PeerCount   int    `json:"peerCount"`
		OrgCount    int    `json:"orgCount"`
		ChannelName string `json:"channelName"`
		Channels    string `json:"channels"`
	}

	if r != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if w != nil {
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}
	}

	s.applyInstall(req.PeerCount, req.OrgCount, req.ChannelName, req.Channels)

	if w != nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

func (s *HLFServer) handleDeploy(w http.ResponseWriter, r *http.Request) {
	s.applyDeploy()

	if w != nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

func (s *HLFServer) applyInstall(peerCount, orgCount int, channelName, channels string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if peerCount > 0 {
		s.peerCount = peerCount
	}
	if orgCount > 0 {
		s.orgCount = orgCount
	}
	if channelName != "" {
		s.channelName = channelName
	}
	if channels != "" {
		parts := []string{}
		for _, part := range strings.Split(channels, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				parts = append(parts, trimmed)
			}
		}
		if len(parts) > 0 {
			s.channels = parts
			if s.channelName == "" || s.channelName == hlf.DefaultChannelID {
				s.channelName = parts[0]
			}
		}
	} else if len(s.channels) == 0 && channelName != "" {
		s.channels = []string{channelName}
	}
	s.installed = true
	s.lastInstalledAt = time.Now().Format("15:04:05 MST")

	for i := range s.phases {
		if i < 4 {
			s.phases[i].Status = "COMPLETED"
		}
	}

	s.addLog("SYSTEM", "Provisioning HLF cluster with "+string(rune('0'+s.peerCount))+" peers across "+string(rune('0'+s.orgCount))+" orgs")
	if len(s.channels) > 0 {
		s.addLog("DOCKER", "Network test-network initialized on channels '"+strings.Join(s.channels, ", ")+"'")
	} else {
		s.addLog("DOCKER", "Network test-network initialized on channel '"+s.channelName+"'")
	}
}

func (s *HLFServer) applyDeploy() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.phases) >= 5 {
		s.phases[4].Status = "COMPLETED"
	}
	s.contractVersion = "hed-cc v1.1 (Deployed)"
	s.installed = true
	if len(s.channels) > 0 {
		s.addLog("CHAINCODE", "Package hed-cc.tar.gz installed and committed to channels '"+strings.Join(s.channels, ", ")+"'")
	} else {
		s.addLog("CHAINCODE", "Package hed-cc.tar.gz installed and committed to channel '"+s.channelName+"'")
	}
}

func (s *HLFServer) BeginLifecycleSimulation() {
	go func() {
		s.addLog("SYSTEM", "Starting Fabric deployment pipeline")
		s.applyInstall(4, 2, s.channelName, strings.Join(s.channels, ","))
		s.addLog("SYSTEM", fmt.Sprintf("Launching Fabric deployer for channel %s", s.channelName))
		if s.deployer != nil {
			go s.deployer.RunDeployment()
		}
		s.applyDeploy()
	}()
}

func (s *HLFServer) IsInstalled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.installed
}

func (s *HLFServer) IsDeployed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.installed && s.contractVersion != "hed-cc v1.0"
}

func (s *HLFServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":   s.logs,
		"txLogs": s.txLogs,
	})
}

func (s *HLFServer) addLog(node, msg string) {
	entry := map[string]string{
		"timestamp": time.Now().Format("15:04:05"),
		"node":      node,
		"message":   msg,
	}
	s.logs = append([]map[string]string{entry}, s.logs...)
	if len(s.logs) > 50 {
		s.logs = s.logs[:50]
	}
}

func (s *HLFServer) addTxLog(node, msg string) {
	entry := map[string]string{
		"timestamp": time.Now().Format("15:04:05"),
		"node":      node,
		"message":   msg,
	}
	s.txLogs = append([]map[string]string{entry}, s.txLogs...)
	if len(s.txLogs) > 50 {
		s.txLogs = s.txLogs[:50]
	}
}
