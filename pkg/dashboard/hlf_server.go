package dashboard

import (
	"encoding/json"
	"net/http"
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
	contractVersion string
	lastInstalledAt string
	installed       bool
	phases          []PhaseStatus
	logs            []map[string]string
}

func NewHLFServer(net *hlf.Network) *HLFServer {
	s := &HLFServer{
		network:         net,
		peerCount:       4,
		orgCount:        2,
		channelName:     "mychannel",
		contractVersion: "hed-cc v1.0",
		lastInstalledAt: "",
		installed:       false,
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
	if s.installed && s.channelName != "" {
		channels = append(channels, s.channelName)
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		PeerCount   int    `json:"peerCount"`
		OrgCount    int    `json:"orgCount"`
		ChannelName string `json:"channelName"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
		s.mu.Lock()
		if req.PeerCount > 0 {
			s.peerCount = req.PeerCount
		}
		if req.OrgCount > 0 {
			s.orgCount = req.OrgCount
		}
		if req.ChannelName != "" {
			s.channelName = req.ChannelName
		}
		s.installed = true
		s.lastInstalledAt = time.Now().Format("15:04:05 MST")

		for i := range s.phases {
			if i < 4 {
				s.phases[i].Status = "COMPLETED"
			}
		}

		s.addLog("SYSTEM", "Provisioning HLF cluster with "+string(rune('0'+s.peerCount))+" peers across "+string(rune('0'+s.orgCount))+" orgs")
		s.addLog("DOCKER", "Network test-network initialized on channel '"+s.channelName+"'")
		s.mu.Unlock()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *HLFServer) handleDeploy(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if len(s.phases) >= 5 {
		s.phases[4].Status = "COMPLETED"
	}
	s.contractVersion = "hed-cc v1.1 (Deployed)"
	s.addLog("CHAINCODE", "Package hed-cc.tar.gz installed and committed to channel '"+s.channelName+"'")
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *HLFServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs": s.logs,
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
