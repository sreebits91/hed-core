package hlf

import (
	"sync"
	"time"
)

type PeerInfo struct {
	Name    string
	Org     string
	Role    string
	Status  string
	MSPID   string
	Blocks  int64
	Latency string
	Address string
}

type ChannelInfo struct {
	Name               string
	CommittedChaincode string
	JoinedPeers        []string
	TxCount            int64
}

type TxRecord struct {
	TxID             string
	Channel          string
	Chaincode        string
	Function         string
	Args             string
	Endorsers        []string
	BlockNumber      uint64
	TxValidationCode string
	Payload          []byte
	Timestamp        time.Time
	Latency          time.Duration
}

type Network struct {
	mu        sync.RWMutex
	peers     []PeerInfo
	channels  []ChannelInfo
	txHistory []TxRecord
	peakTPS   float64
}

func NewNetwork() *Network {
	return &Network{
		peers:     []PeerInfo{},
		channels:  []ChannelInfo{},
		txHistory: []TxRecord{},
	}
}

func (n *Network) GetPeers() []PeerInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}

func (n *Network) GetChannels() []ChannelInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.channels
}

func (n *Network) GetRecentLedgerTransactions() []TxRecord {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.txHistory) > 50 {
		return n.txHistory[len(n.txHistory)-50:]
	}
	return n.txHistory
}

func (n *Network) GetTPSMetrics() (currentTPS float64, peakTPS float64, totalTx int64) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	totalTx = int64(len(n.txHistory))
	if totalTx == 0 {
		return 0, n.peakTPS, 0
	}

	now := time.Now()
	window := 10 * time.Second
	txInWindow := 0

	for i := len(n.txHistory) - 1; i >= 0; i-- {
		if now.Sub(n.txHistory[i].Timestamp) <= window {
			txInWindow++
		} else {
			break
		}
	}

	currentTPS = float64(txInWindow) / window.Seconds()
	if currentTPS > n.peakTPS {
		n.peakTPS = currentTPS
	}

	return currentTPS, n.peakTPS, totalTx
}
