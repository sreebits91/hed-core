package hlf

import "time"

const (
	// Default Hyperledger Fabric Settings
	DefaultFabricVersion = "2.5.4"
	DefaultChannelID     = "mychannel"
	DefaultChaincodeName = "basic"
	DefaultChaincodePath = "../asset-transfer-basic/chaincode-go"
	DefaultChaincodeLang = "go"

	// Self-Healing Constraints
	TargetGoVersion     = "1.22"
	MaxAllowedGoVersion = "1.23"

	// Directory Paths
	FabricSamplesDir = "fabric-samples"
	TestNetworkDir   = "fabric-samples/test-network"
	CommandTimeout   = 5 * time.Minute
)

// DeployOptions holds customizable pipeline settings passed from UI or default constants
type DeployOptions struct {
	FabricVersion string   `json:"fabricVersion"`
	ChannelID     string   `json:"channelId"`
	Channels      []string `json:"channels"`
	ChaincodeName string   `json:"chaincodeName"`
	TargetGoVer   string   `json:"targetGoVer"`
}

func DefaultOptions() DeployOptions {
	return DeployOptions{
		FabricVersion: DefaultFabricVersion,
		ChannelID:     DefaultChannelID,
		Channels:      []string{DefaultChannelID},
		ChaincodeName: DefaultChaincodeName,
		TargetGoVer:   TargetGoVersion,
	}
}
