package hlf

import "time"

const (
	// Default Hyperledger Fabric settings.
	DefaultFabricVersion = "2.5.16"
	DefaultChannelID     = "mychannel"
	DefaultChaincodeName = "basic"
	DefaultChaincodePath = "../asset-transfer-basic/chaincode-go"
	DefaultChaincodeLang = "go"

	// Self-healing / compatibility constraints.
	TargetGoVersion     = "1.22"
	MaxAllowedGoVersion = "1.23"

	// Fabric is runtime state, not repository state. Keeping it outside the
	// checkout prevents stale Fabric trees/submodules from contaminating CI.
	FabricRuntimeDir = ".hed/fabric-samples"
	FabricSamplesDir = FabricRuntimeDir
	TestNetworkDir   = FabricRuntimeDir + "/test-network"

	CommandTimeout = 5 * time.Minute
)

// DeployOptions holds customizable pipeline settings passed from the UI or defaults.
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
