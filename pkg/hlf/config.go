package hlf

import "time"

const (
	DefaultFabricVersion = "2.5.4"
	DefaultChannelID     = "mychannel"
	DefaultChaincodeName = "hed"
	DefaultChaincodePath = "../../chaincode"
	DefaultChaincodeLang = "go"

	TargetGoVersion     = "1.26"
	MaxAllowedGoVersion = "1.27"

	FabricSamplesDir = "fabric-samples"
	TestNetworkDir   = "fabric-samples/test-network"
	CommandTimeout   = 5 * time.Minute
)

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
