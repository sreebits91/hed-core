#!/usr/bin/env bash
set -euo pipefail

CHANNEL_COUNT=32
FABRIC_CFG_PATH="${PWD}/config"

mkdir -p channel-artifacts

echo "=== 1. Generating Orderer Genesis Block ==="
configtxgen -profile TwoOrgsOrdererGenesis -channelID system-channel -outputBlock ./channel-artifacts/genesis.block

echo "=== 2. Creating 32 HLF Channel Tx Artifacts ==="
for i in $(seq 1 "$CHANNEL_COUNT"); do
    CHANNEL_NAME="channel-${i}"
    echo "Configuring ${CHANNEL_NAME}..."
    configtxgen -profile TwoOrgsChannel -outputCreateChannelTx ./channel-artifacts/${CHANNEL_NAME}.tx -channelID ${CHANNEL_NAME}
done

echo "=== 32 Channels successfully created in ./channel-artifacts ==="
