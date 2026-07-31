#!/bin/bash
set -e

echo "--------------------------------------------------------"
echo "🚀 HED Core + Hyperledger Fabric Studio Startup Script"
echo "--------------------------------------------------------"

# 1. Ensure fabric-samples directory and install-fabric.sh exist
mkdir -p fabric-samples
if [ ! -f fabric-samples/install-fabric.sh ]; then
    echo "📥 Downloading install-fabric.sh..."
    curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh -o fabric-samples/install-fabric.sh
    chmod +x fabric-samples/install-fabric.sh
fi

# 2. Clean up stale Fabric networks & Docker volumes
if [ -d "fabric-samples/test-network" ]; then
    echo "🧹 Teardown existing Fabric Docker containers & volumes..."
    cd fabric-samples/test-network
    ./network.sh down || true
    docker volume prune -f || true
    cd ../..
fi

# 3. Free up dashboard port 8081
echo "🔌 Freeing up port 8081..."
fuser -k 8081/tcp || true

# 4. Launch studio dashboard server
echo "⚡ Launching HyperEngine-Drunix Dashboard Server..."
echo "--------------------------------------------------------"
echo "🌐 Open Studio in your browser: http://localhost:8081"
echo "--------------------------------------------------------"

go run cmd/main.go