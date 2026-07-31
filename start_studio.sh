#!/bin/bash
set -e

echo "--------------------------------------------------------"
echo "🚀 HED Core + Hyperledger Fabric Studio Startup Script"
echo "--------------------------------------------------------"

# 1. Clean up stale Fabric networks & Docker volumes
if [ -d "fabric-samples/test-network" ]; then
    echo "🧹 Teardown existing Fabric Docker containers & volumes..."
    cd fabric-samples/test-network
    ./network.sh down || true
    docker volume prune -f || true
    cd ../..
fi

# 2. Kill any processes running on dashboard port 8081
echo "🔌 Freeing up port 8081..."
fuser -k 8081/tcp || true

# 3. Compile check & launch dashboard server
echo "⚡ Launching HyperEngine-Drunix Dashboard Server..."
echo "--------------------------------------------------------"
echo "🌐 Open Studio in your browser: http://localhost:8081"
echo "--------------------------------------------------------"

go run cmd/main.go
