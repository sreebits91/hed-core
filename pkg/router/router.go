package router

import (
	"fmt"
	"hash/fnv"
)

type GatewayRouter struct {
	totalShards int
}

func NewGatewayRouter(shards int) *GatewayRouter {
	return &GatewayRouter{totalShards: shards}
}

func (r *GatewayRouter) RouteToShard(accountID string) string {
	h := fnv.New32a()
	h.Write([]byte(accountID))
	shardID := h.Sum32() % uint32(r.totalShards)
	return fmt.Sprintf("shard_channel_%02d", shardID)
}
