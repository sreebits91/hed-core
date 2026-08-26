module hed-core-fabric-integration

go 1.26.1

require (
    hed-core v0.0.0
    github.com/hyperledger/fabric-gateway v1.12.0
    google.golang.org/grpc v1.83.0
)

replace hed-core => ../..
