package fabricgateway

import (
    "context"
    "crypto/x509"
    "fmt"
    "os"
    "path/filepath"
    "time"

    "hed-core/pkg/v2"
    "github.com/hyperledger/fabric-gateway/pkg/client"
    "github.com/hyperledger/fabric-gateway/pkg/hash"
    "github.com/hyperledger/fabric-gateway/pkg/identity"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
)

type Config struct {
    MSPID string
    CertPath string
    KeyPath string
    TLSCertPath string
    PeerEndpoint string
    GatewayPeer string
    Channel string
    Chaincode string
    Function string
    SubmitTimeout time.Duration
    CommitTimeout time.Duration
}

func (c Config) Validate() error {
    if c.MSPID==""||c.CertPath==""||c.KeyPath==""||c.TLSCertPath==""||c.PeerEndpoint==""||c.GatewayPeer==""||c.Channel==""||c.Chaincode==""||c.Function=="" { return fmt.Errorf("invalid Fabric Gateway configuration") }
    if c.SubmitTimeout<=0 { c.SubmitTimeout=5*time.Second }
    if c.CommitTimeout<=0 { c.CommitTimeout=time.Minute }
    return nil
}

// Backend is a real Fabric Gateway CommitBackend. The HED transaction ID is
// passed as the first chaincode argument so chaincode can enforce idempotency
// across HED retries even when Fabric generates a different transaction ID.
type Backend struct { gateway *client.Gateway; contract *client.Contract; conn *grpc.ClientConn; cfg Config }

func New(cfg Config) (*Backend,error) {
    if err:=cfg.Validate();err!=nil{return nil,err}
    certPEM,err:=os.ReadFile(cfg.TLSCertPath);if err!=nil{return nil,fmt.Errorf("read TLS certificate: %w",err)}
    cert,err:=identity.CertificateFromPEM(certPEM);if err!=nil{return nil,fmt.Errorf("parse TLS certificate: %w",err)}
    pool:=x509.NewCertPool();pool.AddCert(cert)
    conn,err:=grpc.NewClient(cfg.PeerEndpoint,grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool,cfg.GatewayPeer)));if err!=nil{return nil,fmt.Errorf("connect Fabric Gateway endpoint: %w",err)}
    certPEM,err=os.ReadFile(cfg.CertPath);if err!=nil{conn.Close();return nil,fmt.Errorf("read identity certificate: %w",err)}
    x509Cert,err:=identity.CertificateFromPEM(certPEM);if err!=nil{conn.Close();return nil,fmt.Errorf("parse identity certificate: %w",err)}
    id,err:=identity.NewX509Identity(cfg.MSPID,x509Cert);if err!=nil{conn.Close();return nil,err}
    keyPEM,err:=os.ReadFile(cfg.KeyPath);if err!=nil{conn.Close();return nil,fmt.Errorf("read signing key: %w",err)}
    key,err:=identity.PrivateKeyFromPEM(keyPEM);if err!=nil{conn.Close();return nil,err}
    sign,err:=identity.NewPrivateKeySign(key);if err!=nil{conn.Close();return nil,err}
    gw,err:=client.Connect(id,client.WithSign(sign),client.WithHash(hash.SHA256),client.WithClientConnection(conn),client.WithSubmitTimeout(cfg.SubmitTimeout),client.WithCommitStatusTimeout(cfg.CommitTimeout));if err!=nil{conn.Close();return nil,fmt.Errorf("connect Fabric Gateway: %w",err)}
    return &Backend{gateway:gw,contract:gw.GetNetwork(cfg.Channel).GetContract(cfg.Chaincode),conn:conn,cfg:cfg},nil
}

func(b *Backend)Commit(ctx context.Context,tx v2.Tx)error{
    if b==nil||b.contract==nil{return fmt.Errorf("Fabric Gateway backend is not initialized")}
    submitCtx,cancel:=context.WithTimeout(ctx,b.cfg.SubmitTimeout);defer cancel()
    result,commit,err:=b.contract.SubmitAsync(b.cfg.Function,client.WithArguments(tx.ID,string(tx.Payload)),client.WithSubmitTimeout(b.cfg.SubmitTimeout))
    _=result
    if err!=nil{return fmt.Errorf("Fabric endorsement/order submission failed for HED tx %s: %w",tx.ID,err)}
    select{case<-submitCtx.Done():return submitCtx.Err();default:}
    status,err:=commit.Status();if err!=nil{return fmt.Errorf("Fabric commit confirmation failed for HED tx %s: %w",tx.ID,err)}
    if !status.Successful{return fmt.Errorf("Fabric ledger rejected HED tx %s: validation_code=%d fabric_tx_id=%s",tx.ID,status.Code,status.TransactionID)}
    return nil
}

func(b *Backend)Close()error{if b==nil{return nil};if b.gateway!=nil{b.gateway.Close()};if b.conn!=nil{return b.conn.Close()};return nil}

func DefaultConfigFromEnv() Config{return Config{MSPID:os.Getenv("FABRIC_MSP_ID"),CertPath:os.Getenv("FABRIC_CERT_PATH"),KeyPath:os.Getenv("FABRIC_KEY_PATH"),TLSCertPath:os.Getenv("FABRIC_TLS_CERT_PATH"),PeerEndpoint:os.Getenv("FABRIC_PEER_ENDPOINT"),GatewayPeer:os.Getenv("FABRIC_GATEWAY_PEER"),Channel:os.Getenv("FABRIC_CHANNEL"),Chaincode:os.Getenv("FABRIC_CHAINCODE"),Function:os.Getenv("FABRIC_FUNCTION"),SubmitTimeout:5*time.Second,CommitTimeout:time.Minute}}

func DefaultLocalConfig(networkRoot string) Config{return Config{MSPID:"Org1MSP",CertPath:filepath.Join(networkRoot,"organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts/cert.pem"),KeyPath:filepath.Join(networkRoot,"organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore"),TLSCertPath:filepath.Join(networkRoot,"organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"),PeerEndpoint:"dns:///localhost:7051",GatewayPeer:"peer0.org1.example.com",Channel:"mychannel",Chaincode:"basic",Function:"CreateAsset",SubmitTimeout:5*time.Second,CommitTimeout:time.Minute}}
