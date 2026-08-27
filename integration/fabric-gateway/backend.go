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

type Config struct { MSPID, CertPath, KeyPath, TLSCertPath, PeerEndpoint, GatewayPeer, Channel, Chaincode, Function string; SubmitTimeout, CommitTimeout time.Duration }
func (c Config) Validate() error { if c.MSPID==""||c.CertPath==""||c.KeyPath==""||c.TLSCertPath==""||c.PeerEndpoint==""||c.GatewayPeer==""||c.Channel==""||c.Chaincode==""||c.Function=="" { return fmt.Errorf("invalid Fabric Gateway configuration") }; return nil }
func readPEM(path string) ([]byte,error) { st,e:=os.Stat(path); if e!=nil{return nil,e}; if !st.IsDir(){return os.ReadFile(path)}; f,e:=os.Open(path);if e!=nil{return nil,e};defer f.Close();names,e:=f.Readdirnames(1);if e!=nil{return nil,e};return os.ReadFile(filepath.Join(path,names[0])) }

type Backend struct { gateway *client.Gateway; contract *client.Contract; conn *grpc.ClientConn; cfg Config }
func New(cfg Config)(*Backend,error){
	if e:=cfg.Validate();e!=nil{return nil,e}; if cfg.SubmitTimeout<=0{cfg.SubmitTimeout=5*time.Second};if cfg.CommitTimeout<=0{cfg.CommitTimeout=time.Minute}
	pem,e:=readPEM(cfg.TLSCertPath);if e!=nil{return nil,fmt.Errorf("read TLS certificate: %w",e)};cert,e:=identity.CertificateFromPEM(pem);if e!=nil{return nil,e};pool:=x509.NewCertPool();pool.AddCert(cert)
	conn,e:=grpc.NewClient(cfg.PeerEndpoint,grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool,cfg.GatewayPeer)));if e!=nil{return nil,e}
	pem,e=readPEM(cfg.CertPath);if e!=nil{conn.Close();return nil,e};xcert,e:=identity.CertificateFromPEM(pem);if e!=nil{conn.Close();return nil,e};id,e:=identity.NewX509Identity(cfg.MSPID,xcert);if e!=nil{conn.Close();return nil,e}
	pem,e=readPEM(cfg.KeyPath);if e!=nil{conn.Close();return nil,e};key,e:=identity.PrivateKeyFromPEM(pem);if e!=nil{conn.Close();return nil,e};sign,e:=identity.NewPrivateKeySign(key);if e!=nil{conn.Close();return nil,e}
	gw,e:=client.Connect(id,client.WithSign(sign),client.WithHash(hash.SHA256),client.WithClientConnection(conn),client.WithSubmitTimeout(cfg.SubmitTimeout),client.WithCommitStatusTimeout(cfg.CommitTimeout));if e!=nil{conn.Close();return nil,e}
	return &Backend{gateway:gw,contract:gw.GetNetwork(cfg.Channel).GetContract(cfg.Chaincode),conn:conn,cfg:cfg},nil
}

func (b *Backend) Commit(ctx context.Context,tx v2.Tx) error { if b==nil||b.contract==nil{return fmt.Errorf("Fabric Gateway backend is not initialized")};if e:=ctx.Err();e!=nil{return e};_,commit,e:=b.contract.SubmitAsync(b.cfg.Function,client.WithArguments(tx.ID,string(tx.Payload)));if e!=nil{return fmt.Errorf("Fabric submission failed for HED tx %s: %w",tx.ID,e)};status,e:=commit.Status();if e!=nil{return fmt.Errorf("Fabric commit confirmation failed for HED tx %s: %w",tx.ID,e)};if !status.Successful{return fmt.Errorf("Fabric ledger rejected HED tx %s: validation_code=%d fabric_tx_id=%s",tx.ID,status.Code,status.TransactionID)};return nil }

// Status implements v2.LedgerState. It reads the HED idempotency key directly
// from the ledger, which lets recovery distinguish a committed transaction
// from an absent one after a lost commit confirmation.
func (b *Backend) Status(ctx context.Context,txID string)(v2.LedgerTxStatus,error){
	if b==nil||b.contract==nil{return v2.LedgerUnknown,fmt.Errorf("Fabric Gateway backend is not initialized")};if e:=ctx.Err();e!=nil{return v2.LedgerUnknown,e};result,e:=b.contract.EvaluateTransaction("GetByHEDID",txID);if e!=nil{return v2.LedgerUnknown,fmt.Errorf("Fabric ledger status query failed for HED tx %s: %w",txID,e)};if len(result)==0{return v2.LedgerNotCommitted,nil};return v2.LedgerCommitted,nil
}
func (b *Backend) Close()error{if b==nil{return nil};if b.gateway!=nil{b.gateway.Close()};if b.conn!=nil{return b.conn.Close()};return nil}
func DefaultConfigFromEnv()Config{return Config{MSPID:os.Getenv("FABRIC_MSP_ID"),CertPath:os.Getenv("FABRIC_CERT_PATH"),KeyPath:os.Getenv("FABRIC_KEY_PATH"),TLSCertPath:os.Getenv("FABRIC_TLS_CERT_PATH"),PeerEndpoint:os.Getenv("FABRIC_PEER_ENDPOINT"),GatewayPeer:os.Getenv("FABRIC_GATEWAY_PEER"),Channel:os.Getenv("FABRIC_CHANNEL"),Chaincode:os.Getenv("FABRIC_CHAINCODE"),Function:os.Getenv("FABRIC_FUNCTION"),SubmitTimeout:5*time.Second,CommitTimeout:time.Minute}}
func DefaultLocalConfig(root string)Config{return Config{MSPID:"Org1MSP",CertPath:filepath.Join(root,"organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts"),KeyPath:filepath.Join(root,"organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore"),TLSCertPath:filepath.Join(root,"organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"),PeerEndpoint:"dns:///localhost:7051",GatewayPeer:"peer0.org1.example.com",Channel:"mychannel",Chaincode:"hed",Function:"Commit",SubmitTimeout:5*time.Second,CommitTimeout:time.Minute}}
