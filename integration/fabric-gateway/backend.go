package fabricgateway

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"hed-core/pkg/v2"
	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/hash"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	MSPID, CertPath, KeyPath, TLSCertPath, PeerEndpoint, GatewayPeer, Channel, Chaincode, Function string
	SubmitTimeout, CommitTimeout time.Duration
}

func (c Config) Validate() error {
	if c.MSPID == "" || c.CertPath == "" || c.KeyPath == "" || c.TLSCertPath == "" ||
		c.PeerEndpoint == "" || c.GatewayPeer == "" || c.Channel == "" || c.Chaincode == "" || c.Function == "" {
		return fmt.Errorf("invalid Fabric Gateway configuration")
	}
	return nil
}

// readPEM deterministically selects a usable PEM file from either a file or a
// Fabric keystore/signcert directory. It never depends on directory iteration
// order and ignores unrelated files.
func readPEM(path string, allowedTypes ...string) ([]byte, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(allowedTypes) > 0 && !pemHasType(data, allowedTypes...) {
			return nil, fmt.Errorf("PEM file %q does not contain an expected block", path)
		}
		return data, nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(path, name))
		if err != nil {
			continue
		}
		if len(allowedTypes) == 0 || pemHasType(data, allowedTypes...) {
			return data, nil
		}
	}
	return nil, fmt.Errorf("no suitable PEM file found in %q", path)
}

func pemHasType(data []byte, allowedTypes ...string) bool {
	for rest := data; len(rest) > 0; {
		block, next := pem.Decode(rest)
		if block == nil {
			return false
		}
		for _, typ := range allowedTypes {
			if block.Type == typ {
				return true
			}
		}
		rest = next
	}
	return false
}

type Backend struct {
	gateway  *client.Gateway
	contract *client.Contract
	conn     *grpc.ClientConn
	cfg      Config
}

func New(cfg Config) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.SubmitTimeout <= 0 {
		cfg.SubmitTimeout = 5 * time.Second
	}
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = time.Minute
	}

	pemData, err := readPEM(cfg.TLSCertPath, "CERTIFICATE")
	if err != nil {
		return nil, fmt.Errorf("read TLS certificate: %w", err)
	}
	cert, err := identity.CertificateFromPEM(pemData)
	if err != nil {
		return nil, fmt.Errorf("parse TLS certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)

	conn, err := grpc.NewClient(cfg.PeerEndpoint, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, cfg.GatewayPeer)))
	if err != nil {
		return nil, fmt.Errorf("create Fabric gRPC client: %w", err)
	}
	closeConn := func(e error) (*Backend, error) {
		_ = conn.Close()
		return nil, e
	}

	pemData, err = readPEM(cfg.CertPath, "CERTIFICATE")
	if err != nil {
		return closeConn(fmt.Errorf("read identity certificate: %w", err))
	}
	xcert, err := identity.CertificateFromPEM(pemData)
	if err != nil {
		return closeConn(fmt.Errorf("parse identity certificate: %w", err))
	}
	id, err := identity.NewX509Identity(cfg.MSPID, xcert)
	if err != nil {
		return closeConn(fmt.Errorf("create Fabric identity: %w", err))
	}

	pemData, err = readPEM(cfg.KeyPath, "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY")
	if err != nil {
		return closeConn(fmt.Errorf("read identity private key: %w", err))
	}
	key, err := identity.PrivateKeyFromPEM(pemData)
	if err != nil {
		return closeConn(fmt.Errorf("parse identity private key: %w", err))
	}
	sign, err := identity.NewPrivateKeySign(key)
	if err != nil {
		return closeConn(fmt.Errorf("create Fabric signer: %w", err))
	}

	gw, err := client.Connect(id, client.WithSign(sign), client.WithHash(hash.SHA256),
		client.WithClientConnection(conn), client.WithSubmitTimeout(cfg.SubmitTimeout),
		client.WithCommitStatusTimeout(cfg.CommitTimeout))
	if err != nil {
		return closeConn(fmt.Errorf("connect Fabric Gateway: %w", err))
	}
	return &Backend{gateway: gw, contract: gw.GetNetwork(cfg.Channel).GetContract(cfg.Chaincode), conn: conn, cfg: cfg}, nil
}

func (b *Backend) Commit(ctx context.Context, tx v2.Tx) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.contract == nil {
		return fmt.Errorf("Fabric Gateway backend is not initialized")
	}
	_, commit, err := b.contract.SubmitAsync(b.cfg.Function, client.WithArguments(tx.ID, string(tx.Payload)))
	if err != nil {
		return fmt.Errorf("Fabric submission failed for HED tx %s: %w", tx.ID, err)
	}
	status, err := commit.Status()
	if err != nil {
		return fmt.Errorf("Fabric commit confirmation failed for HED tx %s: %w", tx.ID, err)
	}
	if !status.Successful {
		return fmt.Errorf("Fabric ledger rejected HED tx %s: validation_code=%d fabric_tx_id=%s", tx.ID, status.Code, status.TransactionID)
	}
	return nil
}

// Status implements v2.LedgerState. It reads the HED idempotency key directly
// from the ledger, which lets recovery distinguish a committed transaction
// from an absent one after a lost commit confirmation.
func (b *Backend) Status(ctx context.Context, txID string) (v2.LedgerTxStatus, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if b == nil || b.contract == nil {
		return v2.LedgerUnknown, fmt.Errorf("Fabric Gateway backend is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return v2.LedgerUnknown, err
	}
	result, err := b.contract.EvaluateTransaction("GetByHEDID", txID)
	if err != nil {
		return v2.LedgerUnknown, fmt.Errorf("Fabric ledger status query failed for HED tx %s: %w", txID, err)
	}
	if len(result) == 0 {
		return v2.LedgerNotCommitted, nil
	}
	return v2.LedgerCommitted, nil
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	if b.gateway != nil {
		b.gateway.Close()
	}
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

func DefaultConfigFromEnv() Config {
	return Config{
		MSPID: os.Getenv("FABRIC_MSP_ID"), CertPath: os.Getenv("FABRIC_CERT_PATH"),
		KeyPath: os.Getenv("FABRIC_KEY_PATH"), TLSCertPath: os.Getenv("FABRIC_TLS_CERT_PATH"),
		PeerEndpoint: os.Getenv("FABRIC_PEER_ENDPOINT"), GatewayPeer: os.Getenv("FABRIC_GATEWAY_PEER"),
		Channel: os.Getenv("FABRIC_CHANNEL"), Chaincode: os.Getenv("FABRIC_CHAINCODE"),
		Function: os.Getenv("FABRIC_FUNCTION"), SubmitTimeout: 5 * time.Second, CommitTimeout: time.Minute,
	}
}

func DefaultLocalConfig(root string) Config {
	return Config{
		MSPID: "Org1MSP",
		CertPath: filepath.Join(root, "organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts"),
		KeyPath: filepath.Join(root, "organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore"),
		TLSCertPath: filepath.Join(root, "organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"),
		PeerEndpoint: "dns:///localhost:7051", GatewayPeer: "peer0.org1.example.com",
		Channel: "mychannel", Chaincode: "hed", Function: "Commit", SubmitTimeout: 5 * time.Second, CommitTimeout: time.Minute,
	}
}
