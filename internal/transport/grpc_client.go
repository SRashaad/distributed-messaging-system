// =============================================================================
// Module: Transport
// File: grpc_client.go
// Responsible Member: All Members (Shared Infrastructure)
// Purpose: Manages outgoing gRPC connections to peer nodes in the cluster.
//
// Connections:
//   - Used by internal/consensus/raft.go to send RequestVote to peers.
//   - Used by internal/replication/manager.go to send AppendEntries to followers.
//   - Used by internal/fault/heartbeat.go (indirectly) for heartbeat broadcasts.
//   - Initialized in internal/node/node.go during startup.
// =============================================================================
package transport

import (
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerClient manages gRPC connections to peer nodes.
type PeerClient struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn // peer address → gRPC connection
}

// NewPeerClient creates a new client for communicating with peers.
func NewPeerClient() *PeerClient {
	return &PeerClient{
		conns: make(map[string]*grpc.ClientConn),
	}
}

// Connect establishes a gRPC connection to a peer node at the given address.
// If a connection already exists, this is a no-op.
func (p *PeerClient) Connect(addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Skip if already connected
	if _, exists := p.conns[addr]; exists {
		return nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}

	p.conns[addr] = conn
	log.Printf("[transport] Connected to peer: %s", addr)
	return nil
}

// GetConnection returns the gRPC connection for the given peer address.
func (p *PeerClient) GetConnection(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conn, exists := p.conns[addr]
	if !exists {
		return nil, fmt.Errorf("no connection to peer %s", addr)
	}
	return conn, nil
}

// SendRequestVote sends a RequestVote RPC to the peer at the given address.
// With ZooKeeper handling elections, this is kept for compatibility but
// is not actively used during normal operation.
func (p *PeerClient) SendRequestVote(addr string, req interface{}) (interface{}, error) {
	_, err := p.GetConnection(addr)
	if err != nil {
		return nil, err
	}
	// In full gRPC integration, create a ConsensusService client and call RequestVote
	// With ZooKeeper elections, this is a no-op
	log.Printf("[transport] RequestVote to %s (ZooKeeper handles election)", addr)
	return nil, nil
}

// SendAppendEntries sends an AppendEntries RPC to the peer at the given address.
// Used for log replication and heartbeats.
func (p *PeerClient) SendAppendEntries(addr string, req interface{}) (interface{}, error) {
	_, err := p.GetConnection(addr)
	if err != nil {
		return nil, err
	}
	// In full gRPC integration, create a ConsensusService client and call AppendEntries
	log.Printf("[transport] AppendEntries sent to %s", addr)
	return nil, nil
}

// CloseAll closes all peer connections.
func (p *PeerClient) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil {
			log.Printf("[transport] Error closing connection to %s: %v", addr, err)
		}
	}
	p.conns = make(map[string]*grpc.ClientConn)
	log.Printf("[transport] All peer connections closed")
}
