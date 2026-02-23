// =============================================================================
// Module: Transport
// File: grpc_client.go
// Responsible Member: All Members (Shared Infrastructure)
// Purpose: Manages outgoing gRPC connections to peer nodes in the cluster.
//          Each node maintains a connection pool to all other nodes.
//          These connections are used to send:
//            - RequestVote RPCs (during elections)
//            - AppendEntries RPCs (for log replication and heartbeats)
//
// Connections:
//   - Used by internal/consensus/raft.go to send RequestVote to peers.
//   - Used by internal/replication/manager.go to send AppendEntries to followers.
//   - Used by internal/fault/heartbeat.go (indirectly) for heartbeat broadcasts.
//   - Used by internal/fault/recovery.go to send missing entries to recovering nodes.
//   - Initialized in internal/node/node.go during startup.
//
// Implementation notes:
//   - Uses google.golang.org/grpc with insecure credentials (for academic project).
//   - Connections are lazily established and cached per peer address.
// =============================================================================
package transport

import "sync"

// PeerClient manages gRPC connections to peer nodes.
type PeerClient struct {
	mu    sync.RWMutex
	// TODO: Add a map of peer address → *grpc.ClientConn
}

// NewPeerClient creates a new client for communicating with peers.
//
// TODO: Initialize the connection map.
func NewPeerClient() *PeerClient {
	return nil
}

// Connect establishes a gRPC connection to a peer node at the given address.
// If a connection already exists, this is a no-op.
//
// TODO: Implement:
//   1. Check if a connection already exists for this address.
//   2. If not, create a new gRPC client connection.
//   3. Store the connection in the map.
func (p *PeerClient) Connect(addr string) error {
	return nil
}

// GetConnection returns the gRPC connection for the given peer address.
// Returns an error if no connection exists.
//
// TODO: Look up the connection in the map and return it.
func (p *PeerClient) GetConnection(addr string) (interface{}, error) {
	return nil, nil
}

// SendRequestVote sends a RequestVote RPC to the peer at the given address.
// Used during leader elections.
//
// TODO: Implement:
//   1. Get the connection for the given address.
//   2. Create a ConsensusService client from the connection.
//   3. Call RequestVote with the provided request.
//   4. Return the response.
func (p *PeerClient) SendRequestVote(addr string, req interface{}) (interface{}, error) {
	return nil, nil
}

// SendAppendEntries sends an AppendEntries RPC to the peer at the given address.
// Used for log replication and heartbeats.
//
// TODO: Implement:
//   1. Get the connection for the given address.
//   2. Create a ConsensusService client from the connection.
//   3. Call AppendEntries with the provided request.
//   4. Return the response.
func (p *PeerClient) SendAppendEntries(addr string, req interface{}) (interface{}, error) {
	return nil, nil
}

// CloseAll closes all peer connections.
//
// TODO: Iterate over all connections, close each one, and clear the map.
func (p *PeerClient) CloseAll() {
	// TODO: implement
}
