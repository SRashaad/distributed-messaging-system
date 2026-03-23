// =============================================================================
// Module: Transport
// File: grpc_server.go
// Responsible Member: All Members (Shared Infrastructure)
// Purpose: gRPC server that listens for incoming RPCs from peers and clients.
//          This is the network entry point for the node. It receives:
//            - RequestVote RPCs (from candidates during elections)
//            - AppendEntries RPCs (from the leader for replication/heartbeats)
//            - Publish/Consume RPCs (from clients)
//          and delegates them to the appropriate module.
//
// Connections:
//   - Delegates consensus RPCs to internal/consensus/raft.go.
//   - Delegates messaging RPCs to internal/node/node.go.
//   - Reports received heartbeats to internal/fault/detector.go.
//   - Proto definitions are in internal/transport/proto/messaging.proto.
//   - Started by internal/node/node.go during initialization.
//
// Implementation notes:
//   - Uses google.golang.org/grpc for the gRPC framework.
//   - Register service handlers (ConsensusService, MessagingService)
//     defined in the proto file.
// =============================================================================
package transport

// Server wraps a gRPC server with node-specific configuration.
type Server struct {
	port int // TCP port to listen on

	// TODO: Add fields for:
	//   grpcServer *grpc.Server       → the underlying gRPC server
	//   consensus  reference          → to delegate RequestVote/AppendEntries
	//   node       reference          → to delegate Publish/Consume
}

// NewServer creates a new gRPC server bound to the given port.
//
// TODO: Initialize a grpc.Server and store the port.
func NewServer(port int) *Server {
	return nil
}

// RegisterConsensusHandler registers the consensus module as the handler
// for RequestVote and AppendEntries RPCs.
//
// TODO: Implement gRPC service registration for ConsensusService.
func (s *Server) RegisterConsensusHandler() {
	// TODO: implement
}

// RegisterMessagingHandler registers the node as the handler
// for Publish and Consume RPCs from clients.
//
// TODO: Implement gRPC service registration for MessagingService.
func (s *Server) RegisterMessagingHandler() {
	// TODO: implement
}

// Start begins listening for incoming RPCs on the configured port.
// This method blocks until the server is stopped.
//
// TODO: Implement:
//   1. Create a TCP listener on s.port.
//   2. Register all service handlers.
//   3. Call grpcServer.Serve(listener) — this blocks.
func (s *Server) Start() error {
	return nil
}

// Stop gracefully stops the gRPC server.
// Waits for in-flight RPCs to complete before shutting down.
//
// TODO: Call grpcServer.GracefulStop().
func (s *Server) Stop() {
	// TODO: implement
}
