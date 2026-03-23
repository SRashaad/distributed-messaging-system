// Module: Transport
// Phase: Foundation
// Purpose: Provides a minimal gRPC server wrapper used by node startup.
// Extended in later phases by full RPC registration and request handling.
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
	return &Server{port: port}
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
	_ = s.port // TODO: Bind listener and start grpc.Server.Serve.
	return nil
}

// Stop gracefully stops the gRPC server.
// Waits for in-flight RPCs to complete before shutting down.
//
// TODO: Call grpcServer.GracefulStop().
func (s *Server) Stop() {
	// TODO: implement
}
