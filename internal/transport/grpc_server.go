// Module: Transport
// Phase: Foundation + Consensus Integration
// Purpose: Provides a gRPC server wrapper used by node startup.
// Handles incoming RPCs for consensus (RequestVote, AppendEntries) and
// messaging (Publish, Consume).
package transport

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

// Server wraps a gRPC server with node-specific configuration.
type Server struct {
	port       int          // TCP port to listen on
	grpcServer *grpc.Server // the underlying gRPC server
}

// NewServer creates a new gRPC server bound to the given port.
func NewServer(port int) *Server {
	return &Server{
		port:       port,
		grpcServer: grpc.NewServer(),
	}
}

// GetGRPCServer returns the underlying grpc.Server for service registration.
// Other modules (consensus, messaging) use this to register their handlers.
func (s *Server) GetGRPCServer() *grpc.Server {
	return s.grpcServer
}

// RegisterConsensusHandler is a placeholder for registering the consensus
// module's gRPC handlers. With ZooKeeper handling election, this primarily
// handles AppendEntries for log replication.
func (s *Server) RegisterConsensusHandler() {
	log.Printf("[transport] Consensus handler registered (ZooKeeper handles election)")
}

// RegisterMessagingHandler is a placeholder for registering the messaging
// service handlers for Publish and Consume RPCs from clients.
func (s *Server) RegisterMessagingHandler() {
	log.Printf("[transport] Messaging handler registered")
}

// Start begins listening for incoming RPCs on the configured port.
// This method blocks until the server is stopped.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	log.Printf("[transport] gRPC server listening on %s", addr)

	// Serve blocks, so run it in a goroutine
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			log.Printf("[transport] gRPC server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		log.Printf("[transport] Stopping gRPC server gracefully")
		s.grpcServer.GracefulStop()
	}
}
