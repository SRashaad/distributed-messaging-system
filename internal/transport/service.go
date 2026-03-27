// Module: Transport
// Phase: Foundation
// Purpose: Declares shared RPC contracts used by node-to-node communication.
// These types bridge the transport layer with consensus and replication modules.
package transport

import "context"

// SendMessageRequest carries a payload between nodes.
type SendMessageRequest struct {
	FromNodeID string
	Payload    []byte
}

// SendMessageResponse is the basic acknowledgement for SendMessage.
type SendMessageResponse struct {
	Accepted bool
}

// HeartbeatRequest is used by nodes to signal liveness.
type HeartbeatRequest struct {
	NodeID string
}

// HeartbeatResponse confirms a heartbeat was received.
type HeartbeatResponse struct {
	Ok bool
}

// RequestVoteRequest wraps a vote request for transport.
type RequestVoteRequest struct {
	CandidateID string
}

// RequestVoteResponse wraps a vote result for transport.
type RequestVoteResponse struct {
	VoteGranted bool
}

// Service defines foundational RPC handlers for node-to-node communication.
type Service interface {
	// SendMessage delivers a node message to another node.
	SendMessage(context.Context, *SendMessageRequest) (*SendMessageResponse, error)
	// Heartbeat reports that a node is alive.
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// RequestVote is used for consensus leader election.
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
}
