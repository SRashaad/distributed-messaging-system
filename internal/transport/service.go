// Module: Transport
// Phase: Foundation
// Purpose: Declares shared RPC contracts used by node-to-node communication.
// Extended in later phases with full protobuf-generated handlers and implementations.
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

// RequestVoteRequest is a placeholder for future election flow.
type RequestVoteRequest struct {
	CandidateID string
}

// RequestVoteResponse is a placeholder vote result.
type RequestVoteResponse struct {
	VoteGranted bool
}

// Service defines foundational RPC handlers without implementation.
type Service interface {
	// SendMessage delivers a node message to another node.
	SendMessage(context.Context, *SendMessageRequest) (*SendMessageResponse, error)
	// Heartbeat reports that a node is alive.
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// RequestVote is reserved for future consensus work.
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
}
