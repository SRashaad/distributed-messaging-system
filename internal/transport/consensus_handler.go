package transport

import (
	"context"

	"distributed-messaging-system/internal/consensus"
	"distributed-messaging-system/internal/transport/proto"
)

// ConsensusAPI defines the operations required for consensus networking.
type ConsensusAPI interface {
	HandleAppendEntries(req *consensus.AppendEntriesRequest) *consensus.AppendEntriesResponse
	HandleRequestVote(req *consensus.RequestVoteRequest) *consensus.RequestVoteResponse
	// Replicate incoming log entries during AppendEntries
	OnAppendEntriesRecv(entries []consensus.LogEntry, leaderCommit uint64)
}

type ConsensusHandler struct {
	proto.UnimplementedConsensusServiceServer
	api ConsensusAPI
}

func NewConsensusHandler(api ConsensusAPI) *ConsensusHandler {
	return &ConsensusHandler{api: api}
}

func (h *ConsensusHandler) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	// Convert proto request to consensus request
	entries := make([]consensus.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = consensus.LogEntry{
			Index:     e.Index,
			Term:      e.Term,
			Timestamp: e.Timestamp,
			Data:      e.Data,
		}
	}

	cReq := &consensus.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	cResp := h.api.HandleAppendEntries(cReq)

	// If successful and there are entries, tell the node to store them
	if cResp.Success {
		h.api.OnAppendEntriesRecv(entries, req.LeaderCommit)
	}

	return &proto.AppendEntriesResponse{
		Term:    cResp.Term,
		Success: cResp.Success,
	}, nil
}

func (h *ConsensusHandler) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	cReq := &consensus.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	cResp := h.api.HandleRequestVote(cReq)

	return &proto.RequestVoteResponse{
		Term:        cResp.Term,
		VoteGranted: cResp.VoteGranted,
	}, nil
}
