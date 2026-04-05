package transport

import (
	"context"
	"sort"
	"strconv"

	"distributed-messaging-system/internal/storage"
	"distributed-messaging-system/internal/transport/proto"
)

// MessagingAPI defines the node operations required by the MessagingService.
type MessagingAPI interface {
	PublishMessage(data []byte) (uint64, uint64, error)
	GetStore() *storage.MessageStore
	GetStatus(ctx context.Context) (*proto.StatusResponse, error)
}

// MessagingHandler implements proto.MessagingServiceServer
type MessagingHandler struct {
	proto.UnimplementedMessagingServiceServer
	nodeAPI MessagingAPI
}

// NewMessagingHandler creates a new gRPC handler for MessagingService.
func NewMessagingHandler(api MessagingAPI) *MessagingHandler {
	return &MessagingHandler{
		nodeAPI: api,
	}
}

// Publish translates a gRPC PublishRequest into a node PublishMessage call.
func (h *MessagingHandler) Publish(ctx context.Context, req *proto.PublishRequest) (*proto.PublishResponse, error) {
	index, timestamp, err := h.nodeAPI.PublishMessage(req.Message)
	if err != nil {
		return nil, err
	}

	return &proto.PublishResponse{
		Success:   true,
		Index:     index,
		Timestamp: timestamp,
	}, nil
}

// Consume retrieves all committed messages starting from the given index.
func (h *MessagingHandler) Consume(ctx context.Context, req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	store := h.nodeAPI.GetStore()
	allMessages := store.GetAll()
	
	var responses []*proto.LogEntry

	for id, msgData := range allMessages {
		index, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			continue
		}
		if index >= req.FromIndex {
			responses = append(responses, &proto.LogEntry{
				Index:     index,
				Term:      msgData.Term,
				Timestamp: msgData.Timestamp,
				Data:      []byte(msgData.Data),
			})
		}
	}

	// Member 3: Time Synchronization
	// Sort results by Lamport timestamp for true causal ordering (out-of-order resolution).
	// If timestamps are identical, use the Raft log index as a deterministic tie-breaker.
	sort.Slice(responses, func(i, j int) bool {
		if responses[i].Timestamp == responses[j].Timestamp {
			return responses[i].Index < responses[j].Index
		}
		return responses[i].Timestamp < responses[j].Timestamp
	})

	return &proto.ConsumeResponse{
		Messages: responses,
	}, nil
}

// GetStatus returns identity, advertised gRPC address, role, and log metrics from the node.
func (h *MessagingHandler) GetStatus(ctx context.Context, _ *proto.StatusRequest) (*proto.StatusResponse, error) {
	return h.nodeAPI.GetStatus(ctx)
}
