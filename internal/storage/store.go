package storage

import "sync"

// MessageStore keeps committed messages in memory.
//
// This is used after consensus commit, when an entry is considered safe and
// can be applied to the node state.
//
// Storage must be consistent so every node exposes the same committed data,
// which is essential for correctness in distributed systems.
//
// This module will be used by replication/consensus after commit phase.
type MessageStore struct {
	mu       sync.RWMutex
	messages map[string]string // message ID -> message content
}

// NewMessageStore creates an empty in-memory message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string]string),
	}
}

// Apply stores a committed message by ID.
func (s *MessageStore) Apply(id string, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[id] = message
}

// Get retrieves a committed message by ID.
// The bool return indicates whether the message exists.
func (s *MessageStore) Get(id string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.messages[id]
	return value, ok
}
