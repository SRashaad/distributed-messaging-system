package storage

import "sync"

// StoredMessage holds a committed message along with its Lamport timestamp.
type StoredMessage struct {
	Data      string
	Timestamp uint64
	Term      uint64
}

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
	messages map[string]StoredMessage // message ID -> stored message
}

// NewMessageStore creates an empty in-memory message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string]StoredMessage),
	}
}

// Apply stores a committed message by ID with its Lamport timestamp.
func (s *MessageStore) Apply(id string, message string, timestamp uint64, term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[id] = StoredMessage{Data: message, Timestamp: timestamp, Term: term}
}

// Get retrieves a committed message by ID.
// The bool return indicates whether the message exists.
func (s *MessageStore) Get(id string) (StoredMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.messages[id]
	return value, ok
}

// GetAll returns a copy of all committed messages.
func (s *MessageStore) GetAll() map[string]StoredMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp := make(map[string]StoredMessage, len(s.messages))
	for k, v := range s.messages {
		cp[k] = v
	}
	return cp
}
