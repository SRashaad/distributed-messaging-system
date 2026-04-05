package replication

import (
	"strconv"

	"distributed-messaging-system/internal/storage"
)

// ApplyLogToStore is a temporary adapter used until consensus is implemented.
//
// It connects replication output (log index + byte payload) to the current
// storage input format (string ID + string message) after commit/apply.
func ApplyLogToStore(store *storage.MessageStore, index uint64, data []byte, timestamp uint64, term uint64) {
	id := strconv.FormatUint(index, 10)
	message := string(data)
	store.Apply(id, message, timestamp, term)
}
