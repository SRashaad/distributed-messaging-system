package timesync

import "sort"

// SortEvents returns events in deterministic order for distributed processing.
//
// Ordering matters in distributed systems because events can arrive in
// different sequences on different nodes. Lamport timestamps provide a shared
// logical ordering; NodeID is used as a stable tie-breaker when timestamps are equal.
func SortEvents(events []Event) []Event {
	sorted := make([]Event, len(events))
	copy(sorted, events)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Timestamp == sorted[j].Timestamp {
			return sorted[i].NodeID < sorted[j].NodeID
		}
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	return sorted
}
