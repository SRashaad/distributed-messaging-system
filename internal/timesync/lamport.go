package timesync

import "sync"

// LamportClock is a logical clock used to order events in distributed systems.
//
// Physical clocks on different machines can drift, so wall-clock time is not
// reliable for event ordering across nodes. A Lamport clock provides a simple
// counter-based way to preserve causality ordering between events.
type LamportClock struct {
	mu      sync.Mutex
	counter uint64
}

// Tick increments the clock by 1 for a local event and returns the new value.
func (c *LamportClock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter++
	return c.counter
}

// Update merges a received Lamport timestamp into the local clock.
//
// Rule: clock = max(current, received) + 1
// This ensures causally later events always get a larger timestamp.
func (c *LamportClock) Update(received uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if received > c.counter {
		c.counter = received
	}
	c.counter++
	return c.counter
}

// Current returns the current Lamport time without modifying it.
func (c *LamportClock) Current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.counter
}
