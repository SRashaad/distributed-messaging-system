// =============================================================================
// Module: Fault Tolerance
// File: heartbeat_test.go
// Tests for the HeartbeatEmitter component.
// =============================================================================
package fault

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatEmitter_SendsHeartbeats(t *testing.T) {
	var count int32
	sendFn := func() { atomic.AddInt32(&count, 1) }

	emitter := NewHeartbeatEmitter(20*time.Millisecond, sendFn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go emitter.Start(ctx)
	time.Sleep(90 * time.Millisecond)
	cancel()

	got := atomic.LoadInt32(&count)
	// With 20ms interval over 90ms we expect at least 3 heartbeats
	if got < 3 {
		t.Errorf("expected at least 3 heartbeats in 90ms, got %d", got)
	}
}

func TestHeartbeatEmitter_StopsOnContextCancel(t *testing.T) {
	var count int32
	sendFn := func() { atomic.AddInt32(&count, 1) }

	emitter := NewHeartbeatEmitter(20*time.Millisecond, sendFn)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		emitter.Start(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Error("HeartbeatEmitter did not stop after context cancellation")
	}
}

func TestHeartbeatEmitter_NoSendBeforeFirstTick(t *testing.T) {
	var count int32
	sendFn := func() { atomic.AddInt32(&count, 1) }

	// 500ms interval — nothing should fire in the first 50ms
	emitter := NewHeartbeatEmitter(500*time.Millisecond, sendFn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go emitter.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	if atomic.LoadInt32(&count) != 0 {
		t.Error("expected no heartbeat before first tick interval elapsed")
	}
}

func TestHeartbeatEmitter_IntervalRespected(t *testing.T) {
	var timestamps []time.Time
	var mu sync.Mutex // protect slice in goroutine

	sendFn := func() {
		mu.Lock()
		timestamps = append(timestamps, time.Now())
		mu.Unlock()
	}

	emitter := NewHeartbeatEmitter(30*time.Millisecond, sendFn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go emitter.Start(ctx)
	time.Sleep(130 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond) // let goroutine settle

	mu.Lock()
	n := len(timestamps)
	mu.Unlock()

	if n < 3 {
		t.Fatalf("expected at least 3 heartbeats, got %d", n)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(timestamps); i++ {
		gap := timestamps[i].Sub(timestamps[i-1])
		// Allow generous tolerance: interval ± 15ms
		if gap < 15*time.Millisecond || gap > 60*time.Millisecond {
			t.Errorf("heartbeat %d gap = %v, expected ~30ms", i, gap)
		}
	}
}

func TestHeartbeatEmitter_MultipleStartStop(t *testing.T) {
	var count int32
	sendFn := func() { atomic.AddInt32(&count, 1) }
	emitter := NewHeartbeatEmitter(20*time.Millisecond, sendFn)

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go emitter.Start(ctx)
		time.Sleep(50 * time.Millisecond)
		cancel()
		time.Sleep(10 * time.Millisecond)
	}

	// Each 50ms window at 20ms interval = ~2 heartbeats × 3 rounds = at least 6
	if atomic.LoadInt32(&count) < 6 {
		t.Errorf("expected at least 6 heartbeats over 3 start/stop cycles, got %d", count)
	}
}