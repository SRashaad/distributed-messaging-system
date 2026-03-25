package fault

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestDetector_ReportHeartbeat_NodeIsAlive(t *testing.T) {
	d := NewDetector(100 * time.Millisecond)
	d.ReportHeartbeat("node1")
	if !d.IsAlive("node1") {
		t.Error("node1 should be alive immediately after heartbeat")
	}
}

func TestDetector_UnknownNode_IsNotAlive(t *testing.T) {
	d := NewDetector(100 * time.Millisecond)
	if d.IsAlive("ghost") {
		t.Error("unknown node should not be considered alive")
	}
}

func TestDetector_Timeout_NodeDeclaredDead(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	d.ReportHeartbeat("node1")
	time.Sleep(80 * time.Millisecond)
	if d.IsAlive("node1") {
		t.Error("node1 should be dead after timeout")
	}
}

func TestDetector_OnFailure_CallbackFired(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	var failed string
	d.OnFailure(func(nodeID string) { failed = nodeID })
	d.ReportHeartbeat("node1")
	time.Sleep(80 * time.Millisecond)
	d.checkNodes()
	if failed != "node1" {
		t.Errorf("expected failure callback for node1, got %q", failed)
	}
}

func TestDetector_OnFailure_CallbackFiredOnlyOnce(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	var count int32
	d.OnFailure(func(nodeID string) { atomic.AddInt32(&count, 1) })
	d.ReportHeartbeat("node1")
	time.Sleep(80 * time.Millisecond)
	d.checkNodes()
	d.checkNodes()
	d.checkNodes()
	if atomic.LoadInt32(&count) != 1 {
		t.Errorf("expected callback to fire exactly once, fired %d times", count)
	}
}

func TestDetector_HeartbeatResetsFailedFlag(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	var callCount int32
	d.OnFailure(func(_ string) { atomic.AddInt32(&callCount, 1) })
	d.ReportHeartbeat("node1")
	time.Sleep(80 * time.Millisecond)
	d.checkNodes()
	d.ReportHeartbeat("node1")
	if !d.IsAlive("node1") {
		t.Error("node1 should be alive after recovery heartbeat")
	}
	time.Sleep(80 * time.Millisecond)
	d.checkNodes()
	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("expected 2 failure callbacks, got %d", callCount)
	}
}

func TestDetector_MultipleCallbacks(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	var a, b int32
	d.OnFailure(func(_ string) { atomic.AddInt32(&a, 1) })
	d.OnFailure(func(_ string) { atomic.AddInt32(&b, 1) })
	d.ReportHeartbeat("node1")
	time.Sleep(80 * time.Millisecond)
	d.checkNodes()
	if atomic.LoadInt32(&a) != 1 || atomic.LoadInt32(&b) != 1 {
		t.Errorf("both callbacks should fire: a=%d b=%d", a, b)
	}
}

// atomicString is a race-safe string holder for use across goroutines.
type atomicString struct{ p unsafe.Pointer }

func (s *atomicString) Store(v string) {
	atomic.StorePointer(&s.p, unsafe.Pointer(&v))
}

func (s *atomicString) Load() string {
	p := atomic.LoadPointer(&s.p)
	if p == nil {
		return ""
	}
	return *(*string)(p)
}

func TestDetector_StartMonitoring_DetectsFailure(t *testing.T) {
	d := NewDetector(60 * time.Millisecond)

	// Use atomicString so the callback goroutine and test goroutine
	// don't race on the same plain string variable.
	var failed atomicString
	d.OnFailure(func(nodeID string) { failed.Store(nodeID) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.StartMonitoring(ctx)

	d.ReportHeartbeat("node2")
	time.Sleep(200 * time.Millisecond)

	if failed.Load() != "node2" {
		t.Errorf("StartMonitoring should have detected node2 failure, got %q", failed.Load())
	}
}

func TestDetector_StartMonitoring_StopsOnContextCancel(t *testing.T) {
	d := NewDetector(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.StartMonitoring(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("StartMonitoring did not stop after context cancellation")
	}
}

func TestDetector_MultipleNodes(t *testing.T) {
	d := NewDetector(100 * time.Millisecond)
	d.ReportHeartbeat("node1")
	d.ReportHeartbeat("node2")
	d.ReportHeartbeat("node3")
	if !d.IsAlive("node1") || !d.IsAlive("node2") || !d.IsAlive("node3") {
		t.Error("all nodes should be alive after heartbeat")
	}
	time.Sleep(80 * time.Millisecond)
	d.ReportHeartbeat("node2")
	time.Sleep(80 * time.Millisecond)
	if d.IsAlive("node1") {
		t.Error("node1 should be dead (no heartbeat)")
	}
	if !d.IsAlive("node2") {
		t.Error("node2 should still be alive (sent heartbeat)")
	}
	if d.IsAlive("node3") {
		t.Error("node3 should be dead (no heartbeat)")
	}
}