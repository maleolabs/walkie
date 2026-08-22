package clock

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// These tests cover the Timer extension to the Clock substrate. The timer
// exists because req:device-presence (TTL refresh), ts:reconnect-resume
// (backoff cancellation) and sto:offline-queue (retention deadlines) all arm
// timers they must retract; see NewTimer on the Clock interface.

func TestTimerFiresOnAdvance(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(45 * time.Second)

	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d after NewTimer, want 1", n)
	}

	f.Advance(44 * time.Second)
	select {
	case v := <-tm.C():
		t.Fatalf("timer fired one second early, at %v", v)
	default:
	}

	f.Advance(time.Second)
	select {
	case v := <-tm.C():
		if want := epoch.Add(45 * time.Second); !v.Equal(want) {
			t.Errorf("fired at %v, want %v", v, want)
		}
	default:
		t.Fatal("timer did not fire when due")
	}
}

func TestStopPreventsAFire(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(time.Minute)

	if !tm.Stop() {
		t.Fatal("Stop on a pending timer reported false, want true")
	}
	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d after Stop, want 0 — a stopped timer must not linger", n)
	}

	f.Advance(time.Minute)
	select {
	case v := <-tm.C():
		t.Fatalf("stopped timer fired at %v", v)
	default:
	}
}

func TestStopReportsFalseAfterFire(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(time.Second)

	f.Advance(time.Second)
	<-tm.C()

	if tm.Stop() {
		t.Fatal("Stop after the fire was delivered reported true, want false")
	}
}

func TestResetMovesTheDeadline(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(30 * time.Second)

	// The heartbeat-refresh shape from req:device-presence: rearm well before
	// the original deadline and expect only the new one.
	if !tm.Reset(5 * time.Second) {
		t.Fatal("Reset over a pending fire reported false, want true")
	}
	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d after Reset, want exactly the new deadline", n)
	}

	f.Advance(5 * time.Second)
	select {
	case v := <-tm.C():
		if want := epoch.Add(5 * time.Second); !v.Equal(want) {
			t.Errorf("fired at %v, want %v", v, want)
		}
	default:
		t.Fatal("reset deadline did not fire")
	}

	// The old 30s deadline must be gone, not merely superseded.
	f.Advance(25 * time.Second)
	select {
	case v := <-tm.C():
		t.Fatalf("superseded deadline fired at %v", v)
	default:
	}
}

func TestResetAfterFireReportsFalse(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(time.Second)

	f.Advance(time.Second)
	<-tm.C()

	if tm.Reset(time.Second) {
		t.Fatal("Reset after a delivered fire reported true, want false")
	}
	f.Advance(time.Second)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer did not rearm after Reset following a delivered fire")
	}
}

func TestZeroTimerFiresImmediately(t *testing.T) {
	f := NewFake(epoch)
	tm := f.NewTimer(0)

	if n := f.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d for an already-fired timer, want 0", n)
	}
	select {
	case v := <-tm.C():
		if !v.Equal(epoch) {
			t.Errorf("fired at %v, want %v", v, epoch)
		}
	default:
		t.Fatal("zero-duration timer did not fire immediately")
	}
	if tm.Stop() {
		t.Error("Stop on an already-fired timer reported true, want false")
	}
}

// This test pins the property the whole package leans on: fires are delivered
// after Advance releases its lock. If Advance ever fired waiters while holding
// the mutex, the goroutine here could not rearm inside its handler and the
// test would deadlock instead of passing.
func TestFiredGoroutineCanRearmWithoutDeadlock(t *testing.T) {
	f := NewFake(epoch)
	first := f.After(10 * time.Second)

	rearmed := make(chan Timer, 1)
	go func() {
		<-first
		rearmed <- f.NewTimer(10 * time.Second)
	}()

	for f.Waiters() == 0 {
		runtime.Gosched()
	}
	f.Advance(10 * time.Second)

	tm := <-rearmed // reaching here proves the handler got past receiving the fire
	f.Advance(10 * time.Second)
	select {
	case <-tm.C():
	default:
		t.Fatal("timer armed from a fired handler did not fire")
	}
}

// Not an assertion about outcomes — interleavings here are scheduler-dependent
// by design. It is race-detector fodder: concurrent arming, advancing,
// stopping and resetting must all be safe, because production code arms timers
// from connection goroutines while tests advance the clock from the test
// goroutine. Run under `make test-race`; a data race fails the gate there.
func TestConcurrentArmingAdvancingAndStoppingIsSafe(t *testing.T) {
	f := NewFake(epoch)

	const workers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				tm := f.NewTimer(time.Duration(j%7+1) * time.Millisecond)
				if j%3 == i%3 {
					tm.Stop()
					continue
				}
				select {
				case <-tm.C():
				case <-stop:
					return
				}
			}
		}(i)
	}

	for range 200 {
		f.Advance(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

func TestRealClockTimerIsWired(t *testing.T) {
	c := Real()
	tm := c.NewTimer(time.Millisecond)

	select {
	case <-tm.C():
	case <-time.After(2 * time.Second):
		t.Fatal("real NewTimer did not fire")
	}
	if tm.Stop() {
		t.Error("real Stop after a delivered fire reported true, want false")
	}

	live := c.NewTimer(time.Minute)
	if !live.Stop() {
		t.Error("real Stop on a pending timer reported false, want true")
	}
}

// Until is the absolute-deadline park the fleet simulator leans on: the
// deadline is registered atomically under the clock lock, so a driver
// advancing the shared clock between a caller's deadline computation and its
// park cannot displace the fire. A past deadline fires immediately.
func TestUntilRegistersAnAbsoluteDeadline(t *testing.T) {
	f := NewFake(epoch)

	deadline := epoch.Add(45 * time.Second)
	ch := f.Until(deadline)
	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d after Until, want 1", n)
	}

	f.Advance(44 * time.Second)
	select {
	case v := <-ch:
		t.Fatalf("fired one second early, at %v", v)
	default:
	}

	f.Advance(time.Second)
	select {
	case v := <-ch:
		if !v.Equal(epoch.Add(45 * time.Second)) {
			t.Errorf("fired at %v, want %v", v, epoch.Add(45*time.Second))
		}
	default:
		t.Fatal("did not fire when the clock crossed the deadline")
	}

	// Already-crossed deadlines fire immediately with the current time.
	if v := <-f.Until(epoch); !v.Equal(f.Now()) {
		t.Errorf("past-deadline Until fired at %v, want %v", v, f.Now())
	}
}
