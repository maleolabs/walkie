package clock

import (
	"runtime"
	"testing"
	"time"
)

// A fixed origin. Seeding a fake clock from the wall clock would reintroduce
// the non-determinism this package exists to remove.
var epoch = time.Date(2026, time.August, 20, 0, 0, 0, 0, time.UTC)

func TestFakeDoesNotMoveOnItsOwn(t *testing.T) {
	f := NewFake(epoch)
	if got := f.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", got, epoch)
	}
	if got := f.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() moved without Advance: %v", got)
	}
}

func TestAdvanceMovesNow(t *testing.T) {
	f := NewFake(epoch)
	f.Advance(90 * time.Second)

	if got, want := f.Now(), epoch.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v", got, want)
	}
	if got, want := f.Since(epoch), 90*time.Second; got != want {
		t.Fatalf("Since(epoch) = %v, want %v", got, want)
	}
}

// The shape every liveness TTL and backoff test will use: arm a timer, confirm
// it does not fire early, advance to the boundary, confirm it fires.
func TestAfterFiresOnlyWhenDue(t *testing.T) {
	f := NewFake(epoch)
	ch := f.After(45 * time.Second)

	if n := f.Waiters(); n != 1 {
		t.Fatalf("Waiters() = %d, want 1", n)
	}
	select {
	case v := <-ch:
		t.Fatalf("timer fired before any advance, at %v", v)
	default:
	}

	f.Advance(44 * time.Second)
	select {
	case v := <-ch:
		t.Fatalf("timer fired one second early, at %v", v)
	default:
	}

	f.Advance(time.Second)
	select {
	case v := <-ch:
		if want := epoch.Add(45 * time.Second); !v.Equal(want) {
			t.Errorf("fired at %v, want %v", v, want)
		}
	default:
		t.Fatal("timer did not fire when due")
	}

	if n := f.Waiters(); n != 0 {
		t.Errorf("Waiters() = %d after firing, want 0", n)
	}
}

func TestAfterNonPositiveFiresImmediately(t *testing.T) {
	f := NewFake(epoch)

	select {
	case v := <-f.After(0):
		if !v.Equal(epoch) {
			t.Errorf("fired at %v, want %v", v, epoch)
		}
	default:
		t.Fatal("After(0) did not fire immediately")
	}
	if n := f.Waiters(); n != 0 {
		t.Errorf("Waiters() = %d, want 0", n)
	}
}

func TestAdvanceFiresEveryDueWaiter(t *testing.T) {
	f := NewFake(epoch)
	at10 := f.After(10 * time.Second)
	at20 := f.After(20 * time.Second)
	at30 := f.After(30 * time.Second)

	f.Advance(25 * time.Second)

	for name, ch := range map[string]<-chan time.Time{"10s": at10, "20s": at20} {
		select {
		case <-ch:
		default:
			t.Errorf("waiter due at %s did not fire after advancing 25s", name)
		}
	}
	select {
	case <-at30:
		t.Error("waiter due at 30s fired after advancing only 25s")
	default:
	}

	if n := f.Waiters(); n != 1 {
		t.Errorf("Waiters() = %d, want 1 (the 30s waiter)", n)
	}
}

func TestSleepUnblocksOnAdvance(t *testing.T) {
	f := NewFake(epoch)

	done := make(chan struct{})
	go func() {
		f.Sleep(time.Minute)
		close(done)
	}()

	// Wait until the sleeper has actually armed its timer, so Advance cannot
	// race ahead of it and leave the goroutine parked.
	for f.Waiters() == 0 {
		runtime.Gosched()
	}
	f.Advance(time.Minute)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sleep did not return after the clock advanced past its deadline")
	}
}

func TestRealClockIsWired(t *testing.T) {
	c := Real()

	if d := c.Since(c.Now()); d < 0 {
		t.Errorf("Since returned a negative duration: %v", d)
	}
	select {
	case <-c.After(time.Millisecond):
	case <-time.After(2 * time.Second):
		t.Fatal("real After did not fire")
	}
}
