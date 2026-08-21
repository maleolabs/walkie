package testnet

import (
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

var epoch = time.Date(2026, time.August, 20, 0, 0, 0, 0, time.UTC)

// net.Pipe is synchronous, so every write needs a concurrent reader. That is a
// property of the harness, not of these tests.
func TestPipeCarriesBytes(t *testing.T) {
	l := NewLink(clock.Real(), Conditions{})
	a, b := l.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	want := []byte("hello")
	errc := make(chan error, 1)
	go func() {
		_, err := a.Write(want)
		errc <- err
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(b, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPartitionBreaksBothDirections(t *testing.T) {
	l := NewLink(clock.Real(), Conditions{})
	a, b := l.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	l.Partition()
	if !l.Partitioned() {
		t.Fatal("Partitioned() = false immediately after Partition()")
	}

	if _, err := a.Write([]byte("x")); !errors.Is(err, ErrPartitioned) {
		t.Errorf("write during partition: got %v, want ErrPartitioned", err)
	}
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, ErrPartitioned) {
		t.Errorf("read during partition: got %v, want ErrPartitioned", err)
	}
}

func TestHealRestoresTheLink(t *testing.T) {
	l := NewLink(clock.Real(), Conditions{})
	a, b := l.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	l.Partition()
	l.Heal()
	if l.Partitioned() {
		t.Fatal("Partitioned() = true after Heal()")
	}

	errc := make(chan error, 1)
	go func() {
		_, err := a.Write([]byte("y"))
		errc <- err
	}()

	if _, err := io.ReadFull(b, make([]byte, 1)); err != nil {
		t.Fatalf("read after heal: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write after heal: %v", err)
	}
}

// The property that makes this harness worth having: added latency is paid on
// the injected clock, so no test waits in real time.
func TestLatencyIsPaidOnTheInjectedClock(t *testing.T) {
	fake := clock.NewFake(epoch)
	l := NewLink(fake, Conditions{Latency: 250 * time.Millisecond})
	a, b := l.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	errc := make(chan error, 1)
	go func() {
		_, err := a.Write([]byte("z"))
		errc <- err
	}()

	// Wait until the writer is parked on the fake clock.
	for fake.Waiters() == 0 {
		runtime.Gosched()
	}
	select {
	case err := <-errc:
		t.Fatalf("write completed before the clock advanced: %v", err)
	default:
	}

	fake.Advance(250 * time.Millisecond)

	if _, err := io.ReadFull(b, make([]byte, 1)); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
}
