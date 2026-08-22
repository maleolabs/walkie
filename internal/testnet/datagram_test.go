package testnet

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// The datagram tests pin the two properties ts:test-harness asks of loss
// injection: it is paid on the injected clock (criterion 2), and the drop
// pattern is a pure function of the seed (criterion 3). Every test that
// asserts an exact drop count serialises its sends, because concurrent
// senders interleave draws from the shared source — see the determinism note
// on DatagramLink.

func TestDatagramCarriesPayloadBothWays(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(1, 2)), DatagramConditions{})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	if err := a.Send([]byte("ping")); err != nil {
		t.Fatalf("a.Send: %v", err)
	}
	got, err := b.Recv()
	if err != nil {
		t.Fatalf("b.Recv: %v", err)
	}
	if string(got) != "ping" {
		t.Errorf("got %q, want %q", got, "ping")
	}

	if err := b.Send([]byte("pong")); err != nil {
		t.Fatalf("b.Send: %v", err)
	}
	got, err = a.Recv()
	if err != nil {
		t.Fatalf("a.Recv: %v", err)
	}
	if string(got) != "pong" {
		t.Errorf("got %q, want %q", got, "pong")
	}

	if s := l.Stats(); s.Delivered != 2 || s.Dropped != 0 || s.Sent != 2 {
		t.Errorf("Stats() = %+v, want 2 sent, 0 dropped, 2 delivered", s)
	}
}

// The core determinism proof: with one sending goroutine, the number of
// dropped datagrams is exactly what replaying the same PCG stream predicts —
// not approximately, not statistically, exactly. Two links built from the
// same seed produce identical counts.
func TestDatagramLossIsAFunctionOfTheSeed(t *testing.T) {
	const (
		seedA, seedB = 0x5EED, 0xC0FFEE
		sends        = 200
		loss         = 0.5
	)

	runOnce := func() DatagramStats {
		// QueueDepth equals the send count, so overflow is impossible by
		// construction and Dropped reflects only loss draws. A concurrent
		// drainer would make that property depend on scheduler pacing, which
		// is exactly what this test exists to exclude.
		l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(seedA, seedB)), DatagramConditions{
			Loss:       loss,
			QueueDepth: sends,
		})
		a, b := l.Endpoints()
		t.Cleanup(func() {
			_ = a.Close()
			_ = b.Close()
		})

		for i := range sends {
			if err := a.Send([]byte{byte(i)}); err != nil {
				t.Fatalf("send %d: %v", i, err)
			}
		}
		return l.Stats()
	}

	first := runOnce()

	// Replay the source independently: one Float64 per Send, in order.
	rng := rand.New(rand.NewPCG(seedA, seedB))
	wantDropped := 0
	for range sends {
		if rng.Float64() < loss {
			wantDropped++
		}
	}

	if first.Sent != sends {
		t.Errorf("Sent = %d, want %d", first.Sent, sends)
	}
	if first.Dropped != uint64(wantDropped) {
		t.Errorf("Dropped = %d, want %d (the exact count the seeded source predicts)", first.Dropped, wantDropped)
	}
	if second := runOnce(); second != first {
		t.Errorf("second run Stats() = %+v, want %+v — same seed must reproduce the same losses", second, first)
	}

	// The complement holds too: draining the queue delivers exactly the
	// survivors, no more, no fewer.
	a2, b2 := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(seedA, seedB)), DatagramConditions{
		Loss:       loss,
		QueueDepth: sends,
	}).Endpoints()
	for i := range sends {
		if err := a2.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	_ = b2.Close()
	received := 0
	for {
		if _, err := b2.Recv(); err != nil {
			break
		}
		received++
	}
	if got := uint64(sends - wantDropped); uint64(received) != got {
		t.Errorf("drained %d datagrams, want exactly the %d survivors the seed predicts", received, got)
	}
}

// Loss must actually lose something at p=0.5 over 200 sends; if this ever
// fails the RNG wiring is broken, and it fails loudly rather than passing a
// loss test with zero losses.
func TestDatagramLossActuallyDrops(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(7, 8)), DatagramConditions{Loss: 0.5, QueueDepth: 100})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	for i := range 100 {
		if err := a.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// Drain synchronously: everything that survived is already queued.
	_ = b.Close()
	delivered := 0
	for {
		if _, err := b.Recv(); err != nil {
			break
		}
		delivered++
	}

	if s := l.Stats(); s.Dropped == 0 || delivered == 0 {
		t.Errorf("Stats() = %+v with %d drained, want both drops and deliveries at p=0.5 over 100 sends", s, delivered)
	}
}

// The property that makes loss injection usable on the fake clock: latency is
// parked on Advance, never on the wall clock.
func TestDatagramLatencyIsPaidOnTheInjectedClock(t *testing.T) {
	fake := clock.NewFake(epoch)
	l := NewDatagramLink(fake, rand.New(rand.NewPCG(3, 4)), DatagramConditions{Latency: 5 * time.Millisecond})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	errc := make(chan error, 1)
	go func() {
		errc <- a.Send([]byte("late"))
	}()

	// Wait until the sender is parked on the fake clock.
	waitForPark(t, fake)
	select {
	case err := <-errc:
		t.Fatalf("Send completed before the clock advanced: %v", err)
	default:
	}

	fake.Advance(5 * time.Millisecond)

	if err := <-errc; err != nil {
		t.Fatalf("Send: %v", err)
	}
	got, err := b.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != "late" {
		t.Errorf("got %q, want %q", got, "late")
	}
}

// A datagram in flight when the link is severed is destroyed, not delivered
// late; new sends are refused outright.
func TestDatagramPartitionDropsInFlightAndRefusesSends(t *testing.T) {
	fake := clock.NewFake(epoch)
	l := NewDatagramLink(fake, rand.New(rand.NewPCG(5, 6)), DatagramConditions{Latency: 5 * time.Millisecond})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	errc := make(chan error, 1)
	go func() {
		errc <- a.Send([]byte("in flight"))
	}()
	waitForPark(t, fake)

	l.Partition()
	fake.Advance(5 * time.Millisecond)

	if err := <-errc; err != nil {
		t.Fatalf("Send across a mid-flight partition returned %v, want nil (UDP send success)", err)
	}
	if s := l.Stats(); s.Dropped != 1 || s.Delivered != 0 {
		t.Errorf("Stats() = %+v, want the in-flight datagram counted as dropped", s)
	}
	// Nothing was delivered: the Stats assertion above is the proof. Calling
	// Recv here would block forever — the endpoint is open and its queue is
	// empty — which is exactly the silence a real partition produces.

	if err := a.Send([]byte("refused")); !errors.Is(err, ErrPartitioned) {
		t.Errorf("Send during partition: got %v, want ErrPartitioned", err)
	}

	l.Heal()
	errc2 := make(chan error, 1)
	go func() {
		errc2 <- a.Send([]byte("after heal"))
	}()
	waitForPark(t, fake)
	fake.Advance(5 * time.Millisecond)
	if err := <-errc2; err != nil {
		t.Fatalf("Send after heal: %v", err)
	}
	if got, err := b.Recv(); err != nil || string(got) != "after heal" {
		t.Errorf("after heal got %q, %v; want %q, nil", got, err, "after heal")
	}
}

// A full receive queue destroys arrivals, exactly like an OS socket whose
// application cannot keep up. The survivor is the head of the queue: FIFO.
func TestDatagramQueueOverflowDropsTheNewcomer(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(9, 10)), DatagramConditions{QueueDepth: 1})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	for _, p := range []string{"first", "second", "third"} {
		if err := a.Send([]byte(p)); err != nil {
			t.Fatalf("Send(%q): %v", p, err)
		}
	}

	if s := l.Stats(); s.Sent != 3 || s.Dropped != 2 || s.Delivered != 0 {
		t.Fatalf("Stats() = %+v, want 3 sent, 2 dropped by overflow, 0 delivered", s)
	}

	got, err := b.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("survivor = %q, want %q (queue is FIFO)", got, "first")
	}
}

// Sends from a single goroutine arrive in order — the guarantee phase 2's RTP
// sequencing can lean on in tests. Concurrent senders get no such guarantee,
// matching UDP; that case is deliberately not asserted here.
func TestDatagramSingleSenderOrderIsPreserved(t *testing.T) {
	fake := clock.NewFake(epoch)
	l := NewDatagramLink(fake, rand.New(rand.NewPCG(11, 12)), DatagramConditions{Latency: time.Millisecond})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	const n = 5
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range n {
			if err := a.Send([]byte{byte('A' + i)}); err != nil {
				t.Errorf("Send %d: %v", i, err)
				return
			}
		}
	}()

	// One latency per send, released one at a time. Each iteration waits for
	// the sender to park on the fake clock so each Advance releases exactly
	// the send that is waiting — no scheduling luck involved.
	for range n {
		waitForPark(t, fake)
		fake.Advance(time.Millisecond)
	}
	<-sent

	for i := range n {
		got, err := b.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if want := byte('A' + i); got[0] != want {
			t.Fatalf("position %d got %q, want %q — single-sender order broken", i, got, want)
		}
	}
}

func TestDatagramCloseUnblocksRecvAndCountsLateArrivals(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(13, 14)), DatagramConditions{})
	a, b := l.Endpoints()
	t.Cleanup(func() { _ = a.Close() })

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := b.Recv(); !errors.Is(err, ErrClosed) {
		t.Errorf("Recv on closed endpoint: got %v, want ErrClosed", err)
	}

	// A send toward a closed endpoint succeeds from the sender's view — UDP
	// has no delivery receipts — but the link counts the datagram as dropped.
	if err := a.Send([]byte("nowhere")); err != nil {
		t.Errorf("Send toward closed endpoint: got %v, want nil", err)
	}
	if s := l.Stats(); s.Dropped != 1 {
		t.Errorf("Stats() = %+v, want the arrival at a closed endpoint counted as dropped", s)
	}
}

// Partitioned is the state query outage tests read between Partition and
// Heal; pinning it to both transitions keeps the query honest rather than a
// constant in disguise.
func TestDatagramPartitionedTracksPartitionAndHeal(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(17, 18)), DatagramConditions{})
	a, b := l.Endpoints()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})

	l.Partition()
	if !l.Partitioned() {
		t.Fatal("Partitioned() = false right after Partition, want true")
	}
	if err := a.Send([]byte("severed")); !errors.Is(err, ErrPartitioned) {
		t.Errorf("Send while partitioned: got %v, want ErrPartitioned", err)
	}

	l.Heal()
	if l.Partitioned() {
		t.Fatal("Partitioned() = true right after Heal, want false")
	}
}

func TestDatagramSetLossRejectsOutOfRange(t *testing.T) {
	l := NewDatagramLink(clock.Real(), rand.New(rand.NewPCG(15, 16)), DatagramConditions{})

	for _, p := range []float64{-0.1, 1.0, 1.5} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("SetLoss(%v) did not panic; a clamped probability would make typos look like passing loss tests", p)
				}
			}()
			l.SetLoss(p)
		}()
	}
}
