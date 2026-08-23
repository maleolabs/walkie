package control

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/testnet"
)

// Criterion 5: reconnect behaviour verified through the deterministic
// harness with injected loss, latency and partition — reproducibly, not by
// hand. Every scenario here runs the real supervisor over testnet links on
// the fake clock; nothing sleeps in real time and every outcome is a pure
// function of its seeds.

// condConfig is this file's shrunken interval set. The dead-peer interval is
// set far beyond any scenario duration here: these tests inject errors and
// partitions, not silence, and a mid-scenario dead-peer teardown would add
// unscheduled reconnect cycles that muddy the walk assertions. Criterion 3's
// detection timing is exercised by TestSilentPeerRunsTheFullDegradedArc and
// pinned exactly by the watchdog unit tests.
func condConfig() Config {
	return Config{
		BackoffBase:      time.Second,
		BackoffCap:       4 * time.Second,
		HeartbeatPeriod:  2 * time.Second,
		DeadPeerInterval: 10 * time.Minute,
		HandshakeTimeout: 3 * time.Second,
	}
}

func TestPartitionSeversAndHealRecoversWithoutUserAction(t *testing.T) {
	// A partition surfaces ErrPartitioned on the next read or write — the
	// harness's model of a severed link (testnet's own doc explains why it
	// is an error here rather than silence). The client must treat that as
	// failure mode (b): immediate disconnected, retries failing while the
	// partition holds, and a full recovery to online once the link heals —
	// with no caller involvement at any point.
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	link := testnet.NewLink(fake, testnet.Conditions{})
	coord := newFakeCoordinator("dev", echoBehavior("dev"))
	cl, err := NewClient(condConfig(), mach, dialThrough(link, coord), 1, 2, fake, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cc := newChangeCollector(mach.Subscribe())
	cl.Start()
	t.Cleanup(cl.Stop)

	settled := false
	for i := 0; i < 4_000; i++ {
		if mach.State() == StateOnline {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	if !settled {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Logf("DEBUG partition stacks:\n%s", buf[:n])
		t.Fatalf("initial connect never settled (state=%s)", mach.State())
	}

	// Sever. The very next heartbeat write must fail and be recorded as a
	// DIRECT online -> disconnected verdict — no degraded waypoint for a
	// surfaced transport error; that distinction IS criterion 3's other
	// failure mode.
	link.Partition()
	settled = false
	for i := 0; i < 4_000; i++ {
		if _, ok := firstChangeIn(cc.snapshot(), func(ch Change) bool {
			return ch.From == StateOnline && ch.To == StateDisconnected
		}); ok {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	if !settled {
		t.Logf("DEBUG partition walk: state=%s events=%+v", mach.State(), cc.snapshot())
		t.Fatal("post-partition teardown never recorded")
	}

	// Heal. The next retry — armed by the supervisor itself — succeeds.
	link.Heal()
	drive(t, fake, 100*time.Millisecond, 4_000, func() bool { return mach.State() == StateOnline })
	// Catch the collector up with the machine while the clock is frozen:
	// wait for an ONLINE change that follows the post-partition teardown.
	runtime_GoschedUntilAll(t, func() bool {
		sawDisc := false
		for _, ch := range cc.snapshot() {
			if ch.From == StateOnline && ch.To == StateDisconnected {
				sawDisc = true
			}
			if sawDisc && ch.To == StateOnline {
				return true
			}
		}
		return false
	})

	var sawDirectDisconnect, sawFinalOnline bool
	for _, ch := range cc.snapshot() {
		if ch.From == StateOnline && ch.To == StateDisconnected {
			sawDirectDisconnect = true
			continue
		}
		if ch.To == StateOnline && sawDirectDisconnect {
			sawFinalOnline = true
		}
	}
	if !sawDirectDisconnect || !sawFinalOnline {
		t.Fatalf("event walk incomplete: direct disconnect=%v final online=%v\n%+v",
			sawDirectDisconnect, sawFinalOnline, cc.snapshot())
	}
}

func firstChangeIn(events []Change, pred func(Change) bool) (Change, bool) {
	for _, ch := range events {
		if pred(ch) {
			return ch, true
		}
	}
	return Change{}, false
}

func TestAddedLatencySlowsButDoesNotBreakReconnect(t *testing.T) {
	// Latency parks every write on the injected clock; the handshake and
	// heartbeats must still complete once the driver advances through the
	// delays, both on initial connect and across a crash-reconnect cycle.
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	link := testnet.NewLink(fake, testnet.Conditions{Latency: 750 * time.Millisecond})
	coord := newFakeCoordinator("dev", echoBehavior("dev"))
	cl, err := NewClient(condConfig(), mach, dialThrough(link, coord), 3, 4, fake, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cl.Start()
	t.Cleanup(cl.Stop)

	drive(t, fake, 250*time.Millisecond, 3_000, func() bool { return mach.State() == StateOnline })

	coord.killConns() // crash under latency

	drive(t, fake, 250*time.Millisecond, 4_000, func() bool { return mach.State() == StateOnline })
	if mach.State() != StateOnline {
		t.Fatalf("state = %s, want online after crash-reconnect under latency", mach.State())
	}
}

// lossyGateDial builds a DialFunc whose every attempt must cross a lossy
// DatagramLink hop before the coordinator becomes reachable — SYN loss on a
// real network. Loss lives on DatagramLink because the stream Link
// deliberately has no loss knob: dropping bytes from a reliable stream would
// model nothing real (testnet's package comment). Returns also a func
// reading how many dials were attempted.
func lossyGateDial(t *testing.T, fake *clock.Fake, coord *fakeCoordinator, seedA, seedB uint64, loss float64) (DialFunc, func() int, *testnet.DatagramLink) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seedA, seedB))
	dgLink := testnet.NewDatagramLink(fake, rng, testnet.DatagramConditions{Loss: loss})
	gateEnd, netEnd := dgLink.Endpoints()

	gate := make(chan struct{}, 64)
	go func() {
		for {
			if _, err := netEnd.Recv(); err != nil {
				return
			}
			gate <- struct{}{}
		}
	}()

	link := testnet.NewLink(fake, testnet.Conditions{})
	var mu sync.Mutex
	attempts := 0
	dial := DialFunc(func(context.Context) (Session, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		if err := gateEnd.Send([]byte{1}); err != nil {
			return nil, err
		}
		timer := fake.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-gate:
			clientEnd, serverEnd := link.Pipe()
			if err := coord.dialInto(serverEnd); err != nil {
				return nil, err
			}
			return newFramedSession(clientEnd), nil
		case <-timer.C():
			return nil, errors.New("dial lost in transit")
		}
	})
	return dial, func() int {
		mu.Lock()
		defer mu.Unlock()
		return attempts
	}, dgLink
}

func TestLossyDialRetriesUntilTheNetworkLetsItThrough(t *testing.T) {
	const (
		seedA, seedB = uint64(0xD1CE), uint64(0xB0B)
		lossRate     = 0.4
	)

	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	coord := newFakeCoordinator("dev", echoBehavior("dev"))
	dial, attempts, dgLink := lossyGateDial(t, fake, coord, seedA, seedB, lossRate)

	cl, err := NewClient(condConfig(), mach, dial, seedA, seedB, fake, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cl.Start()
	t.Cleanup(cl.Stop)

	drive(t, fake, 100*time.Millisecond, 6_000, func() bool { return mach.State() == StateOnline })

	stats := dgLink.Stats()
	if stats.Dropped == 0 {
		t.Fatal("loss never fired; the scenario proves nothing about lossy dials")
	}
	if stats.Delivered == 0 {
		t.Fatal("no dial ever got through; the seeded loss pattern swallowed everything")
	}
	if stats.Sent != stats.Delivered+stats.Dropped {
		t.Fatalf("datagram accounting broken: sent=%d delivered=%d dropped=%d",
			stats.Sent, stats.Delivered, stats.Dropped)
	}
	if attempts() < 2 {
		t.Fatalf("online reached after %d attempt(s); loss should have forced at least one retry", attempts())
	}
}

func TestQueueDeliveryFramesArriveAfterALossyDialPhase(t *testing.T) {
	// Composition check: loss on the dial path plus backlog delivery after
	// connect — criterion 4's resume machinery reached THROUGH a lossy
	// network. Seeded, so the exact drop pattern replays forever.
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}

	const backlogN = 3
	backlog := make([]*walkiev1.Envelope, 0, backlogN)
	for p := 1; p <= backlogN; p++ {
		backlog = append(backlog, &walkiev1.Envelope{
			MessageId: string(rune('a' + p)),
			Position:  uint64(p),
			Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Sender: "peer", Body: "queued",
			}},
		})
	}
	behavior := func(env *walkiev1.Envelope) []*walkiev1.Envelope {
		switch env.GetPayload().(type) {
		case *walkiev1.Envelope_Hello:
			replies := []*walkiev1.Envelope{helloAckEnvelope("dev")}
			replies = append(replies, backlog...)
			return replies
		default:
			return []*walkiev1.Envelope{env}
		}
	}
	coord := newFakeCoordinator("dev", behavior)

	// Seeds verified to DROP the first dial (first PCG draw 0.303 < 0.4) so
	// the scenario always exercises a lost dial before success.
	dial, _, dgLink := lossyGateDial(t, fake, coord, 0xD1CE, 0xB0B, 0.4)

	// Dead-peer interval beyond the whole scenario: under heavy scheduling
	// pressure an echo can arrive late enough to trip a short deadline and
	// force extra reconnect cycles; this scenario is about lossy DIALS plus
	// resume delivery, not about criterion 3 (covered elsewhere).
	cfg := condConfig()
	cfg.DeadPeerInterval = 10 * time.Minute
	cl, err := NewClient(cfg, mach, dial, 0x99, 0xAA, fake, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	var mu sync.Mutex
	seen := map[uint64]bool{}
	cl.OnEnvelope(func(env *walkiev1.Envelope) {
		if p := env.GetPosition(); p > 0 {
			mu.Lock()
			seen[p] = true
			mu.Unlock()
		}
	})
	cl.Start()
	t.Cleanup(cl.Stop)

	settled := false
	for i := 0; i < 6_000; i++ {
		mu.Lock()
		done := mach.State() == StateOnline && len(seen) == backlogN
		mu.Unlock()
		if done {
			settled = true
			break
		}
		fake.Advance(100 * time.Millisecond)
		yieldTo()
	}
	mu.Lock()
	t.Logf("DEBUG queue-loss: settled=%v state=%s seen=%d stats=%+v", settled, mach.State(), len(seen), dgLink.Stats())
	mu.Unlock()
	if !settled {
		t.Fatalf("lossy-dial queue scenario never settled (state=%s seen=%d)", mach.State(), len(seen))
	}
	if dgLink.Stats().Dropped == 0 {
		t.Fatal("loss never fired; scenario proves nothing")
	}
}
