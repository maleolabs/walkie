package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// Loop-mechanics tests for the reconnect supervisor. Everything here runs on
// the fake clock and scripted sessions: the assertions pin the LOOP's
// behaviour — when it dials, what it sends, how it waits, what it resets —
// exactly, without any transport machinery. The transport itself is proven
// against the real coordinator in ws_e2e_test.go, and the fleet-scale
// behaviour in herd_test.go.

// loopConfig is this file's shrunken interval set: small enough to drive in
// a few hundred fake milliseconds, structurally valid (dead-peer exceeds
// heartbeat), and with no literal duration anywhere in the code under test.
func loopConfig() Config {
	return Config{
		BackoffBase:      time.Second,
		BackoffCap:       time.Minute,
		HeartbeatPeriod:  5 * time.Second,
		DeadPeerInterval: 12 * time.Second,
		HandshakeTimeout: 3 * time.Second,
	}
}

// stubSession is a fully scripted Session: the test decides what Recv
// returns and whether Send fails, and observes what was sent and when Close
// happened. With autoAck set it plays the coordinator's handshake half (a
// Hello is answered with an honest HelloAck); with autoPong set, every
// Heartbeat sent comes back as one — the pong half. Replies are injected at
// SEND time so they land in strict order after the frame that provoked them,
// with no pump goroutine whose scheduling could reorder the script.
type stubSession struct {
	mu        sync.Mutex
	sent      []*walkiev1.Envelope
	sendErr   error
	autoAck   bool
	autoPong  bool
	inbox     chan *walkiev1.Envelope
	closed    chan struct{}
	closeOnce sync.Once
}

func newStubSession() *stubSession {
	return &stubSession{
		inbox:  make(chan *walkiev1.Envelope, 16),
		closed: make(chan struct{}),
	}
}

func (s *stubSession) Send(env *walkiev1.Envelope) error {
	s.mu.Lock()
	if s.sendErr != nil {
		err := s.sendErr
		s.mu.Unlock()
		return err
	}
	s.sent = append(s.sent, env)
	autoAck := s.autoAck && env.GetHello() != nil
	autoPong := s.autoPong && env.GetHeartbeat() != nil
	s.mu.Unlock()
	switch {
	case autoAck:
		// The scripted ack: written after the Hello was accepted, exactly
		// like a coordinator answering the handshake.
		s.inbox <- helloAckEnvelope("dev")
	case autoPong:
		s.inbox <- &walkiev1.Envelope{Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}}}
	}
	return nil
}

func (s *stubSession) Recv() (*walkiev1.Envelope, error) {
	select {
	case env := <-s.inbox:
		return env, nil
	case <-s.closed:
		return nil, errors.New("stub session closed")
	}
}

func (s *stubSession) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *stubSession) sentHellos() []*walkiev1.Hello {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*walkiev1.Hello
	for _, env := range s.sent {
		if h := env.GetHello(); h != nil {
			out = append(out, h)
		}
	}
	return out
}

func (s *stubSession) sentQueueAcks() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uint64
	for _, env := range s.sent {
		if qa := env.GetQueueAck(); qa != nil {
			out = append(out, qa.GetAcknowledgedPosition())
		}
	}
	return out
}

func (s *stubSession) sentHeartbeats() []*walkiev1.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*walkiev1.Envelope
	for _, env := range s.sent {
		if env.GetHeartbeat() != nil {
			out = append(out, env)
		}
	}
	return out
}

// deliver feeds one envelope to the session's reader as if the peer sent it.
func (s *stubSession) deliver(env *walkiev1.Envelope) {
	s.inbox <- env
}

// buildClient wires machine + client on the fake clock WITHOUT starting it,
// returning everything a test needs. The change collector records the full
// transition history for ordered assertions.
type startedClient struct {
	fake    *clock.Fake
	mach    *Machine
	client  *Client
	changes *changeCollector
}

func buildClient(t *testing.T, cfg Config, dial DialFunc, seedA, seedB uint64) *startedClient {
	t.Helper()
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	cl, err := NewClient(cfg, mach, dial, seedA, seedB, fake, discardLogger())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	cc := newChangeCollector(mach.Subscribe())
	t.Cleanup(cl.Stop)
	return &startedClient{fake: fake, mach: mach, client: cl, changes: cc}
}

func startClient(t *testing.T, cfg Config, dial DialFunc, seedA, seedB uint64) *startedClient {
	t.Helper()
	sc := buildClient(t, cfg, dial, seedA, seedB)
	sc.client.Start()
	return sc
}

// waitState advances the clock until the machine sits in want, failing on
// budget exhaustion. Advancing is harmless when the state needs no time —
// the condition is checked before each step.
func (sc *startedClient) waitState(t *testing.T, want State) {
	t.Helper()
	drive(t, sc.fake, time.Millisecond, 10_000, func() bool {
		return sc.mach.State() == want
	})
}

// firstChange returns the first recorded change matching pred.
func (sc *startedClient) firstChange(pred func(Change) bool) (Change, bool) {
	for _, ch := range sc.changes.snapshot() {
		if pred(ch) {
			return ch, true
		}
	}
	return Change{}, false
}
func TestDialFailureRetriesOnTheClockWithGrowingJitteredDelays(t *testing.T) {
	// The heart of criterion 2's mechanics, pinned exactly: every retry is
	// armed on the injected clock for the FULL drawn delay, delays follow
	// the seeded full-jitter schedule attempt by attempt, and nothing fires
	// early. The driver advances exactly onto each replayed deadline (the
	// timer was armed at the failure instant, so fire time = failure instant
	// + drawn delay), which makes the assertions exact rather than ranged.
	const attempts = 4
	cfg := loopConfig()
	sc := startClient(t, cfg, func(context.Context) (Session, error) {
		return nil, errors.New("coordinator unreachable")
	}, 0xA11CE, 0xB0B)

	b, err := NewBackoff(0xA11CE, 0xB0B, cfg.BackoffBase, cfg.BackoffCap)
	if err != nil {
		t.Fatalf("replay backoff: %v", err)
	}
	delays := make([]time.Duration, attempts)
	for i := range attempts {
		delays[i] = b.Delay(i)
	}

	disconnectedCount := func() int {
		n := 0
		for _, ch := range sc.changes.snapshot() {
			if ch.To == StateDisconnected {
				n++
			}
		}
		return n
	}
	connectingCount := func() int {
		n := 0
		for _, ch := range sc.changes.snapshot() {
			if ch.To == StateConnecting {
				n++
			}
		}
		return n
	}
	connectingAt := func() []time.Time {
		var out []time.Time
		for _, ch := range sc.changes.snapshot() {
			if ch.To == StateConnecting {
				out = append(out, ch.At)
			}
		}
		return out
	}

	for i := range attempts {
		// Attempt i's dial fails with NO clock involvement and no watchdog
		// ever exists in this scenario, so between the failure verdict and
		// the supervisor arming its retry there are exactly ZERO outstanding
		// timers. Waiting for Waiters()==1 with the clock FROZEN therefore
		// observes the arm at the failure instant itself, and advancing
		// exactly delays[i] lands the fire stamp on the deadline exactly —
		// full drawn delay, no scheduler slop in either direction.
		runtime_GoschedUntil(t, func() bool { return disconnectedCount() == i+1 })
		runtime_GoschedUntil(t, func() bool { return sc.fake.Waiters() == 1 })
		events := sc.changes.snapshot()
		var failedAt time.Time
		n := 0
		for _, ch := range events {
			if ch.To == StateDisconnected {
				if !strings.Contains(ch.Reason, "coordinator unreachable") {
					t.Fatalf("reason = %q, want the dial error named", ch.Reason)
				}
				failedAt = ch.At
				n++
			}
		}
		if n != i+1 {
			t.Fatalf("attempt %d: %d failures recorded, want %d", i+1, n, i+1)
		}
		deadline := failedAt.Add(delays[i])
		sc.fake.Advance(delays[i])
		var got time.Time
		runtime_GoschedUntil(t, func() bool { return connectingCount() == i+2 })
		got = connectingAt()[i+1]
		if !got.Equal(deadline) {
			t.Fatalf("retry %d started at %v, want exactly %v (failure instant + full drawn delay %v)",
				i+1, got, deadline, delays[i])
		}
	}
}

// runtime_GoschedUntil yields until cond holds or the yield budget runs out,
// failing loudly. Used where a test must observe a goroutine-driven stamp
// without moving the clock.
func runtime_GoschedUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 100_000; i++ {
		if cond() {
			return
		}
		yieldTo()
	}
	t.Fatal("condition never became true while yielding")
}

const fineStep = 10 * time.Millisecond

func TestAttemptCounterResetsAfterSuccessfulOnline(t *testing.T) {
	// Two failed dials grow the envelope; reaching online resets it; the
	// first retry after the drop draws from the BASE envelope again. The
	// attempt INDEX each wait used is captured through the injectable
	// schedule -- the exact, timing-independent observable: expected sequence
	// [0, 1] across the two failures, then 0 again for the post-drop retry.
	// A counter that failed to reset would show 2 there.
	cfg := loopConfig()
	live := newStubSession()
	live.autoAck = true // answers the handshake, then never speaks again
	go func() { <-live.closed }()

	var mu sync.Mutex
	var dials int
	var attempts []int
	sc := buildClient(t, cfg, func(context.Context) (Session, error) {
		mu.Lock()
		defer mu.Unlock()
		dials++
		if dials <= 2 {
			return nil, errors.New("refused")
		}
		if dials == 3 {
			return live, nil // the session that will be killed
		}
		return newStubSession(), nil
	}, 0x51DE, 0xCA75)
	b, err := NewBackoff(0x51DE, 0xCA75, cfg.BackoffBase, cfg.BackoffCap)
	if err != nil {
		t.Fatalf("replay backoff: %v", err)
	}
	sc.client.schedule = func(attempt int) time.Duration {
		mu.Lock()
		attempts = append(attempts, attempt)
		mu.Unlock()
		return b.Delay(attempt) // the production full-jitter schedule itself
	}
	sc.client.Start()

	sc.waitState(t, StateOnline)

	_ = live.Close() // read error: immediate disconnected, then a retry

	runtime_GoschedUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 3
	})
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(attempts, []int{0, 1, 0}) {
		t.Fatalf("retry attempts drawn = %v, want [0 1 0] -- the third draw must be attempt 0 again (counter reset on online)", attempts)
	}
}

func TestHandshakeTimeoutIsAConfiguredFailureNotAWedge(t *testing.T) {
	// A coordinator that accepts the socket and never answers HelloAck must
	// cost one backoff cycle, not the client's future. The timeout comes
	// from Config; firing it lands handshaking -> disconnected with the
	// timeout named, and the loop retries.
	cfg := loopConfig()
	deaf := newStubSession()
	go func() { <-deaf.closed }()

	sc := startClient(t, cfg, func(context.Context) (Session, error) {
		return deaf, nil
	}, 1, 2)

	// Hello went out...
	drive(t, sc.fake, time.Millisecond, 1_000, func() bool {
		return len(deaf.sentHellos()) > 0
	})
	hello := deaf.sentHellos()[0]
	if hello.GetProtocolVersion() != walkiev1.MaxProtocolVersion {
		t.Fatalf("hello protocol_version = %d, want max %d", hello.GetProtocolVersion(), walkiev1.MaxProtocolVersion)
	}

	// ...and silence past the configured bound is refused.
	drive(t, sc.fake, 100*time.Millisecond, 200, func() bool {
		ch, ok := sc.firstChange(func(ch Change) bool {
			return ch.From == StateHandshaking && ch.To == StateDisconnected
		})
		return ok && strings.Contains(ch.Reason, "timed out")
	})
	// The loop retries against the same deaf peer, so any pre-online state
	// here proves only that it kept going; the recorded reason above is the
	// assertion.
}

func TestVersionRefusalFailsTheHandshakeDiagnosably(t *testing.T) {
	// Both refusal shapes: a structured ProtocolError from the version gate,
	// and a HelloAck echoing an unsupported protocol_version (the silent-skew
	// detector). Each must fail the handshake with the fact named — never
	// silently proceed onto misunderstood ground.
	cfg := loopConfig()

	refusals := []*walkiev1.Envelope{
		{Payload: &walkiev1.Envelope_ProtocolError{ProtocolError: walkiev1.NewVersionUnsupportedError(99)}},
		{Payload: &walkiev1.Envelope_HelloAck{HelloAck: &walkiev1.HelloAck{
			Device:          "dev",
			ProtocolVersion: walkiev1.MaxProtocolVersion + 1, // newer than we speak
		}}},
	}
	for i, refusal := range refusals {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			sess := newStubSession()
			go func() { <-sess.closed }()
			sess.deliver(refusal)

			sc := startClient(t, cfg, func(context.Context) (Session, error) {
				return sess, nil
			}, 3, 4)

			// The refusal needs NO clock to be processed — it is already in
			// flight — so wait by yielding only. Advancing first could fire
			// the handshake deadline before the supervisor read the reply,
			// and a timeout that beats an available answer by scheduler lag
			// is exactly the nondeterminism this suite refuses.
			var reason string
			runtime_GoschedUntil(t, func() bool {
				ch, ok := sc.firstChange(func(ch Change) bool {
					return ch.From == StateHandshaking && ch.To == StateDisconnected
				})
				if ok {
					reason = ch.Reason
				}
				return ok
			})
			if strings.Contains(reason, "timed out") {
				t.Fatalf("refusal reason = %q — the deadline beat an available reply", reason)
			}
			if !strings.Contains(reason, "protocol_version") && !strings.Contains(reason, "refused") {
				t.Fatalf("refusal reason = %q, want the version problem named", reason)
			}
		})
	}
}

func TestSilentPeerRunsTheFullDegradedArcThroughTheClient(t *testing.T) {
	// Criterion 3 through the assembled client, not just the bare watchdog:
	// session online, peer goes silent (socket open, no inbound), the
	// watchdog diagnoses degraded and tears down deliberately, the supervisor
	// closes the dead session, and the loop reconnects on its own once a
	// peer answers again. No user action anywhere.
	cfg := Config{
		BackoffBase:      time.Second,
		BackoffCap:       4 * time.Second,
		HeartbeatPeriod:  2 * time.Second,
		DeadPeerInterval: 6 * time.Second,
		HandshakeTimeout: 3 * time.Second,
	}

	live := newStubSession() // answers the handshake, then never speaks again
	live.autoAck = true      // the scenario: silence after the handshake
	revived := newStubSession()
	revived.autoAck = true  // the recovery: a fully healthy peer
	revived.autoPong = true // every beat answered keeps session two alive

	var mu sync.Mutex
	var current *stubSession
	sc := startClient(t, cfg, func(context.Context) (Session, error) {
		mu.Lock()
		defer mu.Unlock()
		if current == nil {
			current = live
			return live, nil
		}
		current = revived
		return revived, nil
	}, 5, 6)

	sc.waitState(t, StateOnline)
	var online Change
	runtime_GoschedUntil(t, func() bool {
		var ok bool
		online, ok = sc.firstChange(func(ch Change) bool { return ch.To == StateOnline })
		return ok
	})

	// Silence on `live`: no inbound frame ever arrives. The watchdog must
	// diagnose (degraded) and then tear down deliberately (disconnected),
	// in that order, without any user action.
	//
	// Driving discipline: the clock advances in coarse steps until BOTH
	// verdicts are stamped, then the assertions read order and causality
	// only. Stamp deltas are deliberately not asserted here — under
	// scheduler pressure a verdict's stamp can trail its (punctual) timer
	// fire, and re-measuring the interval that
	// TestSilentPeerDetectedWithinConfiguredInterval and
	// TestAnyInboundFrameCountsAsLife already pin exactly would buy
	// flakiness, not rigor.
	silentSettled := false
	for i := 0; i < 200 && !silentSettled; i++ {
		sc.fake.Advance(2 * time.Second)
		yieldTo()
		events := sc.changes.snapshot()
		var degraded, tornDown bool
		for _, ch := range events {
			if ch.To == StateDegraded {
				degraded = true
			}
			if ch.From == StateDegraded && ch.To == StateDisconnected {
				tornDown = true
			}
		}
		silentSettled = degraded && tornDown
	}
	if !silentSettled {
		t.Logf("DEBUG state=%s liveSent=%d changes=%+v", sc.mach.State(), len(live.sent), sc.changes.snapshot())
		t.Fatal("silence never produced a degraded diagnosis and deliberate teardown")
	}

	degraded, ok := sc.firstChange(func(ch Change) bool { return ch.To == StateDegraded })
	if !ok {
		t.Fatal("no degraded verdict recorded")
	}
	if degraded.At.Before(online.At) {
		t.Fatalf("degraded stamped %v, before the session it diagnosed (%v)", degraded.At, online.At)
	}

	// The dead session was closed (serve's actuator half): its Recv
	// unblocked, so the supervisor moved on. Recovery is automatic once a
	// peer answers again.
	sc.waitState(t, StateOnline)
	if got := sc.client.State(); got != StateOnline {
		t.Fatalf("final state = %s, want online after revival", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if current != revived {
		t.Fatal("recovery did not move to the answering session")
	}
}

func TestSendOfflineAndAcknowledgeSemantics(t *testing.T) {
	cfg := loopConfig()
	sess := newStubSession()
	sess.autoAck = true
	go func() { <-sess.closed }()

	// Built but NOT started: there is no session yet, so the offline
	// semantics are observable without racing a dial.
	sc := buildClient(t, cfg, func(context.Context) (Session, error) {
		return sess, nil
	}, 7, 8)

	if err := sc.client.Send(&walkiev1.Envelope{}); !errors.Is(err, ErrOffline) {
		t.Fatalf("offline send = %v, want ErrOffline", err)
	}
	sc.client.Acknowledge(7)
	sc.client.Acknowledge(5) // stale: monotonic high-water wins
	if got := sc.client.LastAcked(); got != 7 {
		t.Fatalf("last acked = %d, want 7", got)
	}

	sc.client.Start()
	sc.waitState(t, StateOnline)
	hellos := sess.sentHellos()
	if len(hellos) == 0 {
		t.Fatal("no Hello reached the wire")
	}
	if hellos[0].GetLastAckedPosition() != 7 {
		t.Fatalf("Hello.last_acked_position = %d, want 7 (recorded while offline)", hellos[0].GetLastAckedPosition())
	}

	// Online acknowledge sends the QueueAck frame.
	sc.client.Acknowledge(9)
	drive(t, sc.fake, time.Millisecond, 1_000, func() bool {
		return slicesContains(sess.sentQueueAcks(), 9)
	})
}

func slicesContains(xs []uint64, want uint64) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func TestInboundFramesFeedActivityAndTheHandler(t *testing.T) {
	// A chatty peer keeps the watchdog fed — no degraded verdict even deep
	// past the original deadline — and every non-handshake frame reaches
	// OnEnvelope in order. Then true silence produces the verdict, proving
	// the refreshes were load-bearing rather than decorative.
	cfg := Config{
		BackoffBase:      time.Second,
		BackoffCap:       time.Minute,
		HeartbeatPeriod:  5 * time.Second,
		DeadPeerInterval: 12 * time.Second,
		HandshakeTimeout: 3 * time.Second,
	}
	sess := newStubSession()
	sess.autoAck = true
	go func() { <-sess.closed }()

	var mu sync.Mutex
	var received []string
	sc := startClient(t, cfg, func(context.Context) (Session, error) {
		return sess, nil
	}, 9, 10)
	sc.client.OnEnvelope(func(env *walkiev1.Envelope) {
		mu.Lock()
		received = append(received, fmt.Sprintf("%T", env.GetPayload()))
		mu.Unlock()
	})

	sc.waitState(t, StateOnline)

	const bursts = 4
	for i := range bursts {
		sess.deliver(&walkiev1.Envelope{
			Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Sender: "peer", Body: fmt.Sprintf("m%d", i),
			}},
		})
	}
	drive(t, sc.fake, time.Millisecond, 1_000, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= bursts
	})
	mu.Lock()
	want := fmt.Sprintf("%T", &walkiev1.Envelope_DirectMessage{})
	for _, got := range received[:bursts] {
		if got != want {
			t.Fatalf("handler saw %q, want only %q", received, want)
		}
	}
	mu.Unlock()

	// One inbound frame per period, three times: each refreshes the silence
	// deadline. Each frame is delivered and then CONFIRMED PROCESSED (the
	// handler counts it) before the next stretch of fake time passes — with
	// the clock frozen while waiting, because processing needs turns, not
	// time. That makes "traffic kept flowing" true by observation rather
	// than by hope; the exact refresh arithmetic is pinned synchronously by
	// TestAnyInboundFrameCountsAsLife.
	for i := range 3 {
		sess.deliver(&walkiev1.Envelope{Payload: &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}}})
		want := bursts + i + 1
		runtime_GoschedUntil(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(received) >= want
		})
		sc.fake.Advance(cfg.HeartbeatPeriod)
		yieldTo()
	}
	if extra := elapsedVerdicts(sc); len(extra) > 0 {
		t.Fatalf("traffic-fed session produced verdicts: %+v", extra)
	}

	// Silence now: the last confirmed activity bounds the deadline at most
	// one DeadPeerInterval later. Advance generously and require the
	// diagnosis to appear — read via Machine.State() directly, because the
	// change-stream collector rides its own pump goroutine and must not be
	// load-bearing inside a hot driver loop.
	silentSettled := false
	for i := 0; i < 200 && !silentSettled; i++ {
		sc.fake.Advance(2 * time.Second)
		yieldTo()
		if st := sc.mach.State(); st == StateDegraded || st == StateDisconnected {
			silentSettled = true
		}
	}
	if !silentSettled {
		t.Fatal("silence after traffic never produced the dead-peer diagnosis")
	}
}

// elapsedVerdicts lists any degraded/disconnected transitions recorded so far.
func elapsedVerdicts(sc *startedClient) []Change {
	var out []Change
	for _, ch := range sc.changes.snapshot() {
		if ch.To == StateDegraded || ch.To == StateDisconnected {
			out = append(out, ch)
		}
	}
	return out
}

func TestStopJoinsAndLeavesAnHonestStream(t *testing.T) {
	cfg := loopConfig()
	sess := newStubSession()
	sess.autoAck = true
	go func() { <-sess.closed }()

	sc := startClient(t, cfg, func(context.Context) (Session, error) {
		return sess, nil
	}, 11, 12)
	sc.waitState(t, StateOnline)

	// Stop joins: when it returns, the supervisor has exited and the
	// session was closed under it.
	sc.client.Stop()

	select {
	case <-sess.closed:
	default:
		t.Fatal("session not closed by Stop")
	}

	// Double Stop is safe; Start after Stop panics loudly.
	sc.client.Stop()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Start after Stop did not panic; a restart would drive the machine from a second supervisor")
			}
		}()
		sc.client.Start()
	}()
}

func TestNewClientRefusesInvalidWiring(t *testing.T) {
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, nil)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	dial := func(context.Context) (Session, error) { return nil, errors.New("no") }
	goodCfg := DefaultConfig()

	if _, err := NewClient(goodCfg, mach, nil, 1, 2, fake, nil); err == nil {
		t.Fatal("nil dial accepted")
	}
	if _, err := NewClient(goodCfg, nil, dial, 1, 2, fake, nil); err == nil {
		t.Fatal("nil machine accepted")
	}
	if _, err := NewClient(goodCfg, mach, dial, 1, 2, nil, nil); err == nil {
		t.Fatal("nil clock accepted")
	}
	bad := goodCfg
	bad.HandshakeTimeout = 0
	if _, err := NewClient(bad, mach, dial, 1, 2, fake, nil); err == nil {
		t.Fatal("zero handshake timeout accepted")
	}
	if _, err := NewClient(DefaultConfig(), mach, dial, 1, 2, fake, nil); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
}

// discardLogger keeps client logs out of test output without going through
// slog's default stderr handler.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }
