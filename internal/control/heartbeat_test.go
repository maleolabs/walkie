package control

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// testConfig is the shrunken interval set every watchdog test runs on. The
// values are arbitrary — that is the point: the logic must hold at any
// configured scale, because no literal duration may exist in the code under
// test. Period divides the dead interval so fake-clock advances land exactly
// on deadlines and assertions can be exact rather than ranged.
func testConfig() Config {
	return Config{
		BackoffBase:      time.Second,
		BackoffCap:       time.Minute,
		HeartbeatPeriod:  5 * time.Second,
		DeadPeerInterval: 12 * time.Second,
	}
}

// countingSender records every heartbeat the watchdog writes and can fail on
// demand. Beats signal on a channel rather than only bumping a counter: the
// beat loop is a separate goroutine, and a counter read without
// synchronisation would test the scheduler, not the watchdog.
type countingSender struct {
	sent  atomic.Int64
	fail  atomic.Bool
	beats chan struct{}
}

func newCountingSender() *countingSender {
	return &countingSender{beats: make(chan struct{}, 64)}
}

func (s *countingSender) send(env *walkiev1.Envelope) error {
	if s.fail.Load() {
		return errors.New("link severed")
	}
	s.sent.Add(1)
	s.beats <- struct{}{}
	return nil
}

// waitBeat blocks until the next successful send happens — the receive IS
// the synchronisation; no test here sleeps in real time.
func waitBeat(t *testing.T, s *countingSender) {
	t.Helper()
	<-s.beats
}

// startOnline builds a machine already in StateOnline (the only state a
// watchdog may start in) with its change stream subscribed, plus the fake
// clock both run on.
func startOnline(t *testing.T) (*clock.Fake, *Machine, *Subscription) {
	t.Helper()
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateOnline)
	sub := m.Subscribe()
	return fake, m, sub
}

func TestSilentPeerDetectedWithinConfiguredInterval(t *testing.T) {
	// Criterion 3's defining scenario: the socket stays open, the peer
	// simply never answers. The watchdog must diagnose degraded and then
	// tear down deliberately, WITHIN the configured dead-peer interval —
	// detected here at exactly t0+DeadPeerInterval, one step of quantization
	// being the most the stepwise clock allows.
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}
	t.Cleanup(wd.Stop)

	// Two heartbeat periods elapse, one beat each. The loop synchronises
	// with every beat: the watchdog arms the NEXT slot before delivering the
	// current beat, so once a beat is observed the following advance is
	// guaranteed to fire — beat counts are exact, never scheduler luck.
	for i := 1; i <= 2; i++ {
		fake.Advance(cfg.HeartbeatPeriod)
		waitBeat(t, sender)
	}
	if got := m.State(); got != StateOnline {
		t.Fatalf("state = %s before the deadline; silence was not yet provable", got)
	}

	// Cross the deadline (two beats consumed 10s; one more step lands exactly
	// on t0+DeadPeerInterval): degraded (diagnosis) then disconnected
	// (deliberate teardown), in that order, stamped at the deadline itself.
	fake.Advance(cfg.DeadPeerInterval - 2*cfg.HeartbeatPeriod)

	degraded := <-sub.C()
	if degraded.From != StateOnline || degraded.To != StateDegraded {
		t.Fatalf("first verdict = %s->%s, want online->degraded", degraded.From, degraded.To)
	}
	if !degraded.At.Equal(epoch.Add(cfg.DeadPeerInterval)) {
		t.Fatalf("degraded stamped %v, want %v (deadline honoured)", degraded.At, epoch.Add(cfg.DeadPeerInterval))
	}
	tornDown := <-sub.C()
	if tornDown.From != StateDegraded || tornDown.To != StateDisconnected {
		t.Fatalf("second verdict = %s->%s, want degraded->disconnected", tornDown.From, tornDown.To)
	}
	if tornDown.Reason == "" {
		t.Fatal("teardown carried no reason")
	}

	// The watchdog retired itself: further silence produces nothing new.
	// beatDone is the retirement proof — after it closes, no send can be in
	// flight, so the counter read below is exact rather than racy.
	<-wd.beatDone
	before := sender.sent.Load()
	fake.Advance(10 * cfg.HeartbeatPeriod)
	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("events after terminal transition: %+v", extra)
	}
	if got := sender.sent.Load(); got != before {
		t.Fatalf("beats continued after retirement (%d -> %d)", before, got)
	}
}

func TestTransportErrorIsImmediateWithoutDegradedWaypoint(t *testing.T) {
	// Failure mode (b): the transport itself reports death. The machine must
	// go straight to disconnected — routing this through degraded would make
	// "we noticed silence" indistinguishable from "the wire said so".
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}
	t.Cleanup(wd.Stop)

	wd.SendFailed(errors.New("link severed"))

	got := <-sub.C()
	if got.From != StateOnline || got.To != StateDisconnected {
		t.Fatalf("verdict = %s->%s, want online->disconnected with NO degraded stop", got.From, got.To)
	}
	if !strings.Contains(got.Reason, "link severed") {
		t.Fatalf("reason = %q, want the transport error named", got.Reason)
	}

	// Immediate means immediate: no clock advance participated.
	if !got.At.Equal(epoch) {
		t.Fatalf("verdict stamped %v, want the un-advanced %v", got.At, epoch)
	}

	// Retired: advancing far produces nothing further.
	fake.Advance(10 * cfg.DeadPeerInterval)
	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("events after transport-error teardown: %+v", extra)
	}
}

func TestHeartbeatSendFailureIsModeBToo(t *testing.T) {
	// A periodic beat whose write fails is the same failure mode arriving by
	// the other path: straight to disconnected, never degraded.
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()
	sender.fail.Store(true)

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}
	t.Cleanup(wd.Stop)

	fake.Advance(cfg.HeartbeatPeriod) // first beat fires; its write fails

	got := <-sub.C()
	if got.From != StateOnline || got.To != StateDisconnected {
		t.Fatalf("verdict = %s->%s, want online->disconnected", got.From, got.To)
	}
	if sender.sent.Load() != 0 {
		t.Fatalf("failed sender recorded %d successful sends", sender.sent.Load())
	}
}

func TestActivityKeepsThePeerAlive(t *testing.T) {
	// A healthy session: every beat is answered, every answer refreshes the
	// silence deadline. Run for many periods — including well past the
	// original deadline — and nothing may fire.
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}
	t.Cleanup(wd.Stop)

	const beats = 8
	for i := 1; i <= beats; i++ {
		fake.Advance(cfg.HeartbeatPeriod)
		waitBeat(t, sender) // beat i has landed before we answer it
		wd.Activity()       // the pong (or any inbound frame) lands
		if got := m.State(); got != StateOnline {
			t.Fatalf("state = %s mid-session at beat %d", got, i)
		}
	}

	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("healthy session produced events: %+v", extra)
	}
}

func TestAnyInboundFrameCountsAsLife(t *testing.T) {
	// The deadline measures SILENCE, not missing pongs: an arbitrary inbound
	// frame at an arbitrary moment extends it. Here traffic arrives 1s before
	// the deadline would have fired; the peer then goes truly silent and
	// detection lands one full interval after THAT frame, not the original.
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}
	t.Cleanup(wd.Stop)

	fake.Advance(cfg.DeadPeerInterval - time.Second)
	wd.Activity() // late presence update, say

	fake.Advance(cfg.DeadPeerInterval - time.Second)
	if got := m.State(); got != StateOnline {
		t.Fatalf("state = %s while inbound traffic kept arriving within interval", got)
	}

	fake.Advance(time.Second) // now the silence crosses the refreshed deadline
	degraded := <-sub.C()
	// The refreshed deadline was armed at t=11s (the Activity call), so the
	// verdict lands at 11s + DeadPeerInterval = 23s — one full interval after
	// the LAST observed life, not after connect.
	wantAt := epoch.Add(cfg.DeadPeerInterval - time.Second + cfg.DeadPeerInterval)
	if degraded.To != StateDegraded || !degraded.At.Equal(wantAt) {
		t.Fatalf("degraded = %s at %v, want degraded at %v", degraded.To, degraded.At, wantAt)
	}
	<-sub.C() // deliberate teardown follows, as everywhere else
}

func TestWatchdogStopHaltsBeatsAndVerdicts(t *testing.T) {
	cfg := testConfig()
	fake, m, sub := startOnline(t)
	sender := newCountingSender()

	wd, err := StartWatchdog(cfg, m, sender.send, fake, nil)
	if err != nil {
		t.Fatalf("start watchdog: %v", err)
	}

	fake.Advance(cfg.HeartbeatPeriod)
	waitBeat(t, sender)

	wd.Stop()
	wd.Stop()     // idempotent
	<-wd.beatDone // the loop has fully exited: no send can be in flight

	fake.Advance(10 * cfg.DeadPeerInterval)
	if extra := collectNonBlocking(sub); len(extra) != 0 {
		t.Fatalf("events after Stop: %+v", extra)
	}
	if got := sender.sent.Load(); got != 1 {
		t.Fatalf("beats continued after Stop (want 1, got %d)", got)
	}
	if got := m.State(); got != StateOnline {
		t.Fatalf("Stop changed state to %s; stopping is not a verdict", got)
	}
}

func TestStartWatchdogRefusesInvalidWiring(t *testing.T) {
	fake := clock.NewFake(epoch)
	m := pathTo(t, fake, StateOnline)
	sender := newCountingSender()

	badCfg := testConfig()
	badCfg.DeadPeerInterval = badCfg.HeartbeatPeriod // deadline races the first pong
	if _, err := StartWatchdog(badCfg, m, sender.send, fake, nil); err == nil {
		t.Fatal("dead-peer interval equal to heartbeat period accepted")
	}

	if _, err := StartWatchdog(testConfig(), nil, sender.send, fake, nil); err == nil {
		t.Fatal("nil machine accepted")
	}
	if _, err := StartWatchdog(testConfig(), m, nil, fake, nil); err == nil {
		t.Fatal("nil sender accepted")
	}
	if _, err := StartWatchdog(testConfig(), m, sender.send, nil, nil); err == nil {
		t.Fatal("nil clock accepted")
	}
}

func TestConfigValidateRejectsNonsense(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("DefaultConfig rejected by its own validator: %v", err)
	}

	cfg.DeadPeerInterval = cfg.HeartbeatPeriod
	if err := cfg.validate(); err == nil {
		t.Fatal("dead-peer interval <= heartbeat period accepted")
	}
}
