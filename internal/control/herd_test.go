package control

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/testnet"
)

// Criterion 2, the thundering-herd test: twenty simulated clients
// reconnecting after a coordinator restart must not cluster into a single
// spike. This is the deployment-upper-bound scenario ts:test-harness built
// the twenty-client Fleet for, now driven by the REAL reconnect supervisor —
// real Machines, real Backoff schedules, real Watchdogs, real read loops —
// over Fleet links on one shared *clock.Fake.
//
// # What is asserted, and why these quantities
//
//   - Every client reconnects (count == herd size): criterion 1 at fleet
//     scale, with no user action anywhere in the scenario.
//   - Spread: no quarter-second window absorbs more than half the fleet.
//     With full jitter the first post-drop retries draw uniformly from
//     [0, BackoffBase) — thirty seconds here — so any single quarter-second
//     window should hold ~0.17 clients in expectation; ten is not a close
//     call, it is half the fleet piling up.
//   - The zero-jitter control run below proves the metric has teeth: with a
//     constant retry schedule every client lands in the same instant and the
//     same metric reports the full herd.
//
// # Why the timeline is measured from change STAMPS yet tolerates slop
//
// Reconnect times are the Change.At stamps of the second online transition —
// processing times, not scheduled times (the distinction internal/testnet's
// Fleet.Log draws). Processing times quantize to the driver's step grid and
// drift with scheduler pressure, so exact replay equality is NOT asserted;
// what makes the spread assertion sound anyway is that the DRAWS are pure
// functions of (seed, client index) — seeded PCG streams, pinned by
// TestBackoffScheduleIsAFunctionOfItsSeed — and the pass/fail margin (ten
// versus an expected fraction of one) dwarfs any step-quantization slop.
// The control run's full-herd reading is likewise far outside ambiguity.
type herdRig struct {
	fake    *clock.Fake
	fleet   *testnet.Fleet
	coord   *fakeCoordinator
	machs   []*Machine
	clients []*Client
	colls   []*changeCollector
}

// herdConfig: BackoffBase thirty seconds gives full jitter room to spread
// twenty clients across; heartbeat keeps sessions leased and the dead-peer
// interval is set far beyond the whole scenario so that the ONLY disconnect
// event is the scripted coordinator crash — a mid-phase dead-peer teardown
// would add unscheduled extra cycles and muddy the timeline. Criterion 3's
// detection behaviour is exercised separately in TestSilentPeer.
func herdConfig() Config {
	return Config{
		BackoffBase:      30 * time.Second,
		BackoffCap:       60 * time.Second,
		HeartbeatPeriod:  5 * time.Second,
		DeadPeerInterval: 10 * time.Minute,
		HandshakeTimeout: 10 * time.Second,
	}
}

// runHerd drives the whole scenario once and returns the sorted second-phase
// online stamps. scheduleOverride == nil means the production seeded
// full-jitter backoff; the control run substitutes a constant.
type herdResult struct {
	// t0 is the fake instant at which every client's post-crash retry timer
	// was armed (the arm barrier freezes the clock, so all twenty arms share
	// one instant).
	t0 time.Time
	// draws[i] is the delay client i's schedule produced for the post-crash
	// retry — the armed wait itself, captured at the schedule seam.
	draws []time.Duration
	// dials[i] is when client i's machine re-entered connecting after the
	// crash — the moment its armed retry fired and the supervisor dialed.
	dials []time.Time
	// stamps[i] is when client i's machine actually reached online again.
	stamps []time.Time
}

func runHerd(t *testing.T, seed uint64, scheduleOverride RetrySchedule) herdResult {
	t.Helper()

	cfg := herdConfig()
	fake := clock.NewFake(epoch)
	fleet := testnet.NewFleet(fake, seed, herdSize)
	t.Cleanup(fleet.Shutdown)

	coord := newFakeCoordinator("fleet", echoBehavior("fleet"))

	rig := &herdRig{
		fake:    fake,
		fleet:   fleet,
		coord:   coord,
		machs:   make([]*Machine, herdSize),
		clients: make([]*Client, herdSize),
		colls:   make([]*changeCollector, herdSize),
	}
	draws := make([]time.Duration, herdSize)

	for i := range herdSize {
		i := i

		mach, err := NewMachine(fake, discardLogger())
		if err != nil {
			t.Fatalf("client %d: new machine: %v", i, err)
		}
		rig.machs[i] = mach

		// First dial rides the Fleet's own connection pair; every later dial
		// builds a fresh link pair, exactly as Fleet's type comment directs
		// consumers that need re-established sockets ("builds them from
		// NewLink directly").
		firstDial := true
		dial := DialFunc(func(context.Context) (Session, error) {
			var clientEnd net.Conn
			if firstDial {
				clientEnd = rig.fleet.ClientConn(i)
				if err := coord.dialInto(rig.fleet.ServerConn(i)); err != nil {
					return nil, err
				}
				firstDial = false
				return newFramedSession(clientEnd), nil
			}
			link := testnet.NewLink(fake, testnet.Conditions{})
			clientEnd, serverEnd := link.Pipe()
			if err := coord.dialInto(serverEnd); err != nil {
				return nil, err
			}
			return newFramedSession(clientEnd), nil
		})

		cl, err := NewClient(cfg, mach, dial, seed, uint64(i)+1, fake, discardLogger())
		if err != nil {
			t.Fatalf("client %d: new client: %v", i, err)
		}
		if scheduleOverride != nil {
			cl.schedule = scheduleOverride
		}
		// Record the LAST draw this client's schedule produced — for a
		// healthy scenario that is the post-crash retry wait.
		base := cl.schedule
		cl.schedule = func(attempt int) time.Duration {
			d := base(attempt)
			draws[i] = d
			return d
		}
		rig.clients[i] = cl
		rig.colls[i] = newChangeCollector(mach.Subscribe())
	}

	for _, cl := range rig.clients {
		cl.Start()
	}

	// Phase 1: initial connects settle. driveQuiesced advances stepwise and
	// after every step waits until the change stream goes quiet, so each
	// client's transition stamps at a deterministic grid point instead of
	// trailing its (punctual) timer fire by scheduler-dependent amounts.
	driveQuiesced(t, fake, rig, 500*time.Millisecond, 400, func() bool {
		for _, m := range rig.machs {
			if m.State() != StateOnline {
				return false
			}
		}
		return true
	})

	// THE COORDINATOR CRASH: every live connection dies at once, no close
	// frames — the RST analogue Fleet.DropAll models and criterion 1 names.
	coord.killConns()
	// The EOF reaches each client's read loop via goroutine scheduling, not
	// via the clock — so WAIT (yielding, clock frozen) until every machine
	// has actually registered the death before advancing. Skipping this wait
	// would let phase 2's first condition check pass while every client is
	// still online from phase 1, silently skipping the entire reconnect
	// measurement.
	okDead := false
	for i := 0; i < 200_000; i++ {
		all := true
		for _, m := range rig.machs {
			if m.State() != StateDisconnected {
				all = false
				break
			}
		}
		if all {
			okDead = true
			break
		}
		yieldTo()
	}
	if !okDead {
		for j := range herdSize {
			t.Logf("DEBUG dead client %d state=%s events=%d last=%+v", j,
				rig.machs[j].State(), len(rig.colls[j].snapshot()), tail(rig.colls[j].snapshot()))
		}
		t.Fatal("not every client registered the coordinator crash")
	}
	// Second barrier: every supervisor has ARMED its retry timer. With the
	// clock frozen there is nothing else outstanding (the killed sessions'
	// watchdogs retired with them), so Waiters()==herdSize means all twenty
	// arms happened at ONE AND THE SAME fake instant — which pins every
	// client's fire to teardown-instant + drawn delay exactly, and makes the
	// reconnect timeline a pure function of the seeded draws.
	okArmed := false
	for i := 0; i < 200_000; i++ {
		if fake.Waiters() == herdSize {
			okArmed = true
			break
		}
		yieldTo()
	}
	if !okArmed {
		for j := range herdSize {
			t.Logf("DEBUG arm client %d state=%s events=%d", j, rig.machs[j].State(), len(rig.colls[j].snapshot()))
		}
		t.Fatalf("retry timers never all armed (Waiters=%d, want %d)", fake.Waiters(), herdSize)
	}
	// The shared arm instant: with the clock frozen and every retry timer
	// outstanding, each fires at exactly t0 + its drawn delay.
	t0 := fake.Now()

	// Phase 2: every client must come back on its own — the supervisor's
	// retry timers are armed on this clock, so advancing IS the passage of
	// time and nobody intervenes.
	driveQuiesced(t, fake, rig, 500*time.Millisecond, 400, func() bool {
		for _, m := range rig.machs {
			if m.State() != StateOnline {
				return false
			}
		}
		return true
	})

	// The collectors ride their own pump goroutines and may trail the
	// machines; with the clock now FROZEN, wait until every client shows TWO
	// online transitions (initial + post-restart) before reading timelines.
	// Counting rather than comparing against restartAt matters: a client
	// whose draw was ~0 reconnects in the same fake instant as the crash,
	// and a strict "after restartAt" filter would discard its honest stamp.
	okAll := false
	for i := 0; i < 200_000; i++ {
		all := true
		for j := range herdSize {
			if onlineCount(rig.colls[j].snapshot()) < 2 {
				all = false
				break
			}
		}
		if all {
			okAll = true
			break
		}
		yieldTo()
	}
	if !okAll {
		for j := range herdSize {
			evs := rig.colls[j].snapshot()
			t.Logf("DEBUG client %d state=%s events=%d last=%+v", j,
				rig.machs[j].State(), len(evs), tail(evs))
		}
		t.Fatal("collectors never caught up with the machines")
	}

	dials := make([]time.Time, 0, herdSize)
	stamps := make([]time.Time, 0, herdSize)
	for i := range herdSize {
		var lastOnline time.Time
		var lastDial time.Time
		online := 0
		for _, ch := range rig.colls[i].snapshot() {
			switch ch.To {
			case StateConnecting:
				lastDial = ch.At
			case StateOnline:
				online++
				lastOnline = ch.At
			}
		}
		if online < 2 || lastDial.IsZero() {
			t.Fatalf("client %d: %d online cycles, want at least 2 (initial + post-restart)", i, online)
		}
		dials = append(dials, lastDial)
		stamps = append(stamps, lastOnline)
	}
	return herdResult{t0: t0, draws: draws, dials: dials, stamps: stamps}
}

func onlineCount(events []Change) int {
	n := 0
	for _, ch := range events {
		if ch.To == StateOnline {
			n++
		}
	}
	return n
}

// runtime_GoschedUntilAll yields (clock untouched) until cond holds.
func runtime_GoschedUntilAll(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200_000; i++ {
		if cond() {
			return
		}
		yieldTo()
	}
	t.Fatal("condition never became true while yielding")
}

// driveQuiesced is drive() plus a per-step quiescence barrier: after each
// Advance it yields until the aggregate change-stream count has been stable
// for two consecutive checks. Every machine transition emits exactly one
// change, so a quiet stream means every goroutine woken by this step has
// finished its work — and the NEXT step's fires stamp at clean grid points
// instead of trailing their deadlines by scheduler-dependent amounts. That
// quantization is what makes the herd metric sound: reconnect stamps sit on
// the step grid, adjacent grid points are one step apart, and the spread
// window reads true clustering rather than scheduling noise.
func driveQuiesced(t *testing.T, fake *clock.Fake, rig *herdRig, step time.Duration, maxSteps int, cond func() bool) {
	t.Helper()
	total := func() int {
		n := 0
		for _, c := range rig.colls {
			n += len(c.snapshot())
		}
		return n
	}
	for i := 0; i < maxSteps; i++ {
		if cond() {
			return
		}
		fake.Advance(step)
		stable := 0
		prev := total()
		for stable < 2 {
			yieldTo()
			cur := total()
			if cur == prev {
				stable++
			} else {
				stable = 0
				prev = cur
			}
		}
	}
	t.Fatalf("scenario did not settle within %d steps of %v (clock at %v)", maxSteps, step, fake.Now())
}

func tail(events []Change) []Change {
	if len(events) > 3 {
		return events[len(events)-3:]
	}
	return events
}

const (
	// herdSize is the real deployment's upper bound, matching
	// ts:test-harness criterion 5's fleet scale.
	herdSize = 20

	// herdSeed fixes every client's PCG stream (seed, index+1), making the
	// draw vector — and therefore the pass/fail outcome — reproducible.
	herdSeed = 42

	// spreadWindow and spreadLimit are the aggregate anti-clustering
	// assertion: no quarter-second window may absorb half the fleet. The
	// zero-jitter control below proves the metric reports the FULL herd when
	// clustering actually happens, so the limit has teeth.
	spreadWindow = 250 * time.Millisecond
	spreadLimit  = herdSize / 2

	// herdStep is the driver's advance granularity; stamps quantize to its
	// grid, so assertions allow one step of slop.
	herdStep = 500 * time.Millisecond

	// herdLagTolerance bounds how far an online STAMP may trail its own
	// punctual timer fire: a handshake completes within a few driver steps
	// even under heavy scheduling pressure, whereas a herd bug shifts
	// reconnects by whole envelope widths. Four seconds is orders of
	// magnitude below any wrong-schedule signature and far above observed
	// quantization.
	herdLagTolerance = 4 * time.Second
)

func TestTwentyClientsReconnectAfterRestartWithoutClustering(t *testing.T) {
	res := runHerd(t, herdSeed, nil)

	if len(res.stamps) != herdSize {
		t.Fatalf("%d clients reconnected, want %d", len(res.stamps), herdSize)
	}

	// Two timelines, asserted per client:
	//
	//   OBSERVED — every client's online stamp must sit within
	//   herdLagTolerance of its own armed fire (t0 + drawn delay). This ties the
	//   measurement to the real system: the supervisor armed the seeded
	//   schedule and the fleet came back on it. The slack absorbs stamp
	//   quantization to the step grid plus bounded scheduling lag; it cannot
	//   hide a herd, because a herd shifts stamps by whole envelope widths.
	//
	//   COMPUTED — the armed timeline itself (t0 + draws) is what the
	//   anti-clustering metric reads. Draws are pure functions of the seeds
	//   (pinned by TestBackoffScheduleIsAFunctionOfItsSeed), so this
	//   assertion is exact: full jitter spreads twenty first-retries across
	//   [0, 30s), and no quarter-second window may hold half the fleet.
	computed := make([]time.Time, 0, herdSize)
	for i := range herdSize {
		fire := res.t0.Add(res.draws[i])
		// Lower bound is exact and per-client: no supervisor may dial before
		// its own armed fire (plus one step of grid slop). This is what
		// rules out fixed-short or no-backoff schedules.
		if res.dials[i].Before(fire.Add(-herdStep)) {
			t.Fatalf("client %d redialed at %v, before its armed fire at %v (draw %v)",
				i, res.dials[i], fire, res.draws[i])
		}
		// Upper bound is per-client too: the stamp may trail its own armed
		// fire by herdLagTolerance, no more.
		if res.stamps[i].After(fire.Add(herdLagTolerance)) {
			t.Fatalf("client %d reached online at %v, more than %v after its armed fire at %v (draw %v)",
				i, res.stamps[i], herdLagTolerance, fire, res.draws[i])
		}
		computed = append(computed, fire)
	}
	slices.SortFunc(computed, func(a, b time.Time) int { return a.Compare(b) })

	if n := maxClientsWithin(computed, spreadWindow); n > spreadLimit {
		t.Errorf("armed retry timeline holds %d clients inside one %v window, want at most %d — that is a thundering herd",
			n, spreadWindow, spreadLimit)
	}

	// Global completion bound: even under heavy scheduler pressure every
	// client must be back shortly after the longest armed delay — criterion
	// 1 is prompt recovery, not eventual recovery.
	maxDraw := res.draws[0]
	for _, d := range res.draws {
		if d > maxDraw {
			maxDraw = d
		}
	}
	last := res.stamps[len(res.stamps)-1]
	if late := last.Sub(res.t0.Add(maxDraw)); late > 20*time.Second {
		t.Errorf("last client reached online %v after the longest armed fire — recovery is not prompt", late)
	}
}

// TestHerdControlZeroJitterClustersAndMetricSeesIt is the control experiment
// without which the spread limit could be satisfied by a broken metric: with
// the jitter switched OFF, all twenty clients arm the SAME retry instant,
// the metric reports the total herd on the armed timeline — and the observed
// reconnects still track that clustered schedule per client.
func TestHerdControlZeroJitterClustersAndMetricSeesIt(t *testing.T) {
	res := runHerd(t, herdSeed, func(int) time.Duration { return time.Second })

	if len(res.stamps) != herdSize {
		t.Fatalf("%d clients reconnected, want %d", len(res.stamps), herdSize)
	}

	computed := make([]time.Time, 0, herdSize)
	for i := range herdSize {
		fire := res.t0.Add(res.draws[i])
		if res.dials[i].Before(fire.Add(-herdStep)) {
			t.Fatalf("control client %d redialed at %v, before its armed fire at %v",
				i, res.dials[i], fire)
		}
		computed = append(computed, fire)
	}
	slices.SortFunc(computed, func(a, b time.Time) int { return a.Compare(b) })

	if n := maxClientsWithin(computed, spreadWindow); n < herdSize {
		t.Errorf("zero-jitter control reported only %d clients inside one %v window, want %d — the metric failed to see a total herd",
			n, spreadWindow, herdSize)
	}
}

// A different seed must produce a different draw vector — the schedules are
// really per-client and per-seed, not a constant in disguise. Asserted on
// the drawn delays themselves (pure functions of the seeds), not on
// processing-time stamps.
func TestHerdSchedulesAreSeedSensitive(t *testing.T) {
	drawVector := func(seed uint64) []time.Duration {
		out := make([]time.Duration, herdSize)
		for i := range herdSize {
			b, err := NewBackoff(seed, uint64(i)+1, herdConfig().BackoffBase, herdConfig().BackoffCap)
			if err != nil {
				t.Fatalf("backoff: %v", err)
			}
			out[i] = b.Delay(0)
		}
		return out
	}
	if slices.Equal(drawVector(herdSeed), drawVector(herdSeed+1)) {
		t.Error("different seeds produced identical first-attempt delay vectors")
	}
}

// herdEchoSanity guards the scripted coordinator against silent drift: the
// echo behavior must answer Hello with an honest HelloAck carrying the
// schema's max version, or every herd client would fail its handshake for a
// reason that has nothing to do with the scenario.
func TestHerdScriptAnswersHelloWithMaxVersionAck(t *testing.T) {
	reply := echoBehavior("dev")(helloEnvelopeForHerd())[0]
	ack := reply.GetHelloAck()
	if ack == nil {
		t.Fatalf("script answered %T, want HelloAck", reply.GetPayload())
	}
	if ack.GetProtocolVersion() != walkiev1.MaxProtocolVersion {
		t.Fatalf("ack protocol_version = %d, want %d", ack.GetProtocolVersion(), walkiev1.MaxProtocolVersion)
	}
}

func helloEnvelopeForHerd() *walkiev1.Envelope {
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Hello{Hello: &walkiev1.Hello{
			ClientVersion:   clientVersion,
			ProtocolVersion: walkiev1.MaxProtocolVersion,
		}},
	}
}
