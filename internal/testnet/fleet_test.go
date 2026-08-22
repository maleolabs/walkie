package testnet

import (
	"errors"
	"io"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// The fleet tests pin ts:test-harness criterion 5: a twenty-client fleet in one
// process, on one injected clock, assertable on aggregate behaviour — built for
// ts:reconnect-resume's thundering-herd call site ("twenty simulated clients
// reconnecting after a coordinator restart do not cluster into one spike").
//
// What is asserted here is the substrate: twenty independent endpoints whose
// schedules are pure functions of the seed, one shared fake clock driven
// without a single real-time sleep, an aggregate timeline, and a driver whose
// exhaustion fails loudly instead of hanging or passing silently. The
// production reconnect policy and its definitive herd assertion belong to
// ts:reconnect-resume, which inherits this harness.

const (
	// herdSize is the real deployment's upper bound, not a round number.
	herdSize = 20

	// herdSeed is the scenario seed. The spread limit below is checked against
	// the timeline this seed produces; because that timeline is a pure
	// function of the seed, the check holds forever once it holds here.
	herdSeed = 42

	// backoffBase and backoffCap bound the simulated reconnect delay; attempt
	// five of a full-jitter exponential from base reaches the cap, so each
	// client draws uniformly from [0, cap) — the regime in which jitter does
	// its herd-breaking work.
	backoffBase = time.Second
	backoffCap  = 30 * time.Second

	// restartDelay is how long after the initial connects the coordinator
	// restart happens, so the two phases cannot bleed into each other.
	restartDelay = 5 * time.Minute

	// driverStep and driverBudget bound AdvanceUntilQuiescent: 2000 half-second
	// steps cover the 5m30s the scenario needs several times over. Exhaustion
	// would mean the scenario never settles — a bug to surface loudly, not a
	// budget to widen blindly.
	driverStep   = 500 * time.Millisecond
	driverBudget = 2000

	// spreadWindow and spreadLimit are the aggregate anti-clustering assertion:
	// with full jitter over thirty seconds, no quarter-second window may
	// contain more than five reconnects. The zero-jitter control below proves
	// the same metric reports twenty when clustering actually happens, so the
	// limit has teeth rather than being a number that passes by construction.
	spreadWindow = 250 * time.Millisecond
	spreadLimit  = 5
)

// runHerd drives the full scenario once: connects settle, the coordinator
// restarts (DropAll), every client reconnects after its own seeded backoff,
// and the sorted reconnect timeline comes back for aggregate assertions.
//
// The timeline is deterministic by construction: every logged At is a
// scheduled time, every schedule anchors on the previous event's scheduled
// time, and every jitter draw comes from the client's own PCG(seed, i) stream.
// Goroutine interleaving can change when handlers run, never what they log.
func runHerd(t *testing.T, seed uint64) []time.Time {
	t.Helper()

	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, seed, herdSize)
	t.Cleanup(fleet.Shutdown)

	startEchoServers(fleet)

	for i := range herdSize {
		fleet.Schedule(i, epoch, "connect")
	}
	fleet.Start(func(ev FleetEvent) {
		switch ev.Kind {
		case "connect":
			fleet.Log(ev)
		case "restart":
			fleet.Log(ev)
			d := jitteredBackoff(fleet.Rand(ev.Client), backoffBase, backoffCap, 5)
			// Anchor on ev.At, not clock.Now(): processing time drifts with
			// driver step alignment, scheduled time does not. Anchoring here
			// is what makes the whole timeline reproducible.
			fleet.Schedule(ev.Client, ev.At.Add(d), "reconnect")
		case "reconnect":
			fleet.Log(ev)
		}
	})

	fleet.AdvanceUntilQuiescent(t, driverStep, driverBudget)
	assertAllConnected(t, fleet)

	// Coordinator restart: established connections die exactly as they do in
	// production, and each client reacts on its own seeded schedule.
	fleet.DropAll()
	for i := range herdSize {
		fleet.Schedule(i, epoch.Add(restartDelay), "restart")
	}
	fleet.AdvanceUntilQuiescent(t, driverStep, driverBudget)

	var reconnects []time.Time
	for _, ev := range fleet.Events() {
		if ev.Kind == "reconnect" {
			reconnects = append(reconnects, ev.At)
		}
	}
	if len(reconnects) != herdSize {
		t.Fatalf("got %d reconnect events, want %d", len(reconnects), herdSize)
	}
	slices.SortFunc(reconnects, func(a, b time.Time) int { return a.Compare(b) })
	return reconnects
}

// startEchoServers stands up one server-side echo loop per connection. They
// prove the twenty links carry live traffic without touching the clock: reads
// and writes block on net.Pipe semantics only, so they add no scheduling
// dependence to the timing assertions.
func startEchoServers(fleet *Fleet) {
	for i := range fleet.Size() {
		conn := fleet.ServerConn(i)
		go func() {
			buf := make([]byte, 1)
			for {
				if _, err := conn.Read(buf); err != nil {
					return
				}
				if _, err := conn.Write(buf[:1]); err != nil {
					return
				}
			}
		}()
	}
}

// assertAllConnected round-trips one byte through every client connection,
// sequentially, so twenty live pipes are proven rather than assumed.
func assertAllConnected(t *testing.T, fleet *Fleet) {
	t.Helper()
	for i := range fleet.Size() {
		if _, err := fleet.ClientConn(i).Write([]byte{byte(i)}); err != nil {
			t.Fatalf("client %d write: %v", i, err)
		}
		got := make([]byte, 1)
		if _, err := io.ReadFull(fleet.ClientConn(i), got); err != nil {
			t.Fatalf("client %d read-back: %v", i, err)
		}
		if got[0] != byte(i) {
			t.Fatalf("client %d round-trip returned %d", i, got[0])
		}
	}
}

// maxClientsWithin reports the largest number of timestamps falling inside any
// single window — the "one spike" metric the herd call site asserts against.
func maxClientsWithin(sorted []time.Time, window time.Duration) int {
	best := 0
	for j := range sorted {
		n := 0
		for k := range sorted {
			if !sorted[k].Before(sorted[j]) && sorted[k].Before(sorted[j].Add(window)) {
				n++
			}
		}
		best = max(best, n)
	}
	return best
}

func TestTwentyClientFleetReconnectsSpreadNotClustered(t *testing.T) {
	timeline := runHerd(t, herdSeed)

	// Every client made it back inside its backoff envelope.
	for i, r := range timeline {
		if r.Before(epoch.Add(restartDelay)) || !r.Before(epoch.Add(restartDelay+backoffCap)) {
			t.Fatalf("reconnect %d at %v, want within [%v, %v)",
				i, r, epoch.Add(restartDelay), epoch.Add(restartDelay+backoffCap))
		}
	}

	if n := maxClientsWithin(timeline, spreadWindow); n > spreadLimit {
		t.Errorf("%d clients reconnected inside one %v window, want at most %d — that is a thundering herd",
			n, spreadWindow, spreadLimit)
	}

	// Same seed, same execution: the whole timeline must replay identically.
	if replay := runHerd(t, herdSeed); !slices.Equal(timeline, replay) {
		t.Errorf("same seed produced different timelines:\nfirst  %v\nsecond %v", timeline, replay)
	}

	// A different seed produces a different schedule — the source really is
	// per-client and per-seed, not a constant in disguise.
	if other := runHerd(t, herdSeed+1); slices.Equal(timeline, other) {
		t.Error("different seed produced identical timelines")
	}
}

// The control experiment for the spread assertion: with jitter removed, all
// twenty clients reconnect at the same instant, and the metric must say so.
// Without this half, spreadLimit could be satisfied by a broken metric.
func TestFleetHerdControlDetectsClustering(t *testing.T) {
	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, herdSeed, herdSize)
	t.Cleanup(fleet.Shutdown)

	for i := range herdSize {
		fleet.Schedule(i, epoch, "connect")
	}
	fleet.Start(func(ev FleetEvent) {
		switch ev.Kind {
		case "restart":
			// No draw at all: everyone retries exactly one second later.
			fleet.Schedule(ev.Client, ev.At.Add(backoffBase), "reconnect")
		default:
			fleet.Log(ev)
		}
	})

	fleet.AdvanceUntilQuiescent(t, driverStep, driverBudget)
	fleet.DropAll()
	for i := range herdSize {
		fleet.Schedule(i, epoch.Add(restartDelay), "restart")
	}
	fleet.AdvanceUntilQuiescent(t, driverStep, driverBudget)

	var reconnects []time.Time
	for _, ev := range fleet.Events() {
		if ev.Kind == "reconnect" {
			reconnects = append(reconnects, ev.At)
		}
	}
	if len(reconnects) != herdSize {
		t.Fatalf("got %d reconnect events, want %d", len(reconnects), herdSize)
	}
	sortTimes(reconnects)
	if n := maxClientsWithin(reconnects, spreadWindow); n < herdSize {
		t.Errorf("zero-jitter control reported %d clients in one %v window, want %d — the metric failed to see a total herd",
			n, spreadWindow, herdSize)
	}
}

// A handler that reschedules itself forever never settles. The bounded driver
// must give up with an error naming the budget — that is the loud failure the
// brief demands in place of both a deadlock and a silent pass.
func TestAdvanceUntilQuiescentFailsLoudlyOnUnsettledScenario(t *testing.T) {
	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, 7, 1)
	t.Cleanup(fleet.Shutdown)

	fleet.Schedule(0, epoch, "tick")
	fleet.Start(func(ev FleetEvent) {
		fleet.Log(ev)
		fleet.Schedule(ev.Client, ev.At.Add(time.Second), "tick")
	})

	const steps = 50
	if err := fleet.advanceUntilQuiescent(time.Second, steps); err == nil {
		t.Fatal("perpetual scenario settled; the bounded driver must fail loudly")
	}
	// The budget bounds clock advances, and every advance here moves exactly
	// one second, so exhaustion must land the clock on the budget precisely.
	if got, want := fake.Now(), epoch.Add(steps*time.Second); !got.Equal(want) {
		t.Errorf("clock at %v, want %v — exhaustion must stop at the advance budget", got, want)
	}
}

// The success path of the same driver: a scenario that settles returns nil and
// leaves exactly the events that were due processed.
func TestAdvanceUntilQuiescentReturnsWhenScenarioSettles(t *testing.T) {
	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, 7, 1)
	t.Cleanup(fleet.Shutdown)

	const ticks = 5
	fleet.Schedule(0, epoch, "tick")
	fleet.Start(func(ev FleetEvent) {
		fleet.Log(ev)
		if len(fleet.Events()) < ticks {
			fleet.Schedule(ev.Client, ev.At.Add(time.Second), "tick")
		}
	})

	if err := fleet.advanceUntilQuiescent(time.Second, 100); err != nil {
		t.Fatalf("settling scenario errored: %v", err)
	}
	if n := len(fleet.Events()); n != ticks {
		t.Errorf("logged %d ticks, want %d", n, ticks)
	}
}

// Scheduling while an event is already outstanding queues behind it, and an
// event whose deadline passed while its client was parked still logs its own
// scheduled time — the oversleep behaviour documented on Schedule.
func TestScheduleQueuesBehindTheOutstandingEvent(t *testing.T) {
	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, 7, 1)
	t.Cleanup(fleet.Shutdown)

	fleet.Schedule(0, epoch.Add(10*time.Second), "late-first")
	fleet.Schedule(0, epoch.Add(5*time.Second), "queued-second")
	fleet.Start(func(ev FleetEvent) { fleet.Log(ev) })

	if err := fleet.advanceUntilQuiescent(time.Second, 100); err != nil {
		t.Fatalf("scenario errored: %v", err)
	}
	events := fleet.Events()
	if len(events) != 2 ||
		events[0].Kind != "late-first" || !events[0].At.Equal(epoch.Add(10*time.Second)) ||
		events[1].Kind != "queued-second" || !events[1].At.Equal(epoch.Add(5*time.Second)) {
		t.Errorf("events = %+v, want late-first@+10s then queued-second@+5s (processed in queue order, logged at scheduled times)", events)
	}
}

// PartitionAll and HealAll are the fleet-wide outage vocabulary a restart
// scenario reaches for — sever every link at once, then restore them. These
// smoke tests pin that semantics to per-link Partition/Heal so the delegates
// cannot silently drift from the single-link behaviour they wrap.
func TestPartitionAllSeversEveryLinkAndHealAllRestores(t *testing.T) {
	fake := clock.NewFake(epoch)
	fleet := NewFleet(fake, herdSeed, 2)
	t.Cleanup(fleet.Shutdown)
	startEchoServers(fleet)

	fleet.PartitionAll()
	for i := range fleet.Size() {
		if !fleet.Link(i).Partitioned() {
			t.Errorf("client %d's link not partitioned after PartitionAll", i)
		}
		if _, err := fleet.ClientConn(i).Write([]byte("x")); !errors.Is(err, ErrPartitioned) {
			t.Errorf("client %d write during fleet-wide partition: got %v, want ErrPartitioned", i, err)
		}
	}

	fleet.HealAll()
	for i := range fleet.Size() {
		if fleet.Link(i).Partitioned() {
			t.Errorf("client %d's link still partitioned after HealAll", i)
		}
	}
	// Traffic must actually flow again, not merely report healed: round-trip
	// one byte through every connection.
	assertAllConnected(t, fleet)
}

func TestNewFleetRejectsEmptyFleet(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewFleet with zero clients did not panic; an empty fleet passes tests while simulating nothing")
		}
	}()
	NewFleet(clock.NewFake(epoch), 1, 0)
}

func TestStartTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Start called twice did not panic; a mid-simulation handler swap would make behaviour depend on swap timing")
		}
	}()
	fleet := NewFleet(clock.NewFake(epoch), 7, 1)
	defer fleet.Shutdown()
	fleet.Start(func(FleetEvent) {})
	fleet.Start(func(FleetEvent) {})
}

func sortTimes(ts []time.Time) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
}
