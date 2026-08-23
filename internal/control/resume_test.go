package control

import (
	"sync"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/testnet"
)

// Criterion 4 from the CLIENT side: reconnect resumes from the acknowledged
// position instead of replaying the whole queue. sto:offline-queue's own
// tests count what crosses the wire on the SERVER side; this file does the
// same counting from the client's view — what the client sent in Hello, and
// what came back — against a scripted coordinator that keeps a per-device
// backlog exactly like queue.Queue's Resume.

// resumeBehavior plays a coordinator with a per-position backlog: Hello gets
// an honest HelloAck (pending_count = undelivered suffix) followed by the
// suffix frames oldest-first; QueueAck is recorded; everything else echoes.
func resumeBehavior(coord *fakeCoordinator, backlog map[uint64]*walkiev1.Envelope) peerBehavior {
	return func(env *walkiev1.Envelope) []*walkiev1.Envelope {
		switch env.GetPayload().(type) {
		case *walkiev1.Envelope_Hello:
			last := coord.lastHelloPosition()
			replies := []*walkiev1.Envelope{{
				Payload: &walkiev1.Envelope_HelloAck{HelloAck: &walkiev1.HelloAck{
					Device:          coord.device,
					ProtocolVersion: walkiev1.MaxProtocolVersion,
					PendingCount:    uint64(len(backlog)) - min(last, uint64(len(backlog))),
				}},
			}}
			for p := last + 1; p <= uint64(len(backlog)); p++ {
				replies = append(replies, backlog[p])
			}
			return replies
		default:
			return []*walkiev1.Envelope{env}
		}
	}
}

func TestReconnectResumesFromAcknowledgedPositionNotFullReplay(t *testing.T) {
	fake := clock.NewFake(epoch)
	mach, err := NewMachine(fake, discardLogger())
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}

	// Seven queued messages, all populated BEFORE any goroutine exists: the
	// scripted coordinator reads this map from its serve goroutine, so the
	// map must be immutable once the scenario runs.
	const total = 7
	backlog := map[uint64]*walkiev1.Envelope{}
	for p := 1; p <= total; p++ {
		backlog[uint64(p)] = &walkiev1.Envelope{
			MessageId: string(rune('a' + p)),
			Position:  uint64(p),
			Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Sender: "peer", Body: "queued message",
			}},
		}
	}

	// The coordinator's behavior closure reads the backlog map. Silence for
	// everything except Hello: with DeadPeerInterval beyond the scenario
	// length the session needs no echo traffic, and a quiet wire keeps
	// suffix delivery free of concurrent-write interleaving on the
	// unbuffered test pipes.
	coord := newFakeCoordinator("dev", nil)
	coord.behavior = resumeBehavior(coord, backlog)

	link := testnet.NewLink(fake, testnet.Conditions{})
	var mu sync.Mutex
	received := map[uint64]int{}

	mkClient := func(seedB uint64) *Client {
		cl, err := NewClient(condConfig(), mach, dialThrough(link, coord), 0xBEEF, seedB, fake, discardLogger())
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		cl.OnEnvelope(func(env *walkiev1.Envelope) {
			if p := env.GetPosition(); p > 0 {
				mu.Lock()
				received[p]++
				mu.Unlock()
			}
		})
		return cl
	}

	// --- Session 1: initial connect, full backlog delivered, positions
	// 1..3 acknowledged from the consumer's own goroutine (the handler must
	// never block the read loop, and over an unbuffered test pipe a
	// synchronous ack-write would deadlock against the coordinator's suffix
	// writes).
	cl := mkClient(0xC0DE)
	cl.Start()
	drive(t, fake, 100*time.Millisecond, 3_000, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == total
	})
	cl.Acknowledge(3)
	runtime_GoschedUntilAll(t, func() bool {
		acks := coord.ackPositions()
		return len(acks) > 0 && acks[len(acks)-1] == 3
	})

	// Mark the wire position; everything the server writes FROM HERE is the
	// post-reconnect traffic the assertions read.
	mark := len(coord.sentAfter(0))

	// --- Session 2: stop session 1 (a clean local teardown drives the
	// machine to disconnected through the normal death path) and let a fresh
	// client instance reconnect. Same machine, same link, same coordinator —
	// exactly what a real reconnect is, minus the crash detection whose
	// timing the herd and partition scenarios already cover.
	cl.Stop()
	runtime_GoschedUntilAll(t, func() bool { return mach.State() == StateDisconnected })

	// The restarted client loads its ack high-water from durable state —
	// the same seam a store-backed client wires at process start. Without
	// it the fresh instance would greet the coordinator with position 0 and
	// every reconnect would replay from zero.
	cl2 := mkClient(0xD0DE)
	cl2.RestoreAckPosition(cl.LastAcked())
	cl2.Start()
	t.Cleanup(cl2.Stop)
	drive(t, fake, 100*time.Millisecond, 3_000, func() bool {
		return mach.State() == StateOnline
	})
	// Wait until the coordinator has finished WRITING the suffix — client
	// receipt trails the wire by scheduling, and the wire record is what
	// the assertion reads.
	runtime_GoschedUntilAll(t, func() bool {
		sp := map[uint64]bool{}
		for _, f := range coord.sentAfterMark(mark) {
			if f.pos > 0 {
				sp[f.pos] = true
			}
		}
		return len(sp) == 4
	})

	// THE ASSERTIONS — criterion 4, client side:
	//
	//   1. The reconnecting Hello carried last_acked_position=3, so the
	//      coordinator drained only the unacked suffix.
	//   2. The post-reconnect wire carried positions 4..7 and NOTHING at or
	//      below the acknowledged position — a full replay would have re-sent
	//      1..7.
	hellos := coord.helloPositions()
	if len(hellos) < 2 {
		t.Fatalf("%d Hellos observed, want at least 2 (initial + reconnect)", len(hellos))
	}
	if hellos[0] != 0 {
		t.Fatalf("initial Hello.last_acked_position = %d, want 0", hellos[0])
	}
	if got := hellos[len(hellos)-1]; got != 3 {
		t.Fatalf("reconnect Hello.last_acked_position = %d, want 3 (the acknowledged high-water)", got)
	}

	sentPositions := map[uint64]bool{}
	for _, f := range coord.sentAfterMark(mark) {
		if f.pos > 0 {
			sentPositions[f.pos] = true
		}
	}
	if len(sentPositions) != 4 {
		t.Fatalf("post-reconnect wire carried %d distinct queued positions (%v), want exactly the unacked suffix 4..7",
			len(sentPositions), sentPositions)
	}
	for p := uint64(1); p <= 3; p++ {
		if sentPositions[p] {
			t.Fatalf("acknowledged position %d crossed the wire again — that is a replay, not a resume", p)
		}
	}
	for p := uint64(4); p <= 7; p++ {
		if !sentPositions[p] {
			t.Fatalf("unacked position %d never arrived after reconnect", p)
		}
	}

	// Client-side view: every suffix position reached the handler across the
	// two sessions (4..7 may arrive on either), and nothing was duplicated
	// within a session.
	mu.Lock()
	defer mu.Unlock()
	for p := uint64(4); p <= 7; p++ {
		if received[p] == 0 {
			t.Fatalf("position %d never reached the client's handler", p)
		}
	}
}
