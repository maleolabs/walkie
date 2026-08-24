package control

import (
	"context"
	"fmt"
	"sync"
	"testing"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// The initial-roster seam (sto:terminal-ui): the coordinator writes the full
// presence snapshot to a fresh connection BEFORE its HelloAck (server.go
// serveConn). The handshake must hand those frames to the consumer instead of
// dropping them — otherwise every client's roster stays empty until the first
// TTL-driven refresh, which is precisely the gap this forwarding closes.

func TestHandshakeForwardsPreAckPresenceSnapshotToHandler(t *testing.T) {
	cfg := loopConfig()
	sess := newStubSession()
	sess.autoAck = true
	go func() { <-sess.closed }()

	// Preload the wire the way the coordinator orders it: snapshot first,
	// ack second (autoAck appends on Send, strictly after the queued frame).
	sess.deliver(&walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceUpdate{PresenceUpdate: &walkiev1.PresenceUpdate{
			Device: "alice.tail-scale.ts.net.",
			State:  walkiev1.PresenceState_PRESENCE_STATE_ONLINE,
		}},
	})

	var mu sync.Mutex
	var seen []string
	sc := buildClient(t, cfg, func(context.Context) (Session, error) {
		return sess, nil
	}, 13, 14)
	// Registered BEFORE Start: the handshake window precedes any state the
	// test could otherwise synchronize on.
	sc.client.OnEnvelope(func(env *walkiev1.Envelope) {
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%T", env.GetPayload()))
		mu.Unlock()
	})
	sc.client.Start()
	sc.waitState(t, StateOnline)

	mu.Lock()
	defer mu.Unlock()
	want := fmt.Sprintf("%T", &walkiev1.Envelope_PresenceUpdate{})
	for _, got := range seen {
		if got == want {
			return // the snapshot reached the consumer despite preceding the ack
		}
	}
	t.Fatalf("initial roster snapshot was consumed and dropped pre-ack; handler saw %v", seen)
}

func TestHandshakeStillSkipsUnknownPreAckPayloads(t *testing.T) {
	cfg := loopConfig()
	sess := newStubSession()
	sess.autoAck = true
	go func() { <-sess.closed }()

	// Mixed-fleet chatter ahead of the ack (adr:003) stays skipped: only
	// presence is promised before the ack, and forwarding anything else would
	// hand a consumer work it cannot yet orient (identity lands WITH the ack).
	sess.deliver(&walkiev1.Envelope{
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Sender: "stranger", Body: "too early",
		}},
	})

	var mu sync.Mutex
	var seen []string
	sc := buildClient(t, cfg, func(context.Context) (Session, error) {
		return sess, nil
	}, 15, 16)
	sc.client.OnEnvelope(func(env *walkiev1.Envelope) {
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%T", env.GetPayload()))
		mu.Unlock()
	})
	sc.client.Start()
	sc.waitState(t, StateOnline)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 0 {
		t.Fatalf("non-presence pre-ack payload reached the handler: %v", seen)
	}
}
