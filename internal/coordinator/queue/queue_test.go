package queue

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
)

// The tests in this file pin the queue core on a *clock.Fake and a real
// store: every TTL claim is made true by advancing the clock, eviction is
// OBSERVED as an event (Queue.Swept) rather than slept out, every count is
// asserted exactly — req:offline-delivery criterion 4's tight bound starts
// here, because Resume returns the suffix and these tests COUNT it. No test
// sleeps in real time (ts:test-harness).

var epoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

const (
	testTTL     = time.Hour
	testMaxSize = 8
)

func testClock() *clock.Fake { return clock.NewFake(epoch) }

func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

func mustOpenStore(t *testing.T, path string, clk clock.Clock) *store.Store {
	t.Helper()
	st, err := store.Open(path, clk)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return st
}

func mustNewQueue(t *testing.T, st *store.Store, clk clock.Clock, buf *bytes.Buffer) *Queue {
	t.Helper()
	q, err := New(st, clk, testTTL, testMaxSize, testLogger(buf))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(q.Close)
	return q
}

// awaitSweep blocks until one sweep has completed, failing loudly on timeout.
// Sweeps run on the watcher goroutine (or inline at startup), so the event
// can lag the Advance that caused it; this is a blocking receive with a loud
// failure mode, never a sleep.
func awaitSweep(t *testing.T, q *Queue) {
	t.Helper()
	select {
	case <-q.Swept():
	case <-time.After(5 * time.Second):
		t.Fatal("no TTL sweep completed; the watcher never fired")
	}
}

// directEnv builds a stamped envelope as routing.go's stampedDelivery would
// hand it to the sink: identity fields set, position zero (live traffic).
func directEnv(id, body string, at time.Time) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId:  id,
		SentAt:     timestamppb.New(at),
		ReceivedAt: timestamppb.New(at),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: "phone.tail-scale.ts.net.",
			Sender:    "laptop.tail-scale.ts.net.",
			Body:      body,
		}},
	}
}

func enqueueOK(t *testing.T, q *Queue, recipient string, env *walkiev1.Envelope) uint64 {
	t.Helper()
	pos, err := q.Enqueue(recipient, env)
	if err != nil {
		t.Fatalf("enqueue %q for %q: %v", env.GetMessageId(), recipient, err)
	}
	return pos
}

func resumePositions(t *testing.T, q *Queue, recipient string, after uint64) []uint64 {
	t.Helper()
	got, err := q.Resume(recipient, after)
	if err != nil {
		t.Fatalf("resume %q from %d: %v", recipient, after, err)
	}
	out := make([]uint64, len(got))
	for i, d := range got {
		out[i] = d.Position
	}
	return out
}

func assertPositions(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("positions = %v, want %v (count mismatch)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("positions = %v, want %v", got, want)
		}
	}
}

// TestEnqueuePositionsMonotonicPerRecipientIndependent: positions are
// monotonic PER RECIPIENT — two inboxes count independently, and neither's
// counter moves when the other enqueues.
func TestEnqueuePositionsMonotonicPerRecipientIndependent(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/coord.db", testClock())
	q := mustNewQueue(t, st, testClock(), &bytes.Buffer{})

	laptop := "laptop.tail-scale.ts.net."
	phone := "phone.tail-scale.ts.net."

	assertPositions(t, []uint64{
		enqueueOK(t, q, laptop, directEnv("m-a1", "a1", epoch)),
		enqueueOK(t, q, phone, directEnv("m-b1", "b1", epoch)),
		enqueueOK(t, q, laptop, directEnv("m-a2", "a2", epoch)),
	}, 1, 1, 2)

	assertPositions(t, []uint64{
		enqueueOK(t, q, laptop, directEnv("m-a3", "a3", epoch)),
		enqueueOK(t, q, phone, directEnv("m-b2", "b2", epoch)),
	}, 3, 2)

	// Each inbox resumes independently too.
	assertPositions(t, resumePositions(t, q, laptop, 0), 1, 2, 3)
	assertPositions(t, resumePositions(t, q, phone, 0), 1, 2)
}

// TestResumeReturnsExactlyUnackedSuffix pins resumption by position: after
// acking a high-water mark, Resume returns EXACTLY what follows it — counted,
// not approximated. This is the core slice 2's wire-level measurement of
// criterion 4 builds on.
func TestResumeReturnsExactlyUnackedSuffix(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/coord.db", testClock())
	q := mustNewQueue(t, st, testClock(), &bytes.Buffer{})
	recipient := "phone.tail-scale.ts.net."

	for i := 1; i <= 5; i++ {
		enqueueOK(t, q, recipient, directEnv(msgID(i), "body", epoch))
	}

	// Nothing acked: everything.
	assertPositions(t, resumePositions(t, q, recipient, 0), 1, 2, 3, 4, 5)

	// Ack 3: the suffix is exactly {4, 5} — two messages cross, not five.
	if err := q.Ack(recipient, 3); err != nil {
		t.Fatalf("ack: %v", err)
	}
	suffix, err := q.Resume(recipient, 0)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(suffix) != 2 {
		t.Fatalf("suffix length = %d, want exactly 2", len(suffix))
	}
	assertPositions(t, resumePositions(t, q, recipient, 0), 4, 5)

	// A client reporting its own last_acked gets the same suffix...
	assertPositions(t, resumePositions(t, q, recipient, 3), 4, 5)
	// ...and one reporting BEYOND the coordinator's mark gets nothing extra.
	assertPositions(t, resumePositions(t, q, recipient, 5))

	// Ack everything: empty.
	if err := q.Ack(recipient, 5); err != nil {
		t.Fatalf("ack: %v", err)
	}
	assertPositions(t, resumePositions(t, q, recipient, 0))
}

// TestAckIsMonotonicHighWaterMark: a stale or duplicated ack with a lower
// position never regresses the cursor nor resurrects anything.
func TestAckIsMonotonicHighWaterMark(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/coord.db", testClock())
	q := mustNewQueue(t, st, testClock(), &bytes.Buffer{})
	recipient := "phone.tail-scale.ts.net."

	for i := 1; i <= 4; i++ {
		enqueueOK(t, q, recipient, directEnv(msgID(i), "body", epoch))
	}
	if err := q.Ack(recipient, 3); err != nil {
		t.Fatalf("ack 3: %v", err)
	}
	if err := q.Ack(recipient, 2); err != nil {
		t.Fatalf("late ack 2: %v", err)
	}
	// The late low ack changed nothing: suffix still {4}.
	assertPositions(t, resumePositions(t, q, recipient, 0), 4)

	// Acking forward past existing rows is fine; new messages continue AFTER
	// the counter, never reusing positions.
	if err := q.Ack(recipient, 10); err != nil {
		t.Fatalf("ack 10: %v", err)
	}
	if pos := enqueueOK(t, q, recipient, directEnv("m-next", "body", epoch)); pos != 5 {
		t.Fatalf("next position = %d, want 5 (counter never rewinds)", pos)
	}
	assertPositions(t, resumePositions(t, q, recipient, 0), 5)
}

// TestTTLEvictionRemovesAndLogsContentFree is sto:offline-queue criterion 5 /
// req:offline-delivery criterion 4: past the TTL the message is REMOVED — not
// merely hidden at read time — and the removal is logged without carrying
// message content. Expiry also frees cap space: eviction is retention
// working, not an error.
func TestTTLEvictionRemovesAndLogsContentFree(t *testing.T) {
	clk := testClock()
	logs := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)
	q := mustNewQueue(t, st, clk, logs)
	recipient := "phone.tail-scale.ts.net."

	secretBody := "the launch codes are under the mat"
	enqueueOK(t, q, recipient, directEnv("m-old", secretBody, clk.Now()))

	// Half the TTL later, a second message lands. It must OUTLIVE the first.
	clk.Advance(testTTL / 2)
	enqueueOK(t, q, recipient, directEnv("m-new", "harmless", clk.Now()))
	assertPositions(t, resumePositions(t, q, recipient, 0), 1, 2)

	// Cross the FIRST message's deadline only: exactly it expires. The sweep
	// runs on the watcher goroutine; observe it as the event it is.
	clk.Advance(testTTL / 2)
	awaitSweep(t, q)
	assertPositions(t, resumePositions(t, q, recipient, 0), 2)

	out := logs.String()
	if !strings.Contains(out, "queued message expired") ||
		!strings.Contains(out, "message_id=m-old") ||
		!strings.Contains(out, "recipient="+recipient) {
		t.Errorf("eviction log should identify recipient and message_id content-free; log:\n%s", out)
	}
	if strings.Contains(out, secretBody) {
		t.Errorf("eviction log carries message BODY — the no-content rule is violated; log:\n%s", out)
	}

	// Cross the second deadline: the queue empties itself with NO further
	// traffic — proving the watcher owns expiry, not read-time laziness.
	clk.Advance(testTTL / 2)
	awaitSweep(t, q)
	assertPositions(t, resumePositions(t, q, recipient, 0))

	// Positions never restart after eviction: the next message continues the
	// counter, so no future resume can mistake it for an old position.
	if pos := enqueueOK(t, q, recipient, directEnv("m-after", "body", clk.Now())); pos != 3 {
		t.Fatalf("position after eviction = %d, want 3 (counter never restarts)", pos)
	}
}

// TestExpiredDuringDowntimeEvictedAtStartup makes the reload semantics
// explicit: a message that lapsed while NO process was running is evicted and
// logged at startup, never delivered stale.
func TestExpiredDuringDowntimeEvictedAtStartup(t *testing.T) {
	clk := testClock()
	path := t.TempDir() + "/coord.db"

	st1 := mustOpenStore(t, path, clk)
	q1, err := New(st1, clk, testTTL, testMaxSize, testLogger(&bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	enqueueOK(t, q1, "phone.tail-scale.ts.net.", directEnv("m1", "body", clk.Now()))
	q1.Close()
	st1.Close()

	// Downtime: the clock runs past the TTL with nobody watching.
	clk.Advance(2 * testTTL)

	logs := &bytes.Buffer{}
	st2 := mustOpenStore(t, path, clk)
	defer st2.Close()
	q2, err := New(st2, clk, testTTL, testMaxSize, testLogger(logs))
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	defer q2.Close()

	// New sweeps synchronously before returning, so state AND log are already
	// settled here — no wait needed.
	assertPositions(t, resumePositions(t, q2, "phone.tail-scale.ts.net.", 0))
	if out := logs.String(); !strings.Contains(out, "queued message expired") {
		t.Errorf("startup sweep did not log the downtime expiry; log:\n%s", out)
	}

	// The cursor survived downtime too: the next position continues.
	if pos := enqueueOK(t, q2, "phone.tail-scale.ts.net.", directEnv("m2", "body", clk.Now())); pos != 2 {
		t.Fatalf("position after restart = %d, want 2", pos)
	}
}

// TestCapReachedRefusesTypedNoGrowth is criterion 6's queue half: reaching
// the size cap produces a TYPED refusal, the coordinator does not grow past
// the cap, and the messages already retained are intact. Refusal is also not
// permanent: expiry frees space again.
func TestCapReachedRefusesTypedNoGrowth(t *testing.T) {
	clk := testClock()
	logs := &bytes.Buffer{}
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)
	const cap = 3
	q, err := New(st, clk, testTTL, cap, testLogger(logs))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	t.Cleanup(q.Close)
	recipient := "phone.tail-scale.ts.net."

	var kept []uint64
	for i := 1; i <= cap; i++ {
		kept = append(kept, enqueueOK(t, q, recipient, directEnv(msgID(i), "kept body", epoch)))
	}

	_, err = q.Enqueue(recipient, directEnv("m-over", "overflows", epoch))
	var capErr *CapacityError
	if !errors.As(err, &capErr) {
		t.Fatalf("enqueue past cap: err = %v, want *CapacityError", err)
	}
	if capErr.Recipient != recipient || capErr.Cap != cap {
		t.Errorf("CapacityError = %+v, want recipient %q cap %d", capErr, recipient, cap)
	}

	// No growth past the cap, and the retained messages are intact.
	assertPositions(t, resumePositions(t, q, recipient, 0), kept...)

	// Other recipients are unaffected: the cap is per-recipient.
	if pos := enqueueOK(t, q, "tablet.tail-scale.ts.net.", directEnv("m-other", "body", epoch)); pos != 1 {
		t.Fatalf("other recipient position = %d, want 1", pos)
	}

	// Expiry frees space: refusal was about capacity, not a ban.
	clk.Advance(2 * testTTL)
	awaitSweep(t, q)
	if pos := enqueueOK(t, q, recipient, directEnv("m-fresh", "body", clk.Now())); pos != cap+1 {
		t.Fatalf("post-expiry position = %d, want %d", pos, cap+1)
	}
	assertPositions(t, resumePositions(t, q, recipient, 0), cap+1)
}

// TestRestartSurvivesQueueAndAcksNothingResurrected is the restart contract:
// queued messages survive, acknowledged positions survive, and reopening
// resurrects NOTHING that was acknowledged — even against a client reporting
// last_acked_position zero.
func TestRestartSurvivesQueueAndAcksNothingResurrected(t *testing.T) {
	clk := testClock()
	path := t.TempDir() + "/coord.db"
	recipient := "phone.tail-scale.ts.net."

	st1 := mustOpenStore(t, path, clk)
	q1, err := New(st1, clk, testTTL, testMaxSize, testLogger(&bytes.Buffer{}))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	for i := 1; i <= 4; i++ {
		enqueueOK(t, q1, recipient, directEnv(msgID(i), "body", epoch))
	}
	if err := q1.Ack(recipient, 2); err != nil {
		t.Fatalf("ack: %v", err)
	}
	q1.Close()
	st1.Close()

	st2 := mustOpenStore(t, path, clk)
	defer st2.Close()
	q2, err := New(st2, clk, testTTL, testMaxSize, testLogger(&bytes.Buffer{}))
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	t.Cleanup(q2.Close)

	// Exactly the unacked suffix survives — positions 3 and 4, counted.
	assertPositions(t, resumePositions(t, q2, recipient, 0), 3, 4)

	// PendingCount agrees with Resume; the two can never disagree.
	n, err := q2.PendingCount(recipient, 0)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if n != 2 {
		t.Fatalf("pending count = %d, want 2", n)
	}

	// The next position survived: no reuse of 1..4.
	if pos := enqueueOK(t, q2, recipient, directEnv("m-new", "body", epoch)); pos != 5 {
		t.Fatalf("post-restart position = %d, want 5", pos)
	}
}

// TestDeliverRejectsNonDirectPayload: the sink refuses payloads it could not
// faithfully replay rather than storing something lossy, and stores nothing.
func TestDeliverRejectsNonDirectPayload(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/coord.db", testClock())
	q := mustNewQueue(t, st, testClock(), &bytes.Buffer{})
	recipient := "phone.tail-scale.ts.net."

	err := q.Deliver(recipient, &walkiev1.Envelope{
		MessageId: "hb-1",
		Payload:   &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}},
	})
	if err == nil {
		t.Fatal("heartbeat payload accepted by the sink; want refusal")
	}
	assertPositions(t, resumePositions(t, q, recipient, 0))

	// DirectMessages flow through Deliver normally, position assigned.
	if err := q.Deliver(recipient, directEnv("m1", "held", epoch)); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	assertPositions(t, resumePositions(t, q, recipient, 0), 1)
}

// TestResumeRebuildsEnvelopeFaithfully: what comes back is the stamped
// envelope that went in — identity fields and BOTH timestamps intact, body
// byte-exact — plus the one field replay legitimately adds: the position.
func TestResumeRebuildsEnvelopeFaithfully(t *testing.T) {
	st := mustOpenStore(t, t.TempDir()+"/coord.db", testClock())
	q := mustNewQueue(t, st, testClock(), &bytes.Buffer{})
	recipient := "phone.tail-scale.ts.net."

	sentAt := epoch.Add(-30 * time.Second)
	receivedAt := epoch
	in := directEnv("m-1", "exact body bytes", receivedAt)
	in.SentAt = timestamppb.New(sentAt)

	enqueueOK(t, q, recipient, in)

	got, err := q.Resume(recipient, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("resume: %d deliveries, err %v", len(got), err)
	}
	env := got[0].Envelope
	if got[0].Position != 1 || env.GetPosition() != 1 {
		t.Fatalf("delivery position = %d/%d, want 1/1 (replay stamp)", got[0].Position, env.GetPosition())
	}
	if env.GetMessageId() != "m-1" {
		t.Errorf("message_id = %q, want m-1 (dedup keys on it surviving replay)", env.GetMessageId())
	}
	dm := env.GetDirectMessage()
	if dm == nil || dm.GetBody() != "exact body bytes" || dm.GetSender() != "laptop.tail-scale.ts.net." || dm.GetRecipient() != "phone.tail-scale.ts.net." {
		t.Errorf("payload rebuilt = %+v, want original intact", dm)
	}
	if !env.GetSentAt().AsTime().Equal(sentAt) {
		t.Errorf("sent_at = %v, want %v (sender half preserved)", env.GetSentAt().AsTime(), sentAt)
	}
	if !env.GetReceivedAt().AsTime().Equal(receivedAt) {
		t.Errorf("received_at = %v, want %v (ingress stamp never rewritten)", env.GetReceivedAt().AsTime(), receivedAt)
	}
}

// TestConstructorRefusesWiringErrors: non-positive ttl/cap or missing
// store/clock are programmer errors refused at construction, per the house
// rule — a queue that expires instantly or refuses everything is a wiring
// bug, not a mode.
func TestConstructorRefusesWiringErrors(t *testing.T) {
	clk := testClock()
	st := mustOpenStore(t, t.TempDir()+"/coord.db", clk)

	cases := []struct {
		name    string
		st      *store.Store
		clk     clock.Clock
		ttl     time.Duration
		maxSize int
	}{
		{"nil store", nil, clk, testTTL, testMaxSize},
		{"nil clock", st, nil, testTTL, testMaxSize},
		{"zero ttl", st, clk, 0, testMaxSize},
		{"negative ttl", st, clk, -time.Second, testMaxSize},
		{"zero cap", st, clk, testTTL, 0},
		{"negative cap", st, clk, testTTL, -1},
	}
	for _, tc := range cases {
		if _, err := New(tc.st, tc.clk, tc.ttl, tc.maxSize, nil); err == nil {
			t.Errorf("%s: construction succeeded, want error", tc.name)
		}
	}
}

// msgID gives each test message a distinct id.
func msgID(i int) string { return "test-" + strings.Repeat("m", i) }
