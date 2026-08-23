package queue

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
	"google.golang.org/protobuf/proto"
)

// Queue is the coordinator's per-recipient store-and-forward inbox: bounded
// retention (TTL AND size cap), monotonic per-recipient positions,
// acknowledgement by high-water mark, and survival across coordinator
// restarts. It implements the coordinator package's OfflineSink seam plus the
// drain half the handshake needs.
//
// Owning work item:
//
//	eka get walkie/sto:offline-queue
//
// See the package comment (doc.go) for the delivery semantics this type
// implements; see req:offline-delivery for the requirement behind them.
//
// # What is opaque here, on purpose
//
// The stored row keeps the stamped envelope as one BLOB the queue never
// opens. Enqueue renders it through the at-rest seam (sealed box when wired
// via NewSealed — the production wiring — verbatim-plus-marker when not);
// Resume reverses the FRAMING and puts the result back on the wire with its
// position stamped: a sealed row ships as a SealedDelivery frame carrying
// the opaque ciphertext, which only the recipient can open. The queue never
// inspects payload fields, never indexes or searches on content — which is
// exactly the shape ts:queue-sealed-box needed: ciphertext replaced plaintext
// at the same two boundary points without touching this logic or the schema.
// The one open dependency sealed.go used to record (production enablement
// awaited a sealed-delivery carrier) is closed: Envelope.sealed_delivery = 25
// is that carrier, and NewSealed is what cmd/walkie-coordinator wires.
//
// # Concurrency shape
//
// One mutex guards the sweep timer and serialises the check-then-insert
// sequences (cap admission, cursor advance) that must be atomic. The store's
// single connection already serialises statements; the mutex is what makes a
// SEQUENCE of statements atomic against another goroutine's enqueue. One
// watcher goroutine owns TTL expiry so eviction happens even when no traffic
// touches the queue — criterion 5 says messages are REMOVED past the TTL, not
// merely invisible at read time.
type Queue struct {
	db      *sql.DB
	clk     clock.Clock
	ttl     time.Duration
	maxSize int
	logger  *slog.Logger

	mu        sync.Mutex
	timer     clock.Timer // armed at the earliest outstanding deadline; nil when empty
	closed    chan struct{}
	closeOnce sync.Once

	// wake nudges the watcher that the timer changed (armed, rearmed or
	// disarmed). Buffered, sent non-blocking: a coalesced nudge is enough —
	// the watcher re-reads the timer under the lock each iteration.
	wake chan struct{}

	// swept signals that a sweep finished, for callers that must observe
	// eviction as an event rather than poll for it (tests on the fake clock;
	// ts:test-harness forbids sleeping a race away). Buffered, sent
	// non-blocking: nobody listening loses nothing.
	swept chan struct{}

	// atRest is ts:queue-sealed-box's encryption seam (see sealed.go): what
	// a body looks like while it rests in storage. Nil — the plain [New]
	// shape — stores and reads bytes verbatim; NewSealed wires the sealed
	// box (the production wiring). Every code path here treats bodies as
	// opaque either way; the seam is applied at exactly two points,
	// sealForStorage in Enqueue and deliveryFromStorage in Resume, and
	// nowhere else.
	atRest AtRest
}

// CapacityError is the typed refusal returned by [Queue.Enqueue] when the
// recipient's inbox is at its configured size cap.
//
// Why a typed error rather than a sentinel: the routing layer must turn THIS
// condition into QueueRefused{SIZE_CAP} on the SENDER's connection
// (req:offline-delivery criterion 5 / sto:offline-queue criterion 6) while any
// other error is an internal failure that must NOT be shaped into wire blame.
// errors.As discrimination keeps those two paths from ever being confused.
type CapacityError struct {
	// Recipient names the full inbox. Safe for logs: a device name, never
	// message content.
	Recipient string
	// Cap is the configured maximum number of retained messages.
	Cap int
}

func (e *CapacityError) Error() string {
	return fmt.Sprintf("queue: recipient %q inbox at size cap (%d retained messages)", e.Recipient, e.Cap)
}

// Delivery is one queued message ready to cross the wire on resume: the
// stamped envelope with its per-recipient position set. Position is the whole
// resumption mechanism (Envelope.position in walkie.v1); the client acks it
// once durably processed.
type Delivery struct {
	Position uint64
	Envelope *walkiev1.Envelope
}

// New returns a Queue backed by st, driven by clk, retaining messages for ttl
// and refusing beyond maxSize retained messages per recipient.
//
// Reload semantics, explicit because restart survival is the item's point:
// rows and cursors are read from the database lazily per operation, so a
// reopened Queue sees exactly what the previous process committed — queued
// messages, next-position counters and acknowledged high-water marks all
// survive; nothing is resurrected that was acknowledged, because Ack deletes
// covered rows in the same transaction that advances the cursor. Expired
// during downtime counts as expired: New sweeps immediately, so messages that
// lapsed while no process was watching are evicted and logged at startup, not
// delivered stale.
//
// ttl and maxSize must be positive: a non-positive TTL would expire messages
// instantly and a zero cap would refuse everything — both wiring bugs, not
// modes, refused per the house rule for programmer errors.
func New(st *store.Store, clk clock.Clock, ttl time.Duration, maxSize int, logger *slog.Logger) (*Queue, error) {
	return newQueue(st, clk, ttl, maxSize, logger)
}

// newQueue is the shared constructor body of [New] and [NewSealed]; see New
// for the full contract and NewSealed for the encrypted-at-rest variant.
func newQueue(st *store.Store, clk clock.Clock, ttl time.Duration, maxSize int, logger *slog.Logger) (*Queue, error) {
	if st == nil {
		return nil, fmt.Errorf("queue: new: st must not be nil")
	}
	if clk == nil {
		return nil, fmt.Errorf("queue: new: clk must not be nil")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("queue: new: ttl must be positive (got %s)", ttl)
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("queue: new: max size must be positive (got %d)", maxSize)
	}
	if logger == nil {
		logger = slog.Default()
	}

	q := &Queue{
		db:      st.DB(),
		clk:     clk,
		ttl:     ttl,
		maxSize: maxSize,
		logger:  logger,
		closed:  make(chan struct{}),
		wake:    make(chan struct{}, 1),
		swept:   make(chan struct{}, 1),
	}

	// Startup sweep: whatever lapsed while the process was down is evicted
	// now, with the same logged removal as any other expiry. This also arms
	// the sweep timer for the earliest surviving deadline.
	if err := q.sweep(); err != nil {
		return nil, fmt.Errorf("queue: startup sweep: %w", err)
	}
	go q.watch()

	logger.Info("offline queue started",
		slog.Duration("ttl", ttl),
		slog.Int("max_per_recipient", maxSize),
	)
	return q, nil
}

// Deliver holds env for recipient — the OfflineSink seam. It returns a
// *CapacityError when the recipient's inbox is at its size cap (the routing
// layer turns that into QueueRefused{SIZE_CAP} to the sender); any other
// error is an internal failure the caller must log loudly but not blame the
// sender for.
//
// A payload other than DirectMessage is refused with an error and stores
// nothing: today only direct messages arrive here (routing hands off in
// deliverUnroutable), and silently storing something the replay path could
// not faithfully rebuild would trade a loud bug for a quiet one.
func (q *Queue) Deliver(recipient string, env *walkiev1.Envelope) error {
	dm := env.GetDirectMessage()
	if dm == nil {
		return fmt.Errorf("queue: deliver to %q: unsupported payload %T (only direct messages are queued)", recipient, env.GetPayload())
	}
	_, err := q.Enqueue(recipient, env)
	return err
}

// Enqueue stores env for recipient under the next monotonic position and
// returns it. Positions start at 1 and NEVER restart — not after a full
// drain, not after TTL eviction — because a reused position would collide
// with an old acknowledged position and hide the new message from every
// future resume.
//
// At the size cap the message is REFUSED, not dropped-and-kept-silent: the
// returned *CapacityError is the sender's legible "not accepted" signal, the
// caller logs it, and the coordinator has grown nothing (admission is checked
// before any insert). Expiry by TTL is a different mechanism entirely — quiet,
// logged eviction of messages already accepted; see sweep.
func (q *Queue) Enqueue(recipient string, env *walkiev1.Envelope) (uint64, error) {
	marshalled, err := marshalOpaque(env)
	if err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: %w", recipient, err)
	}
	// The at-rest seam (ts:queue-sealed-box): the stored BLOB is the framed
	// output of Seal — a sealed box when the queue is wired with NewSealed
	// and the recipient's key was pinned, the marshalled envelope under the
	// plain marker when it was not (the bootstrap rule on AtRest). Either
	// way this code sees only opaque bytes; a seal failure refuses the
	// enqueue and stores nothing, exactly like any other internal failure.
	body, err := q.sealForStorage(recipient, marshalled)
	if err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: %w", recipient, err)
	}
	messageID := env.GetMessageId()

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.isClosed() {
		return 0, fmt.Errorf("queue: enqueue for %q: queue closed", recipient)
	}

	tx, err := q.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: begin: %w", recipient, err)
	}
	defer tx.Rollback() // no-op after Commit

	// Ensure the cursor row exists before reading it. INSERT OR IGNORE makes
	// first-contact idempotent: positions start at 1, acked at 0.
	if _, err := tx.Exec(
		`INSERT OR IGNORE INTO queue_cursor (recipient, next_position, acked_position) VALUES (?, 1, 0)`,
		recipient,
	); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: ensure cursor: %w", recipient, err)
	}

	// Cap admission BEFORE any insert: reaching the cap refuses, it does not
	// evict-on-behalf-of and it does not grow the table. Counting RETAINED
	// rows is the honest measure — acknowledged rows were deleted at ack
	// time and expired rows at sweep time, so the count IS the live backlog.
	var retained int
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM queue_inbox WHERE recipient = ?`, recipient,
	).Scan(&retained); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: count: %w", recipient, err)
	}
	if retained >= q.maxSize {
		return 0, &CapacityError{Recipient: recipient, Cap: q.maxSize}
	}

	var position uint64
	if err := tx.QueryRow(
		`SELECT next_position FROM queue_cursor WHERE recipient = ?`, recipient,
	).Scan(&position); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: read cursor: %w", recipient, err)
	}
	if _, err := tx.Exec(
		`UPDATE queue_cursor SET next_position = next_position + 1 WHERE recipient = ?`,
		recipient,
	); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: advance cursor: %w", recipient, err)
	}

	enqueuedAt := q.clk.Now()
	if _, err := tx.Exec(
		`INSERT INTO queue_inbox (recipient, position, message_id, body, enqueued_at) VALUES (?, ?, ?, ?, ?)`,
		recipient, int64(position), messageID, body, formatStoredTime(enqueuedAt),
	); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: insert: %w", recipient, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("queue: enqueue for %q: commit: %w", recipient, err)
	}

	// The new entry may carry the earliest deadline; re-arm regardless —
	// Reset is cheap and correct whether the deadline moved or not.
	q.rearmLocked()

	return position, nil
}

// Resume returns every retained message for recipient whose position is
// strictly greater than after, oldest first — exactly the unacked suffix, no
// full-replay (req:offline-delivery forbids replaying everything).
//
// after is normally the client's Hello.last_acked_position. There is
// deliberately NO additional floor from the stored acked cursor here: Ack
// already DELETED every covered row in the same transaction that advanced
// the cursor, so the retained set cannot contain anything acknowledged, and
// a client that lost local state and reports 0 simply gets every retained
// message — never a resurrection. Filtering by the stored cursor too would
// do the opposite of protect: a client that acked BEYOND reality (a bug,
// but the wire permits it) would pin the floor above future positions and
// silently hide every later message. Delivering what is retained is the
// safe side of at-least-once; the receiver dedups by ULID regardless.
//
// Sealed rows (ts:queue-sealed-box) leave here as SealedDelivery frames —
// the ciphertext crosses VERBATIM, because the coordinator can neither open
// nor authenticate it, and any byte it "helpfully" normalized would break
// the AEAD at the recipient. The recipient opens each box with its own
// identity key; a box that fails authentication is dropped whole there,
// loudly, and its position acked — a forfeited message must not freeze the
// ack high-water behind it.
//
// Counting the result is the natural way to verify resumption transfers
// only the suffix — slice 2's wire-level measurement builds directly on
// this.
func (q *Queue) Resume(recipient string, after uint64) ([]Delivery, error) {
	rows, err := q.db.Query(
		// message_id rides along for the unreadable-row failure logs: it is
		// row metadata from its own column, never read out of the (possibly
		// sealed) body.
		`SELECT position, message_id, body FROM queue_inbox WHERE recipient = ? AND position > ? ORDER BY position`,
		recipient, int64(after),
	)
	if err != nil {
		return nil, fmt.Errorf("queue: resume %q from %d: query: %w", recipient, after, err)
	}
	defer rows.Close()

	var out []Delivery
	for rows.Next() {
		var pos int64
		var messageID string
		var body []byte
		if err := rows.Scan(&pos, &messageID, &body); err != nil {
			return nil, fmt.Errorf("queue: resume %q position %d: scan: %w", recipient, pos, err)
		}
		// The at-rest seam's read half (ts:queue-sealed-box): unframe what
		// rested — sealed rows become SealedDelivery frames carrying the
		// opaque box, plain rows decode to their original stamped envelope.
		// An unreadable row (empty, undecodable, unknown future format) is
		// SKIPPED whole, loudly logged, never partially processed; the
		// resume continues, because one dead row must not withhold every
		// message behind it, and Ack's high-water delete bounds how long a
		// skipped row lingers.
		env, deliverable := q.deliveryFromStorage(recipient, pos, messageID, body)
		if !deliverable {
			continue
		}
		out = append(out, Delivery{Position: uint64(pos), Envelope: env})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: resume %q: iterate: %w", recipient, err)
	}
	return out, nil
}

// PendingCount reports how many retained messages recipient has beyond
// after — the number HelloAck.pending_count promises, so the client can tell
// its user instead of appearing to hang. Same filter as Resume; the two can
// never disagree.
func (q *Queue) PendingCount(recipient string, after uint64) (uint64, error) {
	var n int
	if err := q.db.QueryRow(
		`SELECT COUNT(*) FROM queue_inbox WHERE recipient = ? AND position > ?`,
		recipient, int64(after),
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("queue: pending count %q: %w", recipient, err)
	}
	return uint64(n), nil
}

// Ack advances the per-recipient high-water mark to position and deletes
// every covered row, in one transaction. Everything ≤ position is durably
// processed by the client and leaves retention now — retained means
// undelivered-or-unacked, nothing else.
//
// Monotonic by construction: a stale or duplicated ack (at-least-once makes
// these routine) with a lower position neither regresses the cursor nor
// resurrects anything — the UPDATE fires only forward, and the rows it would
// cover are long gone.
func (q *Queue) Ack(recipient string, position uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.isClosed() {
		return fmt.Errorf("queue: ack for %q: queue closed", recipient)
	}

	tx, err := q.db.Begin()
	if err != nil {
		return fmt.Errorf("queue: ack for %q: begin: %w", recipient, err)
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.Exec(
		`INSERT OR IGNORE INTO queue_cursor (recipient, next_position, acked_position) VALUES (?, 1, 0)`,
		recipient,
	); err != nil {
		return fmt.Errorf("queue: ack for %q: ensure cursor: %w", recipient, err)
	}
	// Forward-only: the WHERE guard makes a late low ack a no-op rather than
	// a regression, without needing to read-compare-write.
	if _, err := tx.Exec(
		`UPDATE queue_cursor SET acked_position = ? WHERE recipient = ? AND acked_position < ?`,
		int64(position), recipient, int64(position),
	); err != nil {
		return fmt.Errorf("queue: ack for %q: advance: %w", recipient, err)
	}
	if _, err := tx.Exec(
		`DELETE FROM queue_inbox WHERE recipient = ? AND position <= ?`,
		recipient, int64(position),
	); err != nil {
		return fmt.Errorf("queue: ack for %q: delete covered: %w", recipient, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("queue: ack for %q: commit: %w", recipient, err)
	}

	// Deletion cannot move the earliest deadline earlier, so no re-arm is
	// needed here — rearmLocked exists for enqueues, which can.
	return nil
}

// Close stops the watcher and the sweep timer. Persisted state needs no
// flush: every mutation committed before returning, so closing loses nothing.
func (q *Queue) Close() {
	q.closeOnce.Do(func() {
		close(q.closed)
		q.mu.Lock()
		if q.timer != nil {
			q.timer.Stop()
			q.timer = nil
		}
		q.mu.Unlock()
	})
}

func (q *Queue) isClosed() bool {
	select {
	case <-q.closed:
		return true
	default:
		return false
	}
}

// watch owns TTL expiry. It loops until Close, firing a sweep whenever the
// armed timer lapses and re-reading the timer whenever a mutation signals via
// wake. With nothing outstanding the timer is nil; its (also nil) channel is
// what the select waits on — receiving from a nil channel blocks forever,
// exactly the idle we want, woken only by the next enqueue's nudge.
func (q *Queue) watch() {
	for {
		q.mu.Lock()
		tm := q.timer
		q.mu.Unlock()

		// Extract the channel BEFORE the select: calling tm.C() on a nil
		// Timer interface would panic, whereas selecting on a nil channel
		// simply never fires.
		var timerC <-chan time.Time
		if tm != nil {
			timerC = tm.C()
		}

		select {
		case <-q.closed:
			return
		case <-q.wake:
			// Timer changed; loop and take the new one.
		case <-timerC:
			if err := q.sweep(); err != nil {
				q.logger.Error("queue: ttl sweep failed", slog.String("reason", err.Error()))
				// Retry on the next wake; a stuck sweep must not kill the
				// watcher, because criterion 5's removals all ride it.
				q.nudge()
			}
		}
	}
}

// nudge coalesces a wake signal for the watcher goroutine.
func (q *Queue) nudge() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// sweep evicts every message past the TTL and re-arms the timer at the
// earliest surviving deadline. Eviction is LOGGED, content-free: recipient,
// position, message_id and age — protocol metadata that identifies without
// disclosing, never the body (the no-content rule).
//
// This is TTL EXPIRY, deliberately distinct from the size cap: expiry is
// bounded retention working as designed — quiet on the wire, visible in the
// logs — where the cap refuses NEW messages loudly. Conflating them is the
// exact failure mode the acceptance criteria separate.
func (q *Queue) sweep() error {
	now := q.clk.Now()
	cutoff := formatStoredTime(now.Add(-q.ttl))

	// Collect then delete: parsing timestamps in Go rather than comparing
	// RFC3339Nano strings in SQL (trailing-zero trimming breaks lexicographic
	// order — see the migration comment for the fixed-width layout that
	// makes the STORED form comparable; the cutoff comparison below is safe
	// because both sides use that same fixed-width layout).
	rows, err := q.db.Query(
		`SELECT recipient, position, message_id, enqueued_at FROM queue_inbox WHERE enqueued_at <= ?`,
		cutoff,
	)
	if err != nil {
		return fmt.Errorf("query expired: %w", err)
	}
	type expired struct {
		recipient string
		position  int64
		messageID string
		enqueued  time.Time
	}
	var due []expired
	for rows.Next() {
		var e expired
		var enqueued string
		if err := rows.Scan(&e.recipient, &e.position, &e.messageID, &enqueued); err != nil {
			rows.Close()
			return fmt.Errorf("scan expired: %w", err)
		}
		e.enqueued, err = parseStoredTime(enqueued)
		if err != nil {
			rows.Close()
			return fmt.Errorf("parse expired row: %w", err)
		}
		due = append(due, e)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate expired: %w", err)
	}
	rows.Close()

	for _, e := range due {
		if _, err := q.db.Exec(
			`DELETE FROM queue_inbox WHERE recipient = ? AND position = ?`,
			e.recipient, e.position,
		); err != nil {
			return fmt.Errorf("evict %q position %d: %w", e.recipient, e.position, err)
		}
		q.logger.Info("queued message expired",
			slog.String("recipient", e.recipient),
			slog.Int64("position", e.position),
			slog.String("message_id", e.messageID),
			slog.Duration("age", now.Sub(e.enqueued)),
		)
	}

	q.mu.Lock()
	q.rearmLocked()
	// Announce completion AFTER re-arming, so an observer that wakes on this
	// sees the queue's timer state already consistent with the sweep. Only
	// sweeps that actually EVICTED something signal: an empty sweep is not
	// the event anyone waits for, and a silent startup sweep keeps the
	// channel clean for real expiries.
	if len(due) > 0 {
		select {
		case q.swept <- struct{}{}:
		default:
		}
	}
	q.mu.Unlock()
	return nil
}

// Swept returns a channel receiving one signal per completed TTL sweep. It
// exists so tests on the fake clock can OBSERVE eviction as an event instead
// of sleeping out the watcher goroutine's scheduling (ts:test-harness: no
// test sleeps in real time). Production code has no reason to call it.
func (q *Queue) Swept() <-chan struct{} { return q.swept }

// rearmLocked points the sweep timer at the earliest outstanding deadline,
// or disarms it when retention is empty. Callers hold q.mu.
//
// Recomputed from the table rather than tracked incrementally: the fleet is
// under 20 devices at human messaging rates (arc:system-overview scale
// posture), so a MIN scan per mutation is noise, and one source of truth
// beats a cached minimum that every delete path must remember to maintain.
func (q *Queue) rearmLocked() {
	var earliest sql.NullString
	err := q.db.QueryRow(
		`SELECT MIN(enqueued_at) FROM queue_inbox`,
	).Scan(&earliest)
	if err != nil || !earliest.Valid || earliest.String == "" {
		// Empty retention (or unreadable): disarm. A nil timer parks the
		// watcher on a nil channel until the next enqueue nudges.
		if q.timer != nil {
			q.timer.Stop()
			q.timer = nil
		}
		q.nudge()
		return
	}
	at, err := parseStoredTime(earliest.String)
	if err != nil {
		// Unparseable stored time would break every future sweep; log loudly
		// and keep the old armament rather than spin.
		q.logger.Error("queue: parse earliest deadline failed",
			slog.String("reason", err.Error()),
		)
		return
	}
	delay := at.Add(q.ttl).Sub(q.clk.Now())
	if delay < 0 {
		delay = 0 // already due: fire immediately, sweep will remove it
	}
	if q.timer == nil {
		q.timer = q.clk.NewTimer(delay)
		return
	}
	q.timer.Reset(delay)
	q.nudge()
}

// marshalOpaque renders the stamped envelope into the stored BLOB. The
// envelope rides VERBATIM — message_id, both timestamps, payload — so replay
// cannot restamp received_at even by accident (the schema pins the ingress
// stamp; routing.go's stampedDelivery applied it once, at ingress).
func marshalOpaque(env *walkiev1.Envelope) ([]byte, error) {
	b, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return b, nil
}

// unmarshalOpaque reverses marshalOpaque. Position is deliberately NOT taken
// from the bytes: the stored form always carries zero (live-traffic marker);
// Resume stamps the real position from the row's key column.
func unmarshalOpaque(body []byte) (*walkiev1.Envelope, error) {
	var env walkiev1.Envelope
	if err := proto.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &env, nil
}

// stored-time helpers. UTC with a FIXED 9-digit fraction: unlike
// RFC3339Nano (which trims trailing zeros and thereby mis-orders strings),
// this layout sorts lexicographically in chronological order, which the TTL
// sweep's SQL comparison relies on. Same convention as migration 2's DDL
// comment; presence's tables use plain RFC3339Nano because they never
// compare timestamps in SQL.
const storedTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

func formatStoredTime(ts time.Time) string {
	return ts.UTC().Format(storedTimeLayout)
}

func parseStoredTime(s string) (time.Time, error) {
	ts, err := time.Parse(storedTimeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored time %q: %w", s, err)
	}
	return ts.UTC(), nil
}
