package presence

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/store"
)

// Tracker is the server-authoritative presence core: it derives each device's
// online state from what the coordinator OBSERVES — a connection accepted, a
// heartbeat arriving, a connection ending, or a liveness deadline lapsing —
// and from nothing else.
//
// Owning work item:
//
//	eka get walkie/sto:device-presence
//
// # Why there is no "set online" method
//
// Criterion 5 of req:device-presence is architectural, not a validation rule:
// no code path may let a client assert its own online state. This type holds
// that invariant by construction. Its exported surface has exactly three
// inputs — [Tracker.ObserveHeartbeat], [Tracker.ConnectionEstablished] and
// [Tracker.ConnectionLost] — plus TTL expiry, and every one of them is an
// event the SERVER witnesses. There is no flag to flip, no "MarkOnline" to
// call, no client-sent payload this type would accept as liveness evidence.
// A future refactor that wanted to trust client-announced presence could not
// do it by misusing this API; it would have to invent a new one, and that is
// precisely the review checkpoint req:device-presence wants.
//
// The distinction matters because of how devices actually die. A killed
// process cannot send a goodbye: SIGKILL runs no handler, a power cut closes
// no socket politely, a pulled network cable delivers nothing. Any design
// where "offline" must be announced is a design where dead devices stay
// online forever — which req:device-presence calls worse than showing
// nothing, because it invites someone to rely on a device that is not there.
//
// # What each input means
//
//   - ConnectionEstablished: the accept loop resolved a tailnet identity and
//     upgraded the WebSocket (adr:004-security-model gate). That is first-hand
//     evidence of life, so the device goes online immediately — criterion 1,
//     "a connecting device appears online", taken literally.
//   - ObserveHeartbeat: an application-level Heartbeat arrived. Refreshes the
//     liveness lease; activity keeps a device alive.
//   - ConnectionLost: the connection's handler exited. A CLEAN close proves
//     aliveness up to that moment, so the device goes offline at once with
//     last-seen set (criterion 2). An abrupt severance produces the same call
//     eventually; when nothing arrives at all, only the TTL below catches it.
//   - TTL expiry: no heartbeat within [ttl] of the last observation. This is
//     the silent-death path — criterion 3, the defining test: a device whose
//     process died without announcing anything goes offline within the TTL,
//     having said nothing.
//
// Connected-but-silent therefore lapses after one TTL by design: the lease is
// what makes a half-open socket unable to masquerade as a live device.
//
// # Restart semantics — deliberate, not incidental
//
// On startup every loaded device is OFFLINE, and stays offline until it is
// observed alive again. This is not a limitation; it is the only honest
// answer. Whatever the previous process believed, those beliefs died with it:
// every device it might have marked online lost its connection when the
// process stopped, and none of them can be trusted to still be there.
//
// The strong form of the guarantee lives in the schema (migration 1 in
// internal/store): there is NO "online" column anywhere. Only liveness FACTS
// (last-seen) and user-authored LABELS (status) are persisted, so a stale
// "online" row cannot be resurrected on startup — not because the load path
// is careful, but because the database cannot represent the state. See the
// migration comment before adding such a column.
//
// # Concurrency shape
//
// One mutex guards all per-device state and the subscription registry. Timer
// firings arrive on per-lease watcher goroutines and re-enter the mutex; a
// generation counter per lease makes any fire that races a refresh harmless
// (see watch). Persistence failures are logged and non-fatal: memory stays
// authoritative for liveness, disk mirrors it so last-seen survives a
// coordinator restart. SetStatus is the exception — its persistence failure
// IS the caller's error, because criterion 4 makes survival a requirement,
// not a convenience.
type Tracker struct {
	ttl    time.Duration
	clk    clock.Clock
	db     *sql.DB
	logger *slog.Logger

	mu        sync.Mutex
	devices   map[string]*deviceState
	subs      map[*Subscription]struct{}
	closeOnce sync.Once
	closed    chan struct{}
}

// deviceState is everything the tracker knows about one device. Its two
// halves are deliberately distinct types: fact is server-derived liveness,
// label is user-authored text. Keeping them apart in the type structure is
// the point-of-read reminder that they have different authorities, different
// persistence tables and different rules — folding them into one flat struct
// is how "the status setter accidentally asserts liveness" bugs are born.
type deviceState struct {
	device string

	// conns counts live control connections for this device. A refcount
	// rather than a bool because reconnects overlap in practice: the new
	// connection's handler can start before the old one's exit callback
	// runs, and without counting, that ordering would flicker the device
	// offline between two facts of continuous life.
	conns int

	fact  liveness
	label statusLabel
}

// liveness is the server-derived half of a device's state: the FACT side.
// Everything here is written only by the tracker's observation paths.
type liveness struct {
	online   bool
	lastSeen time.Time

	// timer, quit and gen describe the CURRENT liveness lease. gen increments
	// on every observation; a watcher whose captured gen no longer matches is
	// watching a superseded lease and must not expire anything.
	timer clock.Timer
	quit  chan struct{}
	gen   uint64
}

// errTTLNotPositive reports a non-positive liveness TTL at construction.
var errTTLNotPositive = errors.New("presence: ttl must be positive")

// NewTracker returns a Tracker with the given liveness TTL, backed by st and
// driven by clk. It loads persisted liveness and status rows; everything
// loads OFFLINE — see "Restart semantics" on the type comment.
//
// ttl must be positive: a zero or negative TTL would expire devices the
// instant they were observed, which is a wiring bug, not a mode. Refusing
// rather than guessing follows the house rule for programmer errors.
func NewTracker(st *store.Store, clk clock.Clock, ttl time.Duration, logger *slog.Logger) (*Tracker, error) {
	if st == nil {
		return nil, fmt.Errorf("presence: new tracker: st must not be nil")
	}
	if clk == nil {
		return nil, fmt.Errorf("presence: new tracker: clk must not be nil")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("presence: new tracker: %w (got %s)", errTTLNotPositive, ttl)
	}
	if logger == nil {
		logger = slog.Default()
	}

	t := &Tracker{
		ttl:     ttl,
		clk:     clk,
		db:      st.DB(),
		logger:  logger,
		devices: make(map[string]*deviceState),
		subs:    make(map[*Subscription]struct{}),
		closed:  make(chan struct{}),
	}
	if err := t.loadPersisted(); err != nil {
		return nil, fmt.Errorf("presence: load persisted presence: %w", err)
	}
	logger.Info("presence tracker started",
		slog.Duration("ttl", ttl),
		slog.Int("devices_known", len(t.devices)),
	)
	return t, nil
}

// loadPersisted reads liveness facts and status labels into memory. Every
// device loads offline regardless of what the previous process believed —
// see "Restart semantics" on the type comment for why that is deliberate.
func (t *Tracker) loadPersisted() error {
	rows, err := t.db.Query(`SELECT device, last_seen FROM presence_liveness`)
	if err != nil {
		return fmt.Errorf("read presence_liveness: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var device, seen string
		if err := rows.Scan(&device, &seen); err != nil {
			return fmt.Errorf("scan presence_liveness: %w", err)
		}
		lastSeen, err := parseStoredTime(seen)
		if err != nil {
			return fmt.Errorf("presence_liveness row %q: %w", device, err)
		}
		d := t.deviceLocked(device)
		d.fact.lastSeen = lastSeen
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate presence_liveness: %w", err)
	}
	rows.Close()

	statusRows, err := t.db.Query(`SELECT device, status, updated_at FROM presence_status`)
	if err != nil {
		return fmt.Errorf("read presence_status: %w", err)
	}
	defer statusRows.Close()
	for statusRows.Next() {
		var device, text, updated string
		if err := statusRows.Scan(&device, &text, &updated); err != nil {
			return fmt.Errorf("scan presence_status: %w", err)
		}
		updatedAt, err := parseStoredTime(updated)
		if err != nil {
			return fmt.Errorf("presence_status row %q: %w", device, err)
		}
		d := t.deviceLocked(device)
		d.label.text = text
		d.label.updatedAt = updatedAt
	}
	if err := statusRows.Err(); err != nil {
		return fmt.Errorf("iterate presence_status: %w", err)
	}
	return nil
}

// ObserveHeartbeat records that device sent an application-level Heartbeat.
// It is the only client-originated input to liveness, and it carries no
// assertion — the fact is that bytes arrived, not anything the client claims.
func (t *Tracker) ObserveHeartbeat(device string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		return
	}
	d := t.deviceLocked(device)
	t.markAliveLocked(d, "heartbeat")
}

// ConnectionEstablished records that device's control connection completed
// the identity gate and is now live. Called by the accept loop after
// authentication succeeds — never by client action alone.
func (t *Tracker) ConnectionEstablished(device string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		return
	}
	d := t.deviceLocked(device)
	d.conns++
	t.markAliveLocked(d, "connect")
}

// ConnectionLost records that one of device's connections ended, cleanly or
// otherwise. When the last connection goes, the device goes offline
// immediately with last-seen set to now — a close, even an abrupt one,
// happened at a moment the device was demonstrably there.
//
// If a newer connection already re-established itself (handlers overlap
// during reconnect), the refcount absorbs the stale loss and the device
// stays online; see deviceState.conns.
func (t *Tracker) ConnectionLost(device string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		return
	}
	d := t.devices[device]
	if d == nil || d.conns == 0 {
		// No tracked connection to lose: the accept loop never told us one
		// began, or it was already accounted. Nothing observed changed.
		return
	}
	d.conns--
	if d.conns > 0 {
		return // another live connection holds the device up
	}
	if !d.fact.online {
		// Already expired by TTL while the zombie socket lingered. The
		// expiry recorded the honest last-seen; a late loss adds nothing.
		return
	}
	t.markOfflineLocked(d, "disconnect", t.clk.Now())
}

// Close stops every outstanding liveness lease and releases the watchers.
// It writes nothing: online state was never persisted, so there is nothing
// to flush and nothing to clear. Subscriptions are left for their owners to
// Close.
func (t *Tracker) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		t.mu.Lock()
		defer t.mu.Unlock()
		for _, d := range t.devices {
			t.endLeaseLocked(d)
		}
	})
	return nil
}

// checkClosed reports whether the tracker has been closed. Callers inside
// the mutex treat a closed tracker as a no-op rather than a panic: shutdown
// races (a heartbeat landing mid-Close) are ordinary, not defects.
func (t *Tracker) checkClosed() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}

// deviceLocked returns the state for device, creating an empty shell if
// needed. Creation implies nothing about liveness: a device known to the
// tracker starts offline until observed alive. Callers hold t.mu.
func (t *Tracker) deviceLocked(device string) *deviceState {
	d, ok := t.devices[device]
	if !ok {
		d = &deviceState{device: device}
		t.devices[device] = d
	}
	return d
}

// markAliveLocked records an observation of life at the current clock time
// and moves the liveness lease forward. reason distinguishes the observing
// path ("connect", "heartbeat") for logs and change events. Callers hold
// t.mu.
//
// Each observation gets a FRESH timer rather than resetting the old one.
// Reset-and-fire race windows differ subtly between the real and fake clock
// implementations (a fire committed just before a Reset can still sit in the
// channel); rotating the timer makes every watcher own exactly one deadline
// with no stale-fire ambiguity at all. At fleet scale — under 20 devices
// heartbeating at human intervals (arc:system-overview scale posture) — the
// allocation cost is noise next to the correctness it buys.
func (t *Tracker) markAliveLocked(d *deviceState, reason string) {
	now := t.clk.Now()
	wasOnline := d.fact.online
	d.fact.lastSeen = now
	d.fact.gen++

	// Rotate the lease: retire whatever watched the previous deadline, arm
	// a fresh timer, and give it a fresh watcher carrying the new gen.
	t.endLeaseLocked(d)
	tm := t.clk.NewTimer(t.ttl)
	d.fact.timer = tm
	d.fact.quit = make(chan struct{})
	go t.watch(d.device, tm, d.fact.gen, d.fact.quit)

	if !wasOnline {
		d.fact.online = true
		t.logger.Info("device online",
			slog.String("device", d.device),
			slog.String("reason", reason),
		)
		t.notifyLocked(Change{
			Device:   d.device,
			Online:   true,
			LastSeen: now,
			Status:   d.label.text,
		})
	} else {
		t.logger.Debug("heartbeat refreshed",
			slog.String("device", d.device),
		)
	}

	// Mirror last-seen to disk on EVERY observation, not only transitions:
	// the value's whole job is surviving a coordinator restart, and a
	// restart can land anywhere. Under 20 devices at human heartbeat rates
	// keeps this far inside SQLite's comfort zone (store.Open's durability
	// notes cover the write-amplification reasoning).
	t.saveLiveness(d.device, now)
}

// markOfflineLocked ends a device's online period. seenAt is the timestamp
// the offline transition reports as last-seen: NOW for a disconnect (the
// close proves presence up to that moment), the LAST OBSERVATION for a TTL
// expiry (the expiry moment says nothing about the device; the last time we
// saw it does). Callers hold t.mu.
func (t *Tracker) markOfflineLocked(d *deviceState, reason string, seenAt time.Time) {
	d.fact.online = false
	d.fact.lastSeen = seenAt
	t.endLeaseLocked(d)

	t.logger.Info("device offline",
		slog.String("device", d.device),
		slog.String("reason", reason),
		slog.Time("last_seen", seenAt),
	)
	t.notifyLocked(Change{
		Device:   d.device,
		Online:   false,
		LastSeen: seenAt,
		Status:   d.label.text,
	})
	t.saveLiveness(d.device, seenAt)
}

// endLeaseLocked retires the current liveness lease, if any: stops the timer
// and releases its watcher via quit. Safe to call with no lease outstanding.
// Callers hold t.mu.
func (t *Tracker) endLeaseLocked(d *deviceState) {
	if d.fact.timer != nil {
		d.fact.timer.Stop()
		d.fact.timer = nil
	}
	if d.fact.quit != nil {
		close(d.fact.quit)
		d.fact.quit = nil
	}
}

// watch waits for one lease's deadline and expires the device when it fires.
// One goroutine per lease; exits via quit (lease rotated or device offline)
// or by delivering the expiry.
//
// The gen check inside expire is what makes rotation safe: if this watcher's
// lease was superseded in the instant its timer fired, the fire is stale and
// must expire nothing — the fresher watcher owns the truth.
func (t *Tracker) watch(device string, tm clock.Timer, gen uint64, quit <-chan struct{}) {
	select {
	case <-quit:
	case <-tm.C():
		t.expire(device, gen)
	}
}

// expire applies a TTL lapse if gen is still current. Runs on the watcher
// goroutine; takes t.mu like every other mutation.
func (t *Tracker) expire(device string, gen uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		return
	}
	d := t.devices[device]
	if d == nil || !d.fact.online || d.fact.gen != gen {
		return // superseded by a newer observation; the fire is stale
	}
	// Criterion 3's path: nothing was announced — the device simply went
	// quiet past the TTL. Last-seen stays at the last observation, NOT the
	// expiry moment: the expiry says when WE noticed, the last observation
	// says when the device was last real.
	t.markOfflineLocked(d, "ttl-expiry", d.fact.lastSeen)
}

// saveLiveness mirrors one device's last-seen into presence_liveness.
// Failures are logged, never fatal: memory remains authoritative for
// liveness, and the mirrored value is display-grade history, not a
// correctness input. See the concurrency note on the type comment.
func (t *Tracker) saveLiveness(device string, lastSeen time.Time) {
	if _, err := t.db.Exec(
		`INSERT INTO presence_liveness (device, last_seen) VALUES (?, ?)
		 ON CONFLICT(device) DO UPDATE SET last_seen = excluded.last_seen`,
		device, formatStoredTime(lastSeen),
	); err != nil {
		t.logger.Error("presence: persist last-seen failed",
			slog.String("device", device),
			slog.String("reason", err.Error()),
		)
	}
}

// formatStoredTime renders a timestamp for a TEXT column: UTC RFC3339Nano,
// matching schema_migrations.applied_at so every stored time in the file is
// comparable by the same rule.
func formatStoredTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339Nano)
}

// parseStoredTime reverses formatStoredTime.
func parseStoredTime(s string) (time.Time, error) {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored time %q: %w", s, err)
	}
	return ts.UTC(), nil
}
