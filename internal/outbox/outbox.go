// Package outbox is the client-side hold for messages composed while
// disconnected: they are persisted before any wire byte exists and drained in
// composition order on reconnect.
//
// Owning work item:
//
//	eka get walkie/sto:offline-queue
//
// req:offline-delivery criterion 3 is this package's whole reason to exist:
// "a message typed while the sender is offline is not lost — the sender holds
// it locally and transmits on reconnect."
//
// # Durability decision, stated rather than implied
//
// The outbox PERSISTS through the same store seam as everything else: an
// envelope handed to [Outbox.Enqueue] is committed to SQLite before Enqueue
// returns, so it survives the client process crashing or being killed while
// disconnected. What it does NOT survive is deletion of the store file — the
// same thing no other local state survives — and, until ts:queue-sealed-box's
// sibling work lands for local history, the bytes rest as plaintext under the
// restrictive file permissions store.Open enforces (local encryption is a
// recorded gap with no phase; it is not silently pretended away here).
//
// # Removal semantics are at-least-once, on purpose
//
// An entry leaves the outbox only when its transmit SUCCEEDS ([Outbox.Remove]
// after the transport accepted the frame). A crash between wire-write and
// removal loses nothing but can duplicate one message on the next drain —
// which is exactly the at-least-once contract: delivery is deduplicated at
// the receiver by ULID (Envelope.message_id), never prevented by the
// transport. Attempting exactly-once here would need acks that do not exist
// in the protocol (req:text-messaging has none) and would buy fragility.
//
// # Ordering
//
// Drain order is composition order (FIFO), and a FAILED transmit stops the
// drain with everything from the failure onward still queued. That
// head-of-line blocking is deliberate: per-recipient ordering within a
// conversation is the only ordering walkie promises (see internal/message),
// and skipping a stuck message would deliver its successors first — an
// order the receiver has no way to reconstruct.
//
// # Who drives this
//
// There is deliberately no dialing, reconnect or read loop here:
// ts:reconnect-resume owns the connection lifecycle and will call Enqueue as
// messages are composed offline and Pending/Remove around each successful
// write. This package works against whatever transport function the caller
// provides, which is what keeps it testable without a coordinator.
package outbox

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
	"google.golang.org/protobuf/proto"
)

// Outbox is one device's durable hold of composed-but-untransmitted
// envelopes. Safe for concurrent use: all state lives in the store, whose
// single connection serialises writers (store.Open's ordering promise).
type Outbox struct {
	db     *sql.DB
	clk    clock.Clock
	logger *slog.Logger
}

// New returns an Outbox backed by st. Any rows already in the table were
// committed by a previous process incarnation and remain pending — that IS
// the durability contract, not a reload step.
func New(st *store.Store, clk clock.Clock, logger *slog.Logger) (*Outbox, error) {
	if st == nil {
		return nil, fmt.Errorf("outbox: new: st must not be nil")
	}
	if clk == nil {
		return nil, fmt.Errorf("outbox: new: clk must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Outbox{db: st.DB(), clk: clk, logger: logger}, nil
}

// Enqueue persists env as pending transmission. It returns only after the
// commit is durable (store.Open's promise), which is the whole point: a
// caller may compose while disconnected, crash, and find the message here
// after restart. A duplicate message_id is refused — ULIDs are minted fresh
// per composition (internal/message), so a collision means a bug upstream,
// and storing two rows under one identity would corrupt the removal path.
func (o *Outbox) Enqueue(env *walkiev1.Envelope) error {
	if env.GetMessageId() == "" {
		return fmt.Errorf("outbox: enqueue: envelope has no message_id")
	}
	body, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: enqueue %s: %w", env.GetMessageId(), err)
	}
	if _, err := o.db.Exec(
		`INSERT INTO outbox_message (message_id, envelope, queued_at) VALUES (?, ?, ?)`,
		env.GetMessageId(), body, formatStoredTime(o.clk.Now()),
	); err != nil {
		return fmt.Errorf("outbox: enqueue %s: %w", env.GetMessageId(), err)
	}
	o.logger.Info("message held in outbox",
		slog.String("message_id", env.GetMessageId()),
	)
	return nil
}

// Pending returns every queued envelope in composition order (oldest first).
// The snapshots are independent copies — marshaled bytes decoded fresh — so
// draining while another goroutine composes cannot alias storage.
func (o *Outbox) Pending() ([]*walkiev1.Envelope, error) {
	rows, err := o.db.Query(`SELECT envelope FROM outbox_message ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("outbox: pending: %w", err)
	}
	defer rows.Close()

	var out []*walkiev1.Envelope
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, fmt.Errorf("outbox: pending: scan: %w", err)
		}
		var env walkiev1.Envelope
		if err := proto.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("outbox: pending: decode: %w", err)
		}
		out = append(out, &env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: pending: iterate: %w", err)
	}
	return out, nil
}

// Remove drops one transmitted envelope from the outbox. Call it ONLY after
// the transport accepted the frame: removing earlier converts a failed send
// into a silent loss, the exact outcome this package exists to prevent. An
// unknown message_id is not an error — removal is idempotent so a retry after
// a half-finished drain stays honest.
func (o *Outbox) Remove(messageID string) error {
	res, err := o.db.Exec(`DELETE FROM outbox_message WHERE message_id = ?`, messageID)
	if err != nil {
		return fmt.Errorf("outbox: remove %s: %w", messageID, err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		o.logger.Info("outbox message transmitted",
			slog.String("message_id", messageID),
		)
	}
	return nil
}

// Count reports how many envelopes are pending — diagnostics and tests, not
// a hot path.
func (o *Outbox) Count() (int, error) {
	var n int
	if err := o.db.QueryRow(`SELECT COUNT(*) FROM outbox_message`).Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox: count: %w", err)
	}
	return n, nil
}

// stored-time helper: UTC RFC3339Nano matching schema_migrations.applied_at.
// The outbox never compares timestamps in SQL (order is seq's job), so the
// plain Nano layout — same as presence and the migration ledger — is fine
// here, unlike the queue's fixed-width variant.
func formatStoredTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339Nano)
}
