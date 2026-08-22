package presence

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"
)

// This file is the LABEL half of presence: the custom status message a user
// writes for themselves. It is deliberately the only place client-supplied
// text can enter this package's state, and it must never be confused with
// the liveness FACT half in tracker.go:
//
//   - Liveness is derived by the coordinator from what it observes; no
//     client input can set it (criterion 5, see the Tracker comment).
//   - Status is genuinely client-supplied and that is fine (criterion 4):
//     it is a roster line, not a claim of being alive. Setting one says
//     nothing about being online, and going offline never clears one.
//
// The two live in different types (statusLabel vs liveness), different
// tables (presence_status vs presence_liveness) and different files, so the
// next reader meets the distinction before they meet any code that could
// blur it.

// MaxStatusBytes is the receiver-enforced bound on a custom status: at most
// 256 bytes of UTF-8 — a roster line, not a message.
//
// Why enforced here rather than truncated: ts:protocol-schema-v1's
// PresenceStatusChange field doc requires the receiver to REJECT an
// oversized status rather than quietly shorten it. Truncation would silently
// publish something the user did not write; rejection tells the client to
// push back on its own user instead.
const MaxStatusBytes = 256

// statusLabel is the LABEL half of a device's tracker state: user-authored
// text plus when it was last set. It exists as its own type so the
// fact/label split is visible in every struct that holds both halves.
type statusLabel struct {
	text      string
	updatedAt time.Time
}

var (
	// ErrStatusTooLong reports a status exceeding [MaxStatusBytes] bytes.
	ErrStatusTooLong = errors.New("presence: status exceeds 256-byte bound")

	// ErrStatusNotUTF8 reports a status that is not valid UTF-8. The wire
	// format is protobuf strings, which are UTF-8 by definition; bytes that
	// fail validation are a broken encoder, not text to store.
	ErrStatusNotUTF8 = errors.New("presence: status is not valid UTF-8")
)

// SetStatus records device's user-authored custom status label. An empty
// string clears the status. The label persists immediately (criterion 4: it
// must survive the setting device's reconnect — and a coordinator restart),
// and a change event carrying the CURRENT liveness fact plus the new label
// is emitted so connected clients re-render the roster line.
//
// Setting a status never changes liveness in either direction: an offline
// device's stored label updates all the same, and an online device stays
// online exactly as long as its heartbeats say so.
func (t *Tracker) SetStatus(device string, status string) error {
	if len(status) > MaxStatusBytes {
		return fmt.Errorf("%w: %d > %d bytes", ErrStatusTooLong, len(status), MaxStatusBytes)
	}
	if !utf8.ValidString(status) {
		return ErrStatusNotUTF8
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.checkClosed() {
		return errors.New("presence: tracker closed")
	}

	now := t.clk.Now()

	// Persist BEFORE committing to memory: if the write fails, criterion 4's
	// survival promise has already been broken for any future reconnect, so
	// the caller must hear about it rather than watch the label silently
	// evaporate later. Memory commits only once disk has accepted the label.
	if _, err := t.db.Exec(
		`INSERT INTO presence_status (device, status, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(device) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at`,
		device, status, formatStoredTime(now),
	); err != nil {
		return fmt.Errorf("presence: persist status for %s: %w", device, err)
	}

	d := t.deviceLocked(device)
	d.label.text = status
	d.label.updatedAt = now

	t.logger.Info("device status changed",
		slog.String("device", d.device),
		slog.Int("len_bytes", len(status)),
		slog.Bool("cleared", status == ""),
	)

	t.notifyLocked(Change{
		Device:   d.device,
		Online:   d.fact.online,
		LastSeen: d.fact.lastSeen,
		Status:   d.label.text,
	})
	return nil
}
