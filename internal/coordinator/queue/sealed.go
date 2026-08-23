package queue

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/store"
)

// AtRest is the encryption seam adr:004-security-model puts between the
// coordinator's queue and its own storage: what a queued payload looks like
// while it RESTS in the coordinator's database. It is the concrete form of
// ts:queue-sealed-box's "marshal-before-store, unmarshal-after-load" swap —
// the two boundary points sto:offline-queue's shape reserved for exactly this.
//
// # Who decrypts what, stated once and precisely
//
// Sealing is TO THE RECIPIENT's pinned public key, which is the only key the
// coordinator holds for any peer (crypto.Keystore pins public keys; nothing
// in this process ever constructs a private key). The coordinator can
// therefore seal and store but can never open: only the holder of the
// recipient's X25519 identity key can decrypt a stored body. That asymmetry
// is the point of the item — a compromised staging server exposes queued
// message bodies to nobody, and losing a device's key forfeits exactly that
// device's queued messages (criterion 7), because no other copy of the
// ability to read them exists anywhere.
//
// The Open half is therefore THE RECIPIENT'S operation by design. In the
// final shape the coordinator ships the still-sealed box and the recipient's
// client opens it on receipt. The control-plane schema does not yet carry a
// payload able to hold opaque sealed-box bytes (the QueueDrain contract
// returns decoded envelopes), so until that additive schema change lands,
// the open half is wired on the coordinator side of the drain — in tests
// with the recipient's identity, which is the same key operation the
// recipient will perform. Wiring sealing into production before that carrier
// exists would strand every queued message undeliverable, so production
// keeps the plaintext queue until then; see cmd/walkie-coordinator.
//
// # Phase 2: the relay fallback reuses this seam unchanged
//
// adr:001's relay fallback (not built in the MVP) makes the coordinator a
// relay for payloads between two peers — the second blind spot adr:004
// names. Those payloads seal against the DESTINATION peer's pinned key and
// are opened by its holder, exactly as queued payloads do here; the relay
// stores/forwards opaque boxes through this same boundary. No relay code
// belongs in this package; when phase 2 arrives it consumes AtRest as-is.
//
// # Failure semantics are drop-deliberately, never partial
//
// An Open failure means the box did not authenticate (crypto.ErrAuthFailed)
// or cannot be parsed. There is no plaintext to act on and no way to act on
// part of an AEAD-verified box, so the row is SKIPPED loudly — logged with
// recipient, position and reason class, never delivered, never partially
// processed — and the rest of the resume continues. Deliberate dropping is
// the specified outcome for an undecryptable message (a lost recipient key
// forfeits its queued messages; criterion 7): keeping undecryptable rows
// forever would poison every future resume behind one bad row, while Ack's
// high-water delete already bounds how long a skipped row can linger.
type AtRest interface {
	// Seal renders one marshalled envelope into the bytes that will rest in
	// storage for recipient. Called after the envelope is marshalled, before
	// the INSERT; an error refuses the enqueue (nothing is stored).
	Seal(recipient string, marshalled []byte) ([]byte, error)

	// Open reverses Seal for one row coming back off storage for recipient.
	// Called after the SELECT, before the envelope is unmarshalled. An error
	// skips the row (see the failure semantics above); it never yields
	// partial content.
	Open(recipient string, stored []byte) ([]byte, error)
}

// NewSealed returns a Queue whose resting bodies pass through atRest — the
// ts:queue-sealed-box wiring of [New]. Everything else is identical: same
// retention bounds, same positions, same cursors, same schema. A nil atRest
// is refused here rather than silently behaving like [New], because a caller
// that asked for encrypted-at-rest and got plaintext must fail loudly, not
// quietly.
//
// The plain [New] constructor remains the production wiring until the
// control-plane schema grows a sealed-delivery carrier (see the AtRest comment
// for why enabling earlier would strand deliveries).
func NewSealed(st *store.Store, clk clock.Clock, ttl time.Duration, maxSize int, logger *slog.Logger, atRest AtRest) (*Queue, error) {
	if atRest == nil {
		return nil, fmt.Errorf("queue: new sealed: atRest must not be nil (use New for the plaintext queue)")
	}
	q, err := newQueue(st, clk, ttl, maxSize, logger)
	if err != nil {
		return nil, err
	}
	q.atRest = atRest
	return q, nil
}

// sealForStorage applies the AtRest seam on the enqueue boundary. A nil seam
// (plain [New]) passes bytes through unchanged, byte-for-byte the pre-sealing
// behaviour existing tests pin.
func (q *Queue) sealForStorage(recipient string, marshalled []byte) ([]byte, error) {
	if q.atRest == nil {
		return marshalled, nil
	}
	sealed, err := q.atRest.Seal(recipient, marshalled)
	if err != nil {
		return nil, fmt.Errorf("seal for storage: %w", err)
	}
	return sealed, nil
}

// openFromStorage applies the AtRest seam on the resume boundary and reports
// whether the row is deliverable. A nil seam passes through; an Open failure
// logs loudly and reports false — the caller skips the whole row (never
// delivers, never partially processes) and continues with the rest.
func (q *Queue) openFromStorage(recipient string, position int64, messageID string, stored []byte) ([]byte, bool) {
	if q.atRest == nil {
		return stored, true
	}
	opened, err := q.atRest.Open(recipient, stored)
	if err != nil {
		// Loud, content-free: recipient, position and message_id identify
		// the row; the reason class says why it died. Neither the stored
		// bytes nor any key material belongs in this line.
		q.logger.Error("queued message undecryptable: dropped deliberately",
			slog.String("recipient", recipient),
			slog.Int64("position", position),
			slog.String("message_id", messageID),
			slog.String("reason", err.Error()),
		)
		return nil, false
	}
	return opened, true
}
