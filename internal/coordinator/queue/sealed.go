package queue

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/store"
)

// AtRest is the encryption seam adr:004-security-model puts between the
// coordinator's queue and its own storage: what a queued payload looks like
// while it RESTS in the coordinator's database. It is the concrete form of
// ts:queue-sealed-box's "marshal-before-store, ship-opaque-after-load" swap —
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
// The Open half is THE RECIPIENT'S operation, on the recipient's machine.
// Since the control-plane schema grew the SealedDelivery carrier (Envelope
// field 25), Resume ships the still-sealed box inside that payload and the
// recipient's client opens it with its own identity key — the coordinator
// never holds plaintext on the replay path at all. The old shape, where this
// package opened rows coordinator-side before unmarshalling, existed only
// because the drain contract lacked a carrier for opaque bytes; it is gone,
// not deprecated.
//
// # The bootstrap rule, decided and written down
//
// A message enqueues SEALED only if the recipient's key was ALREADY pinned at
// enqueue time. A recipient that has never announced (PublicKeyAnnounce) has
// no pin to seal to, so its messages rest PLAINTEXT — still TTL- and cap-
// bounded like every other row — and replay as their ordinary payload shape.
// They are deliberately NOT dropped (silence would lie to the sender) and
// NOT encrypted to nothing (a box no key can open is a drop with extra
// steps). Pins never apply retroactively to stored rows: the honest state is
// "sealed from the first pin onward", clients announce on first connect to
// keep the plaintext window one connection wide, and the unsealed hold logs
// an Info line so an operator can see exactly how much rests unencrypted.
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
// # Failure semantics
//
// A Seal error refuses the enqueue — nothing is stored, exactly like any
// other internal failure. There is deliberately NO Open half here to fail:
// authentication failures now surface at the recipient (crypto.ErrAuthFailed),
// which drops the whole frame loudly and acks the position — a box it cannot
// open will never become readable by holding it, and refusing the ack would
// freeze the high-water mark behind a dead position and force eternal
// redelivery of everything after it.
type AtRest interface {
	// Seal renders one marshalled envelope into the bytes that will rest in
	// storage for recipient. Called after the envelope is marshalled, before
	// the INSERT. sealed reports which form the returned bytes take: true
	// for a sealed box (the recipient's key was pinned), false for the
	// marshalled envelope verbatim (no pin yet — the bootstrap rule above).
	// An error refuses the enqueue (nothing is stored).
	Seal(recipient string, marshalled []byte) (stored []byte, sealed bool, err error)
}

// NewSealed returns a Queue whose resting bodies pass through atRest — the
// ts:queue-sealed-box wiring of [New], and the PRODUCTION constructor as of
// the SealedDelivery carrier: Enqueue seals to the recipient's pinned key,
// Resume emits SealedDelivery frames carrying the opaque ciphertext, and the
// coordinator structurally cannot read queued bodies. Everything else is
// identical to [New]: same retention bounds, same positions, same cursors,
// same schema. A nil atRest is refused here rather than silently behaving
// like [New], because a caller that asked for encrypted-at-rest and got
// plaintext must fail loudly, not quietly.
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

// PinnedKeyLookup is the slice of a TOFU key store the sealing seam needs:
// the pinned public key for a peer, if one exists. *crypto.Keystore satisfies
// this structurally; the narrow interface keeps this package from importing
// the keystore implementation and lets tests substitute fakes.
type PinnedKeyLookup interface {
	PinnedKey(peer string) ([32]byte, bool)
}

// keystoreSealer is the PRODUCTION AtRest: it seals each queued body to the
// recipient's pinned public key with the audited primitive (crypto.Seal,
// X25519 + XChaCha20-Poly1305), and — when the recipient has not announced a
// key yet — stores the envelope verbatim under the plain marker, logging the
// hold so the amount of plaintext-at-rest is always visible to the operator
// (the bootstrap rule on AtRest; never a silent drop, never encrypt-to-
// nothing).
type keystoreSealer struct {
	keys   PinnedKeyLookup
	logger *slog.Logger
}

// NewKeystoreSealer returns the production AtRest sealing against ks's pins.
// A nil logger falls back to slog's default: the unsealed-hold line is a
// security-relevant fact and must never be silently discarded.
func NewKeystoreSealer(keys PinnedKeyLookup, logger *slog.Logger) AtRest {
	if keys == nil {
		// Unreachable from correct wiring; refused rather than producing a
		// sealer that plaintexts EVERYTHING by lookup failure.
		panic("queue: NewKeystoreSealer: keys must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &keystoreSealer{keys: keys, logger: logger}
}

func (s *keystoreSealer) Seal(recipient string, marshalled []byte) ([]byte, bool, error) {
	pub, ok := s.keys.PinnedKey(recipient)
	if !ok {
		s.logger.Info("queued message stored UNSEALED: recipient key not pinned yet",
			slog.String("recipient", recipient),
			slog.Int("body_bytes", len(marshalled)),
		)
		return marshalled, false, nil
	}
	box, err := crypto.Seal(pub, marshalled)
	if err != nil {
		return nil, false, err
	}
	return box, true, nil
}

// Stored-body framing. One marker byte prefixes every BLOB this package
// writes, naming what follows:
//
//	storedPlainMarker  (0x00) || marshalled envelope  — rested unsealed
//	storedSealedMarker (0x01) || sealed box           — rested encrypted
//
// Why a marker rather than sniffing the bytes: a sealed box begins with its
// own version byte 0x01 and a marshalled envelope begins with field tags, so
// today the two happen to be distinguishable by inspection — but "happens to"
// is not a format. The marker is one explicit byte the queue itself writes
// from Seal's verdict, making Resume total over storage instead of lucky.
// Introduced together with the sealing flip; no release ever shipped the
// unframed format, so there is nothing to migrate.
const (
	storedPlainMarker  byte = 0x00
	storedSealedMarker byte = 0x01
)

// sealForStorage applies the AtRest seam on the enqueue boundary and frames
// the result with its marker. A nil seam (plain [New]) stores the envelope
// verbatim under the plain marker — byte-for-byte the pre-sealing behaviour
// existing tests pin, plus the one framing byte.
func (q *Queue) sealForStorage(recipient string, marshalled []byte) ([]byte, error) {
	if q.atRest == nil {
		return append([]byte{storedPlainMarker}, marshalled...), nil
	}
	stored, sealed, err := q.atRest.Seal(recipient, marshalled)
	if err != nil {
		return nil, fmt.Errorf("seal for storage: %w", err)
	}
	marker := storedPlainMarker
	if sealed {
		marker = storedSealedMarker
	}
	return append([]byte{marker}, stored...), nil
}

// deliveryFromStorage reverses the framing on the resume boundary and builds
// the wire envelope for one row. A sealed row becomes a SealedDelivery frame
// carrying the ciphertext VERBATIM — the coordinator cannot open it and must
// not mangle it; the recipient authenticates the exact bytes that rested. A
// plain row unmarshals to the original stamped envelope. An unknown marker
// means a row written by a future format this build cannot speak: skipped
// loudly (logged with recipient, position and reason class, never delivered,
// never partially processed) so one alien row cannot withhold the rest of
// the resume — the same posture undecryptable rows had when opening lived
// here.
func (q *Queue) deliveryFromStorage(recipient string, pos int64, messageID string, body []byte) (*walkiev1.Envelope, bool) {
	if len(body) < 1 {
		q.logger.Error("queued message unreadable: empty stored body",
			slog.String("recipient", recipient),
			slog.Int64("position", pos),
			slog.String("message_id", messageID),
		)
		return nil, false
	}
	marker, payload := body[0], body[1:]
	switch marker {
	case storedSealedMarker:
		// Position rides OUTSIDE the box (it is the ack handle); everything
		// else about the message — identity, timestamps, payload — stays
		// encrypted until the recipient opens it.
		return &walkiev1.Envelope{
			Position: uint64(pos),
			Payload: &walkiev1.Envelope_SealedDelivery{SealedDelivery: &walkiev1.SealedDelivery{
				Ciphertext: append([]byte(nil), payload...),
			}},
		}, true
	case storedPlainMarker:
		env, err := unmarshalOpaque(payload)
		if err != nil {
			q.logger.Error("queued message unreadable: body did not decode",
				slog.String("recipient", recipient),
				slog.Int64("position", pos),
				slog.String("message_id", messageID),
				slog.String("reason", err.Error()),
			)
			return nil, false
		}
		env.Position = uint64(pos)
		return env, true
	default:
		q.logger.Error("queued message unreadable: unknown storage format",
			slog.String("recipient", recipient),
			slog.Int64("position", pos),
			slog.String("message_id", messageID),
			slog.Int("format_marker", int(marker)),
		)
		return nil, false
	}
}
