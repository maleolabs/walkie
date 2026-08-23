package coordinator

import (
	"context"
	"errors"
	"log/slog"

	"github.com/maleolabs/walkie/internal/coordinator/tsauth"
	"github.com/maleolabs/walkie/internal/crypto"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// This file is ts:queue-sealed-box's key-distribution half: how X25519 public
// keys travel between clients and the coordinator, and where the TOFU rule
// (crypto.Keystore) is enforced on the wire.
//
// # The flow
//
//   - A client publishes its identity public key with PublicKeyAnnounce (the
//     schema's client→coordinator direction). The coordinator runs it through
//     [Keystore.Authorize]: first key seen for a peer is PINNED; a DIFFERENT
//     key for a known peer warns loudly and is REFUSED unless explicitly
//     confirmed — and this process is headless, so its ConfirmFunc is nil and
//     refusal is the default. Silent acceptance is the defect criterion 5
//     names; there is no flag anywhere here that defaults to accepting.
//   - The coordinator serves the full pin table as a PublicKeyDirectory
//     snapshot: once at Hello (after the queue drain it describes), and again
//     to every live connection whenever a pin is added or confirmed-changed.
//     Snapshot-at-hello plus replace-on-change is the delivery shape the
//     schema comment prescribes: the consumer must be able to assume a
//     complete table before acting on any single key.
//
// # What this does NOT do
//
// It does not encrypt anything on the direct live path — announce and
// directory are metadata distribution, and adr:004 keeps live traffic on
// WireGuard alone. It does not authenticate anything new: the announcer's
// identity is the connection's WhoIs-resolved tailnet name, same as every
// other frame (adr:004); the pin binds THAT name to THIS key, first-seen.
//
// # Recovery when a changed key is refused (headless)
//
// There is deliberately no interactive prompt and no environment variable
// that flips acceptance on: an automation-readable accept switch is assumed
// consent with extra steps. The operator path is manual by construction —
// read both fingerprints from the log lines, verify them out of band with
// the peer, then stop the coordinator and correct or remove that peer's pin
// in the keystore file by hand (owner-only JSON). The friction IS the
// control.

// WireKeys attaches the TOFU keystore and the changed-key confirmation gate.
//
// confirm is the human gate behind a CHANGED key for a known peer; nil means
// refuse — the headless default, and this binary's only mode today. Call
// before Serve, alongside the other pre-serve wiring: a keystore attached
// mid-flight would make the directory snapshot different connections see
// depend on when they connected relative to the wiring, which is state no
// reader can reason about.
//
// A nil ks disables key distribution entirely: announces are then ignored
// like any payload this build does not know (adr:003 mixed-fleet rule), the
// same posture the presence tracker and offline sink have as their nil form.
func (s *Server) WireKeys(ks *crypto.Keystore, confirm crypto.ConfirmFunc) {
	s.keys = ks
	s.confirm = confirm
}

// handleKeyAnnounce applies the TOFU rule to one client→coordinator
// PublicKeyAnnounce.
//
// Outcomes:
//
//   - malformed key bytes: structured MALFORMED ProtocolError to the sender,
//     connection stays open (a wrong-length key is a correctable client bug,
//     same posture as an oversized draft);
//   - first key for the peer: pinned, logged, and a fresh directory snapshot
//     fans out to every live connection;
//   - unchanged key: idempotent no-op (the schema promises this);
//   - changed key: loud warning + refusal unless s.confirm accepts — the
//     keystore owns that whole decision and its logs; here it surfaces as a
//     structured error back to the ANNOUNCER so a client never believes its
//     new key took when it did not.
func (s *Server) handleKeyAnnounce(ctx context.Context, out *connWriter, id tsauth.Identity, ann *walkiev1.PublicKeyAnnounce) bool {
	if s.keys == nil {
		// No keystore wired: ignore quietly, exactly like an unknown payload
		// type — an older coordinator must survive a newer client's frames.
		s.logger.Debug("public_key_announce ignored: no keystore wired",
			slog.String("node_name", id.NodeName),
		)
		return true
	}

	pub, err := crypto.ParsePublicKey(ann.GetPublicKey())
	if err != nil {
		s.logger.Warn("public key announce rejected",
			slog.String("node_name", id.NodeName),
			slog.Int("key_bytes", len(ann.GetPublicKey())),
			slog.String("reason", err.Error()),
		)
		s.rejectMalformed(ctx, out, "public_key_announce.public_key rejected: "+err.Error())
		return true
	}

	device := deviceOf(id)
	if err := s.keys.Authorize(device, pub, s.confirm); err != nil {
		var refused *crypto.KeyChangeRefused
		if errors.As(err, &refused) {
			// The loud warning and the refusal line are already in the log
			// (Keystore.Authorize). On the wire, the announcer learns its
			// key did NOT take — silence here would leave a re-installed
			// device believing peers will seal to a key nobody trusts.
			// MALFORMED is the schema's receiver-enforced-rule code; the
			// rule violated is the TOFU confirmation duty. Fingerprints
			// only in the detail — never key bytes.
			out.write(ctx, &walkiev1.Envelope{
				Payload: &walkiev1.Envelope_ProtocolError{
					ProtocolError: &walkiev1.ProtocolError{
						Code: walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
						Detail: "public_key_announce refused: key differs from the pinned key for this device " +
							"(pinned " + refused.PinnedFingerprint + ", presented " + refused.PresentedFingerprint + "); " +
							"operator confirmation required",
					},
				},
			})
			return true
		}
		// Storage-class failure persisting the pin: internal, logged, no
		// wire blame — the client may retry its announce after reconnect.
		s.logger.Error("public key announce failed",
			slog.String("device", device),
			slog.String("reason", err.Error()),
		)
		return true
	}

	// Pinned now (first contact) or confirmed changed: the fleet's view of
	// "who holds which key" moved, so every live connection gets the fresh
	// snapshot. An unchanged key lands on the idempotent no-op path inside
	// Authorize and reaches neither branch — no directory spam per heartbeat
	// of chatter.
	s.broadcastDirectory(ctx)
	return true
}

// broadcastDirectory pushes the current pin table to every live connection.
// Called after a pin ADDED or CONFIRMED-CHANGED; the announcer receives it
// too, which costs one idempotent snapshot and keeps the rule "every
// connection sees the same table" simpler than special-casing one writer.
func (s *Server) broadcastDirectory(ctx context.Context) {
	env := s.directoryEnvelope()
	if env == nil {
		return
	}
	for _, w := range s.routeTargetsAll() {
		w.write(ctx, env)
	}
}

// directoryEnvelope renders the current pin table as a PublicKeyDirectory
// snapshot envelope, or nil when no keystore is wired. Entries arrive in the
// keystore's stable peer-name order, so two snapshots of an unchanged table
// are byte-identical — a property tests pin and mixed fleets appreciate.
func (s *Server) directoryEnvelope() *walkiev1.Envelope {
	if s.keys == nil {
		return nil
	}
	entries := s.keys.Entries()
	dir := &walkiev1.PublicKeyDirectory{
		Entries: make([]*walkiev1.PublicKeyEntry, 0, len(entries)),
	}
	for _, e := range entries {
		key := e.Key // copy the array so the snapshot cannot alias the store
		dir.Entries = append(dir.Entries, &walkiev1.PublicKeyEntry{
			Device:    e.Peer,
			PublicKey: append([]byte(nil), key[:]...),
		})
	}
	return &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PublicKeyDirectory{PublicKeyDirectory: dir},
	}
}
