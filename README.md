# walkie

A terminal-native communication tool for a small team whose devices already
share a Tailscale tailnet. Text, presence and voice, without asking any device
to install a graphical environment.

## Status

**Not implemented.** The architecture is decided and approved; the code is not
written. The repository currently holds the module scaffold and the committed
engineering knowledge.

The design does not live in these files. It lives in the EKA knowledge base:

```sh
eka context walkie/scp:mvp-text-voicenote --depth engineering --json  # the MVP and what binds it
eka get walkie/arc:system-overview                                    # component architecture
eka get architecture --no-content --compact                           # every decision at a glance
eka view execution                                                    # the work board
eka get containers                                                    # container membership
```

Sixteen work items are planned and ticketed under `walkie/ctr:wave-1`. Read
`CLAUDE.md` before writing code — it carries the invariants that constrain every
package, and the constraints that produce wrong code if you do not know them.

## Two build variants

This is not a convenience; it is `adr:002-runtime-stack`. The default build is
pure Go and fully static, so it cross-compiles to every target — including the
ARM devices in the fleet — with no C toolchain. Audio needs CGO, so it lives
behind the `voice` build tag.

```sh
make build         # default: text, presence, files. Pure Go, static.
make build-voice   # adds audio capture and playback.
make check-cgo     # the guard: the default tree must build with CGO_ENABLED=0
```

A device that cannot install the audio dependencies is still a completely useful
walkie client. Capability is a property of a device's build, not of the
deployment. `walkie --version` reports which variant you have.

## Release artifacts

The release pipeline — the full cross-compile matrix as checksummed,
static-link-verified, reproducibility-checked artifacts, plus the coordinator
container image — is documented in `docs/release/README.md`. Read it before
distributing anything: release artifacts are checksummed but **not signed**
until phase 4's auto-update work, and that file states precisely what that
means and what it does not.

## Development

```sh
make help          # list targets
make test          # default variant
make test-voice    # with the voice build tag
make proto         # regenerate protobuf into internal/genproto
make proto-check   # fail if the committed generated code drifted
make lint          # go vet + buf lint
```

Generated protobuf code is committed on purpose, so `go build` needs no codegen
toolchain. `exchange/` is committed on purpose too — it is the EKA knowledge
snapshot, regenerated with `eka sync`, never by hand.

## Layout

| Path | What |
| --- | --- |
| `cmd/walkie` | the client |
| `cmd/walkie-coordinator` | the coordination server |
| `proto/walkie/v1` | control-plane schema |
| `internal/audio` | capture and codec seam, build-tag isolated |
| `internal/clock`, `internal/testnet` | the injectable time and network the tests need |
| `internal/control` | client-side control plane: reconnect, resume, heartbeat |
| `internal/coordinator` | server: `tsauth`, `presence`, `queue` |
| `internal/crypto` | sealed box and trust-on-first-use keystore |
| `internal/ctlsocket` | local control socket, for scripting |
| `internal/history`, `internal/message`, `internal/obs`, `internal/store`, `internal/tui`, `internal/voicenote` | the rest of the MVP |

Each package's doc comment names the work item that owns it. Phase-2 concerns —
real-time voice, the direct data plane, file transfer — have no packages yet on
purpose; `plan:roadmap-v1` sequences them after the MVP.

## Message history

Every message you send or receive is stored on your device in a plain SQLite
database (default `~/.walkie/history.db`), so history survives restarts.

**Your message history is NOT encrypted.** Anyone who can read your device's
disk can read every message: another user account on a shared machine, someone
holding your stolen laptop, a copied disk image or backup. walkie does not
encrypt local history. This is a deliberately accepted limitation recorded in
`adr:004-security-model`, not an oversight — full-disk encryption is the real
mitigation, and it is configured outside walkie. File permissions are
owner-only (0600 file, 0700 directory), which stops other local users on a
multi-user machine and nothing beyond that.

History is bounded: messages older than 30 days are removed, and the store
never holds more than 10,000 messages (oldest evicted first, loudly logged).
Retention logs record counts, never message content.

Query it from the command line — one JSON object per line, made for scripts:

```sh
walkie history -conversation alice.tail-scale.ts.net.     # one conversation
walkie history -from 2026-08-01T09:00:00Z -to 2026-08-01T17:00:00Z
walkie history -conversation broadcast -db /path/to/history.db
```

See `walkie history -help` for the full flag list; the same security statement
ships in that help output. When the quickstart and runbook land
(`ts:docs-quickstart-runbook`), they must carry this unencrypted-at-rest
statement forward so users keep being told after this README is rewritten.

## Queued messages are encrypted

Messages waiting for an offline device in the coordinator's queue are stored
as ciphertext. Each device generates an X25519 identity key on first run
(stored with owner-only permissions: 0600 file, 0700 directory). The
coordinator seals each queued payload to the RECIPIENT's pinned public key
with X25519 + XChaCha20-Poly1305 (`adr:004-security-model`) before writing it
to its database, and holds only public keys — so a compromised coordinator can
hold queued messages but can never read them. Only the holder of the
recipient's identity key can decrypt what was queued for it; losing that key
forfeits exactly those messages.

Direct live traffic is NOT additionally encrypted — it relies on WireGuard;
that is a deliberate scope boundary, not an oversight.

**Losing your device's key forfeits your queued messages.** Queued messages
are sealed to your key and there is no backup, no recovery and no escrow.
Lose the key file and everything parked for you becomes unreadable padding.

**Key rotation and multi-device identity are not implemented.** Re-installing
walkie generates a fresh key, and every peer will treat that as a key change
and demand confirmation before trusting it. That friction is the design
working.

Trust is on first use: the first key seen for a peer is pinned. If a peer
later presents a different key, walkie warns loudly and requires explicit
confirmation before using it — headless and scripted contexts refuse by
default rather than assume consent. To verify a peer out of band, compare
fingerprints read aloud over a call: a walkie fingerprint is twenty decimal
digits in five groups of four (e.g. `4677 6887 2937 2977 8267`), derived from
SHA-256 of the peer's public key.

Status note: queue encryption is ACTIVE. The coordinator seals every queued
message to the recipient's pinned key before writing it to its database and
replays it as an opaque sealed frame the recipient's client opens with its own
identity key — the coordinator can hold queued messages but structurally cannot
read them. One honest bootstrap window remains: a message held for a device
that has never announced a key rests plaintext (still TTL- and size-bounded)
until that first announce pins the key; pins never apply retroactively, and
every such hold is logged. The key-loss and no-rotation statements above
describe exactly this end state.

When the quickstart and runbook land (`ts:docs-quickstart-runbook`), they must
carry the key-loss statement, the no-rotation statement and the fingerprint
verification procedure forward so users keep being told after this README is
rewritten.

## Remote shell access

walkie does not provide it, by decision rather than omission — see
`adr:005-no-remote-command-execution`. Use **Tailscale SSH**, which already has
ACL-based authorization, audit and session recording. Building a second, less
reviewed remote-execution path would be a net security regression.

## User documentation

The quickstart and the operations runbook are not written yet. They are
`ts:docs-quickstart-runbook`, and its acceptance criteria are worth reading
before starting: the quickstart must get a non-technical reader to a working
session in three commands, verified with a real person rather than assumed.
