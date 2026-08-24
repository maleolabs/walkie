# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository currently is

walkie is a terminal-native communication tool for devices sharing a Tailscale tailnet: text, presence, and voice. The MVP's **text-and-presence core is implemented** — Go module, `cmd/walkie` (client) and `cmd/walkie-coordinator` (server), the `internal/` packages, a `Makefile`, and CI. Thirteen of `ctr:wave-1`'s sixteen work items are done; voice notes (`sto:voice-note`) are still todo and no build records or plays audio today. Later phases (calls, direct data plane, file transfer, rooms, PTY streaming, plugins, auto-update) have no code on purpose.

**The design is not in this repository's files — it is in the EKA knowledge base.** Read it there before writing code. User-facing documentation lives in `docs/quickstart.md` and `docs/runbook.md`; release artifacts are documented in `docs/release/README.md`.

## Read the knowledge base first

The full authority chain lives in the EKA workspace under project/namespace `walkie`. Do not reconstruct intent from code or from `exchange/snapshots/**` JSON — use the retrieval commands:

```sh
eka context walkie/scp:mvp-text-voicenote --depth engineering --json   # the MVP and everything binding it
eka get walkie/arc:system-overview                                     # component architecture
eka get architecture --no-content --compact                            # all decisions at a glance
eka get walkie/adr:001-transport-topology                              # one decision in full
eka get containers                                                     # execution container + membership
eka view execution                                                     # human board projection
```

`eka get <unit> --upstream` walks the traceability chain. **`eka get --downstream` is broken** — it silently returns no `downstream` field even when references exist (maleolabs/eka-cli#24). Never read an absent `downstream` as "nothing depends on this".

Approved in force: `vis:terminal-mesh-comms`, five `req:` units, `arc:system-overview`, `adr:001`–`adr:005`, `scp:mvp-text-voicenote`, `plan:roadmap-v1` (planningState approved). Two findings — `fnd:tailnet-capability-baseline` and `fnd:terminal-audio-constraints` — are deliberately held at `review`, not approved, because their latency figures are calculated rather than measured. `ses:planning-2026-08-20` records why each decision was taken, including one disagreement that was overruled.

## Architecture invariants

These constrain every line of code. They come from accepted ADRs; violating one means the ADR must be revised first, not worked around.

**The tailnet is the substrate (`adr:001`, `fnd:tailnet-capability-baseline`).** Tailscale already provides transport encryption, NAT traversal with DERP fallback, stable addressing, and authenticated peer identity. Never implement STUN/TURN. **Never build authentication** — the coordinator resolves the caller's tailnet identity with a `WhoIs` lookup on the connection's remote address. There is no password, token, session store, or user table anywhere in the design, and adding one is a design regression.

**Two planes, and the split is load-bearing (`arc:system-overview`, `adr:003`).**

- *Control plane* — presence, text, call setup, queue drain, key distribution. One WebSocket per client to the coordinator, protobuf envelopes with a per-type `oneof`, unknown fields ignored for version skew across the fleet.
- *Data plane* — real-time audio over UDP with RTP framing (reuse an RTP implementation; do not invent an audio wire format), file bytes over TCP with a manifest and 1 MiB content-addressed chunks.

The reason for the split: **a control-plane reconnect must never tear down an active call.** The data plane carries its own keepalive and its own lifetime.

**Hybrid topology (`adr:001`).** Attempt direct peer-to-peer, fall back to relaying through the coordinator. The fallback is phase 2 — in the MVP everything already passes through the coordinator, so there is nothing to fall back from yet.

**Go, with CGO quarantined (`adr:002`).** Everything except audio is pure Go, including persistence: the ADR requires a pure-Go SQLite implementation (in practice `modernc.org/sqlite`) rather than a CGO driver, or the default build loses its static-binary property. Audio compiles only under the `voice` build tag (miniaudio binding for capture, libopus for encode; no viable pure-Go Opus encoder exists). Cross-compilation: plain `GOOS`/`GOARCH` for the default build; `zig cc` as the C compiler for the audio build, so no per-target sysroot is needed.

**Application crypto is narrowly scoped (`adr:004`).** Sealed box (X25519 + XChaCha20-Poly1305, from `golang.org/x/crypto`) applies to *only* the coordinator's two blind spots: messages resting in the offline queue, and payloads on the relay-fallback path. Direct live traffic relies on WireGuard. Do not add TLS inside the tunnel. Trust is on first use; a changed key for a known peer must warn loudly and require confirmation.

**No remote command execution (`adr:005`).** Declined, not deferred — delegated to Tailscale SSH. Do not add a shell, an exec endpoint, or a bidirectional PTY. Read-only PTY streaming is a later phase; collaborative terminal means wrapping tmux, not reimplementing a multiplexer.

## Constraints that produce wrong code if you don't know them

- **Presence is server-authoritative with a liveness TTL.** No code path may let a client assert its own online state — a killed device cannot send a goodbye and would stay "online" forever. The defining test is `SIGKILL` a client and assert it goes offline within the TTL.
- **Push-to-talk is a toggle, not hold-to-talk.** Terminals do not report key-release events. Hold-to-talk exists only where the Kitty keyboard protocol is detected, as an enhancement.
- **Bind to the tailnet interface only** — never `0.0.0.0`. This includes `/metrics` and `/healthz`, and it is verified by test, not by inspection.
- **Delivery is at-least-once on the wire, deduplicated by ULID at the receiver.** Do not attempt exactly-once at the transport layer.
- **Retention is always bounded** — both a TTL and a size cap, and reaching the cap produces an explicit refusal rather than silent failure or unbounded growth.
- **File-transfer progress reflects acknowledged bytes**, never bytes handed to the network.
- **The direct-versus-relay ratio metric is mandatory**, and must exist from the MVP even while it reads zero. Without it, a relay fallback silently masks a tailnet that never achieves direct connectivity, making `adr:001` unobservable in production.
- **The sub-100 ms latency target applies to direct paths only**, and only from phase 2. It is calculated, not yet measured on fleet hardware.

## MVP boundary

`scp:mvp-text-voicenote` is text + presence + asynchronous push-to-talk voice notes. Deferred on purpose, with phases recorded in `plan:roadmap-v1`: real-time voice calls, the direct data plane, relay fallback and file transfer (phase 2); rooms and read-only PTY streaming (phase 3); slash commands, webhooks, the subprocess plugin protocol and auto-update (phase 4). Key rotation, multi-device identity and encrypted local history are recorded gaps with no phase.

Do not implement a deferred feature because it seems small. The `ts:control-socket` item is in the MVP precisely so that scriptability exists without a plugin runtime.

`spk:audio-toolchain-viability` was canceled before its hardware measurements (no fleet ARM device, no zig), so `sto:voice-note` remains blocked and **no build records or plays voice notes today** — including voice-tagged ones, which currently compile audio plumbing only. Its failure has a pre-agreed consequence: if the spike is not successfully rerun, voice notes leave the MVP and the first release is text + presence only.

## Working in this repository

`walkie/ctr:wave-1` holds 16 work items, each ticketed. Thirteen are done; `sto:voice-note` is todo and `spk:audio-toolchain-viability` was canceled before hardware measurements. The container's activation state is `/eka-execute`'s concern — activating it locks `plan:roadmap-v1` to immutable, never a side effect of ordinary work.

- `/eka-discuss` — planning; creates and revises knowledge, never touches source code.
- `/eka-execute` — execution; activates the container and drives work items through `eka transition`.

State changes go through `eka transition <unit> <state>` (forward-only, one step at a time — `draft → review → approved` cannot be skipped). Never hand-edit the canonical store or `exchange/snapshots/**`.

**When authoring knowledge, always write relationship targets qualified** (`walkie/adr:001-transport-topology`, not `adr:001-transport-topology`). `eka new` accepts and stores the unqualified form, and `eka publish` accepts it too, but the Runtime resolver then refuses to traverse it — `eka context` and `eka get --upstream` both fail. The `eka new --file` batch example in the CLI's own help text shows the broken form. `eka relate` will not repair it (it adds edges without normalising); the fix is republishing at a new instance version. Tracked as maleolabs/eka-cli#15, unfixed as of v1.2.3.

Verify knowledge changes with `eka validate` and `eka integrity check` (both must report zero) plus `eka context` on a unit that has relationships.

## Build and test commands

The toolchain exists (`ts:build-release-matrix` is done). Two variants, per `adr:002`:

```sh
make build          # both binaries, default variant: pure Go, static, CGO_ENABLED=0
make build-voice    # client with the voice tag (audio plumbing; no voice features yet)
make check-cgo      # the adr:002 guard — the default tree must build with CGO_ENABLED=0
make test           # default variant
make test-voice     # voice-tagged variant
make test-race      # race detector (development check, not a release build)
go vet ./...        # or: make lint (vet + buf lint)
make proto          # regenerate protobuf into internal/genproto
make proto-check    # fail if committed generated code drifted from proto/
make tidy           # go mod tidy; fails if go.mod/go.sum changed
```

Release targets are enumerated in the Makefile and owned by `ts:build-release-matrix`: the default build for linux/amd64, linux/arm64, linux/arm, darwin/arm64, darwin/amd64, windows/amd64; the audio build for linux/amd64, linux/arm64, darwin/amd64 (`make release-audio` fails loudly today — zig is absent and no C units exist yet). `make release`, `release-verify` and `release-repro-check` produce checksummed, static-link-verified, reproducibility-checked artifacts; see `docs/release/README.md`.

Test infrastructure is `ts:test-harness` (done): an injectable network (`internal/testnet`), a fake clock (`internal/clock`) so no test sleeps in real time, and a null audio backend so CI needs no sound hardware.

## Git

Branches `main` and `develop`; `develop` is the working branch. Remote is `git@github.com:maleolabs/walkie.git`. Commits follow Conventional Commits. Implementation work happens on per-item branches in worktrees cut from `develop`.

`exchange/snapshots/` is generated by `eka sync` and committed so knowledge travels with the repository. Regenerate it with `eka sync`, never by hand, and commit the result as its own `chore(eka):` commit.
