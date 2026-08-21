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
