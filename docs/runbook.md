# walkie coordinator runbook

Operating the walkie coordinator: deployment, what your tailnet ACL must do,
retention knobs, reading the metrics, and recovering from a restart. Owning
work item: `eka get walkie/ts:docs-quickstart-runbook`.

Everything here describes shipped behaviour. Where a capability is deferred
(phase 2+), the text says so explicitly rather than leaving a gap a reader
will fill with hope.

## Deployment

The coordinator is a container that joins your tailnet as its own node via
tsnet — no host Tailscale daemon, no sidecar, no host networking. Build and
run it exactly as [deploy/coordinator/README.md](../../deploy/coordinator/README.md)
describes:

```sh
docker build -f deploy/coordinator/Dockerfile -t walkie-coordinator .
docker volume create walkie-coordinator-state

docker run -d --name walkie-coordinator \
  --restart unless-stopped \
  --cap-drop ALL \
  --read-only \
  --tmpfs /tmp:rw,size=64m \
  -v walkie-coordinator-state:/state \
  -e WALKIE_TAILNET_AUTHKEY \
  walkie-coordinator
```

The essentials, and why each is load-bearing:

- **`WALKIE_STATE_DIR` must be `/state` and must persist.** It holds the
  tsnet node keys that ARE the coordinator's identity, the TOFU pin file
  (`peer-keys.json`), and the SQLite store. Lose the volume and the
  coordinator re-joins as a new node with an empty key directory.
- **`WALKIE_TAILNET_AUTHKEY` is needed only on first join.** Once `/state`
  holds a joined node, later restarts work without it. Pass it through the
  environment (`-e VAR`, no `=value`) so it never lands in argv or a file.
- **No `--network host`, no `--cap-add`.** tsnet binds the tailnet IP inside
  the container's userspace stack; that is what makes the tailnet-only bind
  structural. Host networking would expose the control plane on every host
  interface.
- **Runtime behaviour against a live tailnet is still UNVERIFIED** — the
  image builds; nobody has yet joined with it. The verifying checklist
  lives in `deploy/coordinator/README.md`; run it before trusting a
  production deployment.

### Configuration surface (verified against `cmd/walkie-coordinator/main.go`)

Every variable is optional unless stated; defaults shown are the code's.

| Variable | Default | Meaning |
| --- | --- | --- |
| `WALKIE_STATE_DIR` | **required** | tsnet state directory; must persist across restarts |
| `WALKIE_TAILNET_AUTHKEY` | empty | auth key for the first join; unnecessary afterwards |
| `WALKIE_HOSTNAME` | `walkie-coordinator` | node name inside the tailnet (the MagicDNS name clients dial) |
| `WALKIE_STORE_PATH` | `<state>/coordinator.db` | SQLite database path |
| `WALKIE_CONTROL_PORT` | `443` | control-plane WebSocket port |
| `WALKIE_OBS_PORT` | `9464` | `/metrics`, `/healthz`, `/livez` port |
| `WALKIE_PRESENCE_TTL` | `45s` | how long a silent device stays online after its last heartbeat |
| `WALKIE_QUEUE_TTL` | `72h` | offline-queue retention per message |
| `WALKIE_QUEUE_MAX_SIZE` | `256` | offline-queue cap, messages per recipient |
| `WALKIE_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

There are no other knobs. In particular there is no auth token, no user
table and no TLS setting: authentication is a WhoIs lookup of the caller's
tailnet address (`adr:004-security-model`), and transport encryption is
WireGuard's job. If you find yourself wanting to add a credential, stop —
that is a design regression, not hardening.

## Tailnet ACL expectations

walkie **inherits your tailnet ACL and cannot compensate for a permissive
one** (`adr:004` consequence 5). It adds no authorization layer of its own:
any device the ACL admits to the coordinator can connect as its tailnet
identity, appear in everyone's roster, receive broadcast messages and hold
queue entries. For walkie's security properties to hold, the ACL must:

- allow the intended client devices to reach `walkie-coordinator:443`
  (control plane) and `walkie-coordinator:9464` (metrics/health);
- deny everything else you do not explicitly want connected — walkie will
  not narrow an over-broad grant for you;
- treat access to `:9464` as "can read operational telemetry": the metrics
  endpoint is readable by the whole tailnet by design. It carries no
  message content and no per-device series (see the metrics section), but
  availability of the endpoint is the ACL's decision, not walkie's.

A note on what WhoIs gives you: identity on the wire IS tailnet membership.
Whoever can reach the port is, by definition, an authenticated team device.
That is the whole model — there is no second factor to configure, and none
may be added without revising `adr:004`.

## Retention configuration

Retention is bounded everywhere, by construction, and the bounds are loud
when hit:

- **Offline queue, time bound:** `WALKIE_QUEUE_TTL` (default 72h). Messages
  older than the TTL are evicted; each eviction is logged content-free and
  counted in `walkie_queue_evictions_total{reason="ttl"}`.
- **Offline queue, size bound:** `WALKIE_QUEUE_MAX_SIZE` (default 256
  messages per recipient). Reaching the cap produces an **explicit refusal**
  for further messages — never silent dropping and never unbounded growth.
  Refusals are counted in `walkie_queue_evictions_total{reason="size_cap"}`
  (a refusal count, not a removal count).
- **Presence:** `WALKIE_PRESENCE_TTL` (default 45s) bounds how long a dead
  connection stays "online". This is the server-authoritative liveness
  guarantee: kill a client abruptly (SIGKILL) and it drops off rosters
  within the TTL.
- **Client-side message history:** bounded at fixed package defaults —
  30 days and 10,000 messages per device, oldest evicted first. These are
  deliberately NOT environment-configurable today; if a fleet needs
  different bounds, that is a code change, not a config file.

## Reading the metrics

Scrape `http://walkie-coordinator:9464/metrics` from any tailnet device.
Health probes share the port: `/healthz` is readiness (per-component JSON:
`coordinator` and `store`, HTTP 503 when either is down) and `/livez` is
liveness (probes nothing, by design).

| Metric | Type | How to read it |
| --- | --- | --- |
| `walkie_connected_devices` | gauge | Devices with ≥1 live routed connection, recomputed on every route change. |
| `walkie_messages_total` | counter | Valid messages accepted at ingress. Compute msg/s in your scraper; the coordinator keeps no rate. |
| `walkie_reconnects_total` | counter | Connections from devices already seen this process lifetime. Resets at restart (Prometheus convention). A rising rate after coordinator restarts is normal; a rising rate otherwise means clients cannot hold connections. |
| `walkie_auth_refusals_total` | counter | Connections refused at the identity gate (WhoIs failed / remote address unusable). Nonzero means something outside the expected fleet is knocking, or tailscaled is misreporting addresses. Investigate. |
| `walkie_queue_depth` | histogram | Offline-queue depth observed ACROSS recipients, one observation per enqueue/ack/TTL-eviction. No labels — see below. Count ≈ number of depth-moving events since start; sum/count ≈ average backlog per event. |
| `walkie_queue_evictions_total{reason}` | counter | `ttl`: retention evictions (bounded retention working). `size_cap`: admissions REFUSED at the cap — refusals, not removals. Sustained `size_cap` growth means a recipient is offline longer than your TTL×cap budget allows; raise the cap or shorten their absence. |
| `walkie_data_plane_paths_total{mode}` | counter | Phase-2 instrument. See below. |

### `walkie_data_plane_paths_total` reads zero — that is correct

**In this release, `mode="direct"` is zero BY DESIGN, and so is
`mode="relay"`.** There is no peer-to-peer data plane yet: the direct UDP
plane and its relay fallback are phase 2 (`adr:001` hybrid topology), and
until they exist nothing increments either series. Both series are exported
at zero from the first scrape on purpose, so a dashboard shows a real zero
rather than an absent series.

Do not file a bug on a zero direct-path count in this release. The reading
becomes meaningful only after phase 2 ships: from then on, `direct` counts
formed peer-to-peer paths and `relay` counts paths falling back through the
coordinator. **A LATER sustained zero of `direct` while sessions are active
is the investigation signal** — it would mean the hybrid topology silently
never achieves direct connectivity, which is exactly the failure this
mandatory counter exists to make visible.

### What the label-free metrics hide, on purpose

Queue depth carries no labels because the metrics endpoint is readable by
every tailnet peer: a per-recipient series would let anyone learn who holds
mail and whether a named device is offline (send them a message while they
are offline; watch one series move). The histogram trades that precision
for structural privacy: an observer sees aggregate shape only. The accepted
cost: **you cannot answer "is device X's queue stuck?" from metrics** — use
the coordinator logs (device names, positions, content-free) for that.

### Small-fleet caveat: `connected_devices` narrows at two devices

The privacy argument above assumes a population where aggregates stay
aggregate. On a tailnet with only **two** walkie devices, the unlabelled
gauge itself becomes a presence channel: a scraper who is one of the two
devices reads 1↔2 transitions and infers the other device's liveness. If
that inference matters to you, restrict which devices can reach `:9464` in
the ACL — walkie cannot distinguish a curious teammate from a scrape.

## Recovery from a coordinator restart

Restarting the coordinator is a normal operation; `--restart unless-stopped`
does it after host reboots. What happens, in order:

1. **Clients reconnect by themselves.** Each running `walkie` retries with
   backoff and resumes its session; nothing on the client needs a human.
2. **Queued messages survive.** They live in the SQLite store under
   `/state` and drain to recipients once those reconnect.
3. **Presence resets honestly.** Last-seen facts survive (they are in the
   store), but nothing is "online" until observed alive again — after a
   restart every device is offline until it reconnects. Rosters fill back
   in within seconds of clients reconnecting.
4. **No auth key needed** — provided `/state` persisted. If the volume was
   lost, the coordinator joins as a NEW node: clients must be pointed at
   the new MagicDNS name (or DNS updated), and the TOFU key directory is
   empty, so every device re-pins on first contact.
5. **Counters reset.** `walkie_reconnects_total` et al. restart from zero
   at process start; dashboards using `rate()` handle this. Gauge and
   histogram state rebuild from live traffic.

Graceful shutdown (SIGTERM/SIGINT) winds live connections down inside a
grace window; a shutdown that outlives the window is logged as incomplete
and the process still exits cleanly — the supervisor restarting it is the
intended next step, not an error loop.

## Key changes, fingerprints, and the manual recovery path

Trust is on first use: the first key seen for a device is pinned in
`/state/peer-keys.json`. A **changed** key for a known device — typically a
reinstalled walkie — is refused automatically: the coordinator logs
`PEER KEY CHANGED: verification required` with both fingerprints, the
announcing device receives an error quoting `pinned` and `presented`
fingerprints, and the old pin stays untouched. There is no accept switch,
by design.

Operator recovery, in full:

1. Read both fingerprints from the log lines (twenty decimal digits, five
   groups of four).
2. Verify out of band with the device holder — the read-aloud procedure in
   [docs/quickstart.md](quickstart.md) exists for exactly this.
3. Stop the coordinator.
4. Correct or remove that device's entry in `/state/peer-keys.json` by hand
   (owner-only file).
5. Start the coordinator; the device re-pins on its next announce.

Users' own fingerprint questions are answered in the quickstart; today
fingerprints surface in the coordinator's `peer key pinned on first
contact` log lines and in key-refused error messages — there is no separate
fingerprint command.

One honest bootstrap window remains: a message held for a device that has
never announced a key rests plaintext (still TTL- and size-bounded) until
its first announce pins a key. Every such hold is logged. Pins never apply
retroactively.

## Security limitations you operate with

Stated plainly; all recorded in `adr:004-security-model`:

- **Client message history is unencrypted at rest** on each device. Full-
  disk encryption is the mitigation and lives outside walkie.
- **Losing a device key forfeits that device's queued messages.** No
  backup, escrow or recovery exists.
- **No key rotation, no multi-device identity.** One key per device for its
  life; reinstalling generates a fresh key and triggers the changed-key
  refusal everywhere.
- **Live traffic relies on WireGuard alone** — no additional application
  layer on direct hops. Application crypto covers only the coordinator's
  blind spots (queued-at-rest today; the relay-fallback path when phase 2
  builds it).
- **walkie inherits the tailnet ACL** and cannot compensate for a
  permissive one (see the ACL section).

## Remote shell access on coordinator hosts

Use **Tailscale SSH** (`tailscale ssh <host>`), which brings ACL-based
authorization, audit and session recording. walkie declines remote command
execution deliberately (`adr:005-no-remote-command-execution`): a second,
less-reviewed execution path would be a net security regression, and an
execution endpoint on every fleet device is the highest-risk surface a tool
like this could add. The same reasoning applies to the coordinator container
— exec into it via the Docker daemon only when you must, and prefer
`docker logs` plus the metrics endpoint for diagnosis.

## Not implemented in this release

So nobody debugs a phantom: real-time voice calls, the direct data plane,
relay fallback, file transfer, rooms, PTY streaming/screen sharing, slash
commands, webhooks, plugins, auto-update — none exist yet. Voice notes in
particular are **not available in any build**, including voice-tagged ones.
Signing of release artifacts is also deferred (checksummed, not signed) —
see `docs/release/README.md`.
