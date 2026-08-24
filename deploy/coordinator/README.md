# Coordinator container

The tsnet-based coordinator, packaged. Owning work item:
`eka get walkie/ts:build-release-matrix` (criterion 5); the binary itself is
owned by `eka get walkie/ts:coordinator-skeleton`.

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

`-e WALKIE_TAILNET_AUTHKEY` with no `=value` forwards the variable from your
shell — the key never lands in the command line, an image layer, or a file.
Only needed on first join; once `/state` holds a joined node, restarts work
without it.

## Security posture (recorded decisions)

| Requirement | How this image meets it | Why |
| --- | --- | --- |
| Static binary | build stage forces `CGO_ENABLED=0`; runtime stage is distroless static | adr:002-runtime-stack; no libc means no host-OS coupling and nothing to patch |
| Persistent state | named volume at `/state`, owned by uid 65532 | holds the tsnet node keys that ARE the coordinator's identity, the TOFU keystore, and the SQLite store |
| Auth key via env | `-e WALKIE_TAILNET_AUTHKEY`, never argv, never baked | argv is world-readable via ps; no second credential may be added (adr:004) |
| No host networking | run with default bridge network; tsnet listeners bind the tailnet IP inside the container's userspace stack | `--network host` would bypass tsnet entirely and expose the control plane on every host interface |
| No CAP_NET_ADMIN / not privileged | userspace networking creates no tun device; run with `--cap-drop ALL` | the coordinator needs zero capabilities; privilege here would be pure attack surface |

`--read-only` + a small `/tmp` tmpfs are hardening on top of the recorded
posture: the process writes only under `/state`. Drop those two flags if a
future dependency proves otherwise — do not reach for host networking or
privilege instead.

## Runtime behaviour is UNVERIFIED

Everything above is reasoned from the code (`cmd/walkie-coordinator`,
`internal/coordinator/tsauth`) and verified to BUILD. Nobody has yet run this
image against a live tailnet, because doing so needs two resources no
environment so far has had: a Tailscale network to join and an auth key for
it. `ts:coordinator-skeleton` hit the same wall. This statement is precise on
purpose — "the image builds" and "the coordinator joins and serves" are
different claims, and only the first is made here.

### What would verify it

Someone with a tailnet and an auth key (reusable or ephemeral, tagged for the
coordinator) runs:

1. `docker run` exactly as above **without** `--network host` and **without**
   any `--cap-add`, on a machine inside the tailnet's coordination reach.
2. Confirm the node appears: `tailscale status` (from any admin view) shows a
   node named per `WALKIE_HOSTNAME` (default `walkie-coordinator`).
3. From another tailnet device:
   `curl http://walkie-coordinator:9464/healthz` — expect a JSON body with
   both probes ready (coordinator ready, store ready). This also proves the
   tailnet-only bind: the same URL must NOT answer from the container host's
   localhost.
4. Connect a client (`walkie`) from a third device and exchange a message;
   kill the client with SIGKILL and confirm it drops from presence within
   `WALKIE_PRESENCE_TTL` (the defining liveness test).
5. Restart the container WITHOUT the auth key env and confirm it re-joins as
   the same node from `/state` — proving the volume actually persists
   identity.

If step 3 answers on the host's localhost instead of only over the tailnet,
or step 1 fails without added capabilities, the posture claim is broken and
this item's criterion 5 is NOT met — report it rather than loosening the
run flags.
