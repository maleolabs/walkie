# Coordinator container image — walkie/ts:build-release-matrix criterion 5.
#
# Build from the repository root:
#
#	docker build -f deploy/coordinator/Dockerfile -t walkie-coordinator .
#
# The recorded deployment posture this image implements (routed from
# ts:coordinator-skeleton's review):
#
#   - STATIC BINARY. The build stage forces CGO_ENABLED=0 (adr:002), so the
#     runtime stage is distroless static: no libc, no shell, no package
#     manager to patch.
#   - WRITABLE PERSISTENT VOLUME. Everything stateful — the tsnet node keys
#     that ARE the coordinator's tailnet identity, the TOFU keystore, and the
#     SQLite store — lives under WALKIE_STATE_DIR=/state. That must be a
#     mounted volume or every restart re-joins the tailnet as a new node.
#   - AUTH KEY VIA ENVIRONMENT. WALKIE_TAILNET_AUTHKEY is injected with -e,
#     never baked into the image and never passed on argv (argv is
#     world-readable through ps; see cmd/walkie-coordinator's package
#     comment). No other credential exists anywhere in this design
#     (adr:004) — do not add one.
#   - NO HOST NETWORKING. tsnet brings its own userspace WireGuard stack;
#     both listeners bind the node's tailnet IP inside it. `--network host`
#     would bypass that stack entirely and expose the control plane on every
#     host interface — it is forbidden here, not merely unnecessary.
#   - NO CAP_NET_ADMIN, NO --privileged. Userspace networking creates no tun
#     device, so the container needs no capabilities at all; run it with
#     --cap-drop ALL (see deploy/coordinator/README.md).
#
# RUNTIME BEHAVIOUR IS UNVERIFIED. Building this image proves it builds and
# that the binary inside is static; proving a tsnet join works under this
# posture needs a live tailnet and an auth key, which no environment so far
# has had. deploy/coordinator/README.md records exactly what would verify it.
# Do not "fix" that gap locally by adding host networking or privileges —
# that would verify a different, wrong deployment.

FROM golang:1.25.12-alpine AS build
# ^ keep in sync with the `go` directive in go.mod; a mismatch makes the
# toolchain download a second Go at build time, which is slow and surprising.

WORKDIR /src

# Dependencies first so the layer cache survives source edits.
COPY go.mod go.sum ./
RUN go mod download

# internal/genproto is committed on purpose (see .gitignore), so no codegen
# step exists in this image — proto/ is deliberately not copied.
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false \
      -ldflags "-X main.version=${VERSION}" \
      -o /out/walkie-coordinator ./cmd/walkie-coordinator \
 && mkdir -p /out/state

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/walkie-coordinator /walkie-coordinator

# /state must already exist AND be owned by the runtime user (65532), or the
# volume mount is unwritable and the coordinator dies on startup. Distroless
# has no shell or mkdir, so the directory is created in the builder and
# copied with ownership.
COPY --from=build --chown=65532:65532 /out/state /state

ENV WALKIE_STATE_DIR=/state

# Documentary only. Both listeners (control 443, observability 9464) bind the
# node's TAILNET IP inside tsnet's userspace stack — they are not host ports,
# so EXPOSE publishes nothing and -p is not how clients reach them. Peers
# connect over the tailnet, by MagicDNS name.
EXPOSE 443 9464

# No HEALTHCHECK, deliberately: /healthz answers on the tailnet interface,
# which the container's own network namespace cannot reach via localhost —
# any probe this file could run would test the wrong thing and report lies.
# Health is observed from another tailnet device; see
# deploy/coordinator/README.md.

ENTRYPOINT ["/walkie-coordinator"]
