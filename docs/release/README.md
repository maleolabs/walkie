# Release artifacts

How walkie's release pipeline produces what it produces, and what a consumer
may and may not assume about it. Owning work item:
`eka get walkie/ts:build-release-matrix`.

## What the pipeline produces

`make release` (and CI's `release` job, which runs the same target) builds
**both binaries for every default-build target** with `CGO_ENABLED=0`,
`-trimpath`, `-buildvcs=false`, and the version stamped from the commit —
never from a clock:

```
dist/release/
├── SHA256SUMS
├── linux-amd64/{walkie,walkie-coordinator}
├── linux-arm64/{walkie,walkie-coordinator}
├── linux-arm/{walkie,walkie-coordinator}
├── darwin-arm64/{walkie,walkie-coordinator}
├── darwin-amd64/{walkie,walkie-coordinator}
└── windows-amd64/{walkie.exe,walkie-coordinator.exe}
```

No C toolchain is involved anywhere in this path (`adr:002-runtime-stack`);
CI's `release-verify` step asserts on the produced bytes that ELF artifacts
are statically linked with no program interpreter and that every artifact's
embedded buildinfo records `CGO_ENABLED=0`.

## Checksums

Every artifact is covered by `SHA256SUMS`. After downloading a release:

```sh
sha256sum -c SHA256SUMS
```

## These artifacts are checksummed, NOT signed

This is a recorded decision, not an oversight.

Signing is **deferred to the phase-4 auto-update work** (`plan:roadmap-v1`),
because a signature only means something when there is a key distribution and
update-verification story to attach it to; bolting one on earlier would create
a key that consumers trust before any of that exists.

Until then, do **not** assume:

- that a matching checksum proves an artifact came from the walkie project.
  A checksum verifies integrity, not authenticity — anyone can recompute a
  SHA-256 over modified bytes;
- that a download channel is trustworthy because the checksum file is next to
  the artifacts. The checksum file travels the same channels and can be
  substituted along with them.

What a matching checksum DOES tell you: the artifact you received is bit-for-
bit what the pipeline produced, so corruption in transit or truncation is
detectable. Provenance currently rests on the channel you obtained the
artifacts from — until phase 4 ships signing, treat that channel as part of
the trust decision.

## Reproducibility

The same commit produces byte-identical artifacts. This is demonstrated, not
asserted: CI's `reproducibility` job builds the full matrix twice from one
checkout and diffs the digest files, so the property cannot silently rot the
first time someone adds a timestamp to a stamp.

Verify it yourself on any commit:

```sh
make release-repro-check
```

One honest caveat: reproducibility holds per toolchain. Building with the Go
version pinned in `go.mod` reproduces; building with a different Go release
need not, because the toolchain version is part of a binary's build inputs.
The version stamp inside each artifact (`<binary> -version`) is the full
commit SHA, so two artifacts claiming the same version were made from the
same source.

## Audio build

The audio matrix (`linux/amd64`, `linux/arm64`, `darwin/arm64`, `voice` tag,
C compiled by `zig cc` with no per-target sysroot) has a designed pipeline:
`make release-audio`. It deliberately FAILS today rather than reporting green,
for the two reasons recorded on that target: zig is not installed everywhere
yet, and `spk:audio-toolchain-viability` was canceled before settling how
libopus reaches arm64, so there are no C units in the voice tree for zig cc to
compile. A green run today would prove pipeline mechanics, not an audio build.

## Coordinator container

See `deploy/coordinator/Dockerfile` and `deploy/coordinator/README.md`. The
image builds; its runtime behaviour against a live tailnet is unverified, and
that README records exactly what would verify it.

## Fleet ARM smoke test

Criterion 7 requires both variants smoke-tested on a real fleet ARM device
before release — emulation does not count. The precise procedure a device
holder runs is in `arm-smoke.md`.
