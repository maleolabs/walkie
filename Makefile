# walkie build surface.
#
# The release pipeline — the full cross-compile matrix with checksummed,
# static-link-verified, reproducibility-checked artifacts — lives in this file
# and is owned by:
#
#   eka get walkie/ts:build-release-matrix
#
# What is here besides that is the minimum that makes every other work item
# startable, plus
# the one guard that must exist from day one. The two build variants are not a
# convenience; they are adr:002-runtime-stack. The default build is pure Go and
# fully static, audio lives behind the "voice" tag, and `make check-cgo` is the
# mechanical check on that split — an audio library imported from an untagged
# file breaks it immediately, which is the only reliable way to catch a mistake
# that otherwise stays invisible until a cross-compile fails.

GO     ?= go

# buf is pinned as a Go tool dependency (see the `tool` block in go.mod) instead
# of being expected on PATH. Reproducible codegen is an acceptance criterion of
# walkie/ts:protocol-schema-v1, and a PATH `buf` is whatever each machine or CI
# runner happens to have installed — the drift then shows up as a `make
# proto-check` diff that looks like a schema change and is not one. `go tool buf`
# resolves to the exact version in go.mod for everyone, and needs no separately
# installed binary at all.
#
# The one cost: buf is a large program, so the first `go tool buf` on a cold Go
# build cache spends minutes compiling it (about 2 seconds once warm). CI is
# always cold, so the proto job overrides BUF with a prebuilt binary of the
# version it reads back out of go.mod — see .github/workflows/ci.yml.
BUF    ?= $(GO) tool buf

BINDIR ?= bin

# Release pipeline (ts:build-release-matrix). The matrices are adr:002's split
# verbatim: the default build covers every fleet target with no C toolchain;
# the audio build is the zig cc trio. Adding a target to a matrix without
# adding it to the other is exactly the drift this file exists to prevent, so
# both lists sit together and CI consumes them through these targets rather
# than re-listing GOOS/GOARCH pairs in YAML.
DEFAULT_MATRIX := linux/amd64 linux/arm64 linux/arm darwin/arm64 darwin/amd64 windows/amd64
AUDIO_MATRIX   := linux/amd64 linux/arm64 darwin/arm64

RELEASE_DIR ?= dist

# VERSION is stamped from the commit, never the clock. Criterion 4 requires
# that the same commit produces identical artifacts; a timestamp in -ldflags
# would break that by construction, and so would -buildvcs's VCS stamping,
# which embeds whether the tree happened to be dirty. Both are pinned here so
# two builds of one commit cannot disagree about anything but nothing.
VERSION ?= $(shell git rev-parse HEAD)

.DEFAULT_GOAL := help

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build both binaries, default variant (pure Go, static, no CGO)
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BINDIR)/walkie ./cmd/walkie
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BINDIR)/walkie-coordinator ./cmd/walkie-coordinator

.PHONY: build-voice
build-voice: ## Build the client with audio support
	$(GO) build -trimpath -tags voice -o $(BINDIR)/walkie-voice ./cmd/walkie

.PHONY: check-cgo
check-cgo: ## adr:002 guard — the default build must never require CGO
	CGO_ENABLED=0 $(GO) build ./...

# ---- Release pipeline (ts:build-release-matrix) --------------------------
#
# Three targets, one property each:
#
#   release            produce every default-build artifact + SHA256SUMS
#   release-verify     ASSERT the artifacts are static and CGO-free
#   release-repro-check  build twice, diff digests — criterion 4 demonstrated,
#                        not asserted
#
# `release` and `release-verify` are separate on purpose. A pipeline that
# builds and verifies in one step makes the verification invisible; a failing
# verify step should read as "the artifact is bad", not "the build broke".
#
# The checksum file is generated over a sorted, NUL-delimited file list so its
# own bytes are deterministic — release-repro-check diffs two of them, and a
# checksum file whose order depended on filesystem readdir order would fail
# that comparison for a reason that has nothing to do with reproducibility.

.PHONY: release
release: ## Produce checksummed default-build artifacts in $(RELEASE_DIR)/release
	rm -rf $(RELEASE_DIR)/release
	mkdir -p $(RELEASE_DIR)/release
	for t in $(DEFAULT_MATRIX); do \
		os=$${t%/*}; arch=$${t#*/}; ext=""; \
		if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
		mkdir -p $(RELEASE_DIR)/release/$$os-$$arch; \
		for cmd in walkie walkie-coordinator; do \
			CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build \
				-trimpath -buildvcs=false \
				-ldflags "-X main.version=$(VERSION)" \
				-o $(RELEASE_DIR)/release/$$os-$$arch/$$cmd$$ext \
				./cmd/$$cmd || exit 1; \
		done; \
	done
	cd $(RELEASE_DIR)/release && \
		find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS

.PHONY: release-verify
release-verify: ## Assert release artifacts are static and CGO-free (criterion 2)
	for t in $(DEFAULT_MATRIX); do \
		os=$${t%/*}; arch=$${t#*/}; ext=""; \
		if [ "$$os" = "windows" ]; then ext=".exe"; fi; \
		dir=$(RELEASE_DIR)/release/$$os-$$arch; \
		for cmd in walkie walkie-coordinator; do \
			b=$$dir/$$cmd$$ext; \
			test -f "$$b" || { echo "FAIL missing artifact: $$b (run make release)"; exit 1; }; \
			$(GO) version -m "$$b" | grep -q 'CGO_ENABLED=0' \
				|| { echo "FAIL not CGO-free per buildinfo: $$b"; exit 1; }; \
			case $$os in \
			linux) \
				file "$$b" | grep -q 'statically linked' \
					|| { echo "FAIL not statically linked: $$b"; exit 1; }; \
				if readelf -l "$$b" | grep -q 'Requesting program interpreter'; then \
					echo "FAIL has dynamic interpreter: $$b"; exit 1; \
				fi ;; \
			esac; \
		done; \
	done
	@echo "release-verify: all artifacts CGO-free (buildinfo); ELF targets statically linked with no program interpreter"
	@echo "note: darwin/windows linkage is checked via buildinfo only — Mach-O/PE interpreter inspection needs otool/dumpbin, absent from CI runners by design"

# Criterion 4 says "the same commit produces identical artifacts". That is a
# claim about bytes, so it is settled by comparing bytes: two full runs of the
# release target from one checkout, digest files diffed. If this ever fails,
# something started embedding the environment — a timestamp, an absolute path,
# a VCS dirty flag — and the diff output points at which artifact first.
.PHONY: release-repro-check
release-repro-check: ## Demonstrate reproducibility: double build, compare digests (criterion 4)
	rm -rf $(RELEASE_DIR)/repro-a $(RELEASE_DIR)/repro-b
	$(MAKE) RELEASE_DIR=$(RELEASE_DIR)/repro-a release
	$(MAKE) RELEASE_DIR=$(RELEASE_DIR)/repro-b release
	diff $(RELEASE_DIR)/repro-a/release/SHA256SUMS $(RELEASE_DIR)/repro-b/release/SHA256SUMS \
		|| { echo "FAIL builds are NOT reproducible — see differing digests above"; exit 1; }
	rm -rf $(RELEASE_DIR)/repro-a $(RELEASE_DIR)/repro-b
	@echo "release-repro-check: two builds of $(VERSION) produced identical digests"

.PHONY: test
test: ## Run tests, default variant
	CGO_ENABLED=0 $(GO) test ./...

.PHONY: test-voice
test-voice: ## Run tests with the voice build tag
	$(GO) test -tags voice ./...

# The race detector needs CGO on most platforms, so this target deliberately
# does not force CGO_ENABLED=0. That is fine: it is a development check, never
# a release build.
.PHONY: test-race
test-race: ## Run tests under the race detector
	$(GO) test -race ./...

.PHONY: spike-audio
spike-audio: ## spk:audio-toolchain-viability — exercise the voice-tagged audio package
	$(GO) test -v -tags voice ./internal/audio/...

.PHONY: proto
proto: ## Regenerate protobuf code into internal/genproto
	$(BUF) generate

.PHONY: proto-check
proto-check: ## Fail if committed generated code is out of sync with proto/
	$(BUF) generate
	git diff --exit-code -- internal/genproto

.PHONY: lint
lint: ## Vet Go code and lint the schema
	$(GO) vet ./...
	$(BUF) lint

.PHONY: tidy
tidy: ## Tidy the module, and fail if it was not already tidy
	$(GO) mod tidy
	git diff --exit-code -- go.mod go.sum

.PHONY: clean
clean: ## Remove build output
	rm -rf $(BINDIR)
