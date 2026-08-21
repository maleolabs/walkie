# walkie build surface.
#
# The full release pipeline — cross-compile matrix, goreleaser, the coordinator
# container image — belongs to its own work item and is deliberately not here:
#
#   eka get walkie/ts:build-release-matrix
#
# What is here is the minimum that makes every other work item startable, plus
# the one guard that must exist from day one. The two build variants are not a
# convenience; they are adr:002-runtime-stack. The default build is pure Go and
# fully static, audio lives behind the "voice" tag, and `make check-cgo` is the
# mechanical check on that split — an audio library imported from an untagged
# file breaks it immediately, which is the only reliable way to catch a mistake
# that otherwise stays invisible until a cross-compile fails.

GO     ?= go
BUF    ?= buf
BINDIR ?= bin

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
