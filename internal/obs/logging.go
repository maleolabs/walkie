package obs

import (
	"io"
	"log/slog"
)

// This file owns criterion 1 of ts:observability-baseline: structured JSON
// logs with a consistent severity scheme.
//
// # The severity scheme
//
// Exactly four levels, each owning one class of event. A line's level must be
// derivable from the event class alone — two callers seeing the same kind of
// thing must never disagree about severity, because alerting and grep habits
// are built on that consistency:
//
//   - Debug — protocol chatter harmless to drop in production: payloads
//     ignored by design (adr:003 mixed-fleet tolerance), heartbeat refreshes,
//     wiring traces. High-volume by intent; the default level filters it.
//   - Info — normal lifecycle an operator might want to see once: connections
//     accepted, messages routed or held, queue drains, devices online/offline.
//     The default level.
//   - Warn — something refused, degraded or abnormal that the process
//     RECOVERED from or contained: identity refusals, malformed frames,
//     size-cap refusals, shutdown exceeding its polite budget. Worth reading,
//     not worth paging anyone.
//   - Error — an internal failure an operator must act on: storage errors,
//     persistence failures, sweeps that could not run. If Error fires, a
//     guarantee (durability, retention, delivery) may have been dented.
//
// Client misbehaviour is never Error — a hostile or broken peer is a Warn at
// most. Reserving Error for "the coordinator itself is failing" keeps the
// level actionable.
//
// # The content rule is absolute
//
// No log line anywhere carries message content or key material — not at
// Debug, not "temporarily", not behind a flag. A debugging body log is a
// defect, not a convenience (ts:observability-baseline constraint). What MAY
// appear: device names, message IDs (ULIDs identify without disclosing),
// byte lengths, positions, fingerprints (crypto.Keystore logs fingerprints,
// never key bytes), reason classes. The rule is enforced by test — the
// coordinator package runs representative operations over a captured JSON
// stream and fails if any canary body or key byte pattern appears in any
// line (TestLogsCarryNoContentOrKeyMaterial).
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// ParseLevel maps a WALKIE_LOG_LEVEL value onto the scheme above. Unknown
// values fall back to Info rather than erroring: a typo'd level must not take
// a coordinator down at boot — logging verbosity is the safest possible thing
// to degrade.
func ParseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
