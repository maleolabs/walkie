package control

import (
	"fmt"
	"time"
)

// Config carries every interval the control core waits on. There is no other
// source: ts:test-harness requires that all timeouts, backoff intervals and
// TTLs are driven by configuration plus the injected clock, so a literal
// duration inside the state machine, backoff or watchdog logic is a bug by
// definition — the tests would be unable to shrink it, and a test that
// cannot shrink an interval is testing the constant, not the behaviour.
//
// The zero value is not valid; use [DefaultConfig] or set every field.
type Config struct {
	// BackoffBase is the first reconnect attempt's jitter envelope
	// ([0, BackoffBase)); it doubles per attempt up to BackoffCap.
	BackoffBase time.Duration

	// BackoffCap bounds every backoff envelope. With full jitter the cap is
	// also the worst-case wait for any single attempt.
	BackoffCap time.Duration

	// HeartbeatPeriod is how often the watchdog sends an application-level
	// Heartbeat while the session is live. It must sit comfortably inside
	// the coordinator's presence TTL (WALKIE_PRESENCE_TTL, default 45s) so a
	// healthy link never lets the server-side liveness lease lapse — the
	// client's cadence and the server's TTL are two ends of one bargain,
	// tuned together, never independently.
	HeartbeatPeriod time.Duration

	// DeadPeerInterval is how long the peer may stay silent — no inbound
	// frame of ANY kind — before the watchdog declares it dead (criterion
	// 3). Any inbound traffic proves the pipe alive, which is why the
	// deadline measures silence, not missing pongs specifically.
	DeadPeerInterval time.Duration
}

// DefaultConfig returns the provisional production defaults.
//
// Every number here is a starting point to be revisited on fleet evidence,
// not a tuning result — the same provisional posture as the coordinator's
// WALKIE_PRESENCE_TTL default (45s), which these are sized against:
//
//   - HeartbeatPeriod 15s keeps at least two heartbeats inside the server's
//     45s liveness TTL, so ordinary jitter cannot expire a live device.
//   - DeadPeerInterval 45s allows three missed periods before declaring the
//     peer dead — slow enough that one lost ping-pong cannot tear down a
//     call-bearing session, fast enough that criterion 3's detection stays
//     human-visible.
//   - BackoffBase 1s / BackoffCap 60s: the first retry lands within a second
//     of a restart (criterion 1 wants prompt recovery), and the cap keeps a
//     long outage from pinning a client at multi-minute waits when the
//     coordinator comes back.
func DefaultConfig() Config {
	return Config{
		BackoffBase:      1 * time.Second,
		BackoffCap:       60 * time.Second,
		HeartbeatPeriod:  15 * time.Second,
		DeadPeerInterval: 45 * time.Second,
	}
}

// validate refuses configurations that cannot work, rather than producing a
// watchdog whose behaviour would be mysterious later.
//
// DeadPeerInterval must exceed HeartbeatPeriod because the first possible
// inbound proof of life after connect is the reply to the FIRST heartbeat —
// a dead-peer deadline at or below the period fires before any healthy peer
// could answer, turning "detection" into a guaranteed false positive.
func (c Config) validate() error {
	if c.BackoffBase <= 0 {
		return fmt.Errorf("control: config: backoff base must be positive (got %s)", c.BackoffBase)
	}
	if c.BackoffCap <= 0 {
		return fmt.Errorf("control: config: backoff cap must be positive (got %s)", c.BackoffCap)
	}
	if c.BackoffCap < c.BackoffBase {
		return fmt.Errorf("control: config: backoff cap %s must not be below base %s", c.BackoffCap, c.BackoffBase)
	}
	if c.HeartbeatPeriod <= 0 {
		return fmt.Errorf("control: config: heartbeat period must be positive (got %s)", c.HeartbeatPeriod)
	}
	if c.DeadPeerInterval <= c.HeartbeatPeriod {
		return fmt.Errorf("control: config: dead-peer interval %s must exceed heartbeat period %s",
			c.DeadPeerInterval, c.HeartbeatPeriod)
	}
	return nil
}
