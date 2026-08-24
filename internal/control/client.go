package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
)

// ErrOffline is returned by Send and Acknowledge when the client has no live
// session. It is an ordinary condition, not a failure: the caller's contract
// (outbox for outgoing, at-least-once redelivery for acks) makes "not now"
// safe, which is precisely why the reconnect loop exists.
var ErrOffline = errors.New("control: client offline")

// clientVersion rides Hello.client_version for diagnostics only (the schema
// forbids branching protocol behaviour on it). The real build stamp is wired
// by ts:build-release-matrix; until then this constant says the honest thing.
const clientVersion = "walkie-dev"

// RetrySchedule returns how long to wait before reconnect attempt n
// (0-based). Production wires [Backoff.Delay] — full jitter, capped, seeded;
// tests substitute deterministic schedules (the herd test's zero-jitter
// control needs a constant). The attempt index semantics live with the
// caller: 0 is the FIRST retry after a failure or a drop.
type RetrySchedule func(attempt int) time.Duration

// Client is one device's control-plane connection to the coordinator: the
// reconnect supervisor that wires the slice-1 core ([Machine], [Backoff],
// [Watchdog]) to real transports.
//
// Owning work item:
//
//	eka get walkie/ts:reconnect-resume   (criteria 1, 2, 4, 5; 3 and 6 via the core)
//
// # The loop, in one paragraph
//
// Start launches one supervisor goroutine. Each cycle: connecting → dial →
// handshaking → Hello (carrying the last acknowledged queue position) →
// online. Online starts a fresh [Watchdog] and a read loop that feeds it —
// ANY inbound frame counts as life, any write error or read error is death.
// Death of every kind lands the machine on disconnected (directly for
// transport errors, through degraded for silence), tears the session down,
// waits schedule(attempt) on the injected clock, and dials again. Reaching
// online resets the attempt counter, so a client that was healthy draws its
// first post-drop retry from the base envelope rather than a grown one.
//
// # Why the machine is driven from exactly one place
//
// Every transition the loop performs sits in this file, and the watchdog is
// the only other writer (its verdicts are legal edges the table already
// polices). Two drivers recreating boolean drift is the failure mode the
// explicit machine exists to prevent (state.go); one supervisor plus one
// policy object cannot drift apart because neither keeps its own copy of the
// state.
//
// # Data-plane independence (arc:system-overview)
//
// Nothing here touches audio, RTP, or any UDP path. A reconnect tears down
// exactly one WebSocket and rebuilds it; phase 2's data plane carries its own
// keepalive and lifetime, and this package must never grow a hook that lets
// control-plane state reach it.
type Client struct {
	cfg    Config
	mach   *Machine
	dial   DialFunc
	clk    clock.Clock
	logger *slog.Logger

	// schedule produces retry waits. Unexported on purpose: production
	// always wants the seeded full-jitter backoff built in NewClient, and
	// the only caller that overrides it is this package's own herd test,
	// whose zero-jitter control run must be able to switch jitter OFF to
	// prove the spread metric has teeth.
	schedule RetrySchedule

	// onEnv receives every post-handshake inbound envelope (text, presence,
	// queue deliveries, protocol errors). Set before Start; called on the
	// read-loop goroutine, so it must not block — heavy work belongs in the
	// consumer's own goroutine.
	onEnv func(*walkiev1.Envelope)

	// acked is the highest queue position durably processed by this device.
	// It is what Hello.last_acked_position carries on every handshake
	// (criterion 4) and what Acknowledge stamps. Monotonic by construction.
	acked atomic.Uint64

	// sub watches the machine so serve() can tear a session down the moment
	// the watchdog declares it degraded/dead — otherwise a silent peer would
	// leave the read loop parked forever on a socket nothing will ever speak
	// on again.
	sub *Subscription

	mu       sync.Mutex
	sess     Session // live session (online); nil while offline
	inflight Session // dialed but not yet online — must die with Stop too
	identity string  // device name echoed by the last successful HelloAck

	sendMu sync.Mutex // serializes writes onto the live session

	startOnce sync.Once
	started   atomic.Bool
	stopOnce  sync.Once
	quit      chan struct{} // closed by Stop
	done      chan struct{} // closed when the supervisor has fully exited
	dialCtx   context.Context
	dialStop  context.CancelFunc
}

// NewClient validates its wiring and returns a stopped Client. dial is
// required — without a way to open a session there is nothing to supervise —
// as are mach and clk (the same rule as NewMachine and StartWatchdog: a nil
// clock here is a wall-clock leak into a package whose tests all run fake).
// seedA/seedB seed the default full-jitter backoff; pass distinct pairs per
// client (the fleet simulator derives (seed, index) pairs for exactly this).
//
// cfg must satisfy validate; DefaultConfig does.
func NewClient(cfg Config, mach *Machine, dial DialFunc, seedA, seedB uint64, clk clock.Clock, logger *slog.Logger) (*Client, error) {
	if dial == nil {
		return nil, fmt.Errorf("control: new client: dial must not be nil")
	}
	if mach == nil {
		return nil, fmt.Errorf("control: new client: mach must not be nil")
	}
	if clk == nil {
		return nil, fmt.Errorf("control: new client: clk must not be nil")
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("control: new client: %w", err)
	}
	b, err := NewBackoff(seedA, seedB, cfg.BackoffBase, cfg.BackoffCap)
	if err != nil {
		return nil, fmt.Errorf("control: new client: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		cfg:      cfg,
		mach:     mach,
		dial:     dial,
		clk:      clk,
		logger:   logger,
		schedule: b.Delay,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	// Subscribe BEFORE Start so no transition can land unobserved between
	// construction and supervision (the same subscribe-first rule Machine.
	// Subscribe documents for consumers).
	c.sub = mach.Subscribe()
	return c, nil
}

// OnEnvelope registers the inbound handler. Call before Start. A nil handler
// is valid — the client still runs, heartbeats, and resumes the queue; the
// frames are simply not consumed (they ARE still counted against the
// watchdog's liveness deadline, which is about the pipe, not the payload).
func (c *Client) OnEnvelope(fn func(*walkiev1.Envelope)) {
	c.onEnv = fn
}

// Start launches the supervisor loop. Panics on a second call (including
// after Stop): two supervisors driving one machine is exactly the
// two-writers bug the explicit state machine exists to prevent, and a client
// is not restartable — build a new one.
func (c *Client) Start() {
	if !c.started.CompareAndSwap(false, true) {
		panic("control: client started twice")
	}
	c.startOnce.Do(func() {
		c.dialCtx, c.dialStop = context.WithCancel(context.Background())
		go c.run(c.dialCtx)
	})
}

// Stop retires the client: the supervisor exits, the live session (if any)
// is closed — which unblocks its read loop and surfaces as an ordinary
// transport death — and the watchdog is stopped. Idempotent. The machine is
// left wherever the death path leaves it (disconnected, unless the session
// was already gone): Stop does not invent transitions, the normal teardown
// paths perform them, which keeps the event stream an honest record. The
// Machine subscription taken in NewClient stays live past Stop; it ends only
// at Machine.Close(), which downstream consumers holding their own
// subscriptions (sto:terminal-ui) also drive.
func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		close(c.quit)
		if c.dialStop != nil {
			c.dialStop()
		}
		c.mu.Lock()
		sess := c.sess
		inflight := c.inflight
		c.mu.Unlock()
		// Close BOTH shapes of live connection: the installed session and
		// any dial that has returned but not yet reached serve. A handshake
		// blocked in Recv on a session nobody closes would otherwise hang
		// the supervisor past this join — the timer it also waits on runs
		// on a clock only the caller advances.
		if inflight != nil && inflight != sess {
			_ = inflight.Close()
		}
		if sess != nil {
			_ = sess.Close() // unblocks Recv; the read loop performs the transition
		}
	})
	<-c.done // join: after Stop returns, no goroutine of this client remains
}

// State reports the current connection state (criterion 6's direct read;
// subscribe via Machine.Subscribe for the change stream).
func (c *Client) State() State { return c.mach.State() }

// ConnectedAs reports the device name the coordinator resolved and echoed
// in the last successful HelloAck — who the tailnet says this client is.
// Empty while offline. sto:terminal-ui renders it next to the state ("you
// are X, connected/disconnected"), because a user deciding whether they can
// be reached needs both halves.
func (c *Client) ConnectedAs() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.identity
}

func (c *Client) setIdentity(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.identity = name
}

// Machine exposes the underlying state machine for consumers that subscribe
// (sto:terminal-ui renders it; ts:control-socket streams it). The client
// remains the ONLY driver of transitions — exposure is for observation, not
// mutation.
func (c *Client) Machine() *Machine { return c.mach }

// Send writes one envelope to the live coordinator — the outbox drain path.
// ErrOffline means there is no session right now: hold the message and
// retry after the next online transition (at-least-once makes that safe).
// A write error is a transport death: it routes through the watchdog so the
// machine records it exactly like any other failure mode (b), and the error
// is returned to the caller, whose outbox keeps the message queued.
func (c *Client) Send(env *walkiev1.Envelope) error {
	err := c.send(env)
	if errors.Is(err, errNoSession) {
		return ErrOffline
	}
	return err
}

// Acknowledge records that everything up to position pos has been durably
// processed by this device, and tells the coordinator so retention can drop
// the covered suffix. Safe to call offline: the position is recorded locally
// either way, and the NEXT handshake carries it as Hello.last_acked_position
// — the server honours resume input from both QueueAck and Hello, so a lost
// ack frame costs nothing under at-least-once delivery.
func (c *Client) Acknowledge(position uint64) {
	for {
		cur := c.acked.Load()
		if position <= cur {
			return // monotonic: stale or duplicate acks move nothing
		}
		if c.acked.CompareAndSwap(cur, position) {
			break
		}
	}
	if err := c.send(&walkiev1.Envelope{
		Payload: &walkiev1.Envelope_QueueAck{QueueAck: &walkiev1.QueueAck{
			AcknowledgedPosition: position,
		}},
	}); err != nil && !errors.Is(err, errNoSession) {
		// The wire half failed but the local record stands: the next Hello
		// delivers the same fact. Losing an ack must never cost anything.
		c.logger.Warn("control: queue ack send failed",
			slog.Uint64("acknowledged_position", position),
			slog.String("reason", err.Error()),
		)
	}
}

// LastAcked reports the highest acknowledged position (diagnostics and
// tests; the authoritative use is inside the handshake).
func (c *Client) LastAcked() uint64 { return c.acked.Load() }

// RestoreAckPosition seeds the acknowledged high-water from durable state —
// the store's record of this device's acknowledgements, loaded when the
// process starts. Without it a restarted client would forget everything it
// had acknowledged and every reconnect would replay from zero, which is
// precisely the full-replay outcome criterion 4 forbids. Unlike
// Acknowledge this sends nothing: the position came FROM durable agreement
// with the coordinator, not from newly processed work.
func (c *Client) RestoreAckPosition(position uint64) {
	for {
		cur := c.acked.Load()
		if position <= cur {
			return
		}
		if c.acked.CompareAndSwap(cur, position) {
			return
		}
	}
}

// errNoSession is send's internal shape for "offline"; Send maps it to the
// exported ErrOffline so callers can errors.Is against a stable sentinel.
var errNoSession = errors.New("no live session")

// send writes env to the live session, if one exists. Serialization matters:
// heartbeat beats, handshake frames and user sends share one WebSocket, and
// coder/websocket permits one concurrent writer.
func (c *Client) send(env *walkiev1.Envelope) error {
	c.mu.Lock()
	sess := c.sess
	c.mu.Unlock()
	if sess == nil {
		return errNoSession
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return sess.Send(env)
}

// run is the supervisor loop. See the type comment for the shape; the
// attempt-index discipline bears repeating here because it IS criterion 2:
// attempt counts consecutive failures since the last successful handshake,
// the delay for attempt n is drawn BEFORE the counter increments (so the
// first retry always draws from the base envelope), and reaching online
// resets it to zero.
func (c *Client) run(ctx context.Context) {
	defer close(c.done)
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := c.mach.Transition(StateConnecting, "dial attempt started"); err != nil {
			// Machine closed under us (consumer shutdown racing Start-era
			// loops): nothing left to drive.
			c.logger.Debug("control: supervisor exiting", slog.String("reason", err.Error()))
			return
		}

		sess, err := c.dial(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.transition(StateDisconnected, fmt.Sprintf("dial failed: %v", err))
			if !c.backoffWait(ctx, attempt) {
				return
			}
			attempt++
			continue
		}

		if _, err := c.mach.Transition(StateHandshaking, "transport established"); err != nil {
			_ = sess.Close()
			return
		}
		c.setInflight(sess)

		ack, err := c.handshake(sess)
		if err != nil {
			_ = sess.Close()
			c.clearInflight(sess)
			if ctx.Err() != nil {
				return
			}
			c.transition(StateDisconnected, fmt.Sprintf("handshake failed: %v", err))
			if !c.backoffWait(ctx, attempt) {
				return
			}
			attempt++
			continue
		}

		// Publish BEFORE the online transition: from the moment the machine
		// says online, user sends and acknowledges must work — installing
		// any later would make an online client briefly reject sends with
		// ErrOffline, a lie criterion 6's consumers would render.
		c.installSession(sess)
		c.setIdentity(ack.GetDevice())
		if _, err := c.mach.Transition(StateOnline, "handshake completed"); err != nil {
			_ = sess.Close()
			return
		}
		attempt = 0 // reset on successful online: the next drop retries from base

		c.serve(sess)

		if ctx.Err() != nil {
			return
		}
		// The session died after being online. attempt is 0 (reset above),
		// so the first post-drop retry draws from the base envelope — the
		// whole fleet spreads across [0, BackoffBase) instead of piling up.
		if !c.backoffWait(ctx, attempt) {
			return
		}
		attempt++
	}
}

// transition applies one machine edge, absorbing shutdown races (a
// transition refused because the consumer closed the machine is teardown
// ordering, not a defect — the same posture Transition itself documents).
func (c *Client) transition(to State, reason string) {
	if _, err := c.mach.Transition(to, reason); err != nil {
		c.logger.Debug("control: transition refused",
			slog.String("to", to.String()),
			slog.String("reason", err.Error()),
		)
	}
}

// backoffWait sleeps schedule(attempt) on the injected clock — never
// time.Sleep — and reports false if the client was stopped first. The timer
// is armed for the full drawn delay: no polling, no quantization, the exact
// property the herd test measures.
func (c *Client) backoffWait(ctx context.Context, attempt int) bool {
	delay := c.schedule(attempt)
	t := c.clk.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-c.quit:
		return false
	case <-t.C():
		return true
	}
}

// handshake performs the Hello/HelloAck exchange on a freshly dialed session.
//
// The exchange runs on THIS goroutine with a deadline timer, while Recv
// blocks in a helper goroutine feeding a channel — a blocking Recv selected
// directly against a timer could never lose to it, and a coordinator that
// accepts the socket and answers nothing would wedge the client in
// handshaking forever. The timeout is the phase's configured bound
// (Config.HandshakeTimeout); firing it is a handshake failure like any
// other: disconnected, backoff, retry.
//
// Frames before HelloAck are skipped, not fatal: with presence wired the
// roster snapshot precedes the ack by design (server.go), and mixed-fleet
// peers may speak payloads this build ignores (adr:003). The roster snapshot
// is the one exception — it is FORWARDED to the consumer handler, not
// dropped: the coordinator sends it exactly once per connection, ahead of
// the ack (server.go serveConn), so dropping it would leave a fresh client's
// roster empty until the first TTL-driven refresh. Forwarding here is what
// makes sto:terminal-ui's device list immediate instead of one TTL late.
// Everything else pre-ack stays skipped: only presence is promised before
// the ack, and forwarding unknown payloads to a handler that has not yet
// been told who it is (identity lands with the ack) invites misfiled work.
// A ProtocolError,
// though, is the coordinator REFUSING — version gate or malformed Hello —
// and refusing back is correct: retrying unchanged would only repeat it.
// The ack's echoed protocol_version closes the silent-skew hole the schema
// documents: an older coordinator ignoring our version field announces
// itself here and the handshake fails diagnosably instead of proceeding on
// misunderstood ground.
func (c *Client) handshake(sess Session) (*walkiev1.HelloAck, error) {
	hello := &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Hello{Hello: &walkiev1.Hello{
			ClientVersion:     clientVersion,
			ProtocolVersion:   walkiev1.MaxProtocolVersion,
			LastAckedPosition: c.acked.Load(), // criterion 4: resume, do not replay
		}},
	}
	if err := sess.Send(hello); err != nil {
		return nil, fmt.Errorf("send hello: %w", err)
	}

	replies := make(chan envelopeOrErr, 1)
	go func() {
		env, err := sess.Recv()
		replies <- envelopeOrErr{env: env, err: err}
	}()

	timer := c.clk.NewTimer(c.cfg.HandshakeTimeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
			return nil, fmt.Errorf("handshake timed out after %s", c.cfg.HandshakeTimeout)
		case <-c.quit:
			return nil, fmt.Errorf("client stopped mid-handshake")
		case r := <-replies:
			if r.err != nil {
				return nil, fmt.Errorf("await hello_ack: %w", r.err)
			}
			switch payload := r.env.GetPayload().(type) {
			case *walkiev1.Envelope_HelloAck:
				ack := payload.HelloAck
				if !walkiev1.ProtocolVersionSupported(ack.GetProtocolVersion()) {
					return nil, fmt.Errorf("coordinator protocol_version %d outside supported range [%d,%d]",
						ack.GetProtocolVersion(), walkiev1.MinProtocolVersion, walkiev1.MaxProtocolVersion)
				}
				return ack, nil
			case *walkiev1.Envelope_ProtocolError:
				return nil, fmt.Errorf("coordinator refused handshake: %s (%s)",
					payload.ProtocolError.GetDetail(), payload.ProtocolError.GetCode())
			default:
				// Roster chatter ahead of the ack: forward it to the
				// consumer (the snapshot is delivered exactly once, here —
				// see the comment above) and keep waiting for the ack.
				if r.env.GetPresenceUpdate() != nil && c.onEnv != nil {
					c.onEnv(r.env)
				}
			}
			// This iteration consumed the single Recv; arm another reader
			// for the next frame.
			go func() {
				env, err := sess.Recv()
				replies <- envelopeOrErr{env: env, err: err}
			}()
		}
	}
}

type envelopeOrErr struct {
	env *walkiev1.Envelope
	err error
}

// serve owns one established session until it dies: watchdog up, read loop
// running, sends flowing. It returns only when the session is dead AND torn
// down, with the machine already on disconnected (placed there by whichever
// path observed the death first — the watchdog's verdicts or the read loop's
// SendFailed; the transition table absorbs the race).
//
// # Why serve also watches the machine
//
// The dead-peer verdict fires in the WATCHDOG's goroutine: degraded, then a
// deliberate teardown to disconnected. Without someone acting on those
// events, the read loop would stay parked in Recv on a socket the policy has
// already condemned — the diagnosis would be recorded and nothing done. The
// subscription branch below is the actuator: the moment the machine leaves
// online, the session is closed, which unblocks Recv, which funnels back
// through SendFailed (a same-state no-op by then) and releases everything.
func (c *Client) serve(sess Session) {
	// Drain history left over from previous sessions: the machine's current
	// state is authoritative (mach.State()), and a stale "disconnected" from
	// the LAST session must not condemn the fresh one.
	for {
		select {
		case <-c.sub.C():
			continue
		default:
		}
		break
	}

	watchdog, err := StartWatchdog(c.cfg, c.mach, func(env *walkiev1.Envelope) error {
		return c.send(env)
	}, c.clk, c.logger)
	if err != nil {
		// Unreachable in practice: NewClient validated the same cfg. A
		// session that cannot be watched is a session that cannot be kept
		// honest — refuse it rather than run unwatched.
		c.transition(StateDisconnected, fmt.Sprintf("watchdog refused: %v", err))
		_ = sess.Close()
		return
	}

	sessDone := make(chan struct{})
	go c.readLoop(sess, watchdog, sessDone)

	teardown := sync.Once{}
	teardownSession := func() {
		teardown.Do(func() {
			watchdog.Stop()
			_ = sess.Close() // unblocks Recv; readLoop drains out through sessDone
		})
	}

	for {
		select {
		case <-sessDone:
			teardownSession() // idempotent: covers the read-error-first ordering
			c.clearSession(sess)
			return
		case <-c.sub.C():
			// The event is only a HINT that something happened; the machine's
			// CURRENT state decides. Change delivery is asynchronous, so a
			// stale event from a PREVIOUS session can land here after this
			// session went online — tearing down on the event alone would
			// kill a healthy session, leave the machine online, and send the
			// supervisor straight into an illegal online->connecting edge.
			switch c.mach.State() {
			case StateDegraded, StateDisconnected:
				teardownSession()
			}
		}
	}
}

// readLoop is the session's single reader: every inbound frame proves life
// (Watchdog.Activity — ANY frame, not just heartbeats) and is handed to the
// consumer's handler. A read error is failure mode (b): proof of death,
// routed through SendFailed so the machine goes straight to disconnected
// with no degraded waypoint, exactly like a failed write.
func (c *Client) readLoop(sess Session, watchdog *Watchdog, sessDone chan struct{}) {
	defer close(sessDone)
	for {
		env, err := sess.Recv()
		if err != nil {
			watchdog.SendFailed(err)
			return
		}
		watchdog.Activity()
		switch payload := env.GetPayload().(type) {
		case *walkiev1.Envelope_HelloAck:
			// Post-handshake duplicates carry no new information (the
			// handshake already consumed the real one); ignore.
			continue
		case *walkiev1.Envelope_ProtocolError:
			c.logger.Warn("control: coordinator reported protocol error",
				slog.String("code", payload.ProtocolError.GetCode().String()),
				slog.String("detail", payload.ProtocolError.GetDetail()),
			)
		}
		if c.onEnv != nil {
			c.onEnv(env)
		}
	}
}

// clearSession detaches a dead session from the client so later Sends report
// offline instead of writing into a corpse. Guarded by identity: only this
// supervisor installs sessions, sequentially, so the check is insurance
// against future reordering rather than a live race today.
func (c *Client) clearSession(sess Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sess == sess {
		c.sess = nil
	}
}

// setInflight / clearInflight track the dialed-but-not-yet-online session so
// Stop can close a handshake that is blocked in Recv on a clock nobody is
// advancing. Guarded by mu; identity-checked clears are insurance against
// reordering, same as clearSession.
func (c *Client) setInflight(sess Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight = sess
}

func (c *Client) clearInflight(sess Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight == sess {
		c.inflight = nil
	}
}

// installSession publishes the live session so user-facing sends can flow.
// Called only from serve, which the supervisor runs strictly between
// handshakes — there is never a previous live session to race.
func (c *Client) installSession(sess Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sess = sess
	c.inflight = nil
}
