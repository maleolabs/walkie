package ctlsocket

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Protocol bounds. Both exist so one misbehaving local client cannot grow
// server memory without bound; the socket is owner-only, but "local and
// trusted" is not a reason to make every resource unbounded (the same rule
// the offline queue's retention caps follow).
const (
	// maxLineBytes caps one request line. A request is a command plus at most
	// a 4 KiB message body or 256-byte status; 64 KiB leaves orders of
	// magnitude of headroom for JSON overhead while keeping a hostile or
	// broken writer from pinning memory in the read buffer.
	maxLineBytes = 64 << 10

	// DefaultMaxQueuedEvents is the per-subscriber event-queue bound behind
	// the slow-subscriber policy (see the package comment for why drop-and-
	// report rather than disconnect). 256 events is several screens of
	// activity — far more than an interactive reader needs to keep up, small
	// enough that dozens of abandoned subscribers cost nothing.
	DefaultMaxQueuedEvents = 256

	// DefaultWriteTimeout bounds each socket write. A subscriber that stops
	// READING is handled by the queue bound; a subscriber whose kernel buffer
	// stays full (a stopped process, SIGSTOP) would otherwise block its pump
	// goroutine on write forever. The deadline turns that into a disconnect,
	// which is the correct outcome: the connection is dead for any practical
	// purpose. Generous, because a legitimate reader only needs to drain its
	// own socket buffer.
	DefaultWriteTimeout = 10 * time.Second
)

// Config tunes the server. Zero fields take the documented defaults.
type Config struct {
	// MaxQueuedEvents overrides DefaultMaxQueuedEvents when > 0.
	MaxQueuedEvents int

	// WriteTimeout overrides DefaultWriteTimeout when > 0. Tests use this to
	// exercise the stalled-writer disconnect without real time.
	WriteTimeout time.Duration
}

func (c Config) maxQueued() int {
	if c.MaxQueuedEvents > 0 {
		return c.MaxQueuedEvents
	}
	return DefaultMaxQueuedEvents
}

func (c Config) writeTimeout() time.Duration {
	if c.WriteTimeout > 0 {
		return c.WriteTimeout
	}
	return DefaultWriteTimeout
}

// Handlers are the four seams the command surface calls into. Every field is
// required; New refuses nils so a half-wired server fails at assembly instead
// of answering scripts with nil-pointer panics mid-session.
type Handlers struct {
	// Send transmits one message. to == "" means broadcast. It returns an
	// error when the client is offline or the draft is invalid; the error
	// text goes to the scripting client verbatim, so it should name the fix.
	Send func(to, body string) error

	// SetStatus sets this device's custom status label ("" clears it).
	SetStatus func(status string) error

	// Presence returns the full roster snapshot for the "presence" command
	// and the subscribe-time snapshot event.
	Presence func() []PresenceEntry

	// ConnState returns the current control-plane state spelling
	// (control.State.String — wire-stable).
	ConnState func() string
}

// PresenceEntry is one roster line as the protocol carries it. It mirrors
// presenceview.Device with the timestamp already rendered (RFC3339Nano UTC)
// so this package never imports protobuf types or view internals.
type PresenceEntry struct {
	Device   string `json:"device"`
	Online   bool   `json:"online"`
	LastSeen string `json:"last_seen,omitempty"` // "" = online now, or never observed
	Status   string `json:"status,omitempty"`
}

// Server serves the control protocol on a listener produced by [Listen].
//
// Concurrency: Serve runs an accept loop (start it in its own goroutine);
// Publish* are safe from any goroutine — they never block on a slow
// subscriber, which is the load-bearing property behind acceptance criterion
// 5. Close is idempotent and shuts everything down.
//
// There is deliberately no method here that executes anything the connected
// peer asks for beyond the four handlers above — see the package comment on
// adr:005 before adding one.
type Server struct {
	handlers Handlers
	cfg      Config
	logger   *slog.Logger

	mu      sync.Mutex
	conns   map[*serverConn]struct{}
	closed  bool
	closeMu sync.Once
}

// New returns a Server wired to handlers. A nil logger falls back to slog's
// default.
func New(h Handlers, cfg Config, logger *slog.Logger) (*Server, error) {
	if h.Send == nil || h.SetStatus == nil || h.Presence == nil || h.ConnState == nil {
		return nil, errors.New("ctlsocket: New: all Handlers fields must be non-nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		handlers: h,
		cfg:      cfg,
		logger:   logger,
		conns:    make(map[*serverConn]struct{}),
	}, nil
}

// Serve accepts connections until l is closed or Close is called. It returns
// always — nil after a deliberate close, the accept error otherwise. Run it
// in its own goroutine from assembly code.
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			// Failure-class log, not silence: assembly runs Serve in a
			// fire-and-forget goroutine and discards this return, so here is the
			// only place an accept failure would otherwise vanish. The error is
			// between us and the OS — it names no request or event content.
			s.logger.Warn("ctlsocket: accept failed; control surface stops accepting",
				slog.String("error", err.Error()))
			return fmt.Errorf("ctlsocket: accept: %w", err)
		}

		s.mu.Lock()
		if s.closed {
			// Close raced the accept: Close snapshotted conns before this
			// registration, so nothing else will ever tear this socket down.
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		sc := &serverConn{srv: s, c: conn}
		s.conns[sc] = struct{}{}
		s.mu.Unlock()

		go sc.readLoop()
	}
}

// Close shuts the server down: every live connection is dropped and every
// subscription ends. The LISTENER is the caller's — assembly owns Listen's
// result and closes it alongside. Idempotent.
func (s *Server) Close() {
	s.closeMu.Do(func() {
		s.mu.Lock()
		s.closed = true
		conns := make([]*serverConn, 0, len(s.conns))
		for sc := range s.conns {
			conns = append(conns, sc)
		}
		s.conns = nil
		s.mu.Unlock()
		for _, sc := range conns {
			sc.shutdown()
		}
	})
}

// PublishConn emits a conn event. from/to use control.State.String spellings;
// at is pre-rendered by the caller (RFC3339Nano) so this package owns no
// clock — deterministic tests inject whatever string they like.
func (s *Server) PublishConn(from, to, reason, at string) {
	s.publish(map[string]any{
		"from":   from,
		"to":     to,
		"reason": reason,
		"at":     at,
	}, "conn")
}

// PublishPresence emits one presence change.
func (s *Server) PublishPresence(entry PresenceEntry) {
	s.publish(map[string]any{
		"device":    entry.Device,
		"online":    entry.Online,
		"last_seen": entry.LastSeen,
		"status":    entry.Status,
	}, "presence")
}

// PublishMessage emits a message event. It takes metadata fields ONLY — there
// is no body parameter to pass, which makes "no content on the event stream"
// a property of the API shape rather than of caller discipline.
func (s *Server) PublishMessage(id, sender, recipient, conversation string) {
	s.publish(map[string]any{
		"id":           id,
		"from":         sender,
		"to":           recipient,
		"conversation": conversation,
	}, "message")
}

// publish fans one event out to every subscriber. Never blocks: a full
// per-subscriber queue records a drop (reported later as a gap event) and
// moves on. This is the producer-side half of criterion 5.
func (s *Server) publish(fields map[string]any, name string) {
	line := eventLine(name, fields)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for sc := range s.conns {
		if sub := sc.subscriber(); sub != nil {
			sub.enqueue(line)
		}
	}
}

// eventLine renders one NDJSON event. encoding/json sorts map keys, so field
// order is stable across events — diff-friendly and irrelevant to parsers,
// but free determinism is free.
func eventLine(name string, fields map[string]any) []byte {
	m := make(map[string]any, len(fields)+2)
	m["type"] = "event"
	m["event"] = name
	for k, v := range fields {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		// Unreachable for the vocabularies above (all strings/bools), but a
		// silent no-event would be worse than a loud placeholder line.
		return []byte(`{"type":"event","event":"encode-error"}` + "\n")
	}
	return append(b, '\n')
}

// serverConn is one accepted connection: its socket, its optional event
// subscription, and the mutex that serialises writes from the two goroutines
// that may write (the read loop writing responses, the pump writing events).
type serverConn struct {
	srv *Server
	c   net.Conn

	wmu sync.Mutex

	subMu sync.Mutex
	sub   *subscriber
}

func (sc *serverConn) subscriber() *subscriber {
	sc.subMu.Lock()
	defer sc.subMu.Unlock()
	return sc.sub
}

// writeLine writes one already-newline-terminated line under the connection
// write lock, with the configured deadline. Any error closes the connection:
// a failed write on a stream means framed output is no longer trustworthy,
// and both loops notice the close through their own paths.
func (sc *serverConn) writeLine(line []byte) error {
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	_ = sc.c.SetWriteDeadline(time.Now().Add(sc.srv.cfg.writeTimeout()))
	_, err := sc.c.Write(line)
	if err != nil {
		_ = sc.c.Close()
	}
	return err
}

// shutdown tears the connection down once: closes the socket (which unblocks
// both goroutines) and detaches any subscription from the server's fan-out.
// Safe to call from multiple paths — read-loop exit, pump write failure,
// Server.Close — because every step is idempotent.
func (sc *serverConn) shutdown() {
	sc.subMu.Lock()
	sub := sc.sub
	sc.subMu.Unlock()
	if sub != nil {
		sub.close()
	}
	_ = sc.c.Close()

	sc.srv.mu.Lock()
	delete(sc.srv.conns, sc)
	sc.srv.mu.Unlock()
}

// readLoop reads request lines until the peer goes away, dispatching each.
// One goroutine per connection; exits when the socket closes from either
// side.
func (sc *serverConn) readLoop() {
	defer sc.shutdown()

	scan := bufio.NewScanner(sc.c)
	scan.Buffer(make([]byte, 0, 4096), maxLineBytes)
	for scan.Scan() {
		line := scan.Bytes()
		if len(line) == 0 {
			continue // blank lines are noise a shell loop can emit; skip, don't fail
		}
		resp := sc.dispatch(line)
		if resp == nil {
			return // writeLine already closed the connection
		}
		if err := sc.writeLine(resp); err != nil {
			return // writeLine already closed the connection
		}
	}
	// Scanner error (including bufio.ErrTooLong from an oversized line) ends
	// the connection: a lost newline makes line framing unrecoverable, so
	// there is nothing honest to resync to. The error itself is between this
	// walkie and the OS — it names no request content.
	_ = scan.Err()
}

// dispatch parses and runs one request line, returning the response line.
// Malformed input answers with an error response and KEEPS the connection: a
// shell loop iterating over half-built JSON should be able to try again
// without reconnecting. Only framing-level breakage (scanner errors above)
// ends the connection.
func (sc *serverConn) dispatch(line []byte) []byte {
	var req struct {
		Cmd    string          `json:"cmd"`
		ID     json.RawMessage `json:"id"`
		To     string          `json:"to"`
		Body   string          `json:"body"`
		Status string          `json:"status"`
	}
	if err := json.Unmarshal(line, &req); err != nil {
		return sc.response(nil, fmt.Errorf("malformed request line: %w", err))
	}

	switch req.Cmd {
	case "send":
		if err := sc.srv.handlers.Send(req.To, req.Body); err != nil {
			return sc.response(req.ID, err)
		}
		return sc.response(req.ID, nil)
	case "status":
		if err := sc.srv.handlers.SetStatus(req.Status); err != nil {
			return sc.response(req.ID, err)
		}
		return sc.response(req.ID, nil)
	case "presence":
		return sc.responseValue(req.ID, map[string]any{"ok": true, "presence": rosterOrEmpty(sc.srv.handlers.Presence())})
	case "subscribe":
		return sc.subscribe(req.ID)
	default:
		return sc.response(req.ID, fmt.Errorf("unknown cmd %q", req.Cmd))
	}
}

// subscribe attaches the connection to the event stream and seeds it with a
// snapshot, delivered by the connection's new pump goroutine.
//
// Registration happens BEFORE the snapshot is read — the same subscribe-first
// rule internal/control's Machine.Subscribe documents: an event landing in
// between may then appear in BOTH the snapshot and the stream, a harmless
// duplicate of two absolute statements, whereas the other order could MISS
// one outright.
func (sc *serverConn) subscribe(id json.RawMessage) []byte {
	sc.subMu.Lock()
	if sc.sub != nil {
		sc.subMu.Unlock()
		return sc.response(id, errors.New("already subscribed"))
	}
	sub := newSubscriber(sc.srv.cfg.maxQueued())
	sc.sub = sub
	sc.subMu.Unlock()

	sc.srv.mu.Lock()
	if sc.srv.closed {
		sc.srv.mu.Unlock()
		sc.subMu.Lock()
		sc.sub = nil
		sc.subMu.Unlock()
		return sc.response(id, errors.New("server shutting down"))
	}
	sc.srv.mu.Unlock()

	// Seed AFTER registration, into the subscriber's own queue: room is
	// guaranteed (the queue is empty), so the snapshot can never be dropped —
	// a gap event pointing at a snapshot the reader never got would be
	// absurd. It is written HERE, synchronously, before the pump exists and
	// before the response line: a script's first read after subscribing is
	// then the current state. One nuance a reader of this code should not
	// mistake for a race: an event published between registration and this
	// enqueue can precede the snapshot in the queue, so that live event may
	// be read first and then duplicated by the snapshot — duplicate-not-miss,
	// exactly what the registration-first rule above buys.
	snap := eventLine("snapshot", map[string]any{
		"conn":     map[string]any{"state": sc.srv.handlers.ConnState()},
		"presence": rosterOrEmpty(sc.srv.handlers.Presence()),
	})
	sub.enqueue(snap)
	for _, line := range sub.take() {
		if err := sc.writeLine(line); err != nil {
			return nil // connection died mid-subscribe; readLoop exits via shutdown
		}
	}

	go sc.pump(sub)
	return sc.response(id, nil)
}

// rosterOrEmpty keeps the JSON an array rather than null when the roster is
// empty — jq pipelines everywhere are happier for it.
func rosterOrEmpty(r []PresenceEntry) []PresenceEntry {
	if r == nil {
		return []PresenceEntry{}
	}
	return r
}

// response renders a response line. id (optional) is echoed verbatim.
func (sc *serverConn) response(id json.RawMessage, err error) []byte {
	var payload map[string]any
	if err != nil {
		payload = map[string]any{"ok": false, "error": err.Error()}
	} else {
		payload = map[string]any{"ok": true}
	}
	return sc.responseValue(id, payload)
}

func (sc *serverConn) responseValue(id json.RawMessage, payload map[string]any) []byte {
	m := make(map[string]any, len(payload)+2)
	m["type"] = "response"
	for k, v := range payload {
		m[k] = v
	}
	if len(id) > 0 {
		m["id"] = json.RawMessage(id)
	}
	b, err := json.Marshal(m)
	if err != nil {
		b = []byte(`{"type":"response","ok":false,"error":"internal encode error"}`)
	}
	return append(b, '\n')
}

// subscriber is the bounded queue behind the slow-subscriber policy: what the
// publisher appends to (never blocking) and the pump drains from.
//
// Drop accounting: an enqueue that finds a full queue discards the NEW event
// and increments dropped. The pump folds accumulated drops into a gap event
// heading the next batch it delivers, so the reader learns "N events between
// what you last saw and what follows were discarded" BEFORE reading what
// follows — loss is reported positionally, not just counted somewhere.
type subscriber struct {
	queueCap int

	mu      sync.Mutex
	queue   [][]byte
	dropped int
	notify  chan struct{} // capacity-1 wake signal for the pump
	done    chan struct{}
	once    sync.Once
}

func newSubscriber(queueCap int) *subscriber {
	return &subscriber{
		queueCap: queueCap,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// enqueue files one event line, or records a drop when the subscriber's queue
// is full. Called from publish under the server mutex; the subscriber mutex
// is inner, so lock order is always srv.mu → sub.mu.
func (sub *subscriber) enqueue(line []byte) {
	sub.mu.Lock()
	if len(sub.queue) >= sub.queueCap {
		sub.dropped++
	} else {
		sub.queue = append(sub.queue, line)
	}
	sub.mu.Unlock()
	// Nudge on BOTH paths: a drop with no following successful enqueue must
	// still wake the pump, or the gap report would sit undelivered until some
	// later event happened along.
	select {
	case sub.notify <- struct{}{}:
	default:
	}
}

// take snapshots the queue for delivery, folding any accumulated drops into a
// leading gap event. The gap comes FIRST because everything it describes is
// older than everything in the rest of the batch.
func (sub *subscriber) take() [][]byte {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	batch := sub.queue
	sub.queue = nil
	if sub.dropped > 0 {
		gap := eventLine("gap", map[string]any{"dropped": sub.dropped})
		sub.dropped = 0
		batch = append([][]byte{gap}, batch...)
	}
	return batch
}

// close ends the subscription: the pump stops promptly. Fan-out membership
// removal belongs to the connection's shutdown path. Idempotent.
func (sub *subscriber) close() {
	sub.once.Do(func() { close(sub.done) })
}

// pending reports queued-but-undelivered events. Test observability for the
// bounded-memory assertion: the bound lives HERE, so the assertion reads it
// here rather than inferring it from behaviour alone.
func (sub *subscriber) pending() int {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return len(sub.queue)
}

// droppedCount reports drops recorded but not yet delivered as a gap. Same
// test-observability role as pending.
func (sub *subscriber) droppedCount() int {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.dropped
}

// pump delivers queued events to the connection until the subscription ends
// or a write fails. One goroutine per SUBSCRIBED connection (not per
// connection): a command-only connection costs nothing but its read loop.
func (sc *serverConn) pump(sub *subscriber) {
	for {
		select {
		case <-sub.notify:
		case <-sub.done:
			return
		}

		for _, line := range sub.take() {
			if err := sc.writeLine(line); err != nil {
				// Slow-to-the-kernel subscriber (SIGSTOP'd process, vanished
				// peer): the deadline turned the stall into an error. Tear
				// down; the read loop's shutdown path unsubscribes us. Logged
				// as a failure class only — the event line itself never
				// reaches the journal, it belongs to the script on the far
				// side, not to the log.
				sc.srv.logger.Warn("ctlsocket: subscriber write failed past deadline; disconnecting",
					slog.String("error", err.Error()))
				sc.shutdown()
				return
			}
		}

		// Re-check under the same wake discipline as control.Subscription's
		// pump: notify is capacity-1, so a publish racing this drain must not
		// be lost if it arrived after the final take.
		if sub.pending() == 0 {
			select {
			case <-sub.done:
				return
			default:
			}
		}
	}
}
