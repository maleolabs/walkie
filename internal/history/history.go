package history

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/store"
)

// dirMode is the permission enforced on the store's parent directory on every
// Open.
//
// Why the directory and not just the file: a 0600 file inside a 0755
// directory is weaker than it looks — on a multi-user host anyone who can
// traverse the directory can attempt the database through every program that
// runs as them, and SQLite's own journal side files are created with the
// process umask regardless of the main file's mode. The directory is the
// boundary that actually contains the secret, so it gets the same
// enforce-on-every-open treatment store.Open applies to the file: os.Chmod is
// not umask-filtered, so a directory created loose by an older build or
// another tool is tightened rather than inherited.
const dirMode os.FileMode = 0o700

// Store is the client's durable message history: append-only persistence of
// [message.Message] values in one SQLite file, queryable by conversation and
// by time range, with bounded retention.
//
// Owning work item:
//
//	eka get walkie/sto:message-history
//
// # What ordering means here, and what it does not
//
// Rows carry seq, this device's local arrival order. Display on reopen orders
// BY CONVERSATION on that arrival sequence — per-conversation order is the
// whole guarantee, because req:text-messaging provides exactly that and
// explicitly no global total order. The schema could tempt a reader into
// seeing a global order in seq (it does sort across conversations); that
// reading is wrong: seq measures when THIS device observed messages, which
// becomes meaningless the moment two devices are compared or the phase-2
// direct data plane removes the single coordinator choke point. Nothing here
// exposes or promises an order across conversations; see internal/message's
// package comment for why code that leans on one breaks invisibly later.
//
// # Retention is bounded, both ways
//
// A TTL (age measured from stored_at, this device's clock at insert) and a
// size cap (total rows) both apply. Reaching the cap EVICTS THE OLDEST rows
// loudly rather than refusing new writes — the deliberate opposite of the
// coordinator queue's refusal semantics (sto:offline-queue): there the sender
// is present to receive a QueueRefused, here refusing to record a message the
// user just watched arrive would be a silent loss wearing an error's clothes,
// whereas evicting the oldest history is the least-surprising bounded outcome
// and it is logged. Both bounds are enforced on every Append and at Open, so
// boundedness never depends on traffic arriving or a background timer.
//
// Expiry and eviction logs carry counts and bounds only — never bodies, never
// senders (the no-content rule; identifiers identify without disclosing).
type Store struct {
	st      *store.Store
	clk     clock.Clock
	ttl     time.Duration
	maxRows int
	logger  *slog.Logger
	// local is this device's tailnet name; conversation derivation needs it to
	// tell which side of a DM is us. Immutable after Open.
	local string
}

// Options configures a [Store]. All fields are required: a zero TTL would
// expire everything instantly and a zero cap would evict everything — wiring
// bugs, not modes, refused at Open per the house rule (queue.New refuses the
// same way).
type Options struct {
	// Local is this device's tailnet name, as HelloAck reports it. Conversation
	// derivation needs it to tell which side of a DM is us; see
	// [message.ConversationKeyFor].
	Local string

	// TTL is how long a message stays in local history past its storage time.
	TTL time.Duration

	// MaxMessages is the total-row cap across all conversations.
	MaxMessages int

	// Logger receives retention events. nil falls back to slog.Default.
	Logger *slog.Logger
}

// Open opens (creating if necessary) the history database at path and returns
// a handle. The parent directory is created 0700 and the file 0600, both
// enforced on every open — see dirMode and store.Open's fileMode for why each
// level is enforced rather than set once.
//
// A startup sweep runs before Open returns, so messages that lapsed while no
// client was running are evicted and logged at startup, never displayed stale;
// the same sweep rides every Append, which is what keeps retention working
// during a long-lived session without a watcher goroutine. At human messaging
// rates the per-append cost is noise, and dropping the goroutine removes the
// one piece of concurrency this package would otherwise own.
func Open(path string, clk clock.Clock, opts Options) (*Store, error) {
	if clk == nil {
		return nil, fmt.Errorf("history: open %s: clk must not be nil", path)
	}
	if opts.Local == "" {
		return nil, fmt.Errorf("history: open %s: opts.Local must name this device", path)
	}
	if opts.TTL <= 0 {
		return nil, fmt.Errorf("history: open %s: opts.TTL must be positive (got %s)", path, opts.TTL)
	}
	if opts.MaxMessages <= 0 {
		return nil, fmt.Errorf("history: open %s: opts.MaxMessages must be positive (got %d)", path, opts.MaxMessages)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("history: resolve %s: %w", path, err)
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("history: create %s: %w", dir, err)
	}
	// MkdirAll's mode is umask-filtered; Chmod is not. Enforcing on every open
	// also tightens a pre-existing loose directory — same reasoning as
	// store.Open's Chmod on the file.
	if err := os.Chmod(dir, dirMode); err != nil {
		return nil, fmt.Errorf("history: enforce %#o on %s: %w", dirMode, dir, err)
	}

	st, err := store.Open(abs, clk)
	if err != nil {
		return nil, fmt.Errorf("history: open %s: %w", abs, err)
	}

	s := &Store{st: st, clk: clk, ttl: opts.TTL, maxRows: opts.MaxMessages, logger: logger, local: opts.Local}
	if err := s.enforceRetention(); err != nil {
		st.Close()
		return nil, fmt.Errorf("history: startup retention sweep: %w", err)
	}
	return s, nil
}

// Close releases the database handle. Every mutation committed before it
// returns, so closing loses nothing.
func (s *Store) Close() error {
	return s.st.Close()
}

// Append persists msg, filing it into its conversation exactly as the display
// Log would ([message.ConversationKeyFor] — one derivation, so display and
// history can never disagree about where a message lives).
//
// Duplicate ULIDs are ignored silently: delivery is at-least-once on the wire
// and dedup-by-ULID-at-the-receiver is the contract (req:text-messaging), so
// a replayed message arriving here is the mechanism working, not an event
// worth logging. Retention runs after the insert, keeping both bounds honest
// even if this is the only call the process ever makes.
func (s *Store) Append(msg message.Message) error {
	if msg.ID == "" {
		return errors.New("history: append: message ID must not be empty")
	}
	conversation := message.ConversationKeyFor(s.local, msg)
	_, err := s.st.DB().Exec(
		`INSERT OR IGNORE INTO history_message
			(message_id, conversation, sender, recipient, body, sent_at, received_at, stored_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, conversation, msg.Sender, msg.Recipient, msg.Body,
		formatStoredTime(msg.SentAt), formatStoredTime(msg.ReceivedAt), formatStoredTime(s.clk.Now()),
	)
	if err != nil {
		return fmt.Errorf("history: append %s: %w", msg.ID, err)
	}
	if err := s.enforceRetention(); err != nil {
		return fmt.Errorf("history: append %s: retention: %w", msg.ID, err)
	}
	return nil
}

// QueryOptions selects what [Store.Query] returns. Zero-value fields mean
// "no constraint": empty Conversation matches all conversations, nil From/To
// leave the time range open.
type QueryOptions struct {
	// Conversation is a full conversation key ([message.ConversationKey] or
	// [message.BroadcastConversation]), not a peer name — callers derive it
	// with the same helper the display uses.
	Conversation string

	// From and To bound received_at INCLUSIVELY on both ends. received_at is
	// the trustworthy half of the timestamp pair (one coordinator clock all
	// devices agree on); filtering on sent_at would filter on fleet-skewed
	// sender clocks, and req:text-messaging keeps both timestamps precisely so
	// skew stays legible instead of being baked into queries.
	From, To *time.Time
}

// Query returns the matching messages ordered by local arrival (seq), oldest
// first. Within one conversation that IS per-conversation order; across
// conversations it is merely the order this device observed things, which is
// presentation-stable output, not a global order — see the type comment.
func (s *Store) Query(opts QueryOptions) ([]message.Message, error) {
	where := ` WHERE TRUE`
	var args []any
	if opts.Conversation != "" {
		where += ` AND conversation = ?`
		args = append(args, opts.Conversation)
	}
	if opts.From != nil {
		where += ` AND received_at >= ?`
		args = append(args, formatStoredTime(*opts.From))
	}
	if opts.To != nil {
		where += ` AND received_at <= ?`
		args = append(args, formatStoredTime(*opts.To))
	}

	rows, err := s.st.DB().Query(
		`SELECT message_id, conversation, sender, recipient, body, sent_at, received_at
		 FROM history_message`+where+` ORDER BY seq`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("history: query: %w", err)
	}
	defer rows.Close()

	var out []message.Message
	for rows.Next() {
		var (
			m                  message.Message
			conversation       string
			sentAt, receivedAt string
		)
		if err := rows.Scan(&m.ID, &conversation, &m.Sender, &m.Recipient, &m.Body, &sentAt, &receivedAt); err != nil {
			return nil, fmt.Errorf("history: query: scan: %w", err)
		}
		if m.SentAt, err = parseStoredTime(sentAt); err != nil {
			return nil, fmt.Errorf("history: query: %w", err)
		}
		if m.ReceivedAt, err = parseStoredTime(receivedAt); err != nil {
			return nil, fmt.Errorf("history: query: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("history: query: iterate: %w", err)
	}
	return out, nil
}

// Conversation returns one conversation's messages in per-conversation order —
// the reopen path criterion 1 is about. The key comes from
// [message.ConversationKeyFor]; the CLI accepts peer names and derives it.
func (s *Store) Conversation(key string) ([]message.Message, error) {
	return s.Query(QueryOptions{Conversation: key})
}

// enforceRetention applies both bounds: TTL expiry first (age is a property of
// the row alone), then the size cap on whatever survives. Logs carry counts
// and configuration, never content.
func (s *Store) enforceRetention() error {
	cutoff := formatStoredTime(s.clk.Now().Add(-s.ttl))
	res, err := s.st.DB().Exec(`DELETE FROM history_message WHERE stored_at <= ?`, cutoff)
	if err != nil {
		return fmt.Errorf("ttl sweep: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		s.logger.Info("history message expired",
			slog.Int64("count", n),
			slog.Duration("ttl", s.ttl),
		)
	}

	var total int
	if err := s.st.DB().QueryRow(`SELECT COUNT(*) FROM history_message`).Scan(&total); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	if total <= s.maxRows {
		return nil
	}
	evict := total - s.maxRows
	// Oldest first by arrival (seq): newest history is the history still being
	// read; see the type comment for why eviction beats refusal here.
	res, err = s.st.DB().Exec(
		`DELETE FROM history_message WHERE seq IN
			(SELECT seq FROM history_message ORDER BY seq LIMIT ?)`, evict,
	)
	if err != nil {
		return fmt.Errorf("size cap eviction: %w", err)
	}
	if n, _ := res.RowsAffected(); int(n) != evict {
		return fmt.Errorf("size cap eviction: removed %d rows, wanted %d", n, evict)
	}
	s.logger.Warn("history size cap reached, oldest messages evicted",
		slog.Int("evicted", evict),
		slog.Int("cap", s.maxRows),
	)
	return nil
}

// stored-time helpers. UTC with a FIXED 9-digit fraction — the layout where
// lexicographic order IS chronological order, which the SQL comparisons in
// Query and the TTL sweep rely on. Same convention as migration 2/4 and the
// coordinator queue; RFC3339Nano trims trailing zeros and mis-orders strings.
const storedTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

func formatStoredTime(ts time.Time) string {
	return ts.UTC().Format(storedTimeLayout)
}

func parseStoredTime(s string) (time.Time, error) {
	ts, err := time.Parse(storedTimeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored time %q: %w", s, err)
	}
	return ts.UTC(), nil
}
