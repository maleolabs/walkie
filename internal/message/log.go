package message

import "sync"

// DefaultScrollback is the per-conversation bound a [Log] uses when none is
// given. This is DISPLAY scrollback for a terminal session, not retention:
// sto:message-history owns durable storage with its own TTL and size cap and
// its own explicit-refusal semantics. A terminal UI keeps a bounded window of
// recent lines by nature; the default simply makes that nature explicit so
// the log cannot grow without bound in a long-lived client.
const DefaultScrollback = 500

// Log is the receiver-side display model: messages filed per conversation, in
// arrival order, deduplicated by ULID at the point where DISPLAY is decided.
//
// Owning work item:
//
//	eka get walkie/sto:text-messaging
//
// # Why dedup sits HERE, at this type
//
// Criterion 3 is about the displayed result: delivering the same ULID twice
// must produce ONE displayed message. A filter at socket-read alone does not
// satisfy it — between the socket and the screen sit reordering, rendering
// queues and (later) history reloads, and a duplicate that survives any of
// them becomes two lines. So [Log.Append] — the single gate every displayed
// message passes — consults the Dedup itself and reports whether the message
// is new. Callers treat false as "already shown; drop it", wherever they sit.
//
// # The seam for later items, stated plainly
//
//   - sto:message-history persists messages through exactly the [Message]
//     shape this package defines; when it lands it wraps or observes Append,
//     it does not fork a second ingest path. Nothing here opens a database —
//     no half-built store on purpose.
//   - sto:terminal-ui renders from Conversation snapshots plus Append's
//     return value; a headless client (and ts:control-socket behind it) can
//     consume the same two primitives, which is why they carry no UI types.
//
// # Ordering contract, restated where it is enforced
//
// Messages within one conversation keep arrival order under any interleaving
// of other conversations' traffic — each conversation is an independent slice
// appended under one mutex, so cross-conversation scheduling cannot reorder
// inside one. No order ACROSS conversations exists, none is exposed, and
// nothing may depend on one (see the package comment).
type Log struct {
	mu         sync.Mutex
	local      string // this device's tailnet name; decides DM orientation
	scrollback int    // per-conversation retained-message cap
	convs      map[string][]Message
	dedup      *Dedup
}

// NewLog returns a Log attributing messages to the device named local (the
// client's HelloAck device), keeping at most scrollback messages per
// conversation and dedupWindow distinct ULIDs. Non-positive values fall back
// to [DefaultScrollback] / [DefaultDedupWindow].
func NewLog(local string, scrollback, dedupWindow int) *Log {
	if scrollback <= 0 {
		scrollback = DefaultScrollback
	}
	if dedupWindow <= 0 {
		dedupWindow = DefaultDedupWindow
	}
	return &Log{
		local:      local,
		scrollback: scrollback,
		convs:      make(map[string][]Message),
		dedup:      NewDedup(dedupWindow),
	}
}

// Append files msg into its conversation and reports whether it should be
// DISPLAYED: true for a first-seen ULID, false for a duplicate of something
// already shown (criterion 3's enforcement point — see the type comment).
//
// The conversation is derived from msg itself: broadcasts all land in
// [BroadcastConversation]; a direct message joins the conversation with its
// peer, whichever direction it travelled, using local to tell which side of
// Sender/Recipient is us. A message naming neither local as sender nor local
// as recipient is still filed — under its sender, the only honest reading of
// traffic this device received — because guessing harder than that is how
// display models end up confidently wrong.
func (l *Log) Append(msg Message) bool {
	if !l.dedup.First(msg.ID) {
		return false // duplicate: already displayed once, never twice
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	key := l.keyLocked(msg)
	l.convs[key] = append(l.convs[key], msg)
	if over := len(l.convs[key]) - l.scrollback; over > 0 {
		// Drop the OLDEST lines beyond the window. Order among survivors is
		// untouched, which is what the per-conversation guarantee needs;
		// what scrolled off lives in history's hands, not here.
		l.convs[key] = append([]Message(nil), l.convs[key][over:]...)
	}
	return true
}

// Conversation returns a copy of one conversation's messages in arrival
// order. The key comes from [ConversationKey] or [BroadcastConversation].
// The copy is deliberate: callers render at their own pace while Append keeps
// filing, and sharing the backing slice would make that race.
func (l *Log) Conversation(key string) []Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Message, len(l.convs[key]))
	copy(out, l.convs[key])
	return out
}

// Conversations lists the known conversation keys sorted, so a UI can list
// conversations without poking at internals. Sorted output is presentation
// hygiene, not ordering semantics — see the package comment.
func (l *Log) Conversations() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]string, 0, len(l.convs))
	for k := range l.convs {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ { // insertion sort: keys are few
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// keyLocked derives msg's conversation key. Callers hold l.mu only for the
// map write; derivation itself reads only msg and the immutable local name,
// hence the name being about call-site discipline rather than data races.
func (l *Log) keyLocked(msg Message) string {
	return ConversationKeyFor(l.local, msg)
}
