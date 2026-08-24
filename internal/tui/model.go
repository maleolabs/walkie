package tui

import (
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/presenceview"
)

// ConversationSource is the read surface of the messaging seam the pane
// needs. *messagehub.Hub satisfies it; the indirection exists because the hub
// is constructed by the assembly only AFTER the first handshake (its local
// name is the HelloAck echo), so during startup there may be nothing to read
// yet — the model renders an empty pane rather than pretending.
type ConversationSource interface {
	Conversation(key string) []message.Message
	Conversations() []string
}

// Params wires the model to the seams earlier items exposed. Everything here
// is a consumer interface the assembly in cmd/walkie owns the wiring for; the
// model never dials, never touches protobuf types, and never reaches past
// these into coordinator internals (sto:terminal-ui brief).
//
// The three channels are push feeds the assembly pumps: connection changes
// from control.Machine.Subscribe (no-drop by that package's contract),
// roster changes from presenceview.View.Subscribe, and newly filed inbound
// messages from the assembly's Apply path. All three are notifications, not
// state: on any of them the model re-reads Hub/Presence snapshots, so a
// dropped notification costs one late repaint, never stale-forever data.
type Params struct {
	// HubFunc returns the messaging seam, or nil before the first handshake
	// completes (see ConversationSource).
	HubFunc func() ConversationSource

	// Presence is the client-side roster seam.
	Presence *presenceview.View

	// ConnChanges delivers control.Change events until closed.
	ConnChanges <-chan control.Change

	// PresenceChanges delivers roster-change events until closed.
	PresenceChanges <-chan presenceview.Device

	// Inbound delivers each message the assembly newly filed (Apply returned
	// true). Duplicates never appear here — dedup happened in the hub.
	Inbound <-chan message.Message

	// ConnectedAs reports who the tailnet says this client is (empty while
	// unknown). Polled when a change to online arrives, because the
	// coordinator's HelloAck echo lands between the handshake and the online
	// transition — exactly once per connection, not a hot path.
	ConnectedAs func() string

	// Send transmits one composed message into the conversation named key
	// ("broadcast", or "dm:<peer>"). The assembly owns transmit, the offline
	// outbox and history persistence; the model only reports errors.
	Send func(conversation string, body string) error

	// AudioAvailable mirrors audio.Available() at assembly time. It decides
	// whether voice affordances are PRESENTED or ABSENT — never rendered as
	// an error state (req:voice-communication degrade legibly; sto:voice-note
	// builds the real affordances on this flag later).
	AudioAvailable bool

	// ConnState seeds the status line before the first change arrives. The
	// assembly reads it AFTER taking its Machine subscription (the
	// subscribe-first rule in control.Machine.Subscribe), so no transition
	// can fall between the seed and the stream.
	ConnState control.State
}

// Model is the Bubbletea model of walkie's terminal interface: message pane,
// device list, input line, and a status line that is always on screen
// carrying the connection state (criterion 3).
//
// It is deliberately testable without a terminal: drive Update with synthetic
// messages and assert on View() — see model_test.go.
type Model struct {
	p Params

	width, height int

	input textinput.Model

	conversations []string // merged view: hub conversations + roster peers
	current       int      // index into conversations

	devices []presenceview.Device // last roster snapshot

	connState   control.State
	connectedAs string

	// unread counts arrivals per conversation key while that conversation is
	// not selected. Cleared on selection; rendered on the device list.
	unread map[string]int

	helpOpen bool
	sendErr  string // last send failure, cleared on the next keypress/send
}

// New returns the initial model and the command that starts consuming the
// assembly's event channels.
func New(p Params) Model {
	ti := textinput.New()
	ti.Placeholder = "type a message"
	ti.Focus()
	ti.CharLimit = message.MaxBodyBytes
	// bubbles' default cursor glyph is U+2588 FULL BLOCK. Criterion 5 forbids
	// assuming a font with wide/non-ASCII glyph coverage, so the composer's
	// cursor is pinned to ASCII: a block that fails to render would leave the
	// user typing blind in the one line they must type in. A vertical bar is
	// the classic ASCII terminal cursor and renders everywhere.
	ti.Cursor.SetChar("|")

	m := Model{
		p:         p,
		input:     ti,
		width:     80,
		height:    24,
		connState: p.ConnState,
		unread:    map[string]int{},
	}
	if m.p.ConnectedAs != nil {
		m.connectedAs = m.p.ConnectedAs()
	}
	m.refreshConversations()
	if ds := p.Presence.Snapshot(); len(ds) > 0 {
		m.devices = ds
	}
	return m
}

// Init returns the commands that pump the three event streams into Update.
// Each wait* command reads ONE event; Update re-arms it after handling, the
// standard Bubbletea subscription shape for channels this package does not
// own.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		waitConn(m.p.ConnChanges),
		waitPresence(m.p.PresenceChanges),
		waitInbound(m.p.Inbound),
	)
}

// -- event types ---------------------------------------------------------

// ConnMsg carries one control-plane connection change into the model.
type ConnMsg struct{ Change control.Change }

// PresenceMsg carries one roster change into the model.
type PresenceMsg struct{ Device presenceview.Device }

// InboundMsg announces one newly filed message; the pane re-reads the hub
// snapshot rather than trusting the carried copy to be the whole story.
type InboundMsg struct{ Msg message.Message }

func waitConn(ch <-chan control.Change) tea.Cmd {
	return func() tea.Msg {
		c, ok := <-ch
		if !ok {
			return nil // stream over (shutdown); nothing left to report
		}
		return ConnMsg{Change: c}
	}
}

func waitPresence(ch <-chan presenceview.Device) tea.Cmd {
	return func() tea.Msg {
		d, ok := <-ch
		if !ok {
			return nil
		}
		return PresenceMsg{Device: d}
	}
}

func waitInbound(ch <-chan message.Message) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return InboundMsg{Msg: msg}
	}
}

// -- update --------------------------------------------------------------

// Update advances the model. Key handling dispatches through the single-
// source keymap (keymap.go); everything else is one of the three event feeds
// or a resize.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case ConnMsg:
		m.connState = msg.Change.To
		if msg.Change.To == control.StateOnline && m.p.ConnectedAs != nil {
			// Identity is published by the supervisor before the online
			// transition lands, so polling here cannot race it.
			m.connectedAs = m.p.ConnectedAs()
		}
		return m, waitConn(m.p.ConnChanges)

	case PresenceMsg:
		// Identity can land between events on different channels (the
		// machine's pump and the roster's pump are independent), so every
		// event refreshes it — a cheap func call, not a hot path.
		if m.p.ConnectedAs != nil {
			m.connectedAs = m.p.ConnectedAs()
		}
		m.refreshConversations()
		return m, waitPresence(m.p.PresenceChanges)

	case InboundMsg:
		m.refreshConversations()
		// A message for a conversation the user is not looking at becomes an
		// unread marker on that conversation, never a forced view switch:
		// yanking the pane under a typing user is a worse sin than a missed
		// line, and the roster marker keeps the arrival visible (criterion
		// 2's spirit: what arrived is on screen without a command).
		if conv := message.ConversationKeyFor(m.connectedAs, msg.Msg); conv != m.conversationKey() {
			m.unread[conv]++
		}
		return m, waitInbound(m.p.Inbound)

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// handleKey resolves the press against the single-source keymap. Printable
// characters that match no binding fall through to the input line.
func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	b, ok := lookup(key)
	if ok && b.needsFree && m.input.Value() != "" {
		ok = false // guarded key with text composed: it types instead (see keymap.go)
	}
	if !ok {
		// Unmatched printable keys go into the composer; unmatched control
		// keys (and all keys while help is open) are ignored by it.
		if !m.helpOpen {
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			m.sendErr = "" // composing again: the old failure stops shouting
			return m, cmd
		}
		return m, nil
	}

	switch b.action {
	case actSend:
		body := strings.TrimSpace(m.input.Value())
		if body == "" {
			return m, nil // an empty Enter must not file an empty message
		}
		m.input.SetValue("")
		if err := m.send(body); err != nil {
			// Loud, inline, temporary: the user sees why nothing was sent,
			// in their own language of "the line I just typed".
			m.sendErr = err.Error()
		}
		return m, nil

	case actNextConv:
		m.selectConversation((m.current + 1) % len(m.conversations))
		return m, nil

	case actPrevConv:
		m.selectConversation((m.current - 1 + len(m.conversations)) % len(m.conversations))
		return m, nil

	case actJumpConv:
		n := int(key[0] - '1')
		if n < len(m.conversations) {
			m.selectConversation(n)
		}
		return m, nil

	case actHelp:
		m.helpOpen = !m.helpOpen
		return m, nil

	case actCloseHelp:
		if m.helpOpen {
			m.helpOpen = false
		}
		return m, nil

	case actQuit:
		return m, tea.Quit

	case actNone:
		return m, nil
	}
	return m, nil
}

// send hands the composed body to the assembly's transmit closure. The
// conversation key is the model's own currency; the assembly translates it
// (broadcast vs dm:<peer>) at the seam.
func (m *Model) send(body string) error {
	if m.p.Send == nil {
		return nil // degenerate wiring (tests); nothing to transmit
	}
	return m.p.Send(m.conversations[m.current], body)
}

// refreshConversations rebuilds the conversation list from the two live
// sources — the hub's actual conversations and one entry per rostered peer —
// keeping the current selection pointed at the same conversation across
// rebuilds. Peers appear BEFORE any traffic exists, so starting a DM is a
// jump, not something requiring a message first (criterion 2's zero-command
// visibility extends to "who can I talk to").
func (m *Model) refreshConversations() {
	keys := map[string]bool{message.BroadcastConversation: true}
	if src := m.p.HubFunc; src != nil {
		if hub := src(); hub != nil {
			for _, k := range hub.Conversations() {
				keys[k] = true
			}
		}
	}
	for _, d := range m.p.Presence.Snapshot() {
		if d.Device != m.connectedAs { // skip self once identity is known
			keys[message.ConversationKey(d.Device)] = true
		}
	}
	m.devices = m.p.Presence.Snapshot()

	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		// Broadcast pinned first: it is the room everyone shares, the
		// sensible landing conversation for a brand-new session.
		if (out[i] == message.BroadcastConversation) != (out[j] == message.BroadcastConversation) {
			return out[i] == message.BroadcastConversation
		}
		return out[i] < out[j]
	})
	if len(out) == 0 {
		out = []string{message.BroadcastConversation}
	}

	cur := m.conversationKey()
	m.conversations = out
	m.current = 0
	for i, k := range out {
		if k == cur {
			m.current = i
			break
		}
	}
}

func (m *Model) conversationKey() string {
	if m.current < len(m.conversations) {
		return m.conversations[m.current]
	}
	return message.BroadcastConversation
}

// selectConversation moves the pane to conversations[i] and clears that
// conversation's unread marker: having opened it, the user has seen it.
func (m *Model) selectConversation(i int) {
	m.current = i
	delete(m.unread, m.conversationKey())
}

// conversationTitle renders a conversation key for humans: "broadcast" stays,
// "dm:alice.tail-scale.ts.net." becomes "alice".
func conversationTitle(key string) string {
	return strings.TrimPrefix(key, "dm:")
}
