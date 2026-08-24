package tui

import (
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/messagehub"
	"github.com/maleolabs/walkie/internal/presenceview"
)

// These tests drive the model the way the brief prescribes: synthetic
// messages into Update, assertions on View() strings. No terminal, no sleeps,
// no real clock — the machine transitions below are direct legal edges on a
// fake-clock machine, and every timestamp is carried on the messages
// themselves.

var testEpoch = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// harness bundles the seams a model needs, all real in-memory instances.
type harness struct {
	hub      *messagehub.Hub
	presence *presenceview.View
	mach     *control.Machine
	connCh   chan control.Change
	presCh   chan presenceview.Device
	inCh     chan message.Message
	model    Model

	// identity mirrors what control.Client.ConnectedAs reports: empty until
	// the coordinator's HelloAck echo lands, so tests can pin the exact
	// moment the UI may first name the user.
	identity string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := clock.NewFake(testEpoch)
	logger := newStdLogger()
	mach, err := control.NewMachine(clk, logger)
	if err != nil {
		t.Fatalf("new machine: %v", err)
	}
	hub := messagehub.New("laptop.tail-scale.ts.net.", clk, logger)
	pv := presenceview.New(logger)

	// Subscribe-first (control.Machine.Subscribe's rule), then seed the
	// initial state — exactly the order the assembly must use.
	sub := mach.Subscribe()
	connCh := make(chan control.Change, 16)
	go func() {
		for c := range sub.C() {
			connCh <- c
		}
	}()

	h := &harness{
		hub:      hub,
		presence: pv,
		mach:     mach,
		connCh:   connCh,
		presCh:   make(chan presenceview.Device, 16),
		inCh:     make(chan message.Message, 16),
	}
	h.model = New(Params{
		Hub:             hub,
		Presence:        pv,
		ConnChanges:     connCh,
		PresenceChanges: h.presCh,
		Inbound:         h.inCh,
		ConnectedAs:     func() string { return h.identity },
		Send:            func(string, string) error { return nil },
		AudioAvailable:  false,
		ConnState:       mach.State(),
	})
	return h
}

// update feeds one message through Update and returns the resulting model.
func (h *harness) update(t *testing.T, msg tea.Msg) Model {
	t.Helper()
	next, _ := h.model.Update(msg)
	m, ok := next.(Model)
	if !ok {
		t.Fatalf("Update returned %T, want tui.Model", next)
	}
	h.model = m
	return m
}

func key(s string) tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)} }

// -- presence and custom status (criterion 2) ------------------------------

func TestViewShowsPresenceAndCustomStatusWithoutACommand(t *testing.T) {
	h := newHarness(t)

	h.presence.Apply(presenceOnline("alice", "cleaning the garage"))
	h.presence.Apply(presenceOffline("bob", testEpoch.Add(-time.Hour)))

	view := h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}}).View()
	for _, want := range []string{"alice", "online", "cleaning the garage", "bob", "offline"} {
		if !strings.Contains(view, want) {
			t.Errorf("view missing %q after roster change:\n%s", want, view)
		}
	}
	// Offline carries its last-seen fact as text, not just position.
	if !strings.Contains(view, "last seen") {
		t.Errorf("offline device should show last-seen text:\n%s", view)
	}
}

func TestSelfIsMarkedInTheRoster(t *testing.T) {
	h := newHarness(t)
	h.identity = "laptop" // HelloAck echo landed
	h.presence.Apply(presenceOnline("laptop", ""))
	view := h.update(t, PresenceMsg{Device: presenceview.Device{Device: "laptop"}}).View()
	if !strings.Contains(view, "(you)") {
		t.Errorf("self should be marked (you):\n%s", view)
	}
}

// -- connection state always visible, degraded included (criterion 3) ------

func TestStatusLineAlwaysCarriesConnectionStateIncludingDegraded(t *testing.T) {
	h := newHarness(t)

	// The legal happy path plus its failure waypoints, driven as direct
	// legal edges; each change must land in the rendered status line.
	path := []struct {
		to   control.State
		want string
	}{
		{control.StateConnecting, "connecting"},
		{control.StateHandshaking, "handshaking"},
		{control.StateOnline, "online"},
		{control.StateDegraded, "degraded"},
		{control.StateDisconnected, "disconnected"},
	}
	for _, step := range path {
		if _, err := h.mach.Transition(step.to, "test edge"); err != nil {
			t.Fatalf("transition to %s: %v", step.to, err)
		}
		cm := ConnMsg{Change: <-h.connCh}
		view := h.update(t, cm).View()

		first := strings.SplitN(view, "\n", 2)[0]
		if !strings.Contains(first, "["+step.want+"]") {
			t.Errorf("state %s: status line %q lacks [%s]", step.want, first, step.want)
		}
	}
}

func TestStatusLineVisibleEvenAtTinyTerminal(t *testing.T) {
	h := newHarness(t)
	h.update(t, tea.WindowSizeMsg{Width: 80, Height: 3})
	view := h.model.View()
	first := strings.SplitN(view, "\n", 2)[0]
	if !strings.Contains(first, "[") || !strings.Contains(first, "walkie") {
		t.Errorf("status line must survive a 3-row terminal:\n%s", view)
	}
}

func TestIdentityAppearsOnlyOnceOnline(t *testing.T) {
	h := newHarness(t)
	if _, err := h.mach.Transition(control.StateConnecting, "t"); err != nil {
		t.Fatal(err)
	}
	h.update(t, ConnMsg{Change: <-h.connCh})
	if strings.Contains(h.model.View(), "you: laptop") {
		t.Fatal("identity must not render before the coordinator echoed it")
	}
	if _, err := h.mach.Transition(control.StateHandshaking, "t"); err != nil {
		t.Fatal(err)
	}
	h.update(t, ConnMsg{Change: <-h.connCh})
	h.identity = "laptop" // the supervisor publishes identity before going online
	if _, err := h.mach.Transition(control.StateOnline, "t"); err != nil {
		t.Fatal(err)
	}
	view := h.update(t, ConnMsg{Change: <-h.connCh}).View()
	if !strings.Contains(view, "you: laptop") {
		t.Errorf("online render should name who the tailnet says we are:\n%s", view)
	}
}

// -- message pane updates + both timestamps + unstamped sends ---------------

func TestMessagePaneShowsBothTimestampsAndSkew(t *testing.T) {
	h := newHarness(t)
	h.presence.Apply(presenceOnline("alice", ""))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}})
	h.update(t, tabKey()) // pane follows the conversation with alice

	sent := testEpoch                           // sender clock
	received := testEpoch.Add(90 * time.Second) // coordinator clock: 90s skew stays legible
	env := directEnvelope("alice", "laptop", "skew check", sent, received)
	msg, shown := h.hub.Apply(env)
	if !shown {
		t.Fatal("first delivery must be new")
	}

	view := h.update(t, InboundMsg{Msg: msg}).View()
	if !strings.Contains(view, "sent 12:00:00") || !strings.Contains(view, "recv 12:01:30") {
		t.Errorf("both timestamps must render unmerged:\n%s", view)
	}
	if !strings.Contains(view, "skew check") {
		t.Errorf("body missing:\n%s", view)
	}
}

func TestLocallyFiledSendRendersUnstampedDistinctly(t *testing.T) {
	h := newHarness(t)
	// A broadcast files into the default conversation, so no navigation is
	// needed; the property under test is the missing coordinator stamp.
	if _, err := h.hub.SendBroadcast("from me"); err != nil {
		t.Fatalf("send: %v", err)
	}

	view := h.model.View()
	if !strings.Contains(view, "recv -") {
		t.Errorf("unstamped local send must render '-' not a fake time:\n%s", view)
	}
	if !strings.Contains(view, "sent 12:00:00") {
		t.Errorf("sent stamp missing:\n%s", view)
	}
}

func TestDuplicateDeliveryNeverRepaintsThePaneTwice(t *testing.T) {
	h := newHarness(t)
	h.presence.Apply(presenceOnline("alice", ""))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}})
	h.update(t, tabKey()) // watch the conversation with alice

	env := directEnvelope("alice", "laptop", "once only", testEpoch, testEpoch)
	msg1, shown1 := h.hub.Apply(env)
	if !shown1 {
		t.Fatal("first delivery must be new")
	}
	h.update(t, InboundMsg{Msg: msg1})

	_, shown2 := h.hub.Apply(env) // same ULID: at-least-once wire replay
	if shown2 {
		t.Fatal("duplicate must not be reported new")
	}
	before := h.model.View()
	// Even if an assembly bug forwarded the duplicate event, the pane content
	// for this conversation cannot gain a second line from the hub snapshot.
	after := h.update(t, InboundMsg{Msg: msg1}).View()
	if got := strings.Count(after, "once only"); got != 1 {
		t.Errorf("body displayed %d times, want 1", got)
	}
	if before != after {
		t.Error("duplicate event must leave the pane unchanged")
	}
}

// -- composing and sending ---------------------------------------------------

func sendHarness(h *harness, sendErr error) {
	h.model = New(Params{
		Hub:             h.hub,
		Presence:        h.presence,
		ConnChanges:     h.connCh,
		PresenceChanges: h.presCh,
		Inbound:         h.inCh,
		ConnectedAs:     func() string { return "laptop" },
		Send: func(string, string) error {
			return sendErr
		},
		AudioAvailable: false,
		ConnState:      control.StateOnline,
	})
}

func TestEnterSendsComposedBodyToCurrentConversation(t *testing.T) {
	h := newHarness(t)
	var gotConv, gotBody string
	h.model = New(Params{
		Hub:             h.hub,
		Presence:        h.presence,
		ConnChanges:     h.connCh,
		PresenceChanges: h.presCh,
		Inbound:         h.inCh,
		ConnectedAs:     func() string { return "laptop" },
		Send: func(conv, body string) error {
			gotConv, gotBody = conv, body
			return nil
		},
		AudioAvailable: false,
		ConnState:      control.StateOnline,
	})

	h.model = typeText(t, h.model, "hello fleet")
	h.model = h.update(t, enterKey())

	if gotConv != message.BroadcastConversation || gotBody != "hello fleet" {
		t.Errorf("send got (%q,%q), want (broadcast, hello fleet)", gotConv, gotBody)
	}
	if h.model.input.Value() != "" {
		t.Error("input should clear after send")
	}
}

func TestSendErrorSurfacesInlineAndClearsOnNextKeypress(t *testing.T) {
	h := newHarness(t)
	sendHarness(h, errors.New("offline: held in outbox"))

	h.model = typeText(t, h.model, "hi")
	h.model = h.update(t, enterKey())
	if !strings.Contains(h.model.View(), "! offline: held in outbox") {
		t.Errorf("send failure must surface inline:\n%s", h.model.View())
	}

	// Next keypress clears it: the error describes one attempt, not a state.
	h.model = typeText(t, h.model, "x")
	if strings.Contains(h.model.View(), "held in outbox") {
		t.Errorf("stale send error must clear on the next keypress:\n%s", h.model.View())
	}
}

func TestEmptyEnterSendsNothing(t *testing.T) {
	h := newHarness(t)
	sent := false
	h.model = New(Params{
		Hub:             h.hub,
		Presence:        h.presence,
		ConnChanges:     h.connCh,
		PresenceChanges: h.presCh,
		Inbound:         h.inCh,
		ConnectedAs:     func() string { return "laptop" },
		Send:            func(string, string) error { sent = true; return nil },
		AudioAvailable:  false,
		ConnState:       control.StateOnline,
	})
	h.update(t, enterKey())
	if sent {
		t.Error("empty Enter must not transmit")
	}
}

// -- conversation navigation -------------------------------------------------

func TestTabCyclesConversationsAndDigitsJump(t *testing.T) {
	h := newHarness(t)
	h.presence.Apply(presenceOnline("alice", ""))
	h.presence.Apply(presenceOnline("bob", ""))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}})

	// broadcast pinned first, then peers alphabetically.
	wantOrder := []string{
		message.BroadcastConversation,
		message.ConversationKey("alice"),
		message.ConversationKey("bob"),
	}
	got := h.model.conversations
	if len(got) != len(wantOrder) {
		t.Fatalf("conversations %v, want %v", got, wantOrder)
	}
	for i := range wantOrder {
		if got[i] != wantOrder[i] {
			t.Fatalf("conversation order %v, want %v", got, wantOrder)
		}
	}

	h.model = h.update(t, tabKey()) // -> alice
	if h.model.conversationKey() != wantOrder[1] {
		t.Errorf("after tab: %q, want %q", h.model.conversationKey(), wantOrder[1])
	}
	h.model = h.update(t, key("3")) // jump -> bob
	if h.model.conversationKey() != wantOrder[2] {
		t.Errorf("after '3': %q, want %q", h.model.conversationKey(), wantOrder[2])
	}
	// Guarded keys with text composed type literally instead of jumping.
	h.model = typeText(t, h.model, "see you at ")
	h.model = h.update(t, key("3"))
	if h.model.conversationKey() != wantOrder[2] {
		t.Error("digit with text composed must type, not jump")
	}
	if !strings.HasSuffix(h.model.input.Value(), "3") {
		t.Errorf("digit should land in the composer, got %q", h.model.input.Value())
	}
}

// -- help: single source, complete (criterion 6) -----------------------------

func TestHelpOverlayListsEveryBindingFromTheSingleSource(t *testing.T) {
	h := newHarness(t)
	h.update(t, key("?"))
	view := h.model.View()

	for _, b := range Keymap() {
		if !strings.Contains(view, b[0]) || !strings.Contains(view, b[1]) {
			t.Errorf("help overlay must list binding %q (%q); help derives from the same table as dispatch:\n%s", b[0], b[1], view)
		}
	}
	if !strings.Contains(view, freeInputRule) {
		t.Errorf("help must state the guarded-key rule:\n%s", view)
	}
	// Status line survives the overlay: criterion 3 has no help exception.
	if first := strings.SplitN(view, "\n", 2)[0]; !strings.Contains(first, "[") {
		t.Errorf("status line must stay above the help overlay:\n%s", view)
	}

	h.update(t, escKey())
	if h.model.helpOpen {
		t.Error("esc must close help")
	}
}

func TestVoiceCapabilityStatedAsPresentOrAbsentNeverError(t *testing.T) {
	h := newHarness(t)
	h.update(t, key("?"))
	if !strings.Contains(h.model.View(), "voice notes: not in this build") {
		t.Errorf("no-audio build must state absence plainly:\n%s", h.model.View())
	}

	h2 := newHarness(t)
	h2.model.p.AudioAvailable = true
	h2.update(t, key("?"))
	if !strings.Contains(h2.model.View(), "voice notes: available") {
		t.Errorf("audio build must state presence plainly:\n%s", h2.model.View())
	}
}

// ctrl+c quits from anywhere, including with text composed.
func TestCtrlCQuits(t *testing.T) {
	h := newHarness(t)
	h.model = typeText(t, h.model, "draft")
	_, cmd := h.model.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Fatal("ctrl+c must produce the quit command")
	}
}

// -- helpers -----------------------------------------------------------------

func typeText(t *testing.T, m Model, s string) Model {
	t.Helper()
	for _, r := range s {
		next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = next.(Model)
	}
	return m
}

func enterKey() tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyEnter} }
func tabKey() tea.KeyMsg   { return tea.KeyMsg{Type: tea.KeyTab} }
func escKey() tea.KeyMsg   { return tea.KeyMsg{Type: tea.KeyEsc} }
