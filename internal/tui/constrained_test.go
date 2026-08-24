package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/presenceview"
)

// Slice 2's gates: the constrained-terminal rules from sto:terminal-ui
// criteria 4 and 5, plus the completion of criterion 6 (help overlay equals
// the keymap enumeration exactly). Everything here drives Update with
// synthetic messages and asserts on View() strings — no terminal, no sleeps,
// no real clock, per the brief's honest-TUI-testing rule.
//
// What these tests deliberately CANNOT cover is recorded in doc.go and the
// implementation note: bubbletea's Program (input decoding, alt-screen
// repaint) never runs here because there is no TTY.

// populatedHarness returns a harness whose model has seen a roster, a
// message, an identity and a full connection arc — the busiest realistic
// frame, so layout assertions run against content, not against an empty pane.
func populatedHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.identity = "laptop.tail-scale.ts.net."

	h.presence.Apply(presenceOnline("alice.tail-scale.ts.net.", "cleaning the garage"))
	h.presence.Apply(presenceOffline("bob.tail-scale.ts.net.", testEpoch.Add(-30*time.Minute)))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}})

	if _, err := h.hub.SendBroadcast("status meeting moved to 14:00"); err != nil {
		t.Fatalf("seed broadcast: %v", err)
	}
	env := directEnvelope("alice.tail-scale.ts.net.", "laptop.tail-scale.ts.net.", "ack, see you there", testEpoch, testEpoch)
	msg, shown := h.hub.Apply(env)
	if !shown {
		t.Fatal("seed dm must be new")
	}
	h.update(t, InboundMsg{Msg: msg})

	for _, st := range []control.State{control.StateConnecting, control.StateHandshaking, control.StateOnline} {
		if _, err := h.mach.Transition(st, "test arc"); err != nil {
			t.Fatalf("transition %s: %v", st, err)
		}
		h.update(t, ConnMsg{Change: <-h.connCh})
	}

	h.update(t, tea.WindowSizeMsg{Width: 80, Height: 24})
	return h
}

// -- criterion 4a: usable at EXACTLY 80 columns ------------------------------

func TestViewFitsExactlyEightyColumns(t *testing.T) {
	h := populatedHarness(t)

	// Stress the clippers: names, statuses, bodies and errors long enough to
	// overflow 80 cells if any pane forgot its budget.
	h.presence.Apply(presenceOnline(
		"verylongdevicename.tail-scale.ts.net.",
		"a custom status that goes on well past the eightieth column of the roster line",
	))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "verylongdevicename"}})

	sendHarness(h, fmt.Errorf("offline - held; %s", strings.Repeat("x", 120)))
	h.model = typeText(t, h.model, strings.Repeat("y", 120))
	h.model = h.update(t, enterKey())

	view := h.model.View()
	if view == "" {
		t.Fatal("view must not be empty at 80 columns")
	}
	for i, line := range strings.Split(view, "\n") {
		if n := utf8.RuneCountInString(line); n > 80 {
			t.Errorf("line %d is %d cells (> 80): %q", i, n, line)
		}
	}

	// "Fits" also means nothing load-bearing was clipped away at this width:
	// the state word, both presence verdicts and the hint line survive.
	for _, want := range []string{"[online]", "online", "offline", "keys:"} {
		if !strings.Contains(view, want) {
			t.Errorf("80-column view lost %q:\n%s", want, view)
		}
	}
}

// -- criterion 4a: below 80 fails LEGIBLY, not corruptly ----------------------

func TestBelowEightyColumnsFailsLegiblyNotCorruptly(t *testing.T) {
	h := populatedHarness(t)

	for _, w := range []int{79, 60, 40} {
		h.update(t, tea.WindowSizeMsg{Width: w, Height: 24})
		view := h.model.View()

		if !strings.Contains(view, "walkie needs 80 columns") {
			t.Errorf("width %d: refusal must name the floor:\n%s", w, view)
		}
		if !strings.Contains(view, fmt.Sprintf("This terminal has %d.", w)) {
			t.Errorf("width %d: refusal must report the actual width:\n%s", w, view)
		}
		if !strings.Contains(view, "Widen the window.") {
			t.Errorf("width %d: refusal must say what to do:\n%s", w, view)
		}
		// Legible means structured: every line whole (never mid-word
		// truncation), which holds only if each message line fits the width.
		for i, line := range strings.Split(view, "\n") {
			if n := utf8.RuneCountInString(line); n > w {
				t.Errorf("width %d: refusal line %d is %d cells — corrupted:\n%s", w, i, n, view)
			}
		}
		// Criterion 3 has no size exception: the state stays on screen even
		// in the refusal.
		if first := strings.SplitN(view, "\n", 2)[0]; !strings.Contains(first, "[online]") {
			t.Errorf("width %d: status line must survive the refusal:\n%s", w, view)
		}
		// And it must NOT look like the normal layout half-rendered: no
		// device rows, no pane header, no composer prompt below the floor.
		for _, gone := range []string{"Devices:", "in: ", "> "} {
			if strings.Contains(view, gone) {
				t.Errorf("width %d: refusal leaked normal-layout element %q:\n%s", w, gone, view)
			}
		}
	}

	// The floor itself is inclusive: 80 renders the real interface.
	h.update(t, tea.WindowSizeMsg{Width: 80, Height: 24})
	if !strings.Contains(h.model.View(), "Devices:") {
		t.Error("exactly 80 columns must render the real interface, not the refusal")
	}
}

// -- criterion 4b: colourless degradation carries everything as text ----------

// assertNoANSI fails if the view carries CSI escape sequences (colours,
// styles, cursor moves). The interface must carry NOTHING by colour or style,
// so under no-colour conditions there must be none at all. This holds
// deterministically in a test process (no TTY -> lipgloss/termenv select the
// Ascii profile) and is forced regardless of environment by NO_COLOR /
// TERM=dumb in the env-matrix test below.
func assertNoANSI(t *testing.T, view string) {
	t.Helper()
	if strings.Contains(view, "\x1b[") {
		t.Errorf("view contains ANSI escape sequences; nothing may depend on styling:\n%q", view)
	}
}

func TestPresenceAndStateCarriedByWordsNotColour(t *testing.T) {
	h := populatedHarness(t)

	// Before any transition: alice's custom status is plain text on the
	// roster (criterion 2), alongside her online verdict — and the seeded DM
	// she sent shows as an unread marker, also pure text.
	before := h.model.View()
	assertNoANSI(t, before)
	if !strings.Contains(before, "alice [1 new]  online - cleaning the garage") {
		t.Fatalf("online status must render as words:\n%s", before)
	}

	// Flip one peer offline and bring another online: every fact the roster
	// shows must exist as a WORD or SYMBOL findable in the text, independent
	// of any colour a terminal might have offered. (The offline verdict
	// carries no status by seam semantics, so the status legitimately
	// disappears with it; last-seen takes over as the text-carried detail.)
	h.presence.Apply(presenceOffline("alice.tail-scale.ts.net.", testEpoch.Add(-time.Minute)))
	h.presence.Apply(presenceOnline("bob.tail-scale.ts.net.", ""))
	view := h.update(t, PresenceMsg{Device: presenceview.Device{Device: "bob"}}).View()
	assertNoANSI(t, view)

	for _, want := range []string{
		"[online]",               // connection state: word in brackets, first line
		"alice [1 new]  offline", // presence verdict: word, not just position/symbol
		"last seen",              // offline detail carried as text
		"bob  online",            // the other verdict
		"* bob  online",          // online ALSO has a symbol — redundancy by design
	} {
		if !strings.Contains(view, want) {
			t.Errorf("colourless view missing %q:\n%s", want, view)
		}
	}
}

func TestNoColourEnvironmentsRenderIdenticalInformation(t *testing.T) {
	// Each subtest pins one documented no-colour condition. t.Setenv (not
	// os.Setenv) scopes the change and forbids parallelism while active.
	// lipgloss caches its profile per process, but every path this package
	// renders is plain text before styling ever applies, so the assertions
	// are order-independent across the suite.
	t.Run("NO_COLOR=1", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		checkColourless(t)
	})
	t.Run("TERM=dumb", func(t *testing.T) {
		t.Setenv("TERM", "dumb")
		checkColourless(t)
	})
	t.Run("NO_COLOR=1 and TERM=dumb", func(t *testing.T) {
		t.Setenv("NO_COLOR", "1")
		t.Setenv("TERM", "dumb")
		checkColourless(t)
	})
}

// checkColourless builds a fresh busy frame and asserts the information set
// survives with zero escape sequences.
func checkColourless(t *testing.T) {
	t.Helper()
	h := populatedHarness(t)
	h.presence.Apply(presenceOnline("carol.tail-scale.ts.net.", "on call"))
	view := h.update(t, PresenceMsg{Device: presenceview.Device{Device: "carol"}}).View()

	assertNoANSI(t, view)
	for _, want := range []string{"[online]", "carol  online", "on call", "keys:", "you:"} {
		if !strings.Contains(view, want) {
			t.Errorf("no-colour view missing %q:\n%s", want, view)
		}
	}

	// Help too: discoverability cannot depend on colour either.
	h.update(t, key("?"))
	help := h.model.View()
	assertNoANSI(t, help)
	if !strings.Contains(help, freeInputRule) {
		t.Errorf("no-colour help lost the guarded-key rule:\n%s", help)
	}
}

// -- criterion 5: headless / ASCII -------------------------------------------

func TestRenderedChromeIsPureASCII(t *testing.T) {
	h := populatedHarness(t)

	// ASCII-only synthetic inputs, so ANY non-ASCII byte in the output comes
	// from walkie's own chrome — box drawing, emoji, block glyphs — each of
	// which criterion 5 forbids assuming glyph coverage for. User-authored
	// content (a CJK message body, an emoji status) is data, not chrome;
	// rendering it is the terminal's job, not an assumption we make.
	view := h.model.View()
	for i, r := range view {
		if r > 127 {
			t.Errorf("non-ASCII rune %q (U+%04X) at byte offset %d — chrome must be ASCII:\n%s",
				r, r, i, view)
		}
	}

	h.update(t, key("?"))
	for i, r := range h.model.View() {
		if r > 127 {
			t.Errorf("help overlay: non-ASCII rune %q (U+%04X) at byte offset %d", r, r, i)
		}
	}
}

// -- criterion 6 completed: overlay EQUALS the keymap enumeration -------------

// parseHelpRows extracts the command rows from helpText(): the lines between
// the header and the blank footer, each formatted "  label  desc". If a
// future edit changes that shape, this test fails loudly rather than letting
// help drift from dispatch silently.
func parseHelpRows(t *testing.T) [][2]string {
	t.Helper()
	lines := strings.Split(helpText(), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "Commands") {
		t.Fatalf("helpText must open with the Commands header, got %q", lines[0])
	}
	var rows [][2]string
	for _, l := range lines[1:] {
		if l == "" {
			break
		}
		if !strings.HasPrefix(l, "  ") {
			t.Fatalf("help row lacks the two-space indent: %q", l)
		}
		body := strings.TrimPrefix(l, "  ")
		label, desc, ok := strings.Cut(body, "  ")
		if !ok || label == "" || desc == "" {
			t.Fatalf("help row %q is not 'label<2 spaces>desc'", l)
		}
		rows = append(rows, [2]string{label, desc})
	}
	return rows
}

func TestHelpOverlayContentEqualsKeymapEnumerationExactly(t *testing.T) {
	// Equality, not containment: same rows, same order, nothing extra. A
	// binding added to keymap.go without appearing in help (or vice versa) is
	// drift, and drift here is criterion 6's failure mode.
	got := parseHelpRows(t)
	want := Keymap()
	if len(got) != len(want) {
		t.Fatalf("help lists %d commands, keymap enumerates %d:\n got %v\nwant %v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("help row %d = %v, keymap says %v", i, got[i], want[i])
		}
	}

	// The rendered overlay must agree with the parsed table too — this is
	// what the user actually sees.
	h := newHarness(t)
	h.update(t, key("?"))
	view := h.model.View()
	for _, b := range want {
		if !strings.Contains(view, b[0]) || !strings.Contains(view, b[1]) {
			t.Errorf("rendered overlay lost %q (%q):\n%s", b[0], b[1], view)
		}
	}
}

func TestEveryKeymapBindingActuallyDispatches(t *testing.T) {
	// The reverse drift direction: a row in the table whose keys do not fire
	// would be help advertising a lie. Drive each row's representative key
	// through Update and require the documented effect (or, for guarded
	// keys, the documented guard).
	h := newHarness(t)
	h.identity = "laptop"
	h.presence.Apply(presenceOnline("alice", ""))
	h.update(t, PresenceMsg{Device: presenceview.Device{Device: "alice"}})

	sent := false
	h.model.p.Send = func(string, string) error { sent = true; return nil }

	for _, b := range keymap {
		switch b.action {
		case actSend:
			h.model = h.update(t, enterKey())
			if sent {
				t.Errorf("enter with empty input sent (empty-Enter rule broken)")
			}
			h.model = typeText(t, h.model, "ping")
			h.model = h.update(t, enterKey())
			if !sent {
				t.Errorf("binding %q did not send", b.label)
			}
		case actNextConv, actPrevConv, actJumpConv:
			// Two conversations exist (broadcast + dm:alice), so any move
			// changes the selection. Digits share ONE row: exercising "2"
			// proves the row's dispatch; every digit resolves to the same
			// action by the same lookup.
			k := b.keys[0]
			if b.action == actJumpConv {
				k = "2"
			}
			var km tea.KeyMsg
			switch k {
			case "tab":
				km = tea.KeyMsg{Type: tea.KeyTab}
			case "shift+tab":
				km = tea.KeyMsg{Type: tea.KeyShiftTab}
			default:
				km = key(k)
			}
			before := h.model.conversationKey()
			h.model = h.update(t, km)
			if h.model.conversationKey() == before {
				t.Errorf("binding %q (%v) did not move the conversation", b.label, b.keys)
			}
			h.model.selectConversation(0) // reset for the next row
		case actHelp:
			h.model.helpOpen = false
			h.model = h.update(t, key("?"))
			if !h.model.helpOpen {
				t.Errorf("binding %q did not open help", b.label)
			}
		case actCloseHelp:
			h.model.helpOpen = true
			h.model = h.update(t, escKey())
			if h.model.helpOpen {
				t.Errorf("binding %q did not close help", b.label)
			}
		case actQuit:
			_, cmd := h.model.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
			if cmd == nil {
				t.Errorf("binding %q did not produce quit", b.label)
			}
		}
	}
}

func TestHintLineIsDerivedFromKeymapLabels(t *testing.T) {
	h := newHarness(t)
	hint := h.model.hintLine()

	labels := map[string]bool{}
	for _, b := range Keymap() {
		labels[b[0]] = true
	}

	body := strings.TrimPrefix(hint, "keys: ")
	if body == hint {
		t.Fatalf("hint line lost its prefix: %q", hint)
	}
	parts := strings.Split(body, " | ")
	if len(parts) == 0 {
		t.Fatalf("hint line lists no keys: %q", hint)
	}
	for _, p := range parts {
		if !labels[p] {
			t.Errorf("hint label %q is not a keymap label — the hint must be derived, not hand-written: %q", p, hint)
		}
	}

	// The four bindings a first-session user needs are always on screen
	// (criterion 6 does not wait for the overlay).
	for _, essential := range []action{actSend, actNextConv, actHelp, actQuit} {
		found := false
		for _, b := range keymap {
			if b.action == essential && strings.Contains(hint, b.label) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("hint line omits essential action %v: %q", essential, hint)
		}
	}
}
