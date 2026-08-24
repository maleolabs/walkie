package tui

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/message"
)

// freeInputRule is the one-sentence statement of the guarded-key rule from
// keymap.go. It lives here because it is user-facing copy; keymap.go cites it.
const freeInputRule = "Digits and ? act as commands only while the input line is empty."

// minColumns is criterion 4's hard floor: 80 columns is the smallest terminal
// the interface promises to be USABLE on. Below it the layout arithmetic
// (three-way pane split, clipped lines) would still produce output, but every
// line would be truncated mid-word and the roster would crowd out the pane —
// a corrupted-looking display that technically renders. That is exactly the
// outcome the brief forbids, so below the floor View refuses with a short,
// actionable message instead (narrowTerminalView). The floor is tested at
// exactly 80 and just below it (constrained_test.go).
const minColumns = 80

// View renders the whole interface. The layout budget is computed top-down
// from the terminal size so the result stays usable at exactly 80 columns and
// at small heights (criterion 4's floor is this package's gate).
//
// Two regions are ALWAYS rendered regardless of size: the status line (first
// line — connection state never scrolls away, criterion 3) and the hint line
// (last line — criterion 6's discoverability does not depend on the pane).
func (m Model) View() string {
	if m.width < minColumns {
		return m.narrowTerminalView()
	}

	var b strings.Builder
	b.WriteString(m.statusLine())
	b.WriteString("\n")

	if m.helpOpen {
		b.WriteString(m.helpBody())
		b.WriteString("\n")
		b.WriteString(m.hintLine())
		return clip(b.String(), m.width)
	}

	deviceLines := m.deviceLines()
	// Fixed chrome: pane header, separator, input, error line (when
	// present), hints.
	fixed := 4 // pane header + separator + input + hints
	if m.sendErr != "" {
		fixed++
	}
	// Devices get at most a third of the body so the message pane keeps the
	// majority share on a 24-row terminal; both panes degrade by truncation,
	// never by corrupting each other.
	maxDevices := (m.height - fixed) / 3
	if maxDevices < 1 {
		maxDevices = 1
	}
	shown := deviceLines
	if len(shown) > maxDevices {
		hidden := len(shown) - maxDevices
		shown = append(append([]string(nil), shown[:maxDevices]...),
			fmt.Sprintf("  ... +%d more (roster continues)", hidden))
	}

	b.WriteString("Devices:\n")
	for _, l := range shown {
		b.WriteString(l)
		b.WriteString("\n")
	}
	b.WriteString(strings.Repeat("-", clamp(m.width, 1, 80)))
	b.WriteString("\n")
	b.WriteString(m.paneHeader())
	b.WriteString("\n")

	for _, l := range m.messageLines(m.height - fixed - 3 - len(shown)) {
		b.WriteString(l)
		b.WriteString("\n")
	}
	if m.sendErr != "" {
		b.WriteString("! ")
		b.WriteString(m.sendErr)
		b.WriteString("\n")
	}
	b.WriteString(m.input.View())
	b.WriteString("\n")
	b.WriteString(m.hintLine())
	return clip(b.String(), m.width)
}

// narrowTerminalView is the below-floor refusal (criterion 4: fail legibly,
// never corrupt). It keeps the status line — criterion 3 has no size
// exception, and "am I connected" matters most precisely when the session
// looks wrong — then states the problem in three short lines. Each line is
// under 26 cells ON PURPOSE: even a badly narrow terminal shows each line
// whole, so the message itself cannot be corrupted by the clipping it
// describes.
func (m Model) narrowTerminalView() string {
	var b strings.Builder
	b.WriteString(m.statusLine())
	b.WriteString("\n")
	b.WriteString("walkie needs 80 columns.")
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("This terminal has %d.", m.width))
	b.WriteString("\n")
	b.WriteString("Widen the window.")
	return clip(b.String(), m.width)
}

// statusLine is criterion 3's contract made visible: connection state on
// screen, always, including degraded and the reconnecting states. The
// spellings come from control.State.String(), which is wire-stable across
// consumers (logs, control socket, here); the reason rides along for every
// non-online state because "what happened just now" is part of an honest
// status, not noise.
func (m Model) statusLine() string {
	id := m.connectedAs
	if id == "" {
		id = "(identity pending)"
	}
	s := fmt.Sprintf("walkie  you: %s  [%s]", shortName(id), m.connState.String())
	if m.connState != control.StateOnline {
		// Reason text arrives with the Change; the model keeps only the
		// current state, so reasons render via the most recent transition's
		// state alone. Keeping a reason string per state would duplicate what
		// the stream already delivered once; the state word is the load-bearing
		// half for a human scanning the corner of a screen.
		s += " (reconnecting)" // connecting/handshaking/degraded/disconnected all read as "not usable yet"
	}
	return clip(s, m.width)
}

// paneHeader names the conversation the pane currently shows, so a jump via
// tab or digits always announces where the user landed.
func (m Model) paneHeader() string {
	return clip("in: "+conversationTitle(m.conversationKey()), m.width)
}

// deviceLines renders the roster: presence verdict AND custom status without
// issuing a command (criterion 2). State is carried twice per line — symbol
// position and the words online/offline — so no-colour terminals lose nothing
// (the legibility half of criterion 4, designed in now, gated in slice 2).
// Unread arrivals are marked per line, so traffic in an unselected
// conversation stays visible without switching views.
func (m Model) deviceLines() []string {
	out := make([]string, 0, len(m.devices))
	for _, d := range m.devices {
		label := shortName(d.Device)
		if d.Device == m.connectedAs {
			label += " (you)"
		}
		if n := m.unread[message.ConversationKey(d.Device)]; n > 0 {
			label += fmt.Sprintf(" [%d new]", n)
		}
		if d.Online {
			line := fmt.Sprintf("* %s  online", label)
			if d.Status != "" {
				line += " - " + d.Status
			}
			out = append(out, line)
			continue
		}
		line := fmt.Sprintf("  %s  offline", label)
		if d.LastSeen != 0 {
			line += fmt.Sprintf(" - last seen %s", time.Unix(0, d.LastSeen).Format("15:04:05"))
		}
		if d.Status != "" {
			line += " - " + d.Status
		}
		out = append(out, line)
	}
	return out
}

// messageLines renders the current conversation, newest at the bottom, at
// most n lines. Both timestamps ride every line (req:text-messaging keeps
// sent_at and received_at unmerged so clock skew stays legible); a locally
// filed send has no coordinator stamp yet and says "-" instead of faking one
// — the unstamped rendering the messagehub package comment requires.
func (m Model) messageLines(n int) []string {
	src := m.p.HubFunc
	if n <= 0 || src == nil {
		return []string{"(no messages yet - type below, tab switches conversation)"}
	}
	// The seam is non-nil from assembly time, but RETURNS nil until the first
	// handshake completes (the hub's local name is the HelloAck echo, so the
	// assembly builds it lazily — see ConversationSource). Bubbletea renders
	// View() on Program.Run, before any of that, so the nil RESULT must be
	// guarded exactly like refreshConversations does; calling Conversation on
	// it panicked on the very first frame.
	var msgs []message.Message
	if hub := src(); hub != nil {
		msgs = hub.Conversation(m.conversationKey())
	}
	if len(msgs) == 0 {
		return []string{"(no messages yet - type below, tab switches conversation)"}
	}
	if len(msgs) > n {
		msgs = msgs[len(msgs)-n:]
	}
	out := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		who := shortName(msg.Sender)
		if msg.Sender == m.connectedAs {
			who = "you"
		}
		recv := msg.ReceivedAt.Format("15:04:05")
		if msg.ReceivedAt.IsZero() {
			recv = "-"
		}
		out = append(out, fmt.Sprintf("%s  sent %s recv %s  %s",
			who, msg.SentAt.Format("15:04:05"), recv, singleLine(msg.Body)))
	}
	return out
}

// hintLine is derived FROM THE KEYMAP, not maintained beside it: filter the
// single-source table down to the actions a first-session user needs and
// render their labels. A binding renamed in keymap.go changes here too —
// that is the point (criterion 6's "kept in sync").
func (m Model) hintLine() string {
	labels := map[action]string{}
	for _, b := range keymap {
		labels[b.action] = b.label
	}
	parts := []string{
		labels[actSend],
		labels[actNextConv],
		labels[actHelp],
		labels[actQuit],
	}
	return clip("keys: "+strings.Join(parts, " | "), m.width)
}

// helpBody is the full-screen help overlay: the complete single-source table
// plus the build's voice capability, stated as present or absent — never as
// an error (audio.Available's degrade-legibly rule).
func (m Model) helpBody() string {
	voice := "voice notes: available in this build"
	if !m.p.AudioAvailable {
		voice = "voice notes: not in this build (text and presence work fully)"
	}
	return helpText() + "\n" + voice + "\n"
}

// -- small rendering helpers ---------------------------------------------

// shortName trims a tailnet device name to its host label for display:
// "alice.tail-scale.ts.net." reads as "alice" on an 80-column line. The full
// name remains the identity everywhere state is stored or keyed.
func shortName(device string) string {
	return strings.SplitN(device, ".", 2)[0]
}

// singleLine flattens a body for one-line rendering: newlines become spaces
// so a multi-line paste cannot corrupt the pane's line budget.
func singleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// clip cuts s to at most w cells counting runes, so no pane can push another
// off-screen horizontally. Rune-counting (not bytes) is what makes CJK input
// stay inside the budget.
func clip(s string, w int) string {
	if w <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if utf8.RuneCountInString(l) > w {
			r := []rune(l)
			lines[i] = string(r[:w])
		}
	}
	return strings.Join(lines, "\n")
}

// clamp bounds v into [lo, hi].
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
