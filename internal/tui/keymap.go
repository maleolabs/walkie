package tui

import "strings"

// The key map is the SINGLE SOURCE for two consumers that must never drift:
// the key dispatcher in Update and the help overlay required by
// sto:terminal-ui criterion 6 ("every command is discoverable from within the
// interface"). A help screen maintained as a second list goes stale the first
// time a binding changes; here there is exactly one table, Update dispatches
// through it, and helpText renders it. Adding a command means adding one
// entry — dispatch and discoverability move together or not at all.
//
// ts:docs-quickstart-runbook later has to enumerate the command surface
// programmatically; Keymap() below is that enumeration, exported so the docs
// item can walk it instead of transcribing it.

// action identifies what a key press does. It exists so the table can carry
// both halves of a binding (which keys, what it is called) while Update keeps
// a plain switch for the behaviour.
type action int

const (
	actNone      action = iota // matched no binding; the key types into the input
	actSend                    // send the composed message
	actNextConv                // next conversation
	actPrevConv                // previous conversation
	actJumpConv                // jump to the Nth conversation (keys "1".."9")
	actHelp                    // toggle the help overlay
	actQuit                    // exit walkie
	actCloseHelp               // close the help overlay without quitting
)

// binding is one row of the single-source table. keys are the bubbletea key
// names the row answers; label is how help shows them; desc is the
// human-readable sentence help renders. One row == one discoverable command.
type binding struct {
	keys      []string
	label     string
	desc      string
	action    action
	needsFree bool // only fires when the input line is empty (see below)
}

// keymap is the whole command surface of this interface, in the order help
// lists it.
//
// needsFree exists because digits and "?" are also ordinary text: a user
// typing "meeting at 3?" must get that text, not a conversation jump and a
// help toggle. The rule, stated once here and echoed verbatim in the help
// footer: command keys that share their keycap with a printable character act
// ONLY when the input line is empty. Keys with no printable form (enter, tab,
// ctrl+c, esc) always fire — they cannot be typed as text, so no guard is
// needed and guarding them would only add surprise.
var keymap = []binding{
	{
		keys:   []string{"enter"},
		label:  "enter",
		desc:   "send the message you typed",
		action: actSend,
	},
	{
		keys:   []string{"tab"},
		label:  "tab",
		desc:   "next conversation",
		action: actNextConv,
	},
	{
		keys:   []string{"shift+tab"},
		label:  "shift+tab",
		desc:   "previous conversation",
		action: actPrevConv,
	},
	{
		keys:      []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
		label:     "1-9",
		desc:      "jump to the Nth conversation",
		action:    actJumpConv,
		needsFree: true,
	},
	{
		keys:      []string{"?"},
		label:     "? (empty input)",
		desc:      "toggle this help",
		action:    actHelp,
		needsFree: true,
	},
	{
		keys:   []string{"esc"},
		label:  "esc",
		desc:   "close help",
		action: actCloseHelp,
	},
	{
		keys:   []string{"ctrl+c"},
		label:  "ctrl+c",
		desc:   "quit walkie",
		action: actQuit,
	},
}

// lookup maps a bubbletea key name onto its binding. First match wins; the
// table is short enough that order is readable rather than load-bearing,
// except that more specific names must precede prefixes of themselves (they
// do: no key name here is a prefix of another).
func lookup(key string) (binding, bool) {
	for _, b := range keymap {
		for _, k := range b.keys {
			if k == key {
				return b, true
			}
		}
	}
	return binding{}, false
}

// Keymap returns the command surface as (label, description) pairs, in help
// order. Exported for the two consumers that must agree with this table by
// construction: the help overlay (same package) and programmatic enumeration
// (ts:docs-quickstart-runbook).
func Keymap() [][2]string {
	out := make([][2]string, 0, len(keymap))
	for _, b := range keymap {
		out = append(out, [2]string{b.label, b.desc})
	}
	return out
}

// HelpText renders the full help body — the command table plus the
// guarded-input rule — for consumers outside this package. Exported alongside
// Keymap for ts:docs-quickstart-runbook's `walkie help`: a second renderer in
// cmd/walkie would be exactly the drift the single-source rule forbids, so the
// CLI prints what the overlay prints.
func HelpText() string { return helpText() }

// helpText renders the single-source table. Plain text, no colour and no
// box-drawing: the help must survive TERM=dumb and an 80-column terminal
// (criteria 4 and 5), and a fixed-width two-column layout under 80 columns is
// the most portable way to say "key — what it does".
func helpText() string {
	var sb strings.Builder
	sb.WriteString("Commands (also always visible in the hint line):\n")
	for _, b := range keymap {
		sb.WriteString("  ")
		sb.WriteString(b.label)
		sb.WriteString("  ")
		sb.WriteString(b.desc)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	sb.WriteString(freeInputRule)
	return sb.String()
}
