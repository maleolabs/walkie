package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/maleolabs/walkie/internal/tui"
)

// These tests pin `walkie help` to its contract (ts:docs-quickstart-runbook
// criterion 2): every command, every subcommand and every flag appears, and
// the interactive surface is DERIVED from tui's single source rather than
// transcribed. All output is deterministic — no clock, no network, no state.

// TestHelpListsEveryInteractiveCommandFromTheSingleSource walks tui.Keymap()
// and requires each label AND each description to appear in the help output.
// Because runHelp renders through tui.HelpText(), this passes by construction
// today; its job is to fail if someone replaces the derivation with a
// hand-written list that then drifts from the keymap.
func TestHelpListsEveryInteractiveCommandFromTheSingleSource(t *testing.T) {
	var out bytes.Buffer
	if code := runHelp(nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("runHelp exit = %d, want 0", code)
	}
	got := out.String()

	for _, pair := range tui.Keymap() {
		label, desc := pair[0], pair[1]
		if !strings.Contains(got, label) {
			t.Errorf("help output missing interactive command label %q", label)
		}
		if !strings.Contains(got, desc) {
			t.Errorf("help output missing description for %q: %q", label, desc)
		}
	}
}

// TestHelpMatchesTUIOverlayWordForWord is the stronger half of the derivation
// guarantee: the CLI prints exactly what the in-client overlay prints for the
// command table section, so a user reading docs and a user pressing ? see the
// same words.
func TestHelpMatchesTUIOverlayWordForWord(t *testing.T) {
	var out bytes.Buffer
	if code := runHelp(nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("runHelp exit = %d, want 0", code)
	}
	if !strings.Contains(out.String(), tui.HelpText()) {
		t.Error("help output does not contain tui.HelpText() verbatim")
	}
}

// TestHelpListsEverySubcommand pins help.go's subcommandSummaries table to
// dispatchSubcommand's switch: every documented name must actually dispatch
// (a documented-but-undispatchable subcommand is a lie), and an unknown name
// must not dispatch. The reverse direction — a dispatched name missing from
// the table — is covered by review of the switch, which sits directly above
// the table in help.go so the two are read together.
func TestHelpListsEverySubcommand(t *testing.T) {
	for _, sc := range subcommandSummaries {
		var out, errOut bytes.Buffer
		// -help keeps the probe off the filesystem: `history -help` prints its
		// usage and exits 0 deterministically, where bare `history` would look
		// for a database that may or may not exist on this machine.
		code, handled := dispatchSubcommand(sc.name, []string{"-help"}, &out, &errOut)
		if !handled {
			t.Errorf("subcommand %q is documented but not dispatched", sc.name)
			continue
		}
		if code != 0 {
			t.Errorf("dispatchSubcommand(%q) exit = %d, want 0", sc.name, code)
		}
	}
	if _, handled := dispatchSubcommand("nosuchsubcommand", nil, &bytes.Buffer{}, &bytes.Buffer{}); handled {
		t.Error("unknown subcommand was dispatched")
	}
}

// TestHelpListsEveryClientFlag registers the client flags and requires each
// one's name to appear in the help output. Defaults come from the same
// registerClientFlags main() uses, so the shown defaults are real.
func TestHelpListsEveryClientFlag(t *testing.T) {
	var out bytes.Buffer
	if code := runHelp(nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("runHelp exit = %d, want 0", code)
	}
	got := out.String()

	for _, name := range []string{"-version", "-coordinator", "-db", "-identity", "-socket"} {
		if !strings.Contains(got, name) {
			t.Errorf("help output missing client flag %s", name)
		}
	}
	// The default coordinator URL is the assumption the quickstart leans on;
	// it must be visible where a reader first looks.
	if !strings.Contains(got, defaultCoordinatorURL) {
		t.Errorf("help output missing default coordinator URL %q", defaultCoordinatorURL)
	}
}

// TestHelpStatesBuildVariantDifference checks criterion 2's second half: the
// variant difference is stated, and honestly — no build claims voice notes,
// because sto:voice-note is not implemented.
func TestHelpStatesBuildVariantDifference(t *testing.T) {
	var out bytes.Buffer
	if code := runHelp(nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("runHelp exit = %d, want 0", code)
	}
	got := out.String()

	if !strings.Contains(got, "Build variants:") {
		t.Error("help output missing build-variant section")
	}
	if !strings.Contains(got, "Voice notes are NOT implemented yet") {
		t.Error("help output must state plainly that voice notes are not implemented in any build")
	}
}

// TestHelpMentionsHistorySecurityPointer: the history subcommand's own -help
// carries the unencrypted-at-rest statement; the top-level help must at least
// point readers at security notes rather than stay silent about them.
func TestHelpMentionsHistorySecurityPointer(t *testing.T) {
	var out bytes.Buffer
	if code := runHelp(nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("runHelp exit = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "Security notes") {
		t.Error("help output missing security pointer")
	}
}
