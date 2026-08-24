// `walkie help` (ts:docs-quickstart-runbook criterion 2): the command surface
// as code, not prose. Every list this command prints is DERIVED from the
// structure that implements it, so help cannot drift from behaviour:
//
//   - interactive keys come from tui.HelpText(), which renders the same
//     single-source table the key dispatcher dispatches through;
//   - subcommands come from subcommandSummaries, and help_test.go pins that
//     table to main's dispatch switch — add one without the other and the
//     test fails;
//   - client flags come from registerClientFlags, the exact registration
//     main() parses with, rendered by the flag package itself.
//
// A transcribed help screen goes stale the first time a binding or flag
// changes; this one has nothing to go stale.
package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/maleolabs/walkie/internal/audio"
	"github.com/maleolabs/walkie/internal/tui"
)

// subcommandSummaries lists walkie's non-interactive subcommands in help
// order. main()'s dispatch table handles these names; help_test.go asserts
// the two agree, so a new subcommand cannot ship undocumented (or documented
// but undispatchable).
var subcommandSummaries = []struct{ name, summary string }{
	{"history", "query this device's stored message history (one JSON object per line)"},
	{"help", "show this help"},
}

// subcommandRunners is the authority on which names are subcommands: a name
// is dispatched if and only if it is a key here. The map exists so the set of
// dispatched names is enumerable — help_test.go walks both directions of the
// pairing with subcommandSummaries, which documents exactly these.
var subcommandRunners = map[string]func(args []string, stdout, stderr io.Writer) int{
	"history": runHistory,
	"help":    runHelp,
}

// dispatchSubcommand runs the named subcommand if one matches and reports
// whether it did, returning the process exit code instead of exiting so tests
// can drive it. main() supplies the os.Exit.
func dispatchSubcommand(name string, args []string, stdout, stderr io.Writer) (int, bool) {
	run, ok := subcommandRunners[name]
	if !ok {
		return 0, false
	}
	return run(args, stdout, stderr), true
}

// runHelp prints the whole command surface and exits 0. It never touches the
// network or local state: help must work on a machine where nothing is
// configured yet, because "what can this do?" precedes "is it set up?".
func runHelp(args []string, stdout, stderr io.Writer) int {
	printVersion(stdout)

	// Every summary starts on one column: tails are padded to the widest,
	// "history [flags]".
	const tailWidth = len("history [flags]")
	row := func(tail, summary string) {
		fmt.Fprintf(stdout, "  walkie %-*s  %s\n", tailWidth, tail, summary)
	}
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Usage:")
	row("", "start the interactive client")
	for _, sc := range subcommandSummaries {
		if sc.name == "history" {
			row("history [flags]", sc.summary+" (-help for its flags)")
			continue
		}
		row(sc.name, sc.summary)
	}
	row("-version", "print version and build variant, then exit")

	// The interactive surface, verbatim from the TUI's single source: same
	// table, same guarded-input rule, same wording the overlay shows.
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Interactive commands (press ? inside the client for the same list):")
	fmt.Fprintln(stdout, tui.HelpText())

	// Build-variant differences (adr:002-runtime-stack), stated against what
	// actually differs today. The voice tag currently gates audio PLUMBING
	// only: sto:voice-note is not implemented, so no build records or plays
	// voice notes yet — saying otherwise would document a deferred feature.
	fmt.Fprintln(stdout, "Build variants:")
	if audio.Available() {
		fmt.Fprintln(stdout, `  This binary was built with the "voice" tag: audio capture/playback plumbing is compiled in.`)
	} else {
		fmt.Fprintln(stdout, `  This binary is the default pure-Go build: no audio plumbing. Text and presence work fully.`)
	}
	fmt.Fprintln(stdout, `  Voice notes are NOT implemented yet in any build; today the tag changes only`)
	fmt.Fprintln(stdout, `  what -version and this help report. A voice-tagged binary needs CGO to build.`)

	// Flags via the flag package itself, registered through the same helper
	// main() parses with — defaults shown here are the defaults in force.
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Client flags:")
	fs := flag.NewFlagSet("walkie", flag.ContinueOnError)
	registerClientFlags(fs)
	fs.SetOutput(stdout)
	fs.PrintDefaults()

	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Security notes and operations docs: see README.md, docs/quickstart.md and docs/runbook.md.")
	return 0
}
