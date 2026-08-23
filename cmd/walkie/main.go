// Command walkie is the terminal client.
//
// The interactive client is still being assembled; its work items are tracked
// in EKA:
//
//	eka get containers
//	eka view execution
//
// Assembly order for the text half (sto:text-messaging), recorded where the
// wiring will land so it is not re-derived later: a control.Client
// (internal/control, ts:reconnect-resume — dialing, Hello/HelloAck with
// last_acked_position, watchdog, jittered retry loop) feeds every received
// envelope to a messagehub.Hub's Apply via OnEnvelope; outgoing Hub envelopes
// go through Client.Send; queue deliveries are acknowledged through
// Client.Acknowledge once durably processed; connection state for rendering
// comes from Client.Machine().Subscribe plus Client.State/ConnectedAs.
// Rendering reads Hub.Conversation snapshots and Apply's display verdict
// (sto:terminal-ui). The seam already exists and is proven end-to-end
// against the real coordinator in internal/coordinator's messaging suite and
// internal/control's WebSocket e2e suite.
//
// What works today:
//
//   - `walkie history` — scriptable query over the local message store
//     (sto:message-history criterion 2): by conversation, by time range, one
//     JSON object per line. See its -help for the security statement that
//     ships with it.
//   - `walkie -version` — which build variant this is, which matters because a
//     fleet running two variants needs a way to tell them apart on the device
//     (adr:002-runtime-stack).
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/maleolabs/walkie/internal/audio"
)

// version is overridden at build time with -ldflags "-X main.version=...".
// Wiring that into the release pipeline belongs to ts:build-release-matrix.
var version = "dev"

func main() {
	// Subcommand dispatch happens before global flag parsing: flag.Parse stops
	// at the first non-flag argument, so "walkie history ..." would otherwise
	// fall through to the not-implemented message below.
	if len(os.Args) > 1 && os.Args[1] == "history" {
		os.Exit(runHistory(os.Args[2:], os.Stdout, os.Stderr))
	}

	showVersion := flag.Bool("version", false, "print version and build variant, then exit")
	flag.Parse()

	if *showVersion {
		printVersion(os.Stdout)
		return
	}

	fmt.Fprintln(os.Stderr, "walkie: the interactive client is not assembled yet.")
	fmt.Fprintln(os.Stderr, "Its work items are planned in EKA; run 'eka view execution' to see them.")
	fmt.Fprintln(os.Stderr, "Available today: 'walkie history -help' queries stored message history.")
	os.Exit(1)
}

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "walkie %s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)

	// The interesting half. A device that cannot install libopus still runs a
	// fully useful text and presence client, so "no audio" is a normal state to
	// report plainly rather than a defect to hide.
	if audio.Available() {
		fmt.Fprintf(w, "audio  available (built with the \"voice\" tag)\n")
	} else {
		fmt.Fprintf(w, "audio  unavailable (built without the \"voice\" tag)\n")
	}
}
