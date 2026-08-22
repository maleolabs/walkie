// Command walkie is the terminal client.
//
// Not implemented yet. The work items that build it are tracked in EKA:
//
//	eka get containers
//	eka view execution
//
// Assembly order for the text half (sto:text-messaging), recorded where the
// wiring will land so it is not re-derived later: a control connection
// (internal/control, ts:reconnect-resume) feeds every received envelope to a
// messagehub.Hub's Apply, and writes whatever Hub.SendDirect/SendBroadcast
// return; rendering reads Hub.Conversation snapshots and Apply's display
// verdict (sto:terminal-ui). The seam already exists and is proven end-to-end
// against the real coordinator in internal/coordinator's messaging suite.
//
// The one thing this binary does today is report which build variant it is,
// which matters because a fleet running two variants needs a way to tell them
// apart on the device. See adr:002-runtime-stack.
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
	showVersion := flag.Bool("version", false, "print version and build variant, then exit")
	flag.Parse()

	if *showVersion {
		printVersion(os.Stdout)
		return
	}

	fmt.Fprintln(os.Stderr, "walkie: the client is not implemented yet.")
	fmt.Fprintln(os.Stderr, "Its work items are planned in EKA; run 'eka view execution' to see them.")
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
