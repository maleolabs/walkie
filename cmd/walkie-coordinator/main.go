// Command walkie-coordinator is the coordination server.
//
// Not implemented yet. It is built by ts:coordinator-skeleton and the work items
// that follow it:
//
//	eka get walkie/ts:coordinator-skeleton
//
// It owns only what the tailnet does not provide — identity resolution,
// presence, the offline queue, relay fallback and observability. It holds no
// credentials and no user table. See internal/coordinator and
// fnd:tailnet-capability-baseline.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version, then exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("walkie-coordinator %s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)
		return
	}

	fmt.Fprintln(os.Stderr, "walkie-coordinator: not implemented yet.")
	fmt.Fprintln(os.Stderr, "See 'eka get walkie/ts:coordinator-skeleton' for what it must do.")
	os.Exit(1)
}
