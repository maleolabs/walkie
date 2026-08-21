// Package ctlsocket exposes a local Unix-domain control socket, so a running
// walkie client can be driven and observed from the shell.
//
// Owning work item:
//
//	eka get walkie/ts:control-socket
//
// # Why this ships in the MVP
//
// Slash commands, webhooks, the subprocess plugin protocol and auto-update are
// all deferred to phase 4 in plan:roadmap-v1. This package is in the MVP anyway,
// deliberately, because it is the substrate all of them would need — and on its
// own it already makes walkie scriptable, which covers most of the practical
// demand for automation.
//
// Delivering it first is what lets the plugin runtime be postponed honestly
// rather than merely dropped.
//
// # Constraints
//
//   - Local only. This socket is never reachable over the network in any form.
//   - Owner-only permissions.
//   - Events are line-delimited and parseable by standard command-line tools.
//     A shell script with no walkie-specific tooling must be able to send a
//     message and read the event stream.
//   - A client with no reader attached to the event stream must not block or
//     leak.
//
// Note that Unix socket paths have a platform length limit of roughly 104 bytes.
// Prefer a short path under the user's runtime or state directory.
package ctlsocket

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// Listen creates the control socket at path and returns a listener.
//
// It handles the case ts:control-socket calls out explicitly: a socket file left
// behind by an unclean shutdown of a previous run must not block startup. The
// naive fix — unlink whatever is there and bind — is wrong, because it silently
// steals the socket from a second walkie that is running perfectly well. So a
// leftover file is probed first: if something answers, Listen refuses; only a
// genuinely dead socket is removed.
//
// The parent directory is created with owner-only permissions before binding,
// which closes the window in which the socket would otherwise exist with
// umask-derived permissions.
func Listen(path string) (net.Listener, error) {
	if path == "" {
		return nil, errors.New("ctlsocket: empty socket path")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("ctlsocket: create socket directory: %w", err)
	}
	if err := clearDeadSocket(path); err != nil {
		return nil, err
	}

	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("ctlsocket: listen on %s: %w", path, err)
	}

	// Belt and braces alongside the 0700 directory. On Windows this only
	// toggles the read-only bit, which is harmless.
	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("ctlsocket: restrict permissions on %s: %w", path, err)
	}
	return l, nil
}

// ErrInUse reports that another process is already listening on the socket.
var ErrInUse = errors.New("ctlsocket: socket is already in use by a running walkie")

// clearDeadSocket removes a leftover socket file, but only after establishing
// that nothing is listening on it.
func clearDeadSocket(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("ctlsocket: inspect %s: %w", path, err)
	}

	if conn, err := net.Dial("unix", path); err == nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %s", ErrInUse, path)
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("ctlsocket: remove stale socket %s: %w", path, err)
	}
	return nil
}
