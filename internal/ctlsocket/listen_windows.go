//go:build windows

package ctlsocket

import (
	"errors"
	"net"
)

// ErrUnavailable reports that the local control socket cannot exist on this
// platform.
//
// ts:control-socket's release matrix includes windows/amd64, so this package
// must COMPILE there; but the control surface is defined as a Unix domain
// socket with owner-only permissions, and Windows has neither in any form this
// design recognises. Rather than widen the surface with a TCP fallback (the
// package contract forbids exactly that) or break the cross-compile, Listen
// degrades to this one clear error. The walkie assembly treats it as "feature
// absent", logs once, and runs on without the socket — a Windows user loses
// scriptability, nothing else.
var ErrUnavailable = errors.New("ctlsocket: local control socket is unavailable on windows")

// Listen always fails on Windows, with [ErrUnavailable].
func Listen(path string) (net.Listener, error) {
	return nil, errors.Join(ErrUnavailable, errors.New("ctlsocket: listen refused"))
}
