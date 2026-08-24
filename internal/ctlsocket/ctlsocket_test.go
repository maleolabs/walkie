//go:build unix

package ctlsocket

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestListenCreatesUsableSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "walkie.sock")

	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial the socket we just created: %v", err)
	}
	_ = conn.Close()
}

func TestListenRestrictsPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry Unix permission bits")
	}
	path := filepath.Join(t.TempDir(), "walkie.sock")

	l, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket permissions = %#o, want 0600", perm)
	}
}

// The crash-recovery case from ts:control-socket: a socket file left behind by a
// previous run must not block startup.
func TestListenRemovesDeadSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "walkie.sock")

	first, err := Listen(path)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	// Close the listener but leave the file behind, which is what an unclean
	// shutdown looks like on disk.
	if err := first.Close(); err != nil {
		t.Fatalf("close first listener: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("simulate leftover socket file: %v", err)
	}

	second, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen over a dead socket should succeed, got: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
}

// The case the naive implementation gets wrong: unlinking whatever is there
// would silently steal the socket from a healthy second instance.
func TestListenRefusesLiveSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "walkie.sock")

	live, err := Listen(path)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	t.Cleanup(func() { _ = live.Close() })

	second, err := Listen(path)
	if err == nil {
		_ = second.Close()
		t.Fatal("Listen stole a socket that another instance was serving")
	}
	if !errors.Is(err, ErrInUse) {
		t.Errorf("got %v, want ErrInUse", err)
	}
}

func TestListenRejectsEmptyPath(t *testing.T) {
	if _, err := Listen(""); err == nil {
		t.Fatal("Listen(\"\") succeeded, want an error")
	}
}
