package tsauth

import (
	"context"
	"errors"
	"net"
	"testing"

	apitype "tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
)

func TestIdentityFromWhoIsMapsFields(t *testing.T) {
	resp := &apitype.WhoIsResponse{
		Node: &tailcfg.Node{Name: "phone.tail-scale.ts.net."},
		UserProfile: &tailcfg.UserProfile{
			LoginName:   "alice@example.com",
			DisplayName: "Alice Smith",
		},
	}

	got := identityFromWhoIs(resp)
	want := Identity{
		LoginName:   "alice@example.com",
		DisplayName: "Alice Smith",
		NodeName:    "phone.tail-scale.ts.net.",
	}
	if got != want {
		t.Fatalf("identityFromWhoIs = %+v, want %+v", got, want)
	}
}

// A mixed-version daemon may omit parts of the answer; the mapping must
// degrade to a thinner identity, never panic inside an accept loop.
func TestIdentityFromWhoIsToleratesMissingParts(t *testing.T) {
	cases := map[string]*apitype.WhoIsResponse{
		"nil response":     nil,
		"nil node":         {UserProfile: &tailcfg.UserProfile{LoginName: "alice@example.com"}},
		"nil user profile": {Node: &tailcfg.Node{Name: "phone.tail-scale.ts.net."}},
	}
	for name, resp := range cases {
		got := identityFromWhoIs(resp) // must not panic
		switch name {
		case "nil response":
			if got != (Identity{}) {
				t.Errorf("%s: got %+v, want zero Identity", name, got)
			}
		case "nil node":
			if got.LoginName != "alice@example.com" || got.NodeName != "" {
				t.Errorf("%s: got %+v, want login only", name, got)
			}
		case "nil user profile":
			if got.NodeName != "phone.tail-scale.ts.net." || got.LoginName != "" {
				t.Errorf("%s: got %+v, want node only", name, got)
			}
		}
	}
}

func TestStaticResolverResolvesByHostIgnoringPort(t *testing.T) {
	alice := Identity{LoginName: "alice@example.com", NodeName: "phone.tail-scale.ts.net."}
	r := NewStaticResolver(map[string]Identity{"100.64.0.7": alice})

	addr := &net.TCPAddr{IP: net.ParseIP("100.64.0.7"), Port: 52814}
	got, err := r.Resolve(context.Background(), addr)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", addr, err)
	}
	if got != alice {
		t.Fatalf("Resolve = %+v, want %+v", got, alice)
	}
}

// The double must refuse unknown hosts exactly like the real resolver refuses
// an unresolvable peer — the criterion-2 test drives THIS error path.
func TestStaticResolverRefusesUnknownHost(t *testing.T) {
	r := NewStaticResolver(nil)

	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.50"), Port: 1234} // non-tailnet address
	if _, err := r.Resolve(context.Background(), addr); err == nil {
		t.Fatal("unknown host resolved without error; the refusal path went untested")
	}
}

func TestStaticResolverSetReplacesIdentity(t *testing.T) {
	r := NewStaticResolver(nil)
	addr := &net.TCPAddr{IP: net.ParseIP("100.64.0.9"), Port: 1}

	if _, err := r.Resolve(context.Background(), addr); err == nil {
		t.Fatal("unseeded host resolved; want refusal")
	}

	r.Set("100.64.0.9", Identity{LoginName: "bob@example.com"})
	got, err := r.Resolve(context.Background(), addr)
	if err != nil || got.LoginName != "bob@example.com" {
		t.Fatalf("after Set: got (%+v, %v), want bob", got, err)
	}
}

func TestResolverFuncPassesThrough(t *testing.T) {
	wantErr := errors.New("daemon down")
	failing := ResolverFunc(func(context.Context, net.Addr) (Identity, error) { return Identity{}, wantErr })

	if _, err := failing.Resolve(context.Background(), &net.TCPAddr{}); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}

	ok := ResolverFunc(func(_ context.Context, remoteAddr net.Addr) (Identity, error) {
		return Identity{LoginName: remoteAddr.String()}, nil
	})
	got, err := ok.Resolve(context.Background(), &net.TCPAddr{IP: net.ParseIP("100.64.0.1"), Port: 2})
	if err != nil || got.LoginName != "100.64.0.1:2" {
		t.Fatalf("got (%+v, %v), want passthrough identity", got, err)
	}
}

// hostOf is what makes StaticResolver keys stable across ephemeral ports;
// pin its behaviour for both shapes it will meet.
func TestHostOfStripsPort(t *testing.T) {
	withPort := &net.TCPAddr{IP: net.ParseIP("100.64.0.3"), Port: 4488}
	if got := hostOf(withPort); got != "100.64.0.3" {
		t.Fatalf("hostOf(%s) = %q, want %q", withPort, got, "100.64.0.3")
	}

	bare := customAddr{value: "100.64.0.3"}
	if got := hostOf(bare); got != "100.64.0.3" {
		t.Fatalf("hostOf(portless) = %q, want %q", got, "100.64.0.3")
	}
}

type customAddr struct{ value string }

func (a customAddr) Network() string { return "tcp" }
func (a customAddr) String() string  { return a.value }
