package tsauth

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// Identity is what the tailnet says about the peer behind a connection.
//
// This struct is walkie's entire authentication story. adr:004-security-model
// resolves the caller's identity with a WhoIs lookup on the connection's
// remote address; authorization is the tailnet ACL, which walkie inherits and
// cannot compensate for. There is deliberately NO password, token, session or
// user record here or anywhere else — adding one is a design regression that
// needs an ADR revision, not an implementation (see the package comment).
//
// The fields mirror what WhoIs answers, thinned to what the coordinator
// skeleton consumes today. Later items needing more of the answer extend this
// struct rather than reaching around the seam.
type Identity struct {
	// LoginName is the tailnet login behind the connection, e.g.
	// "alice@example.com". The stable per-user identity; log lines cite it,
	// never message content (the no-content rule arrives formally with
	// ts:observability-baseline but holds from the first line).
	LoginName string

	// DisplayName is the human-readable name the tailnet records, e.g.
	// "Alice Smith". Presentation only; never an identifier.
	DisplayName string

	// NodeName is the node's MagicDNS FQDN with its trailing dot, e.g.
	// "phone.tail-scale.ts.net.". Identifies the DEVICE; one user may have
	// several.
	NodeName string
}

// Resolver resolves a connection's remote address to an authenticated tailnet
// identity — criterion 2 of ts:coordinator-skeleton.
//
// The contract is refuse-on-failure: every error means the caller must reject
// the connection and log the refusal. There is no anonymous-but-allowed
// fallback, because fnd:tailnet-capability-baseline's whole point is that the
// substrate already authenticated the peer — if we cannot confirm WHO, we do
// not pretend it is nobody-harmful.
//
// Implementations: [*TSNetResolver] is the production path through the local
// Tailscale daemon; [*StaticResolver] and [ResolverFunc] are the test doubles
// that let criteria 2 and 3 be genuinely tested without a live tailnet.
type Resolver interface {
	Resolve(ctx context.Context, remoteAddr net.Addr) (Identity, error)
}

var (
	_ Resolver = ResolverFunc(nil)
	_ Resolver = (*TSNetResolver)(nil)
	_ Resolver = (*StaticResolver)(nil)
)

// ResolverFunc adapts an ordinary function to [Resolver], the way http.HandlerFunc
// does. In production wiring it lets the coordinator inject policy-shaped
// resolvers without declaring a type each; in tests it is the smallest
// possible double.
type ResolverFunc func(ctx context.Context, remoteAddr net.Addr) (Identity, error)

// Resolve calls f.
func (f ResolverFunc) Resolve(ctx context.Context, remoteAddr net.Addr) (Identity, error) {
	return f(ctx, remoteAddr)
}

// StaticResolver is the canned test double backing criteria 2 and 3: the
// coordinator's accept loop can be exercised end-to-end against identities
// this resolver hands out, with no tsnet, no auth key and no network.
//
// It is keyed by the HOST portion of the remote address (the port is
// ephemeral, so a key including it would make every test brittle).
type StaticResolver struct {
	mu     sync.RWMutex
	byHost map[string]Identity
}

// NewStaticResolver returns a double answering with the given identities,
// keyed by host ("100.64.0.1"). The map is copied; mutate via Set.
func NewStaticResolver(byHost map[string]Identity) *StaticResolver {
	m := make(map[string]Identity, len(byHost))
	for k, v := range byHost {
		m[k] = v
	}
	return &StaticResolver{byHost: m}
}

// Set adds or replaces the identity for host. Tests use it to stage peers
// between phases without rebuilding the resolver.
func (r *StaticResolver) Set(host string, id Identity) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byHost[host] = id
}

// Resolve looks up the host portion of remoteAddr.
//
// An unknown host is an ERROR, never an empty-but-valid Identity: the double
// must exercise the same refusal path a real unresolvable peer takes, or the
// criterion-2 test would pass while the refusal branch went untested.
func (r *StaticResolver) Resolve(_ context.Context, remoteAddr net.Addr) (Identity, error) {
	host := hostOf(remoteAddr)

	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.byHost[host]
	if !ok {
		return Identity{}, fmt.Errorf("tsauth: %s (%s): not a known peer", remoteAddr, host)
	}
	return id, nil
}

// hostOf extracts the host portion of an address, tolerating addresses that
// carry no port at all.
func hostOf(remoteAddr net.Addr) string {
	host, _, err := net.SplitHostPort(remoteAddr.String())
	if err != nil {
		return remoteAddr.String()
	}
	return host
}
