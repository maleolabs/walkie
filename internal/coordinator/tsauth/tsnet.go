package tsauth

import (
	"context"
	"errors"
	"fmt"
	"net"

	"tailscale.com/client/local"
	apitype "tailscale.com/client/tailscale/apitype"
	tsnet "tailscale.com/tsnet"
)

// TSNetResolver is the production [Resolver]: it asks the local Tailscale
// daemon of a running [tsnet.Server] who owns a connection's remote address.
//
// The coordinator IS a tailnet node (criterion 1: it joins from inside its
// container via tsnet, no host networking, no sidecar), so its daemon can
// answer WhoIs for every connection accepted on the tailnet interface. That
// lookup is the whole authentication act — adr:004-security-model; there is
// nothing to configure and nothing to store.
//
// Import paths verified against the version pinned in go.mod
// (tailscale.com v1.94.2): the local client lives at tailscale.com/client/local
// and the response type at tailscale.com/client/tailscale/apitype. Both have
// moved between releases before — re-verify on any dependency bump rather than
// trusting this comment's age.
type TSNetResolver struct {
	lc *local.Client
}

// NewTSNetResolver returns a resolver backed by srv's local client.
//
// Note that (*tsnet.Server).LocalClient() starts the server if it has not
// started, so constructing this resolver JOINS THE TAILNET. Callers therefore
// configure Hostname, Dir and AuthKey on srv first; the join itself is wired
// by the coordinator startup (slice 2 of ts:coordinator-skeleton) and cannot
// be exercised here without a live tailnet and auth key.
func NewTSNetResolver(srv *tsnet.Server) (*TSNetResolver, error) {
	if srv == nil {
		return nil, errors.New("tsauth: nil tsnet server")
	}
	lc, err := srv.LocalClient()
	if err != nil {
		return nil, fmt.Errorf("tsauth: local client: %w", err)
	}
	return &TSNetResolver{lc: lc}, nil
}

// Resolve performs the WhoIs lookup for remoteAddr.
//
// Every error path is a refusal under criterion 2: the caller rejects the
// connection and logs. ErrPeerNotFound is called out only so refusal logs can
// distinguish "address outside the tailnet" from "daemon unreachable"; both
// refuse identically.
func (r *TSNetResolver) Resolve(ctx context.Context, remoteAddr net.Addr) (Identity, error) {
	resp, err := r.lc.WhoIs(ctx, remoteAddr.String())
	if err != nil {
		if errors.Is(err, local.ErrPeerNotFound) {
			return Identity{}, fmt.Errorf("tsauth: %s: not a known tailnet peer", remoteAddr)
		}
		return Identity{}, fmt.Errorf("tsauth: whois %s: %w", remoteAddr, err)
	}
	return identityFromWhoIs(resp), nil
}

// identityFromWhoIs maps the daemon's answer onto [Identity].
//
// The pointer fields are tolerated as absent rather than dereferenced blindly:
// the fleet runs mixed daemon versions, and an answer missing a part must
// degrade to a thinner identity — never to a panic inside the accept loop.
func identityFromWhoIs(resp *apitype.WhoIsResponse) Identity {
	var id Identity
	if resp == nil {
		return id
	}
	if u := resp.UserProfile; u != nil {
		id.LoginName = u.LoginName
		id.DisplayName = u.DisplayName
	}
	if n := resp.Node; n != nil {
		id.NodeName = n.Name
	}
	return id
}
