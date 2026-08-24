// Control-socket assembly (ts:control-socket): the glue that exposes the
// running client on a local Unix domain socket.
//
// Everything policy-shaped lives in internal/ctlsocket — the wire format, the
// four-command surface, the slow-subscriber policy, the adr:005 refusal. This
// file only adapts the client's seams to that surface:
//
//   - send     -> app.SendFromUI, so a scripted send gets EXACTLY the
//     semantics of a typed one: filed for display and history,
//     held in the outbox when offline, deduped by ULID;
//   - status   -> a PresenceStatusChange envelope. Status is the one piece of
//     client-authored presence text (req:device-presence); the
//     256-byte/UTF-8 bound is checked HERE so a script gets an
//     immediate local refusal instead of a protocol round trip;
//     liveness itself stays server-derived and unreachable.
//   - presence -> presenceview.Snapshot, the roster the TUI draws;
//   - subscribe-> fed by this file's two pump goroutines, one per source
//     stream (connection machine, presence view), each taking its
//     OWN subscription — every subscription in this codebase is
//     single-consumer by shape.
//
// Degradation posture: the socket is an AUXILIARY surface. Any startup
// failure — another walkie holding the path (ErrInUse), a platform without
// Unix sockets (windows), a path too long, permissions — logs one warning and
// the client runs on without scriptability. Losing text/presence because a
// control socket could not bind would be exactly backwards.
package main

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"

	"github.com/maleolabs/walkie/internal/control"
	"github.com/maleolabs/walkie/internal/ctlsocket"
	"github.com/maleolabs/walkie/internal/message"
	"github.com/maleolabs/walkie/internal/presenceview"
)

// maxStatusBytes mirrors the receiver-enforced bound on PresenceStatusChange
// (proto field doc; enforced coordinator-side by presence.MaxStatusBytes).
// Restated here rather than imported because the coordinator package is not
// something the client imports — the bound is part of the wire contract, and
// each side quotes it in place.
const maxStatusBytes = 256

// defaultSocketPath keeps the socket next to the other per-user state. Short
// by construction (~/.walkie/control.sock), well under the ~104-byte Unix
// sun_path limit for ordinary home directories; Listen fails loudly if a
// deployment's HOME pushes it past the limit, and -socket overrides.
func defaultSocketPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // caller reports it; there is nothing sensible to guess
	}
	return filepath.Join(home, ".walkie", "control.sock")
}

// ctlDeps is everything the control socket needs from the assembled client.
type ctlDeps struct {
	app      *app
	client   controlClient
	mach     *control.Machine
	presence *presenceview.View
}

// startControlSocket listens, wires the handlers, starts the accept loop and
// the two event-feed pumps. It returns the running server (Close it on
// shutdown) or an error the caller degrades on.
func startControlSocket(path string, deps ctlDeps, logger *slog.Logger) (*ctlsocket.Server, error) {
	srv, err := ctlsocket.New(ctlsocket.Handlers{
		Send: func(to, body string) error {
			// Empty "to" broadcasts; anything else names a peer. Same
			// conversation keys the TUI composes under, so scripted sends
			// land in the conversations the user already sees.
			conversation := message.BroadcastConversation
			if to != "" {
				conversation = message.ConversationKey(to)
			}
			return deps.app.SendFromUI(conversation, body)
		},
		SetStatus: func(status string) error { return sendStatus(deps.client, status) },
		Presence:  func() []ctlsocket.PresenceEntry { return rosterToEntries(deps.presence.Snapshot()) },
		ConnState: func() string { return deps.mach.State().String() },
	}, ctlsocket.Config{}, logger)
	if err != nil {
		return nil, err
	}

	l, err := ctlsocket.Listen(path)
	if err != nil {
		return nil, err
	}
	go func() { _ = srv.Serve(l) }()

	// One pump per source stream, each with its own single-consumer
	// subscription. Both end when their streams end (machine/view close at
	// shutdown); Server.Close ends delivery on the far side.
	go pumpConnEvents(srv, deps.mach.Subscribe())
	go pumpPresenceEvents(srv, deps.presence.Subscribe())

	return srv, nil
}

// sendStatus validates and transmits a custom status change. An empty status
// clears the label. Offline is a normal scripting condition, so it comes back
// as a plain error naming the fix rather than being queued anywhere: status
// is a label about RIGHT NOW, and silently sending a stale one after
// reconnect would publish something the user did not say.
func sendStatus(client controlClient, status string) error {
	if len(status) > maxStatusBytes {
		return errors.New("status exceeds 256-byte bound")
	}
	if !utf8.ValidString(status) {
		return errors.New("status is not valid UTF-8")
	}
	env := &walkiev1.Envelope{
		Payload: &walkiev1.Envelope_PresenceStatusChange{PresenceStatusChange: &walkiev1.PresenceStatusChange{
			Status: status,
		}},
	}
	if err := client.Send(env); err != nil {
		if errors.Is(err, control.ErrOffline) {
			return errors.New("offline - status not sent; retry once connected")
		}
		return err
	}
	return nil
}

// pumpConnEvents forwards connection-state changes onto the event stream.
func pumpConnEvents(srv *ctlsocket.Server, sub *control.Subscription) {
	for ch := range sub.C() {
		srv.PublishConn(ch.From.String(), ch.To.String(), ch.Reason, stampRFC3339Nano(ch.At))
	}
}

// pumpPresenceEvents forwards roster changes onto the event stream.
func pumpPresenceEvents(srv *ctlsocket.Server, sub *presenceview.Subscription) {
	for d := range sub.C() {
		srv.PublishPresence(deviceToEntry(d))
	}
}

// rosterToEntries converts a roster snapshot for the presence command and the
// subscribe-time snapshot event.
func rosterToEntries(roster []presenceview.Device) []ctlsocket.PresenceEntry {
	out := make([]ctlsocket.PresenceEntry, 0, len(roster))
	for _, d := range roster {
		out = append(out, deviceToEntry(d))
	}
	return out
}

func deviceToEntry(d presenceview.Device) ctlsocket.PresenceEntry {
	entry := ctlsocket.PresenceEntry{
		Device: d.Device,
		Online: d.Online,
		Status: d.Status,
		// LastSeen stays "" while online or never observed — the JSON omits
		// it, mirroring the wire's "unset while online".
	}
	if d.LastSeen != 0 {
		entry.LastSeen = stampRFC3339Nano(time.Unix(0, d.LastSeen))
	}
	return entry
}

// stampRFC3339Nano renders timestamps for the wire: UTC, RFC3339Nano —
// jq-friendly, sortable as text, no epoch arithmetic for script authors.
func stampRFC3339Nano(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
