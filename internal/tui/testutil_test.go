package tui

import (
	"bytes"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"github.com/maleolabs/walkie/internal/message"
)

// Shared builders for the synthetic-message tests. Everything here exists so
// a test states the scenario it drives instead of protobuf plumbing.

func newStdLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// presenceOnline builds one roster fact saying device is online with a custom
// status label.
func presenceOnline(device, status string) *walkiev1.PresenceUpdate {
	return &walkiev1.PresenceUpdate{
		Device: device,
		State:  walkiev1.PresenceState_PRESENCE_STATE_ONLINE,
		Status: status,
	}
}

// presenceOffline builds the offline verdict with the coordinator's last-seen
// stamp.
func presenceOffline(device string, lastSeen time.Time) *walkiev1.PresenceUpdate {
	return &walkiev1.PresenceUpdate{
		Device:   device,
		State:    walkiev1.PresenceState_PRESENCE_STATE_OFFLINE,
		LastSeen: timestamppb.New(lastSeen),
	}
}

// directEnvelope builds an inbound direct message as the coordinator would
// deliver it: sender attributed server-side, both stamps present.
func directEnvelope(sender, recipient, body string, sentAt, receivedAt time.Time) *walkiev1.Envelope {
	return &walkiev1.Envelope{
		MessageId:  message.NewID(sentAt),
		SentAt:     timestamppb.New(sentAt),
		ReceivedAt: timestamppb.New(receivedAt),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Sender:    sender,
			Recipient: recipient,
			Body:      body,
		}},
	}
}
