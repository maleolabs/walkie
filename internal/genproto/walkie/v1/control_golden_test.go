// Golden wire-format tests for walkie's control-plane schema.
//
// Owning work item: eka get walkie/ts:protocol-schema-v1
//
// These tests pin the EXACT bytes every Envelope shape must encode to. The
// bytes are not decoration: adr:003-wire-protocol commits the fleet to a wire
// format that mixed client versions share, so the only acceptable change to
// these fixtures is an intentional schema revision — never a silent shift from
// a generator or runtime bump. When one of these tests fails, something moved
// on the wire, and the diff must be reviewed as a protocol change.
//
// Where the fixtures came from: each hex string is the encoding produced by
// the pinned toolchain (protoc-gen-go v1.36.12 with google.golang.org/protobuf
// v1.36.12) for the message built alongside it, then hand-checked against the
// raw protobuf wire format for representative cases (tag/length framing,
// varint timestamp values, zero-value omission). The protobuf wire format is
// normative, not implementation-specific: any conforming encoder produces
// these same bytes, so re-deriving a fixture after an INTENTIONAL schema
// change needs no special tooling — encode once with any conforming runtime
// and paste the result.
//
// Coverage contract (acceptance criterion 2): the table below has one case per
// oneof payload in Envelope — all fifteen — plus variants that exercise every
// envelope-level field (message_id, sent_at, position, received_at), both
// PresenceState values, all three ProtocolErrorCode values, the nested
// repeated PublicKeyEntry, bytes fields, an empty payload message, a wholly
// empty envelope, and the coordinator-filled sender field on both text
// payloads (sto:text-messaging's additive attribution fields). PublicKeyEntry
// and the enums have no standalone wire
// presence; they are pinned through the payloads that carry them. If you add a
// payload to the schema, add its case here in the same commit — buf breaking
// will already have forced the question.
package walkiev1_test

import (
	"encoding/hex"
	"sort"
	"testing"
	"time"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fixed clocks, chosen once so the fixtures never move. sent_at is the
// sender's claim; received_at is the coordinator's, 90 seconds later — the
// skew the two-timestamp rule (req:text-messaging) exists to keep legible.
var (
	goldenSentAt     = timeUTC(2026, 8, 20, 0, 0, 0)  // 2026-08-20T00:00:00Z
	goldenReceivedAt = timeUTC(2026, 8, 20, 0, 1, 30) // +90s
)

func timeUTC(y, mo, d, h, mi, s int) *timestamppb.Timestamp {
	return timestamppb.New(timeDate(y, mo, d, h, mi, s))
}

func timeDate(y, mo, d, h, mi, s int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, s, 0, time.UTC)
}

func goldenEnvelope(payload func(*walkiev1.Envelope)) *walkiev1.Envelope {
	e := &walkiev1.Envelope{}
	payload(e)
	return e
}

// deterministic 32-byte patterns standing in for X25519 keys and a SHA-256
// digest; the values carry no meaning, the LENGTH and round-trip fidelity do.
func goldenKey32(seed byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

const (
	goldenMessageID    = "01J8Z9P3Q7V6M4T8J2WXYR5N6C" // ULID-shaped; only its bytes matter here
	goldenAttachmentID = "01J8Z9P4QKV6M4T8J2WXYR5N6D"
	goldenClientVer    = "walkie v0.1.0"
)

// goldenEnvelopes maps a case name to the message under test and the exact
// bytes it must encode to. One entry per oneof payload, plus the variants
// listed in the coverage contract above.
var goldenEnvelopes = map[string]struct {
	build   func() *walkiev1.Envelope
	wantHex string
}{
	"hello_new_device": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_Hello{Hello: &walkiev1.Hello{
				ClientVersion: goldenClientVer, LastAckedPosition: 0, ProtocolVersion: 1,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d40652110a0d77616c6b69652076302e312e301801"},

	"hello_resume_queue": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_Hello{Hello: &walkiev1.Hello{
				ClientVersion: goldenClientVer, LastAckedPosition: 42, ProtocolVersion: 1,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d40652130a0d77616c6b69652076302e312e30102a1801"},

	"hello_ack_pending": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			// Coordinator-originated: SentAt carries the coordinator's clock
			// and ReceivedAt stays unset — there is no second clock involved.
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_HelloAck{HelloAck: &walkiev1.HelloAck{
				Device: "macbook-pro", ReceivedAt: goldenReceivedAt, PendingCount: 7, ProtocolVersion: 1,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d4065a190a0b6d6163626f6f6b2d70726f120608da8799d40618072001"},

	"heartbeat_live": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_Heartbeat{Heartbeat: &walkiev1.Heartbeat{}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4066200"},

	"presence_online": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_PresenceUpdate{PresenceUpdate: &walkiev1.PresenceUpdate{
				Device: "build-server", State: walkiev1.PresenceState_PRESENCE_STATE_ONLINE, Status: "deploying",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d4066a1b0a0c6275696c642d73657276657210011a096465706c6f79696e67"},

	// Offline variant: last_seen set (req:device-presence requires it for
	// offline devices), status empty (cleared), state OFFLINE.
	"presence_offline_last_seen": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_PresenceUpdate{PresenceUpdate: &walkiev1.PresenceUpdate{
				Device: "build-server", State: walkiev1.PresenceState_PRESENCE_STATE_OFFLINE, LastSeen: goldenReceivedAt,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d4066a180a0c6275696c642d7365727665721002220608da8799d406"},

	// Multibyte UTF-8 in a user-authored status ("brb — rebuilding"): pins
	// that length prefixes count BYTES, not runes.
	"status_change_multibyte": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_PresenceStatusChange{PresenceStatusChange: &walkiev1.PresenceStatusChange{
				Status: "brb — rebuilding",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d40672140a1262726220e280942072656275696c64696e67"},

	"dm_live": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Recipient: "nas-01", Body: "wake up, deploy time",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4067a1e0a066e61732d3031121477616b652075702c206465706c6f792074696d65"},

	// The queue-replay shape: EVERY envelope-level field set at once —
	// position stamped by the coordinator, received_at preserving FIRST
	// ingress, sent_at still the original sender's claim.
	"dm_queue_replay_all_envelope_fields": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Position = 43
			e.ReceivedAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Recipient: "nas-01", Body: "queued while you were away",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d406182b220608da8799d4067a240a066e61732d3031121a717565756564207768696c6520796f7520776572652061776179"},

	// The same DM as dm_live WITH the coordinator-filled sender field
	// (sto:text-messaging's server-authoritative attribution, added
	// additively as DirectMessage field 3). Pins that the new field encodes
	// as tag 0x1a INSIDE the payload and that its presence does not move any
	// other byte of the envelope — the additive-evolution guarantee old
	// receivers lean on.
	"dm_sender_attributed": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
				Recipient: "nas-01", Body: "wake up, deploy time", Sender: "laptop.tail-scale.ts.net.",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4067a390a066e61732d3031121477616b652075702c206465706c6f792074696d651a196c6170746f702e7461696c2d7363616c652e74732e6e65742e"},

	"broadcast_maintenance": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: &walkiev1.BroadcastMessage{
				Body: "maintenance window at 02:00 UTC",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4068201210a1f6d61696e74656e616e63652077696e646f772061742030323a303020555443"},

	// The same broadcast WITH the coordinator-filled sender field (added
	// additively as BroadcastMessage field 2). Same pin as
	// dm_sender_attributed: new field inside the payload, zero drift
	// elsewhere.
	"broadcast_sender_attributed": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_BroadcastMessage{BroadcastMessage: &walkiev1.BroadcastMessage{
				Body: "maintenance window at 02:00 UTC", Sender: "laptop.tail-scale.ts.net.",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d40682013c0a1f6d61696e74656e616e63652077696e646f772061742030323a30302055544312196c6170746f702e7461696c2d7363616c652e74732e6e65742e"},

	"attachment_offer_voice_note": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_AttachmentOffer{AttachmentOffer: &walkiev1.AttachmentOffer{
				AttachmentId: goldenAttachmentID, Filename: "2026-08-20T09-15-note.ogg",
				SizeBytes: 184320, MimeType: "audio/ogg", Sha256: goldenKey32(0xA0), ChunkSize: 16384,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4068a016c0a1a30314a385a395034514b56364d3454384a3257585952354e36441219323032362d30382d32305430392d31352d6e6f74652e6f67671880a00b2209617564696f2f6f67672a20a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf30808001"},

	// Chunk data starts with the OggS capture pattern — realistic bytes, and
	// binary content that must survive byte-for-byte.
	"attachment_chunk_ogg_header": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_AttachmentChunk{AttachmentChunk: &walkiev1.AttachmentChunk{
				AttachmentId: goldenAttachmentID, Index: 3,
				Data: []byte{'O', 'g', 'g', 'S', 0x00, 0x02, 0x00, 0x00},
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d4069201280a1a30314a385a395034514b56364d3454384a3257585952354e364410031a084f67675300020000"},

	"attachment_ack_progress": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_AttachmentAck{AttachmentAck: &walkiev1.AttachmentAck{
				AttachmentId: goldenAttachmentID, AcknowledgedBytes: 147456,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d4069a01200a1a30314a385a395034514b56364d3454384a3257585952354e364410808009"},

	"queue_ack_high_water": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_QueueAck{QueueAck: &walkiev1.QueueAck{AcknowledgedPosition: 42}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d406a20102082a"},

	"queue_refused_size_cap": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_QueueRefused{QueueRefused: &walkiev1.QueueRefused{
				Reason: walkiev1.QueueRefusalReason_QUEUE_REFUSAL_REASON_SIZE_CAP,
				Detail: "recipient queue at cap: 1000 messages / 64 MiB",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d406aa01320801122e726563697069656e74207175657565206174206361703a2031303030206d65737361676573202f203634204d6942"},

	"key_announce_x25519": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenSentAt
			e.Payload = &walkiev1.Envelope_PublicKeyAnnounce{PublicKeyAnnounce: &walkiev1.PublicKeyAnnounce{
				PublicKey: goldenKey32(0xB0),
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608808799d406b201220a20b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"},

	// Full directory snapshot: nested repeated PublicKeyEntry, two entries,
	// distinct keys — pins the replace-whole-state shape peers cache from.
	"key_directory_snapshot": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_PublicKeyDirectory{PublicKeyDirectory: &walkiev1.PublicKeyDirectory{
				Entries: []*walkiev1.PublicKeyEntry{
					{Device: "nas-01", PublicKey: goldenKey32(0xC0)},
					{Device: "thinkpad", PublicKey: goldenKey32(0xD0)},
				},
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d406ba015a0a2a0a066e61732d30311220c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf0a2c0a087468696e6b7061641220d0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeef"},

	// The criterion-4 diagnostic itself, pinned as wire bytes: a version
	// refusal is an ordinary sendable message, not a transport event.
	"protocol_error_version_unsupported": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_ProtocolError{ProtocolError: &walkiev1.ProtocolError{
				Code:            walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_VERSION_UNSUPPORTED,
				Detail:          "unsupported protocol version 999: this endpoint speaks versions 1 through 1",
				ProtocolVersion: 999,
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d406c201520801124b756e737570706f727465642070726f746f636f6c2076657273696f6e203939393a207468697320656e64706f696e7420737065616b732076657273696f6e732031207468726f756768203118e707"},

	// Zero-valued protocol_version is ABSENT from the wire (proto3 scalar
	// omission) — pinned deliberately, because the diagnostic for an old peer
	// that never sent a version relies on that absence meaning zero.
	"protocol_error_malformed_zero_version": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_ProtocolError{ProtocolError: &walkiev1.ProtocolError{
				Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_MALFORMED,
				Detail: "envelope carried no payload",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d406c2011f0802121b656e76656c6f70652063617272696564206e6f207061796c6f6164"},

	"protocol_error_device_unknown": {build: func() *walkiev1.Envelope {
		return goldenEnvelope(func(e *walkiev1.Envelope) {
			e.MessageId = goldenMessageID
			e.SentAt = goldenReceivedAt
			e.Payload = &walkiev1.Envelope_ProtocolError{ProtocolError: &walkiev1.ProtocolError{
				Code:   walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_DEVICE_UNKNOWN,
				Detail: "no device 'nas-0l' on this tailnet",
			}}
		})
	}, wantHex: "0a1a30314a385a395033513756364d3454384a3257585952354e3643120608da8799d406c20126080312226e6f2064657669636520276e61732d306c27206f6e2074686973207461696c6e6574"},

	// An empty envelope decodes to an empty envelope. On the wire this shape
	// is MALFORMED at the protocol level (no payload where one is required —
	// see ProtocolErrorCode); the schema level must still tolerate it without
	// error, which is exactly the decode/gate split criterion 4 rests on.
	"empty_envelope_decodable": {build: func() *walkiev1.Envelope {
		return &walkiev1.Envelope{}
	}, wantHex: ""},
}

// TestGoldenEnvelopeWireFormat pins, for every case above:
//
//  1. ENCODE: marshalling the message yields exactly the fixture bytes.
//  2. DECODE: unmarshalling the fixture bytes yields a message equal to the
//     constructed one (proto.Equal compares known fields AND unknown fields).
//  3. STABILITY: re-encoding the decoded message reproduces the fixture bytes,
//     so decode→encode is identity, not merely lossless-ish.
func TestGoldenEnvelopeWireFormat(t *testing.T) {
	names := make([]string, 0, len(goldenEnvelopes))
	for name := range goldenEnvelopes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		tc := goldenEnvelopes[name]
		t.Run(name, func(t *testing.T) {
			want, err := hex.DecodeString(tc.wantHex)
			if err != nil {
				t.Fatalf("fixture %q is not valid hex: %v", name, err)
			}

			msg := tc.build()
			got, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if !equalBytes(got, want) {
				t.Fatalf("encode mismatch\n got: %x\nwant: %x", got, want)
			}

			decoded := &walkiev1.Envelope{}
			if err := proto.Unmarshal(want, decoded); err != nil {
				t.Fatalf("Unmarshal of committed fixture failed: %v", err)
			}
			if !proto.Equal(decoded, msg) {
				t.Fatalf("round-trip fidelity lost:\n got: %+v\nwant: %+v", decoded, msg)
			}

			reencoded, err := proto.Marshal(decoded)
			if err != nil {
				t.Fatalf("re-Marshal of decoded message: %v", err)
			}
			if !equalBytes(reencoded, want) {
				t.Fatalf("re-encode of decoded message differs from fixture\n got: %x\nwant: %x", reencoded, want)
			}
		})
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
