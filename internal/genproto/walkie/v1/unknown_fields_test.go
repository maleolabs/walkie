// Unknown-field preservation tests for walkie's control-plane schema.
//
// Owning work item: eka get walkie/ts:protocol-schema-v1
//
// adr:003-wire-protocol requires that a message carrying fields this build
// does not know DECODES SUCCESSFULLY and keeps those bytes: the fleet runs
// mixed client versions and will not upgrade in lockstep, so a newer peer's
// extra field must survive an older endpoint untouched — dropped unknown
// fields are how a relay silently corrupts a forward-compatible protocol.
//
// The unknown fields here are assembled BY HAND, not generated, on purpose:
// hand-built tag/length framing doubles as documentation of the wire format
// and proves the runtime handles arbitrary well-formed input, not just input
// shaped like its own output. Field numbers 99 and 100 are far beyond any
// current field in walkie.v1; if the schema ever grows that far, these tests
// must move to genuinely-unassigned numbers.
package walkiev1_test

import (
	"encoding/binary"
	"testing"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// varuint encodes v as a protobuf base-128 varint (the encoding/binary
// representation is identical).
func varuint(v uint) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(buf, uint64(v))
	return buf[:n]
}

// Wire fragments for two unknown fields, with the arithmetic spelled out:
//
//	field 99, wire type 0 (varint): tag = 99<<3|0 = 792 → varint 98 06;
//	                                value 1 → 01.                → "9806 01"
//	field 100, wire type 2 (bytes): tag = 100<<3|2 = 802 → varint a2 06;
//	                                length 6, payload "future".  → "a206 06 667574757265"
var (
	unknownVarint   = []byte{0x98, 0x06, 0x01}
	unknownBytes    = []byte{0xa2, 0x06, 0x06, 'f', 'u', 't', 'u', 'r', 'e'}
	unknownAppendix = append(append([]byte{}, unknownVarint...), unknownBytes...)
)

// TestEnvelopeUnknownFieldsPreservedOnRoundTrip is acceptance criterion 3 at
// the envelope level: take a valid encoding of a known message, append fields
// a vNext sender might add, and require that this build (acting as the older
// endpoint) decodes it without error, reads every known field intact, and
// re-emits byte-identical output — unknown fields included, order preserved.
func TestEnvelopeUnknownFieldsPreservedOnRoundTrip(t *testing.T) {
	base := &walkiev1.Envelope{
		MessageId:  "01J8Z9P3Q7V6M4T8J2WXYR5N6C",
		SentAt:     timestamppb.New(timeDate(2026, 8, 20, 0, 0, 0)),
		ReceivedAt: timestamppb.New(timeDate(2026, 8, 20, 0, 1, 30)),
		Payload: &walkiev1.Envelope_DirectMessage{DirectMessage: &walkiev1.DirectMessage{
			Recipient: "nas-01",
			Body:      "wake up, deploy time",
		}},
	}
	known, err := proto.Marshal(base)
	if err != nil {
		t.Fatalf("Marshal baseline: %v", err)
	}

	// A future sender appends its new fields after the known ones — exactly
	// where a conforming encoder emits them, so re-encode equality below also
	// asserts field ORDER survived.
	futuristic := append(append([]byte{}, known...), unknownAppendix...)

	decoded := &walkiev1.Envelope{}
	if err := proto.Unmarshal(futuristic, decoded); err != nil {
		t.Fatalf("decode refused a message carrying unknown fields: %v", err)
	}

	// Every known field must arrive exactly as sent — tolerance for unknown
	// fields must never blur into loss of known ones.
	if decoded.GetMessageId() != base.GetMessageId() {
		t.Errorf("message_id = %q, want %q", decoded.GetMessageId(), base.GetMessageId())
	}
	if got := decoded.GetDirectMessage().GetRecipient(); got != "nas-01" {
		t.Errorf("direct_message.recipient = %q, want %q", got, "nas-01")
	}
	if got := decoded.GetDirectMessage().GetBody(); got != base.GetDirectMessage().GetBody() {
		t.Errorf("direct_message.body = %q, want %q", got, base.GetDirectMessage().GetBody())
	}
	if !decoded.GetSentAt().AsTime().Equal(base.GetSentAt().AsTime()) {
		t.Errorf("sent_at = %v, want %v", decoded.GetSentAt().AsTime(), base.GetSentAt().AsTime())
	}
	if !decoded.GetReceivedAt().AsTime().Equal(base.GetReceivedAt().AsTime()) {
		t.Errorf("received_at = %v, want %v", decoded.GetReceivedAt().AsTime(), base.GetReceivedAt().AsTime())
	}

	reencoded, err := proto.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-Marshal: %v", err)
	}
	if !equalBytes(reencoded, futuristic) {
		t.Fatalf("re-encode dropped or altered unknown fields\n got: %x\nwant: %x", reencoded, futuristic)
	}
}

// TestNestedPayloadUnknownFieldsPreservedOnRoundTrip applies the same
// guarantee one level down, inside a oneof payload. The mixed-fleet scenario
// is not envelope-only: a vNext Hello gains a field, and today's coordinator
// must both read the Hello it does understand and pass the rest through.
//
// The whole envelope is assembled by hand so the test does not depend on the
// encoder whose behaviour it checks: field 10 (hello), wire type 2 → tag
// 10<<3|2 = 82 = 0x52; then length; then the inner Hello bytes followed by
// the same unknown appendix as the envelope-level test.
func TestNestedPayloadUnknownFieldsPreservedOnRoundTrip(t *testing.T) {
	helloKnown, err := proto.Marshal(&walkiev1.Hello{
		ClientVersion:   "walkie v0.1.0",
		ProtocolVersion: 1,
	})
	if err != nil {
		t.Fatalf("Marshal inner Hello: %v", err)
	}
	helloFuturistic := append(append([]byte{}, helloKnown...), unknownAppendix...)

	envelope := append([]byte{0x52}, varuint(uint(len(helloFuturistic)))...)
	envelope = append(envelope, helloFuturistic...)

	decoded := &walkiev1.Envelope{}
	if err := proto.Unmarshal(envelope, decoded); err != nil {
		t.Fatalf("decode refused a Hello carrying unknown fields: %v", err)
	}
	hello := decoded.GetHello()
	if hello == nil {
		t.Fatal("payload oneof did not resolve to hello")
	}
	if hello.GetClientVersion() != "walkie v0.1.0" || hello.GetProtocolVersion() != 1 {
		t.Errorf("known Hello fields damaged by unknown-field decode: %+v", hello)
	}

	// Preservation at the nested level itself: decode the inner bytes as a
	// bare Hello and require identity re-encode.
	roundTripped := &walkiev1.Hello{}
	if err := proto.Unmarshal(helloFuturistic, roundTripped); err != nil {
		t.Fatalf("Unmarshal bare Hello: %v", err)
	}
	reinner, err := proto.Marshal(roundTripped)
	if err != nil {
		t.Fatalf("re-Marshal bare Hello: %v", err)
	}
	if !equalBytes(reinner, helloFuturistic) {
		t.Fatalf("nested re-encode dropped or altered unknown fields\n got: %x\nwant: %x", reinner, helloFuturistic)
	}

	// And through the envelope: the coordinator relays what it cannot read.
	reencoded, err := proto.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-Marshal envelope: %v", err)
	}
	if !equalBytes(reencoded, envelope) {
		t.Fatalf("envelope re-encode dropped or altered nested unknown fields\n got: %x\nwant: %x", reencoded, envelope)
	}
}
