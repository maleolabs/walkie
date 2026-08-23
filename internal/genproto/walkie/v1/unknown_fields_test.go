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

// TestSealedDeliveryFrameIsOldReceiverCompatible pins the additive-evolution
// guarantee for the one field ts:queue-sealed-box added to the oneof
// (Envelope.sealed_delivery = 25): a receiver built BEFORE that field existed
// must decode a frame carrying it without error, read every known field
// intact, and re-emit byte-identical output — adr:003's mixed-fleet rule is
// what lets a sealed queue drain reach a fleet where some clients have not
// upgraded yet.
//
// The frame is assembled BY HAND in the old receiver's terms — field 25 is
// just an unknown bytes-field to it — so the test proves the WIRE SHAPE is
// tolerant, not that this build's own encoder round-trips:
//
//	field 25, wire type 2: tag = 25<<3|2 = 202 → varint ca 01;
//	length 75; payload = SealedDelivery{ciphertext: field 1, len 73}.
func TestSealedDeliveryFrameIsOldReceiverCompatible(t *testing.T) {
	// A real drain frame carries ONLY field 25 as its payload — the text
	// rides encrypted inside the box — so the old receiver's view of it is
	// "envelope metadata I know, payload I do not". Build exactly that.
	base := &walkiev1.Envelope{
		MessageId: "01J8Z9P3Q7V6M4T8J2WXYR5N6C",
		SentAt:    timestamppb.New(timeDate(2026, 8, 20, 0, 0, 0)),
	}
	known, err := proto.Marshal(base)
	if err != nil {
		t.Fatalf("Marshal baseline: %v", err)
	}

	// The hand-built sealed-delivery record, exactly as a NEW coordinator
	// emits it during a queue drain and an OLD client must absorb as
	// unknown bytes.
	box := make([]byte, 73)
	for i := range box {
		box[i] = byte(0xE0 + i%16)
	}
	inner := append([]byte{0x0a, 73}, box...)          // SealedDelivery.ciphertext
	record := append([]byte{0xca, 0x01, 75}, inner...) // Envelope field 25, len 75
	futuristic := append(append([]byte{}, known...), record...)

	decoded := &walkiev1.Envelope{}
	if err := proto.Unmarshal(futuristic, decoded); err != nil {
		t.Fatalf("decode refused a frame carrying sealed_delivery: %v", err)
	}

	// The fields the old receiver DOES know survive untouched — tolerance
	// for the new field must never blur into loss of the old ones.
	if decoded.GetMessageId() != base.GetMessageId() {
		t.Errorf("message_id = %q, want %q", decoded.GetMessageId(), base.GetMessageId())
	}
	if !decoded.GetSentAt().AsTime().Equal(base.GetSentAt().AsTime()) {
		t.Errorf("sent_at = %v, want %v", decoded.GetSentAt().AsTime(), base.GetSentAt().AsTime())
	}

	// A NEW receiver parses the same hand-built bytes into the typed payload,
	// byte-for-byte the box that went in — the two generations read the same
	// wire and disagree about nothing. (The old receiver, per the two
	// unknown-field tests above, keeps the bytes and moves on.)
	if sd := decoded.GetSealedDelivery(); sd == nil {
		t.Fatalf("this build resolved field 25 to %T, want SealedDelivery", decoded.GetPayload())
	} else if !equalBytes(sd.GetCiphertext(), box) {
		t.Errorf("sealed_delivery.ciphertext damaged in decode")
	}

	reencoded, err := proto.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-Marshal: %v", err)
	}
	if !equalBytes(reencoded, futuristic) {
		t.Fatalf("re-encode dropped or altered the sealed_delivery record\n got: %x\nwant: %x", reencoded, futuristic)
	}
}
