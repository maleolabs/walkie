// Schema-version gate tests for walkie's control plane.
//
// Owning work item: eka get walkie/ts:protocol-schema-v1
//
// Acceptance criterion 4: the schema carries an explicit version, and a
// version mismatch produces a clear diagnostic rather than a decode failure.
// The design (full reasoning on Hello.protocol_version in control.proto):
//
//   - The version lives in the HANDSHAKE — Hello.protocol_version outbound,
//     HelloAck.protocol_version echoed back — because skew is a property of a
//     connection, not of a message; an envelope field would tax every send
//     for a value that cannot vary within one connection.
//   - The mismatch path is ProtocolError{VERSION_UNSUPPORTED} naming BOTH
//     versions, sent in band. Decode itself must succeed across versions —
//     that is what makes the diagnostic possible at all: the peer's Hello is
//     perfectly decodable, it is merely incompatible, and "incompatible" and
//     "undecodable" are different failures with different remedies.
//
// These tests pin that split. The gate helpers live in version.go beside the
// generated code; the coordinator's handshake and the client's HelloAck
// comparison are their future callers.
package walkiev1_test

import (
	"strconv"
	"strings"
	"testing"

	walkiev1 "github.com/maleolabs/walkie/internal/genproto/walkie/v1"
	"google.golang.org/protobuf/proto"
)

func TestProtocolVersionSupportedBoundaries(t *testing.T) {
	cases := []struct {
		version uint32
		want    bool
		why     string
	}{
		// 0 is what an OLD peer sends without knowing it: the field did not
		// exist when it was built, so it decodes as unset. Refusing 0 with a
		// legible diagnostic is deliberate — see the golden fixture
		// protocol_error_malformed_zero_version commentary.
		{version: walkiev1.MinProtocolVersion - 1, want: false, why: "below the supported range"},
		{version: walkiev1.MinProtocolVersion, want: true, why: "lower bound inclusive"},
		{version: walkiev1.MaxProtocolVersion, want: true, why: "upper bound inclusive"},
		{version: walkiev1.MaxProtocolVersion + 1, want: false, why: "above the supported range"},
		{version: 999, want: false, why: "far outside any planned range"},
	}
	for _, tc := range cases {
		if got := walkiev1.ProtocolVersionSupported(tc.version); got != tc.want {
			t.Errorf("ProtocolVersionSupported(%d) = %v, want %v (%s)",
				tc.version, got, tc.want, tc.why)
		}
	}
}

// TestVersionMismatchYieldsStructuredDiagnosticNotDecodeFailure walks the
// exact sequence the coordinator will run against an incompatible Hello:
// decode succeeds, the gate refuses, and the refusal is structured, names
// both sides' versions, and echoes the peer's claim as data.
func TestVersionMismatchYieldsStructuredDiagnosticNotDecodeFailure(t *testing.T) {
	const peerVersion = 999

	onWire, err := proto.Marshal(&walkiev1.Envelope{
		Payload: &walkiev1.Envelope_Hello{Hello: &walkiev1.Hello{
			ClientVersion:   "walkie v999.0.0",
			ProtocolVersion: peerVersion,
		}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Step 1 — the mismatch must NOT be a decode failure. If this unmarshal
	// ever errors, the schema has lost forward compatibility and every other
	// assertion here is moot.
	decoded := &walkiev1.Envelope{}
	if err := proto.Unmarshal(onWire, decoded); err != nil {
		t.Fatalf("version-skewed Hello failed to DECODE: %v", err)
	}

	// Step 2 — the gate, not the decoder, is where incompatibility lives.
	if walkiev1.ProtocolVersionSupported(decoded.GetHello().GetProtocolVersion()) {
		t.Fatal("gate accepted a version outside the supported range")
	}

	// Step 3 — the refusal is structured and names BOTH versions: the peer's
	// claim (also carried as the machine-readable protocol_version field) and
	// the range this endpoint speaks, so the operator knows what to move to.
	pe := walkiev1.NewVersionUnsupportedError(peerVersion)
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_VERSION_UNSUPPORTED {
		t.Errorf("code = %v, want VERSION_UNSUPPORTED", pe.GetCode())
	}
	if pe.GetProtocolVersion() != peerVersion {
		t.Errorf("protocol_version echo = %d, want %d", pe.GetProtocolVersion(), peerVersion)
	}
	if pe.GetDetail() == "" {
		t.Fatal("detail empty: nothing a terminal could show the user")
	}
	for _, want := range []string{
		strconv.FormatUint(uint64(peerVersion), 10),
		strconv.FormatUint(uint64(walkiev1.MinProtocolVersion), 10),
		strconv.FormatUint(uint64(walkiev1.MaxProtocolVersion), 10),
	} {
		if !strings.Contains(pe.GetDetail(), want) {
			t.Errorf("detail %q does not name version %s", pe.GetDetail(), want)
		}
	}
}

// TestVersionDiagnosticTravelsInBand proves the refusal is an ordinary
// message: wrapped in an Envelope payload, byte-stable across a round trip,
// and readable field-by-field by a client that has never seen this build.
// A diagnostic that only exists as a Go value — or worse, as a socket close
// code — fails the criterion even if the gate logic is right.
func TestVersionDiagnosticTravelsInBand(t *testing.T) {
	env := &walkiev1.Envelope{
		MessageId: "01J8Z9P3Q7V6M4T8J2WXYR5N6C",
		Payload:   &walkiev1.Envelope_ProtocolError{ProtocolError: walkiev1.NewVersionUnsupportedError(999)},
	}

	onWire, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoded := &walkiev1.Envelope{}
	if err := proto.Unmarshal(onWire, decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(decoded, env) {
		t.Fatalf("diagnostic did not survive the wire round trip:\n got: %+v\nwant: %+v", decoded, env)
	}

	pe := decoded.GetProtocolError()
	if pe == nil {
		t.Fatal("payload oneof did not resolve to protocol_error")
	}
	if pe.GetCode() != walkiev1.ProtocolErrorCode_PROTOCOL_ERROR_CODE_VERSION_UNSUPPORTED ||
		pe.GetProtocolVersion() != 999 ||
		!strings.Contains(pe.GetDetail(), "999") {
		t.Errorf("decoded diagnostic incomplete: %+v", pe)
	}
}
