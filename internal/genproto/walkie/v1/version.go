// Schema-version gate for walkie's control plane.
//
// Hand-written companion to control.pb.go: protoc-gen-go writes only *.pb.go
// files and never touches this one, so it is safe to edit. It lives beside the
// generated code rather than in a caller because the version range and the
// refusal shape ARE schema policy — the coordinator's handshake and the
// client's HelloAck comparison both need the same constants, and letting each
// define its own is how two ends end up disagreeing about what "compatible"
// means.
//
// Placement decision (acceptance criterion 4 of walkie/ts:protocol-schema-v1,
// full reasoning on Hello.protocol_version in control.proto): the explicit
// version lives in the HANDSHAKE, not on the envelope. Version skew is a
// property of a connection, not of an individual message, so an envelope field
// would pay bytes on every message for a value that cannot legitimately vary
// within one. The mismatch path is a structured ProtocolError naming both
// versions — a legible diagnostic — never a decode failure: proto3
// unknown-field tolerance means a skewed message still decodes, and this gate
// is what turns "decoded but incompatible" into words the peer can display.
package walkiev1

import "fmt"

// MinProtocolVersion and MaxProtocolVersion are the inclusive range of schema
// versions this build can interoperate with. Both are 1 today: there is exactly
// one schema version. They are two constants rather than one so that widening
// the range (e.g. supporting v1 peers from a v2 build during a fleet rollout)
// is a one-line change with no call-site churn — the check and the diagnostic
// read the range, they do not hard-code it.
const (
	MinProtocolVersion uint32 = 1
	MaxProtocolVersion uint32 = 1
)

// ProtocolVersionSupported reports whether a peer's claimed protocol_version
// falls inside the range this build interoperates with. The coordinator calls
// this on Hello before accepting a connection; a client calls it on
// HelloAck.protocol_version to detect the silent-skew case where an older
// coordinator ignored its version field entirely.
func ProtocolVersionSupported(v uint32) bool {
	return v >= MinProtocolVersion && v <= MaxProtocolVersion
}

// NewVersionUnsupportedError builds the structured refusal for a peer whose
// protocol_version is outside the supported range.
//
// The diagnostic names BOTH versions — the peer's claim and the range this
// endpoint speaks — because a version mismatch reported as only one number is
// half a bug report: the operator seeing "version 999 unsupported" still has
// to guess what to upgrade to. Detail text is written to be shown to a
// terminal user verbatim; Code carries the machine-readable reason and
// protocol_version echoes the peer's claim so structured consumers get the
// same fact without parsing prose.
func NewVersionUnsupportedError(peerVersion uint32) *ProtocolError {
	return &ProtocolError{
		Code: ProtocolErrorCode_PROTOCOL_ERROR_CODE_VERSION_UNSUPPORTED,
		Detail: fmt.Sprintf(
			"unsupported protocol version %d: this endpoint speaks versions %d through %d",
			peerVersion, MinProtocolVersion, MaxProtocolVersion,
		),
		ProtocolVersion: peerVersion,
	}
}
