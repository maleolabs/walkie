package crypto

import (
	"crypto/sha256"
	"math/big"
	"strings"
)

// Fingerprint returns the human-comparable form of a public key: twenty
// decimal digits in five space-separated groups of four, e.g.
//
//	4821 0067 3390 1258 7744
//
// It is the out-of-band verification anchor adr:004 requires: when a peer's
// key changes, the two fingerprints are what a human compares to decide
// whether the change is a re-install or an interception.
//
// # Why digits, and why this shape
//
// The design target is a NON-TECHNICAL reader comparing two fingerprints
// aloud over a voice call (criterion 6; ts:docs-quickstart-runbook must later
// document that procedure for exactly that reader). That rules out:
//
//   - Hex: letter/digit confusion over a lossy voice channel ("B" vs "D",
//     "five" vs "nine"-class errors) is the classic read-aloud failure mode,
//     and non-technical readers have no compensation for it.
//   - Word lists: they read pleasantly but need an embedded wordlist whose
//     members must be screened against each other for acoustic and visual
//     confusability — a curated corpus is itself attack surface (a swapped
//     word pair quietly weakens every fingerprint), and cross-language teams
//     pronounce them inconsistently.
//
// Digits are what this user population already verifies by voice — bank
// transfer codes, SMS OTPs, conference dial-ins — so the procedure needs no
// explanation. Groups of four match how such codes are conventionally read
// (and how payment cards chunk their digits), keeping place-counting errors
// down. A trailing check would add length without adding a distinct error
// class, so there is none.
//
// # Strength
//
// SHA-256 of the public key, first 10 bytes mapped to decimal, truncated/
// zero-padded to 20 digits ≈ 66 bits. Forging a key whose fingerprint matches
// a victim's requires ~2^66 key generations — infeasible for any attacker
// this model recognises, and the fingerprint is a verification AID on top of
// TOFU pinning, not the sole control. Longer numbers measurably degrade
// read-aloud accuracy, which trades the human procedure (the actual control)
// for margin nobody can spend.
func Fingerprint(pub [pubKeySize]byte) string {
	const digits = 20

	sum := sha256.Sum256(pub[:])
	// 10 bytes = 80 bits > 10^20, so the decimal form always covers 20
	// digits after truncation; leading-digit bias from truncation is
	// irrelevant here (the value is not secret and not chosen by anyone).
	x := new(big.Int).SetBytes(sum[:10])
	s := x.Text(10)
	if len(s) > digits {
		s = s[:digits]
	}
	if len(s) < digits {
		s = strings.Repeat("0", digits-len(s)) + s
	}

	var groups []string
	for i := 0; i < len(s); i += 4 {
		groups = append(groups, s[i:i+4])
	}
	return strings.Join(groups, " ")
}
