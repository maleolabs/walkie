package crypto

import (
	"strings"
	"testing"
)

func TestFingerprintIsStableForSameKey(t *testing.T) {
	pub := fixedIdentity(0x42).PublicKey()
	a := Fingerprint(pub)
	b := Fingerprint(pub)
	if a != b {
		t.Fatalf("same key, different fingerprints: %q vs %q", a, b)
	}
}

func TestFingerprintFormat(t *testing.T) {
	// The format is a user-facing contract (read aloud over a call): twenty
	// decimal digits in five space-separated groups of four. Pin it.
	for seed := byte(1); seed < 16; seed++ {
		fp := Fingerprint(fixedIdentity(seed).PublicKey())
		groups := strings.Split(fp, " ")
		if len(groups) != 5 {
			t.Fatalf("seed %d: %d groups in %q, want 5", seed, len(groups), fp)
		}
		for _, g := range groups {
			if len(g) != 4 {
				t.Fatalf("seed %d: group %q in %q is not 4 digits", seed, g, fp)
			}
			for _, r := range g {
				if r < '0' || r > '9' {
					t.Fatalf("seed %d: non-digit %q in fingerprint %q", seed, r, fp)
				}
			}
		}
	}
}

func TestFingerprintDistinctKeysDiverge(t *testing.T) {
	seen := map[string]byte{}
	for seed := byte(0); seed < 32; seed++ {
		fp := Fingerprint(fixedIdentity(seed).PublicKey())
		if prev, dup := seen[fp]; dup {
			t.Fatalf("seeds %d and %d collide on %q", prev, seed, fp)
		}
		seen[fp] = seed
	}
}

// Golden vectors, computed by an INDEPENDENT implementation of the documented
// spec (SHA-256 → first 10 bytes → big-endian integer → decimal → truncate/
// zero-pad to 20 digits → groups of four), not by this package. They pin the
// format against accidental change: fingerprints must stay stable forever,
// because users compare them across versions and over time.
func TestFingerprintGoldenVectors(t *testing.T) {
	var v1 [pubKeySize]byte
	for i := range v1 {
		v1[i] = byte(i)
	}
	var v2 [pubKeySize]byte
	for i := range v2 {
		v2[i] = byte(255 - i)
	}
	for _, tc := range []struct {
		name string
		pub  [pubKeySize]byte
		want string
	}{
		{"ascending bytes", v1, "4677 6887 2937 2977 8267"},
		{"descending bytes", v2, "1152 1375 4105 0405 1817"},
	} {
		if got := Fingerprint(tc.pub); got != tc.want {
			t.Errorf("%s: Fingerprint = %q, want %q", tc.name, got, tc.want)
		}
	}
}
