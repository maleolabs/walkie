package testnet

import (
	"runtime"
	"testing"

	"github.com/maleolabs/walkie/internal/clock"
)

// maxParkSpins bounds how long a test waits for a goroutine to park on the
// fake clock. Every iteration yields the processor, so a healthy park
// registers within a handful of spins; a million without means the park
// wiring is broken or the sender never started. Unbounded, that failure mode
// is a silent hang; bounded, it is a loud one.
const maxParkSpins = 1 << 20

// waitForPark spins until fake reports at least one waiter, failing t rather
// than hanging if nothing ever parks.
func waitForPark(t testing.TB, fake *clock.Fake) {
	t.Helper()
	for range maxParkSpins {
		if fake.Waiters() > 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("no goroutine parked on the fake clock within the spin budget — park wiring is broken")
}
