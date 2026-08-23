package control

import (
	"context"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/testnet"
)

func TestZZHerd(t *testing.T) {
	cfg := herdConfig()
	fake := clock.NewFake(epoch)
	fleet := testnet.NewFleet(fake, herdSeed, 4)
	t.Cleanup(fleet.Shutdown)
	coord := newFakeCoordinator("fleet", echoBehavior("fleet"))
	const N = 4
	machs := make([]*Machine, N)
	cls := make([]*Client, N)
	colls := make([]*changeCollector, N)
	for i := range N {
		machs[i], _ = NewMachine(fake, discardLogger())
		first := true
		i := i
		dial := DialFunc(func(context.Context) (Session, error) {
			if first {
				first = false
				if err := coord.dialInto(fleet.ServerConn(i)); err != nil {
					return nil, err
				}
				return newFramedSession(fleet.ClientConn(i)), nil
			}
			link := testnet.NewLink(fake, testnet.Conditions{})
			a, b := link.Pipe()
			if err := coord.dialInto(b); err != nil {
				return nil, err
			}
			return newFramedSession(a), nil
		})
		cls[i], _ = NewClient(cfg, machs[i], dial, herdSeed, uint64(i)+1, fake, discardLogger())
		colls[i] = newChangeCollector(machs[i].Subscribe())
		cls[i].Start()
	}
	allOnline := func() bool {
		for _, m := range machs {
			if m.State() != StateOnline {
				return false
			}
		}
		return true
	}
	for i := 0; i < 100 && !allOnline(); i++ {
		fake.Advance(500 * time.Millisecond)
		yieldTo()
	}
	t.Logf("phase1 done at %v states:", fake.Now().Sub(epoch))
	for i := range N {
		t.Logf("  c%d %s ev=%d", i, machs[i].State(), len(colls[i].snapshot()))
	}
	coord.killConns()
	t.Logf("killed; live conns closed")
	dialsTotal := 0
	for i := 0; i < 240; i++ {
		fake.Advance(500 * time.Millisecond)
		yieldTo()
		if allOnline() {
			t.Logf("phase2 all online at step %d t=%v", i, fake.Now().Sub(epoch))
			break
		}
	}
	for i := range N {
		st := machs[i].State()
		t.Logf("  c%d %s ev=%d", i, st, len(colls[i].snapshot()))
		_ = dialsTotal
	}
}
