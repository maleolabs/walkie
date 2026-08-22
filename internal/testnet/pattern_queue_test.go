package testnet

import (
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
)

// Pattern demonstration for ts:test-harness criterion 6: queue resumption.
//
// The offline queue is owned by sto:offline-queue, which does not exist yet;
// this file proves the substrate carries the weight — drain over a stream
// Link, acknowledge a position, lose the link, and resume from the acknowledged
// position rather than replaying from the start. The owning item inherits the
// production version: real frames are protobuf envelopes deduplicated by ULID
// at the receiver (delivery is at-least-once on the wire by design; exactly-
// once is never attempted at the transport layer), and acks ride the control
// plane. Here one byte per frame and an in-process channel stand in for both,
// because the pattern under proof is position bookkeeping, not framing.
func TestPatternQueueResumesFromAcknowledgedPosition(t *testing.T) {
	const (
		totalFrames = 10
		drainedTo   = 5 // frames 0..4 delivered and acknowledged before the cut
		frameDelay  = 2 * time.Millisecond
	)

	fake := clock.NewFake(epoch)
	link := NewLink(fake, Conditions{Latency: frameDelay})
	client, server := link.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	// Acknowledgements flow receiver → sender. In production they ride the
	// control plane; here an in-process channel keeps the demonstration
	// focused on position bookkeeping.
	acks := make(chan int, totalFrames)

	// Receiver: append every frame that arrives, acknowledge its position.
	// Runs until the client side closes, then reports what it saw.
	received := make(chan []int, 1)
	go func() {
		var got []int
		buf := make([]byte, 1)
		for {
			if _, err := server.Read(buf); err != nil {
				received <- got
				return
			}
			got = append(got, int(buf[0]))
			acks <- int(buf[0])
		}
	}()

	// send writes one frame through the link's latency, driving the injected
	// clock while the writer parks on it, and waits for the receiver's ack.
	send := func(pos int) error {
		errc := make(chan error, 1)
		go func() {
			_, err := client.Write([]byte{byte(pos)})
			errc <- err
		}()
		for fake.Waiters() == 0 {
			runtime.Gosched()
		}
		fake.Advance(frameDelay)
		err := <-errc
		if err == nil {
			if got := <-acks; got != pos {
				t.Fatalf("receiver acked position %d, want %d", got, pos)
			}
		}
		return err
	}

	// Phase 1 — drain: frames 0..4 delivered and acknowledged.
	for pos := range drainedTo {
		if err := send(pos); err != nil {
			t.Fatalf("frame %d during drain: %v", pos, err)
		}
	}

	// Phase 2 — partition: the next send is refused outright (the link checks
	// before parking on latency), so nothing after frame 4 ever reached the
	// receiver.
	link.Partition()
	if _, err := client.Write([]byte{byte(drainedTo)}); !errors.Is(err, ErrPartitioned) {
		t.Fatalf("send across partition: got %v, want ErrPartitioned", err)
	}

	// Phase 3 — resume: heal and continue from the acknowledged position. The
	// sender's cursor never rewinds: frames 0..4 are not resent, which is the
	// whole point of resumption-by-position.
	link.Heal()
	for pos := drainedTo; pos < totalFrames; pos++ {
		if err := send(pos); err != nil {
			t.Fatalf("frame %d during resume: %v", pos, err)
		}
	}

	// Close so the receiver can report; every position must have arrived
	// exactly once — complete, and without a full replay on resume.
	_ = client.Close()
	got := <-received
	want := make([]int, totalFrames)
	for i := range want {
		want[i] = i
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("receiver saw %v, want each of %v exactly once — resumption replayed or lost frames", got, want)
	}
}
