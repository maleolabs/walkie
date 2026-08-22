package message

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// base anchors every timestamp below so SentAt/ReceivedAt assertions are
// exact — no wall clock anywhere in this package's tests.
var base = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// dm builds a direct message with distinct, offset timestamps so the two
// halves of the pair can be told apart after a round trip.
func dm(id, sender, recipient, body string, offset time.Duration) Message {
	return Message{
		ID:         id,
		Sender:     sender,
		Recipient:  recipient,
		Body:       body,
		SentAt:     base.Add(offset),
		ReceivedAt: base.Add(offset + time.Minute),
	}
}

// TestAppendDisplaysDuplicateOnlyOnce is criterion 3 at the layer that owns
// it: the display decision. The same ULID appended twice yields one displayed
// message and one conversation entry.
func TestAppendDisplaysDuplicateOnlyOnce(t *testing.T) {
	log := NewLog("me", 0, 0)
	m := dm("id-1", "alice", "me", "hello", 0)

	if !log.Append(m) {
		t.Fatal("first Append reported duplicate; message would never display")
	}
	if log.Append(m) {
		t.Fatal("second Append of the same ULID reported new — criterion 3 broken")
	}
	if got := log.Conversation(ConversationKey("alice")); len(got) != 1 {
		t.Fatalf("conversation holds %d messages, want exactly 1", len(got))
	}
}

func TestSameBodyDifferentIDsBothDisplay(t *testing.T) {
	// Identity-based dedup must not swallow genuinely distinct messages that
	// happen to share text.
	log := NewLog("me", 0, 0)
	first := dm("id-1", "alice", "me", "ok", 0)
	second := dm("id-2", "alice", "me", "ok", time.Second)

	if !log.Append(first) || !log.Append(second) {
		t.Fatal("distinct ULIDs suppressed; dedup leaked from identity to content")
	}
	if got := log.Conversation(ConversationKey("alice")); len(got) != 2 {
		t.Fatalf("conversation holds %d messages, want 2", len(got))
	}
}

// TestTimestampsSurviveToDisplay pins the criterion-4 seam: BOTH timestamps
// reach whatever renders the message, unmerged and unrewritten. A view that
// kept only one would hide skew instead of making it legible.
func TestTimestampsSurviveToDisplay(t *testing.T) {
	log := NewLog("me", 0, 0)
	in := dm("id-1", "alice", "me", "when was this really sent?", 3*time.Second)

	log.Append(in)
	got := log.Conversation(ConversationKey("alice"))[0]

	if !got.SentAt.Equal(in.SentAt) {
		t.Errorf("SentAt = %v, want %v (sender half must survive)", got.SentAt, in.SentAt)
	}
	if !got.ReceivedAt.Equal(in.ReceivedAt) {
		t.Errorf("ReceivedAt = %v, want %v (coordinator half must survive)", got.ReceivedAt, in.ReceivedAt)
	}
}

// TestPerConversationOrderingUnderInterleavedTraffic: traffic for several
// conversations interleaves at append time; each conversation's internal
// order is untouched. Deliberately NO assertion compares positions across
// conversations — cross-conversation order does not exist (package comment).
func TestPerConversationOrderingUnderInterleavedTraffic(t *testing.T) {
	log := NewLog("me", 0, 0)

	// Round-robin across three conversations, three rounds each.
	for round := 0; round < 3; round++ {
		for _, peer := range []string{"alice", "bob", "carol"} {
			log.Append(dm(
				NewID(base.Add(time.Duration(round)*time.Minute)),
				peer, "me",
				peer+" says "+string(rune('0'+round)),
				time.Duration(round)*time.Minute,
			))
		}
	}

	for _, peer := range []string{"alice", "bob", "carol"} {
		got := log.Conversation(ConversationKey(peer))
		if len(got) != 3 {
			t.Fatalf("%s: %d messages, want 3", peer, len(got))
		}
		for i, m := range got {
			want := peer + " says " + string(rune('0'+i))
			if m.Body != want {
				t.Fatalf("%s: position %d = %q, want %q (per-conversation order broken)", peer, i, m.Body, want)
			}
		}
	}
}

// TestPerConversationOrderingUnderConcurrentTraffic stresses the same
// guarantee with real concurrency: one goroutine per conversation appends a
// numbered sequence; whichever way the scheduler interleaves them, each
// conversation must read back strictly in its own send order. The race gate
// runs this under -race.
func TestPerConversationOrderingUnderConcurrentTraffic(t *testing.T) {
	const perConv = 50
	log := NewLog("me", 0, 0)

	var wg sync.WaitGroup
	for _, peer := range []string{"alice", "bob"} {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			for i := 0; i < perConv; i++ {
				at := base.Add(time.Duration(i) * time.Millisecond)
				log.Append(dm(NewID(at), peer, "me", "seq", time.Duration(i)*time.Millisecond))
			}
		}(peer)
	}
	wg.Wait()

	for _, peer := range []string{"alice", "bob"} {
		got := log.Conversation(ConversationKey(peer))
		if len(got) != perConv {
			t.Fatalf("%s: %d messages, want %d", peer, len(got), perConv)
		}
		for i := 1; i < len(got); i++ {
			if !got[i-1].SentAt.Before(got[i].SentAt) {
				t.Fatalf("%s: order inverted at %d (per-conversation ordering must hold under concurrency)", peer, i)
			}
		}
	}
}

func TestDirectMessagesJoinOneConversationRegardlessOfDirection(t *testing.T) {
	// A DM conversation with alice is ONE conversation: what I sent her and
	// what she sent me interleave in it. The key derives from the peer alone.
	log := NewLog("me", 0, 0)

	log.Append(dm("id-1", "alice", "me", "inbound", 0))
	log.Append(dm("id-2", "me", "alice", "outbound", time.Second))

	got := log.Conversation(ConversationKey("alice"))
	if len(got) != 2 {
		t.Fatalf("conversation holds %d messages, want 2 (directions must merge)", len(got))
	}
	if got[0].Body != "inbound" || got[1].Body != "outbound" {
		t.Fatalf("arrival order = [%s, %s], want [inbound, outbound]", got[0].Body, got[1].Body)
	}
}

func TestBroadcastsShareOneKeyDistinctFromAnyDM(t *testing.T) {
	log := NewLog("me", 0, 0)

	log.Append(dm("id-1", "alice", "", "to everyone", 0))         // broadcast
	log.Append(dm("id-2", "broadcast", "me", "from namesake", 0)) // DM from a device literally named "broadcast"

	bc := log.Conversation(BroadcastConversation)
	if len(bc) != 1 || bc[0].ID != "id-1" {
		t.Fatalf("broadcast conversation = %+v, want only id-1", bc)
	}
	namesake := log.Conversation(ConversationKey("broadcast"))
	if len(namesake) != 1 || namesake[0].ID != "id-2" {
		t.Fatalf("namesake DM conversation = %+v, want only id-2 (key namespaces must not collide)", namesake)
	}
}

func TestForeignTrafficFilesUnderSender(t *testing.T) {
	// Traffic naming neither side as local is still received traffic; filing
	// it under its sender is the only honest reading. This cannot happen in
	// a wired client but must not corrupt or panic the log either.
	log := NewLog("me", 0, 0)
	m := dm("id-1", "ghost", "someone-else", "?", 0)

	if !log.Append(m) {
		t.Fatal("foreign message suppressed")
	}
	if got := log.Conversation(ConversationKey("ghost")); len(got) != 1 {
		t.Fatalf("foreign message filed under %d entries, want 1 under sender key", len(got))
	}
}

func TestScrollbackBoundedOldestDroppedOrderKept(t *testing.T) {
	log := NewLog("me", 10, 64)

	for i := 0; i < 25; i++ {
		log.Append(dm(NewID(base.Add(time.Duration(i)*time.Second)), "alice", "me", "m", time.Duration(i)*time.Second))
	}

	got := log.Conversation(ConversationKey("alice"))
	if len(got) != 10 {
		t.Fatalf("scrollback holds %d, want 10 (bound must hold)", len(got))
	}
	// The OLDEST fifteen scrolled off; survivors stay in arrival order.
	for i, m := range got {
		wantOffset := time.Duration(15+i) * time.Second
		if !m.SentAt.Equal(base.Add(wantOffset)) {
			t.Fatalf("survivor %d has SentAt %v, want %v (drop-oldest violated order)", i, m.SentAt, base.Add(wantOffset))
		}
	}
}

func TestConversationsListsKeysSorted(t *testing.T) {
	log := NewLog("me", 0, 0)
	log.Append(dm("id-1", "bob", "me", "x", 0))
	log.Append(dm("id-2", "alice", "me", "y", 0))
	log.Append(dm("id-3", "carol", "", "z", 0))

	want := []string{BroadcastConversation, ConversationKey("alice"), ConversationKey("bob")}
	if got := log.Conversations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Conversations = %v, want %v", got, want)
	}
}

func TestNewIDUniqueAndOrderedWithinOneMillisecond(t *testing.T) {
	// Monotonic entropy's whole job: IDs minted in the same millisecond are
	// unique AND lexicographically ordered by generation. Convenience only —
	// nothing may depend on it (package comment) — which is exactly why it
	// is pinned here rather than trusted silently.
	seen := make(map[string]struct{}, 64)
	prev := ""
	for i := 0; i < 64; i++ {
		id := NewID(base)
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %q within one millisecond", id)
		}
		seen[id] = struct{}{}
		if prev != "" && id <= prev {
			t.Fatalf("IDs not lexicographically increasing within millisecond: %v then %v", prev, id)
		}
		prev = id
	}
}
