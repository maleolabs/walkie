package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/maleolabs/walkie/internal/clock"
	"github.com/maleolabs/walkie/internal/history"
	"github.com/maleolabs/walkie/internal/message"
)

// defaultDBPath is where the client's history lives unless -db says otherwise:
// one directory per user, named after the runtime state .gitignore already
// excludes. The assembled client (sto:terminal-ui) must open its history at
// the SAME path so the query surface and the live view read one store.
func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "" // caller reports it; there is nothing sensible to guess
	}
	return filepath.Join(home, ".walkie", "history.db")
}

// historyLine is one output row of `walkie history`. Field order here is the
// JSON field order on the wire — keep it stable, scripts parse positionally
// by key but humans diff by eye. Timestamps are RFC3339Nano UTC.
type historyLine struct {
	ID           string `json:"id"`
	Conversation string `json:"conversation"`
	Sender       string `json:"sender"`
	Recipient    string `json:"recipient,omitempty"`
	SentAt       string `json:"sent_at"`
	ReceivedAt   string `json:"received_at"`
	Body         string `json:"body"`
}

// runHistory implements `walkie history`: query the local message store by
// conversation and/or time range, one JSON object per line on stdout —
// parseable output for scripts, not prose and not a TUI (criterion 2 of
// sto:message-history; sto:terminal-ui owns the interactive view).
//
// Exit codes: 0 success (including an empty result), 1 failure, 2 usage error.
func runHistory(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	fs.SetOutput(stderr)

	dbPath := fs.String("db", defaultDBPath(), "path to the history database (default ~/.walkie/history.db)")
	conversation := fs.String("conversation", "", "peer device name to list direct messages with, or \"broadcast\"")
	fromStr := fs.String("from", "", "only messages received at or after this time (RFC3339, inclusive)")
	toStr := fs.String("to", "", "only messages received at or before this time (RFC3339, inclusive)")

	fs.Usage = func() { printHistoryUsage(stderr) }
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0 // -help printed its usage; that succeeded
		}
		fmt.Fprintln(stderr, "walkie history:", err)
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "walkie history: unexpected argument %q\n", fs.Arg(0))
		printHistoryUsage(stderr)
		return 2
	}

	var from, to *time.Time
	if *fromStr != "" {
		t, err := time.Parse(time.RFC3339, *fromStr)
		if err != nil {
			fmt.Fprintf(stderr, "walkie history: -from: %v\n", err)
			return 2
		}
		from = &t
	}
	if *toStr != "" {
		t, err := time.Parse(time.RFC3339, *toStr)
		if err != nil {
			fmt.Fprintf(stderr, "walkie history: -to: %v\n", err)
			return 2
		}
		to = &t
	}

	if _, err := os.Stat(*dbPath); err != nil {
		fmt.Fprintf(stderr, "walkie history: no history database at %s\n"+
			"(it is created when the client runs; point -db at an existing file)\n", *dbPath)
		return 1
	}

	// A query opens through the same Open as the client, so permissions are
	// enforced and retention is applied even on a read — bounded storage does
	// not pause because someone looked. The bounds come from the package
	// defaults; they gate eviction only, never what a query may return.
	s, err := history.Open(*dbPath, clock.Real(), history.Options{
		TTL:         history.DefaultTTL,
		MaxMessages: history.DefaultMaxMessages,
	})
	if err != nil {
		fmt.Fprintf(stderr, "walkie history: %v\n", err)
		return 1
	}
	defer s.Close()

	opts := history.QueryOptions{From: from, To: to}
	if *conversation != "" {
		if *conversation == message.BroadcastConversation {
			opts.Conversation = message.BroadcastConversation
		} else {
			opts.Conversation = message.ConversationKey(*conversation)
		}
	}

	msgs, err := s.Query(opts)
	if err != nil {
		fmt.Fprintf(stderr, "walkie history: %v\n", err)
		return 1
	}

	enc := json.NewEncoder(stdout)
	for _, r := range msgs {
		line := historyLine{
			ID:           r.Message.ID,
			Conversation: r.Conversation,
			Sender:       r.Message.Sender,
			Recipient:    r.Message.Recipient,
			SentAt:       r.Message.SentAt.UTC().Format(time.RFC3339Nano),
			ReceivedAt:   r.Message.ReceivedAt.UTC().Format(time.RFC3339Nano),
			Body:         r.Message.Body,
		}
		if err := enc.Encode(line); err != nil {
			fmt.Fprintf(stderr, "walkie history: encode output: %v\n", err)
			return 1
		}
	}
	return 0
}

// printHistoryUsage carries criterion 5's statement: local history is NOT
// encrypted at rest, stated plainly enough that a non-technical reader
// understands the consequence. adr:004-security-model accepted this as a
// limitation; softening it here would undo the acceptance criterion that
// requires users be told.
func printHistoryUsage(w io.Writer) {
	fmt.Fprint(w, `usage: walkie history [flags]

Query this device's stored message history. Output is one JSON object per
line on stdout, ordered oldest first within each conversation.

Flags:
  -db PATH            history database path (default ~/.walkie/history.db)
  -conversation PEER  direct messages exchanged with PEER, or "broadcast";
                      omit for all conversations
  -from TIME          only messages received at or after TIME (RFC3339, inclusive)
  -to TIME            only messages received at or before TIME (RFC3339, inclusive)

SECURITY — READ THIS: your message history is stored UNENCRYPTED in the
database file. Anyone who can read this device's disk can read every
message you sent or received: another user account, someone holding your
stolen laptop, a copied disk image or backup. walkie does not encrypt
local history; this is a deliberately accepted limitation
(walkie/adr:004-security-model), not an oversight. Full-disk encryption
is the mitigation, and it is configured outside walkie. File permissions
are owner-only (0600 file, 0700 directory); that stops other local users
on a shared machine and nothing beyond that.
`)
}
