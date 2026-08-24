# walkie quickstart

walkie is text messaging for your team, inside your terminal. It works over
your team's Tailscale network, needs no accounts or passwords, and keeps a
history of your conversations on your own machine.

This guide gets you to a working session in **three commands**. It assumes
nothing else about you: no programming knowledge, no terminal experience
beyond opening one and typing.

## Before you start

Two things must already be true about your computer. Both are about your
team's network, not about walkie — if either fails, ask whoever set up
Tailscale for your team. This is environment setup, not a walkie step.

1. **Your computer is joined to your team's Tailscale network.** Check by
   running `tailscale status` — it should list your machine and your
   teammates' machines. If the command is not found or shows nothing, your
   machine has not been joined yet.
2. **Your team's walkie coordinator is running at its standard address**
   (the hostname `walkie-coordinator`, port 443). Someone on your team
   operates it; you never see it unless something is wrong.

You also need:

- the walkie program itself, from your team's release channel (ask which
  file matches your kind of computer — `walkie -version` later confirms you
  picked right);
- a terminal window at least **80 columns wide** (a normal-sized window;
  walkie tells you plainly if yours is too narrow).

## The three commands

1. **Put walkie on your computer.** Copy or download the `walkie` file from
   your team's release channel into a folder, and make it executable
   (`chmod +x walkie`). That is the whole install.
2. **Start it:** type `walkie` and press Enter. The first start creates a
   small folder (`~/.walkie`) for your settings-free state — identity,
   history — and connects to your team's coordinator. No questions are
   asked; there is nothing to configure.
3. **Say hello:** type a message and press Enter. It goes to the
   conversation shown above the input line.

That is all three. Count them: install, `walkie`, type-and-Enter.

## Reading the screen

- The **first line** always shows your connection state. If it ever says
  something other than online, keep typing — walkie reconnects by itself and
  delivers what you sent once the connection returns.
- The **Devices** list shows your teammates and whether each is online.
  Presence is decided by the coordinator from real connections, so a device
  that dies abruptly still drops off the list within about a minute.
- Press `?` (on an empty input line) for the full list of keys. The short
  version: `tab` moves to the next conversation, `1`–`9` jumps straight to
  one, `ctrl+c` quits. Digits and `?` type as ordinary text whenever the
  input line has something in it.
- Everyone can also read the shared **broadcast** conversation; switch to it
  like any other.

## If it cannot connect

If your team's coordinator does not run at the standard address, give
walkie its real address once on the command line:

```sh
walkie -coordinator ws://walkie-coordinator.your-team.ts.net:443
```

(Use whatever name your coordinator actually has — this is an example.) With
that, reaching a session took four commands instead of three; the extra one
is a fact of your network's setup, not a missing walkie feature. See
`walkie help` for every flag.

## Remote shell access: use Tailscale SSH

walkie deliberately does **not** let you run commands on another machine —
no shell, no remote terminal. That capability was declined on purpose
(decision `adr:005-no-remote-command-execution`): putting an execution
endpoint on every device would be a new, less-reviewed path for mistakes and
attackers alike, when your team already has a better tool.

For remote shell access use **Tailscale SSH**: `tailscale ssh <machine>`.
It reaches exactly the same machines, and it already has permission rules
(ACLs), audit logs and session recording built in. Missing shell support in
walkie is a decision, not an omission.

## What walkie does not have yet

These are planned but **not implemented** in this release — do not look for
them:

- **Voice notes** — not available in any build today, including ones
  reporting "audio available". Text and presence work fully everywhere.
- Real-time voice calls, direct device-to-device paths, and file transfer.
- Rooms/channels beyond the shared broadcast conversation.
- Screen sharing or shared terminals (read-only terminal viewing comes
  later; see Tailscale SSH above for shells today).
- Slash commands, webhooks, plugins, automatic updates.

## Security facts, stated plainly

These are accepted design decisions recorded in
`adr:004-security-model`, not oversights. You should know them:

- **Your message history is stored unencrypted on your machine.** Anyone who
  can read your disk — another user account, a thief with your laptop, a
  copied backup — can read every message. Turn on your device's full-disk
  encryption; that is the real protection and it lives outside walkie.
- **Messages waiting for an offline teammate ARE encrypted**, sealed to
  their device key; the coordinator stores them but cannot read them.
- **Losing your device's key forfeits messages queued for you.** The key
  lives in `~/.walkie/identity.key`. There is no backup, recovery or
  escrow. Deleting it (or reinstalling walkie) creates a fresh key, and
  every teammate will be asked to confirm the change — that friction is the
  security working.
- **No key rotation and no multi-device identity.** One key per device, for
  its whole life.
- **walkie inherits your tailnet's ACL rules and cannot strengthen them.**
  If your tailnet lets everyone talk to everything, walkie cannot fix that.
  Operators: see the runbook.
- Ordinary live messages travel inside WireGuard encryption and nothing
  more.

## Verifying a teammate's device (fingerprint check)

Every device has a **fingerprint**: twenty decimal digits, shown in five
groups of four, like `4677 6887 2937 2977 8267`. Two people compare theirs
by voice to prove they are really talking to each other.

**Where fingerprints appear.** Today they are visible in two places:

- the coordinator's log line `peer key pinned on first contact`, which
  carries each device's `fingerprint=` the first time it connects;
- a walkie error saying a key was refused, which quotes both the
  `pinned` and the `presented` fingerprint.

Your operator can read any device's fingerprint from the coordinator logs.

**The procedure** (two people, a phone call):

1. Each person gets their device's fingerprint from the coordinator log
   (or from the refusal message, if one appeared).
2. One person reads all twenty digits aloud in the groups of four:
   "four six seven seven, six eight eight seven, …".
3. The other follows on their own fingerprint and confirms each group.
4. Then swap roles and repeat.

**If they match**, the connection is genuine.

**If they do not match, stop. Do not proceed with the conversation.** A
mismatch means one side is not who it claims to be. Tell your operator.

**If a teammate's device suddenly presents a different key than before**
— for example after reinstalling walkie — the coordinator **refuses the
new key automatically**: it logs `PEER KEY CHANGED: verification required`
with both fingerprints, and the reinstalling device sees an error quoting
the `pinned` and `presented` fingerprints. Exactly one of two things is
true: that person reinstalled walkie, or someone is impersonating them.
Verify by voice using the procedure above. There is deliberately no
"accept anyway" switch anywhere: your operator resolves it by comparing
fingerprints out of band and correcting the coordinator's pin file by hand,
with the process stopped.

---

Operations (deploying and running the coordinator) are covered in
[docs/runbook.md](runbook.md).
