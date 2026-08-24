# Fleet ARM smoke test — pre-release gate

Criterion 7 of `eka get walkie/ts:build-release-matrix`: **both variants are
smoke-tested on a real fleet ARM device before release, not only in
emulation.** This file is the procedure for whoever holds the device. It
exists so the test is a checklist, not a judgement call.

Status: NOT YET RUN — no fleet ARM device was available when the pipeline was
built. A release that skips this procedure has not met criterion 7.

## What you need

- A fleet ARM device (linux/arm64; linux/arm if any exist in the fleet) with
  SSH access and nothing else special: no desktop, no audio stack required.
- The artifacts of the release candidate: the `linux-arm64/` directory and
  `SHA256SUMS` from the CI run (or `make release` output).
- The audio-variant artifact for arm64 — **only once the audio build exists**;
  see "Variant 2" below.

## Variant 1 — default build (required for every release)

```sh
# 1. Integrity first.
sha256sum -c SHA256SUMS linux-arm64/walkie linux-arm64/walkie-coordinator
#    (or cd into the artifact root and check the whole file)

# 2. The binary admits what it is.
chmod +x walkie walkie-coordinator
./walkie -version
#    expect EXACTLY:
#      walkie <full-commit-sha> linux/arm64
#      audio  unavailable (built without the "voice" tag)
./walkie-coordinator -version
#    expect: walkie-coordinator <same-full-commit-sha> linux/arm64

# 3. It runs on this device's actual kernel/libc — the thing emulation cannot
#    prove. Static binaries can still hit kernel-version and syscall gaps.
./walkie-coordinator -help > /dev/null && echo "coordinator ok"
./walkie history -help > /dev/null && echo "client ok"

# 4. Client state machinery on real hardware: open, query, close.
./walkie history -db /tmp/smoke-history.db 2>/dev/null; \
  rm -f /tmp/smoke-history.db* && echo "store ok"
```

Record: device model, kernel (`uname -a`), the two `-version` outputs, and
pass/fail per step.

## Variant 2 — audio build (once criterion 3 unblocks it)

The audio build does not exist yet: `spk:audio-toolchain-viability` was
canceled before settling the libopus binding, so there is no voice-tagged
artifact to test. When it exists, add to the above:

```sh
# 5. The variant reports itself honestly.
./walkie-voice -version
#    expect: audio  available (built with the "voice" tag)

# 6. Capture path on headless hardware — the spike's open question #2: an
#    input device must be detected with no GUI or desktop audio stack present.
#    Run whatever capture smoke the audio work item ships; at minimum confirm
#    the process starts, enumerates or fails LOUDLY on missing devices, and
#    exits non-zero rather than hanging.

# 7. Record binary size and resident memory (spike note #6):
ls -l walkie-voice
/usr/bin/time -v ./walkie-voice -version 2>&1 | grep -i 'maximum resident'
```

## Reporting

Attach the recorded outputs to the release checklist. Any FAIL stops the
release: criterion 7 is a gate, not a note. If the audio variant still does
not exist, record that fact explicitly — "variant 2 not smoke-tested because
not produced" is honest; silence is not.
