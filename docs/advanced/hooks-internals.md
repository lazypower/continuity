[Docs](../README.md) › Advanced › Hook internals

# Hook internals

What each of the five hooks does, why a hook can never fail your session, and the guards that keep a pasted document from writing itself into your memory.

**Audience:** engineer · **Read time:** ~9 min

<p align="center">
  <img src="../assets/session-lifecycle.svg" alt="What fires at each of the five hook points during one Claude Code session: memory is injected at session start, trigger phrases save immediately on submit, each tool call is recorded, and end-of-session extraction is declined by default." width="820" />
</p>


## The shape of a hook

Claude Code writes a JSON object on stdin, runs `continuity hook <event>`, and
reads stdout. Five events map to five subcommands, all dispatched through
`hooks.Handle` in `internal/hooks/handler.go`:

| Claude Code event | Subcommand | Writes stdout? |
|---|---|---|
| SessionStart | `continuity hook start` | Yes — `additionalContext` |
| UserPromptSubmit | `continuity hook submit` | No |
| PostToolUse | `continuity hook tool` | No |
| Stop | `continuity hook stop` | No |
| SessionEnd | `continuity hook end` | No |

Every hook is a short-lived process that talks HTTP to the daemon with a **5 s**
client timeout. Hooks run inline in Claude Code's event loop, so the budget is a
latency ceiling for the editor, not a health signal.

Stdin is read through `io.LimitReader` at **10 MB**. `HookInput`
(`internal/hooks/input.go`) declares every field any event might send; each
event populates a subset and the rest decode as zero values.

## The exit-code contract

**Hooks never exit non-zero.** There is exactly one exit path for errors:

```go
// ExitError logs to stderr and exits 0 (hooks must never crash Claude Code).
func ExitError(err error) {
    fmt.Fprintf(os.Stderr, "continuity hook: %v\n", err)
    os.Exit(0)
}
```

Claude Code treats exit 1 as a non-blocking error surfaced to the user and exit
2 as a **blocking** error fed back to Claude. Continuity uses neither. A memory
system that can interrupt the user's session is worse than a memory system that
misses a capture, so every failure — a bad JSON decode, an unreachable server, a
failed POST, an unknown event name — degrades to a stderr line and exit 0.

The degradation is graded by event:

- **`start` with undecodable stdin** → emit an empty
  `additionalContext` and return. The session begins with no memory rather than
  no session.
- **`start` with an unreachable server** → try autostart; if that fails, emit
  empty context and return.
- **Any other event with an unreachable server** → warn once per session
  (below) and return without doing anything.

Note the asymmetry: `start` degrades to *valid empty output*, because Claude
Code is waiting on its stdout. The others degrade to silence, because nothing
is waiting.

## Per-hook behavior

### `start` — SessionStart

Unlike the other four, `start` fetches `/api/health` once via `Client.Status()`
and uses that single round-trip for **both** liveness and skew surfacing. The
previous code called `Healthy()` and then `Status()` — two 5-second-budget trips
at the moment the user is waiting to start work.

- `Status()` errors, or status is not `"ok"` → treat as not healthy, run
  autostart. If autostart succeeds, fall through; otherwise emit empty context.
- `Status()` succeeds → run skew surfacing on the payload just fetched.

Then `handleStart` does one `GET /api/context?session_id=…` and writes the
result as `additionalContext` inside a `SessionStartOutput` envelope. Any
failure — request error or unparseable body — emits empty context rather than
propagating.

The context block itself is assembled server-side in
`internal/server/context.go` under a **4 000-character** budget: a date header,
a gap signal when the last session was over 7 days ago, the synthesized
"Working With You" profile (capped at 1 000 chars), operator pins, up to 3
diversity-sampled Moments, recent sessions, and the current session's tool
count. `?preview=true` renders the same block **without** advancing moment
rotation or writing `shown` events — a preview that consumed the rotation it
displays would change the very thing it claims to show.

### `submit` — UserPromptSubmit

In order:

1. **Recursion guard** — bail if the prompt starts with `[continuity-internal]`.
2. `POST /api/sessions/init` with `session_id` and `project` (the hook's `cwd`).
   This is where a session row is created or reactivated, and where
   `last_active_at` is stamped.
3. **Signal check** — if the prompt clears the gates, `POST
   /api/sessions/{id}/signal` fire-and-forget, errors ignored (the server is
   async anyway). If it *nearly* cleared them, print the visibility note
   described below.

Note that the init POST is the one place in `submit` that uses `ExitError`, so
a failed init still exits 0.

### `tool` — PostToolUse

Skips meta-tools that generate noise rather than observations — `TodoRead`,
`TodoWrite`, `Thinking`, `TaskList`, `TaskCreate`, `TaskGet`, `TaskUpdate` —
then POSTs the raw `tool_input` and `tool_response` JSON as strings to
`/api/sessions/{id}/observations`.

Server-side, each field is truncated at **10 KB** with a log line, and the
session's `tool_count` and `last_active_at` are bumped. These rows are the
scaffolding behind the "Current Session" line in the context block, and they
are the thing observation retention reclaims — see
[Memory lifecycle](memory-lifecycle.md#observation-retention).

### `stop` — Stop

Fires **once per assistant turn**, not once per session. Two calls:

1. `POST /api/sessions/{id}/complete`.
2. If a transcript path is present *and* `shouldExtract` passes, `POST
   /api/sessions/{id}/extract`.

`shouldExtract` mirrors `engine.hasEnoughContent` (3+ user messages, 100+ chars
condensed) purely to skip an HTTP round-trip on the early turns that would be
rejected server-side anyway. If the transcript cannot be read at all, it returns
false and lets SessionEnd sort it out. The server applies the same gate again.

With auto-extraction off — the default — the server answers this with
`{"status":"extraction_disabled"}` and enqueues nothing. See
[Extraction](extraction.md#why-auto-extraction-is-off-by-default).

### `end` — SessionEnd

`POST /api/sessions/{id}/end`, then — deliberately **without** the local content
gate — `POST /api/sessions/{id}/extract` whenever a transcript path exists.

This is belt-and-suspenders. Stop handles the common case per-turn, but
SessionEnd is the last chance for sessions where Stop never ran (terminal
killed) or where the final turn pushed the transcript over the threshold after
the prior Stop. The server's idempotency guard plus content gate make the
duplicate call safe.

## Version-skew detection

A `brew upgrade` swaps the binary while a long-running `serve` keeps the old
code — and possibly the old schema — in memory. Skew detection catches that.

`CompatibilityCheck` (`internal/hooks/compat.go`) compares the local binary's
contract against the server's `/api/health` payload on **two dimensions**:

- `api_version` (`buildinfo.APIVersion`, currently `1`)
- `schema_head` (`store.HeadSchemaVersion()`)

It explicitly does **not** compare version strings. A dev, dirty, or patch build
that shares the same API version and schema head is fully interoperable and must
not be flagged. The human-readable versions ride along on `SkewError` for the
message only.

`decideSkewAction` is a pure function of three inputs, deliberately doing no
I/O so it can be exhaustively unit-tested:

| Skew? | Bounce marker? | Service-managed? | Action |
|---|---|---|---|
| No | — | — | nothing |
| Yes | absent | — | warn |
| Yes | present | yes | warn |
| Yes | present | no | **bounce** |

**Warn** prints "server is running stale code (server X / CLI Y) — run
`continuity restart`" and continues.

**Bounce** is strictly opt-in: it requires `~/.continuity/autostart-bounce` to
exist *and* the server to be bare. `serviceManaged()` checks for a launchd plist
or a systemd user unit on disk, and `classifyServiceStat` is deliberately
conservative — a definitive "does not exist" is the **only** way to conclude
"bare". Any other stat error (permission denied, unreadable parent) is treated
as managed, so the hook warns rather than bare-killing what may be a
service-managed process. The hook never drives launchd or systemd; that is
`continuity restart`'s job.

The bounce itself routes through `ConfirmAndBounce`
(`internal/hooks/kill_path.go`), the shared safe kill path — the same one
`continuity restart` uses, so the safety gate cannot be bypassed by one caller.
Before signalling anything it:

1. **Re-fetches health** (TOCTOU): the process sampled a moment ago may have
   died and the pid been reused.
2. Requires the live payload to pass `IsContinuityServer` — `status == "ok"`,
   `pid > 0`, `api_version > 0`, `schema_head > 0`. The last two are distinctive
   fields an unrelated local server would not emit, which together make a
   coincidental match implausible. A legacy pre-skew-detection Continuity
   server reports zeros and so **fails this gate by design**: it must not be
   bare-killed.
3. Requires the live pid to still equal the intended pid. If it changed, the old
   process is gone; abort rather than chase a moving target.
4. Best-effort OS executable match. A **definite** mismatch refuses outright; an
   indeterminate result (macOS, empty `exe`) is acceptable because steps 2 and 3
   already established strong identity.

The whole critical section — revalidate, signal, respawn — is serialized under
`~/.continuity/restart.lock`, acquired inside `ConfirmAndBounce` so both the CLI
and the hook share one lock at one location. Two session hooks, or a hook racing
a manual restart, would otherwise both confirm the same pid, both SIGTERM, and
then two respawns would fight over the port. A held lock returns a typed error;
the hook prints "a restart/bounce is already in progress — skipping" and moves
on. A stale lock is reaped only when its owner pid is dead, or when the owner is
unclassifiable *and* the file is older than 2 minutes.

The safety principle is stated in the source: this is a single-user localhost
tool, so the threat is **accidental** collision — another process squatting the
port, pid reuse after the real server died — never a malicious forge. Identity
is confirmed hard and re-confirmed immediately before signalling, but no
supervisor framework is built.

Mutation CLI verbs run the same check non-blockingly via `warnIfSkewed`
(`internal/cli/skew_warn.go`), which stays silent when the status call itself
errors — that is a different condition the command surfaces on its own.

## Autostart

`~/.continuity/autostart` is a marker file; its presence is the entire
configuration, no parsing. `TryAutostart` spawns `continuity serve` fully
detached (`Setsid`), logging to `~/.continuity/serve.log` (chmodded to `0600`),
then polls health every 200 ms for up to 3 seconds.

Port binding is the lock: a redundant spawn fails to bind and exits, so
concurrent autostarts are safe.

`startAndReap` launches a background `Wait()` on the child. The child is
detached and persists after the hook exits, but until then the hook is its
parent — so a crash-on-boot becomes a **zombie** the hook owns, and a zombie
answers signal-0 liveness probes as *alive*. That would mask a crash-on-boot
during `continuity restart`'s verify poll. The background reap makes
`ProcessAlive(pid)` report false promptly once the child is really gone.

## The once-per-session unreachable warning

When a non-`start` hook cannot reach the server, the session is silently not
being captured. `warnServerUnreachableOnce` converts that into a visible,
fixable condition:

```
continuity: server unreachable — this session is not being captured
(run `continuity serve` or check the service)
```

Deduplication is the interesting part. Hooks are **separate processes**, so a
per-process guard would print this on every single tool call. The dedup marker
is a file in `os.TempDir()` named
`continuity-unreachable-<sanitized-session-id>`, where sanitization maps
anything outside `[A-Za-z0-9_-]` to `_`.

The claim is atomic: the file is created with `O_CREATE|O_EXCL|O_WRONLY`, and
**only the process that creates it warns**. `O_EXCL` closes the
check-then-create race, so concurrent hooks for the same session cannot both
print.

Three outcomes, all deliberate:

- Created → warn.
- `ErrExist` → another hook already warned; stay silent.
- Any other error (unwritable temp dir) → fall through and warn best-effort. A
  broken dedup marker should not turn into total silence.

A hook with an empty `session_id` skips the marker entirely and always warns.

## Signal-phrase gates

The signal path is the one place a user message can directly cause a memory
write, so it is gated hard. All of this lives in `internal/hooks/submit.go`.

**The trigger list is deliberately tight:**

```
"remember this", "don't forget",
"always use", "never use", "always do", "never do",
"architecture decision",
"root cause was", "the fix was"
```

The comment is explicit that broad phrases like "this pattern" or "the trick is"
fire on normal conversation and were rejected. Matching is
case-insensitive substring.

**Two length gates make the message plausibly human:**

- `maxSignalPromptLen = 2000` — the whole prompt. A trigger phrase buried in a
  large paste is not the operator asking to remember something; it is
  third-party content that happened to transit the session.
- `maxSignalTriggerOffset = 500` — the trigger must appear within the first 500
  characters. A paste whose body contains "always use X" deep inside cannot
  self-author an attacker-controlled memory.

Both must hold. This is the drive-by memory-poisoning vector the design refuses.

**The over-block is made visible.** `signalGatedByLength` detects the one case
the length gate gets wrong from the user's point of view: a deliberate, up-front
"remember this: …" that simply ran long. When that happens the hook prints

```
continuity: memory cue found but the message is too long to fire as an
immediate signal — it will still be considered at session end
```

It deliberately does **not** fire for a cue past the 500-character offset —
that is a paste, not a gated instruction — so the note only ever surfaces the
real over-block.

## The recursion guard

Continuity's LLM calls run through `claude -p`, which spawns a Claude Code
session, which fires hooks — including `UserPromptSubmit` — back into
Continuity. Without a guard, one extraction would trigger a signal check on its
own prompt, which could trigger another extraction.

Every prompt built in `internal/llm/prompts.go` is prefixed with the sentinel:

```
[continuity-internal]
```

`isInternalPrompt` checks `strings.HasPrefix` — **prefix only**, not
`Contains`, so a real user message that happens to quote the string is not
suppressed. `handleSubmit` returns immediately on a match, before session init
and before the signal check.

Two constants define it, `llm.InternalSentinel` and `hooks.internalSentinel`,
each carrying a comment that they must match exactly; a test asserts every
prompt builder emits the prefix.

The `claude -p` subprocess is additionally pinned to a dedicated empty working
directory so it has no surrounding project to scan, and its environment is
filtered.

## Registration

Hook wiring is intentionally absent from `config.toml`. From
`internal/config/config.go`:

> Hook behavior is deliberately absent here. Hooks are configured where Claude
> Code reads them — `~/.claude/settings.json`, or `plugin/hooks/hooks.json` —
> and a second, inert copy of those knobs in `config.toml` only teaches
> operators to tune a file nothing reads.

`plugin/hooks/hooks.json` is the shipped definition. The commands are
one-liners — no runner, no wrapper script; the binary *is* the hook handler.
Each entry carries a Claude Code-side `timeout`: **10 s** for start, submit,
tool, and end, and **120 s** for Stop, which is the one hook that parses and
condenses a transcript before deciding whether to call the server.

`continuity init` is a different job: it upserts Continuity's managed directive
block into `~/.claude/CLAUDE.md` (between `<!-- continuity:managed -->` markers,
so re-running refreshes it in place and leaves everything else verbatim), and
with `--autostart` toggles the `~/.continuity/autostart` marker. It does not
touch `settings.json`.

---

**See also:** [Extraction](extraction.md) · [Architecture](architecture.md) · [Memory lifecycle](memory-lifecycle.md) · [Connect it to Claude Code](../getting-started/claude-code-setup.md) · [Troubleshooting](../guides/troubleshooting.md) · [CLI reference](../reference/cli.md) · [Files and paths](../reference/files-and-paths.md)
