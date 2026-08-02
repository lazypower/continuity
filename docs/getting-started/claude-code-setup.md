[Docs](../README.md) › Getting started › Connect it to Claude Code

# Connect it to Claude Code

Three pieces of wiring. After this, memory works without you doing anything.

**Audience:** operator · **Read time:** ~4 min

---

There are three separate connections, and they do different jobs:

| | What it does |
|---|---|
| **Hooks** | Let Continuity see your sessions and inject memory at the start |
| **MCP tools** | Let your agent read and write memories deliberately |
| **Directives** | Tell your agent to actually use them |

You want all three. If you would rather install them in one step, skip to
[the plugin](#the-one-step-alternative).

## 1. Hooks

Hooks are commands Claude Code runs automatically at fixed points — when a
session starts, when you send a message, after each tool call, and when the
session ends.

Add this to `~/.claude/settings.json`.

**If the file already has a `"hooks"` key, merge rather than replace.** For each
event below that already exists, append Continuity's entry to that event's
existing array — do not overwrite it, or you will silently disable whatever else
was wired to that event.

**What this records:** once `PostToolUse` is enabled, Continuity writes down the
*name* of each tool your agent runs, and when. Arguments and output are not
stored. Those records are deleted automatically after 14 days — see
[What gets remembered](../guides/what-gets-remembered.md).

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [{ "type": "command", "command": "continuity hook start", "timeout": 10 }] }
    ],
    "UserPromptSubmit": [
      { "hooks": [{ "type": "command", "command": "continuity hook submit", "timeout": 10 }] }
    ],
    "PostToolUse": [
      { "hooks": [{ "type": "command", "command": "continuity hook tool", "timeout": 10 }] }
    ],
    "Stop": [
      { "hooks": [{ "type": "command", "command": "continuity hook stop", "timeout": 120 }] }
    ],
    "SessionEnd": [
      { "hooks": [{ "type": "command", "command": "continuity hook end", "timeout": 10 }] }
    ]
  }
}
```

These are designed never to interrupt you. If Continuity is unreachable, the hook
prints one warning and lets the session continue — it cannot fail your session or
block a tool call.

## 2. MCP tools

This gives your agent six memory tools it can call directly — `remember`,
`search`, `show`, `tree`, `profile`, and `retract`. Without it, the agent can only
reach Continuity by running shell commands, which means escaping multi-line text
by hand and getting it wrong.

```bash
claude mcp add --scope user continuity -- continuity mcp
```

`--scope user` makes it available in every project. Restart Claude Code
afterwards.

These tools talk to the same background server, so it needs to be running. See
[MCP tools](../reference/mcp-tools.md) for what each one takes.

## 3. Directives

```bash
continuity init
```

```
Initialized: /Users/you/.claude/CLAUDE.md
Claude Code will now use continuity for memory in all sessions.
```

This writes a block of instructions into `~/.claude/CLAUDE.md` telling your agent
to use Continuity instead of Claude Code's own note-taking. Without it, the tools
are available but the agent has no reason to prefer them.

The block sits between two markers and **only that block is touched** — anything
else in the file is preserved. Re-running `continuity init` updates it in place,
so run it again after upgrades to pick up improved wording. It reports
`Already up to date` when there is nothing to change.

### Optional: start the server automatically

```bash
continuity init --autostart
```

With this, the session-start hook launches the server if it is not already
running. **The process it starts outlives your Claude Code session** — it keeps
running until you stop it or reboot. There is no supervision and no restart on
crash.

For anything long-lived, prefer `continuity install-service`, which is properly
managed. Run `continuity init` without the flag to turn autostart off again.

## The plugin bundle

Steps 1 and 2 — hooks and MCP registration, though **not** the directives — also
ship as a Claude Code plugin. It lives in the source repository rather than in
the installed binary, so it is only useful if you have the repo checked out:

```bash
git clone https://github.com/lazypower/continuity.git
claude --plugin-dir ./continuity/plugin
```

That loads it for one invocation, for trying it out. For a permanent setup, use
the three steps above.

## Check it worked

Restart Claude Code and start a session. Then verify each connection separately —
they fail independently, so a single check would not tell you much.

**Hooks are firing.** A session from the last few minutes should be listed:

```bash
continuity timeline
```

**MCP tools are registered.** In Claude Code, run `/mcp` — `continuity` should
appear with its six tools.

**Directives are in place.** The managed block should be present:

```bash
grep -c "continuity:managed" ~/.claude/CLAUDE.md    # expect 2 — start and end markers
```

Asking your agent what it remembers about you is a reasonable smoke test, but on
a fresh install the honest answer is "nothing yet".

---

**Next:** [Your first session](first-session.md)
**See also:** [Troubleshooting](../guides/troubleshooting.md) · [Hook internals](../advanced/hooks-internals.md)
