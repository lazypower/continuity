[Docs](../README.md) › Guides › Keeping it healthy

# Keeping it healthy

The routine maintenance Continuity does for you, the two things it needs you to
do, and how to check that everything is fine.

**Audience:** operator · **Read time:** ~4 min

---

## The 30-second check

```bash
continuity doctor
```

This reads your database and reports whether search is working, whether every
memory has a search entry, and whether anything is inconsistent. It **never
changes anything**. If it says healthy, you are done.

For a live view, open the web interface:

Open <http://localhost:37777> in a browser (`open` on macOS, `xdg-open` on Linux).

The **Health** tab gives you a plain-language verdict, how much of your memory is
fresh versus fading, and a "needs attention" list.

## What runs automatically

You do not need to schedule any of this.

| Job | When | What it does |
|---|---|---|
| Observation retention | Every start, then daily | Deletes spent tool-call records |
| Relevance decay | Every start, then daily | Ages memories that are not being used |
| Search backfill | Every start | Fills in missing search entries |
| Safety snapshot | Before risky upgrades | Copies your database first |

## Observation retention

Every tool call your agent makes is recorded as an **observation** — just the
tool's name and the time it ran, not its arguments or output. These exist to
count activity in the current session. Once a session is finished, they are
spent.

They used to accumulate forever. On one normal single-user install they reached
**331,966 records and 1 GB** in about four and a half months, against 3 MB of
actual memories. The symptom was not a disk warning — it was every command
reporting the server was down, while the server was healthy and simply too slow
to answer in time.

So Continuity now deletes them. **Observations from sessions that are no longer
running are removed after 14 days.** This is on by default.

**Your memories are never touched by this.** Memories, their search entries, your
sessions, summaries and relational profile are all unaffected.

To change or disable it, set `CONTINUITY_OBSERVATION_RETENTION_DAYS` to `off` or
to a number of days. **The server has to see it**, and a bare assignment in your
shell will not reach a background service:

- **Service install** — add it to the service definition and reload. On macOS
  that is the `EnvironmentVariables` block of
  `~/Library/LaunchAgents/com.continuity.server.plist`; on Linux, an
  `Environment=` line in `~/.config/systemd/user/continuity.service`.
- **Started by hand** — `CONTINUITY_OBSERVATION_RETENTION_DAYS=off continuity serve`

Then restart, and confirm it took effect — `spent_observations` will climb
instead of staying near zero:

```bash
curl -s localhost:37777/api/health
```

If you set this to something Continuity cannot understand, it **disables
retention** rather than guessing — someone typing `of` when they meant `off` is
trying to stop deletion, and the safest reading of a broken setting is the one
that deletes nothing.

## Reclaiming disk space

Deleting records does not shrink the database file. SQLite holds onto the freed
space to reuse later. That is usually the right behavior, but after a large
cleanup you will want the space back:

```bash
continuity prune --dry-run     # report only, changes nothing
continuity prune               # delete spent records and compact the file
```

```
Pruned:   0 observation(s)
Database: 350.2 MB → 43.6 MB (freed 306.6 MB)
```

`Pruned: 0` alongside a large reclaim is normal, not a failure — the daily sweep
had already deleted the records, leaving only free space to return.

Compacting rewrites the whole file, so it needs roughly as much free disk as the
database currently occupies, and can take a few minutes on a large one. Use
`--skip-vacuum` to delete records without compacting.

**This is the one maintenance task worth doing by hand**, and only occasionally —
after an upgrade that reclaims a backlog, or if the file has grown.

## Safety snapshots

Before an upgrade that rewrites existing data, Continuity copies your database
first:

```bash
continuity snapshot list
```

Snapshots are deleted automatically after three successful starts. They are a
short-term undo for a specific class of upgrade, **not a backup** — see
[Upgrading](../upgrading.md#rolling-back).

## What to watch

If you want to keep an eye on things, one endpoint answers everything:

```bash
curl -s localhost:37777/api/health
```

Four fields are worth knowing:

| Field | Means |
|---|---|
| `db_bytes` | Database size on disk. Growing steadily? Run `continuity prune`. |
| `spent_observations` | Records waiting to be reclaimed. Should hover near zero. |
| `vector_identity_locked` | `true` means search is disabled — see [Embedding backends](embedders.md) |
| `schema_current` vs `schema_head` | Different means a migration is pending; restart |

A useful habit: if a Continuity command ever tells you the server is not
responding, check `db_bytes` before assuming the process died. A large database
makes a healthy server look dead.

---

**Next:** [Troubleshooting](troubleshooting.md)
**See also:** [Upgrading](../upgrading.md) · [Embedding backends](embedders.md) · [HTTP API](../reference/http-api.md)
