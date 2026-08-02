[Docs](README.md) › Upgrading

# Upgrading

Read this before upgrading, not after. Some upgrades delete data on the first
start, and the automatic safety copy does not cover all of it.

**Audience:** operator · **Read time:** ~6 min

<p align="center">
  <img src="assets/upgrade-flow.svg" alt="What happens on the first start after an upgrade: a safety copy only when the update rewrites existing records, then the database format update, then automatic cleanup, with disk space reclaimed only by running prune yourself." width="820" />
</p>


---

## Does this upgrade delete anything?

| Upgrading to | Deletes data on first start? |
|---|---|
| **v0.11.0** | **Yes** — old tool-call records. Your memories are not affected. [Details](#upgrading-to-v0110) |
| Any other version | No |

If you are upgrading to v0.11.0, read [that section](#upgrading-to-v0110) before
running anything. If you want to keep those records, you must
[turn the cleanup off **before** you restart](#keep-the-old-records) — once the
first cleanup runs, the records are gone and no automatic copy contains them.

## Step 1 — Back up

Continuity has no backup command, and the database file is being written to while
the server runs, so copying it live can produce an unusable copy. Stop the server
first.

**Stop it**, using whichever matches your install:

```bash
launchctl unload ~/Library/LaunchAgents/com.continuity.server.plist   # macOS service
systemctl --user stop continuity                                      # Linux service
pkill -f 'continuity serve'                                           # started by hand or autostart
```

**Confirm it stopped.** This should fail to connect:

```bash
curl -s localhost:37777/api/health || echo "stopped"
```

**Copy the database**, then start the server again:

```bash
cp ~/.continuity/continuity.db ~/continuity-backup.db
continuity serve            # or re-load the service you stopped above
```

To check the copy is usable, point Continuity at it — this reads the copy and
touches nothing else:

```bash
CONTINUITY_DB=~/continuity-backup.db continuity tree
```

## Step 2 — Upgrade

```bash
brew upgrade continuity          # or: brew upgrade continuity-dev
```

If you installed with the shell script, re-running it downloads and immediately
executes the current installer from GitHub. Read it first if that matters to you:

```bash
curl -fsSL https://raw.githubusercontent.com/lazypower/continuity/main/install.sh -o install.sh
less install.sh
sh install.sh
```

## Step 3 — Restart

Installing a new `continuity` program does not change the server already running
in the background. It keeps running the old version until you restart it:

```bash
continuity restart
```

Continuity warns you when the two are out of step:

```
⚠ continuity server is running dev-2b7c9a2 but this CLI is dev-bbe1a2b —
  schema/API mismatch; run `continuity restart` to pick up the new binary
```

## Step 4 — Check it worked

```bash
continuity version                          # the program you just installed
curl -s localhost:37777/api/health          # what the running server reports
```

In the health output, three things should be true:

- `"version"` matches what `continuity version` printed
- `"schema_current"` equals `"schema_head"` — the database is fully up to date
- `"vector_identity_locked"` is `false` — search is working

Then confirm your memories are still findable:

```bash
continuity search "something you know is stored"
```

If any of that looks wrong, see [Troubleshooting](guides/troubleshooting.md).

## What happens during that first start

1. **A safety copy is made — but only when the update rewrites existing saved
   records.** Updates that merely add a new field skip this, because they do not
   put existing data at risk. This copy is taken *before* the change is applied.
2. **The database format is updated** to match the new version. This happens the
   first time the database is opened, which is usually the server — but **any**
   command that reads the database will do it, including `continuity tree`.
3. **Automatic cleanup runs**: old tool-call records are deleted, and memory
   relevance is aged.

**The safety copy in step 1 does not cover step 3.** The cleanup is not a format
change, so nothing snapshots it. Your own backup is the only way to recover
deleted tool-call records.

## Version notes

### Upgrading to v0.11.0

**This release deletes old tool-call records on the first start.**

Every action your agent takes is recorded. Previous versions also retained the
tool's arguments and its response — depending on the tool, those responses could
include information such as command output or file contents. Nothing ever read
them.

**From v0.11.0 only the tool's name and timestamp are stored.** New records
carry no arguments or output at all. The cleanup below is what clears the old
ones.

They used to accumulate forever. On one normal single-user install they reached
331,966 records and about 1 GB in four and a half months. Your own numbers will
differ — this is an example of the scale, not a prediction.

**What gets deleted:** a record is removed once **both** are true — the session
that produced it has finished, **and** the record is at least 14 days old. Records
from sessions still running are never touched, at any age.

**What is not affected by this cleanup:** your memories, their search data, your
sessions, summaries, and the profile of how you work. None of those live in the
tool-call records.

<a name="keep-the-old-records"></a>
**To keep the old records**, set this **before** you restart. Pick one:

```bash
CONTINUITY_OBSERVATION_RETENTION_DAYS=off
```

```bash
CONTINUITY_OBSERVATION_RETENTION_DAYS=90
```

A bare assignment in your shell will not reach a background service. Put it where
the server will actually see it:

- **Service install** — add it to the service definition, then reload it. On
  macOS that is the `EnvironmentVariables` block of
  `~/Library/LaunchAgents/com.continuity.server.plist`; on Linux, an
  `Environment=` line in `~/.config/systemd/user/continuity.service`.
- **Started by hand** — `CONTINUITY_OBSERVATION_RETENTION_DAYS=off continuity serve`

If you set it to something Continuity cannot read, it turns the cleanup **off**
rather than guessing.

**Getting the disk space back.** Deleting records does not shrink the file — the
file stays the same size and Continuity reuses the empty space inside it. To
return the space to your disk:

```bash
continuity prune
```

```
Pruned:   0 observation(s)
Database: 350.2 MB → 43.6 MB (freed 306.6 MB)
```

`Pruned: 0` next to a large reclaim is normal, not a failure — the first start
already deleted the records, leaving only empty space to reclaim. This rewrites
the whole file, so it needs about as much free disk as the database currently
uses, and can take a few minutes.

### Other v0.11.0 changes

- **A new default search method.** New installs get a built-in one that matches on
  meaning rather than shared words. **Existing installs keep whatever they already
  had** — nothing is switched automatically. See [Embedding
  backends](guides/embedders.md).
- **`~/.continuity/config.toml` is now actually read** by the server. If you put
  settings in that file previously and nothing happened, they will start taking
  effect. Check the file before you restart. See
  [Configuration](guides/configuration.md).
- **`continuity import` was removed.** It was never implemented.
- **The `[hooks]` section of `config.toml` was removed.** It was never read — real
  hook settings live in `~/.claude/settings.json`. Leaving the old block in your
  file is harmless; unknown sections are ignored.

## Rolling back

Rolling back means restoring **both** the database and the program. Restoring the
database alone does not work: the newer program will simply update the format
again on first use, and re-run the same cleanup.

**1. Stop the server** — see [Step 1](#step-1--back-up).

**2. Install the previous version.** Download the release you were on from
[the releases page](https://github.com/lazypower/continuity/releases) and put it
back on your `PATH`.

**3. Restore the database.** Use your own backup if you have one:

```bash
cp ~/continuity-backup.db ~/.continuity/continuity.db
```

Or an automatic safety copy, if the upgrade you are undoing rewrote existing
records:

```bash
continuity snapshot list
```

```
~/.continuity/snapshots/continuity.db/
  continuity-pre-v9-2026-06-20T20-00-06Z.db   (auto-deletes after 2 more starts)
```

The `v9` in the filename is the database format the copy was taken *before*.
Copy the one you want into place:

```bash
cp ~/.continuity/snapshots/continuity.db/continuity-pre-v9-2026-06-20T20-00-06Z.db \
   ~/.continuity/continuity.db
```

**4. Start the server.**

Two limits worth knowing. Automatic copies are deleted after three successful
server starts — a "start" only counts when the server actually comes up and
begins listening, so ordinary commands do not burn through them, but they are
still short-lived. And they are only ever taken before format changes that rewrite
existing records. **Neither is a backup.** Take your own.

## Which upgrades are tested

Every release runs real databases, created by real previously released versions,
forward to the current format:

| Upgrading from | What it exercises |
|---|---|
| v0.1.0 – v0.2.2 | The longest path, crossing both updates that rewrite existing records |
| v0.4.0 | An intermediate format |
| v0.5.0 | The most recent format before the current run, including the case where the format update and the first cleanup happen in the same start |

**v0.1.0 is the oldest tested starting point.** If you are on something older
there is no tested path — back up, then upgrade to a recent version and check the
[Step 4](#step-4--check-it-worked) results carefully before continuing. If
anything looks wrong, [open an
issue](https://github.com/lazypower/continuity/issues).

---

**Next:** [Keeping it healthy](guides/keeping-it-healthy.md)
**See also:** [Troubleshooting](guides/troubleshooting.md) · [Configuration](guides/configuration.md)
