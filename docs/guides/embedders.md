[Docs](../README.md) › Guides › Embedding backends

# Embedding backends

How Continuity searches by meaning instead of by keyword, which engine you are
using, and how to change it safely.

**Audience:** operator · **Read time:** ~3 min

<p align="center">
  <img src="../assets/embedder-selection.svg" alt="How Continuity picks a search backend at startup: environment variable first, then the config file, then the method your existing memories were built with, then the built-in default for a fresh database." width="820" />
</p>


---

## What this is

When Continuity stores a memory, it also converts the text into a list of
numbers that captures roughly what the text *means*. Searching compares those
numbers instead of the words themselves. That is why searching for "database
locking" can find a memory about "SQLite write contention" even though the two
share no words.

The component that does that conversion is the **embedding backend**. Continuity
ships with three, and you almost certainly do not need to change the one you have.

## The three options

| Backend | Name in commands | Setup | Finds meaning? |
|---|---|---|---|
| **Built-in semantic** (default) | `model2vec` | none | yes — downloads a ~33 MB model file once, then runs inside Continuity |
| **Ollama** | `ollama` | install [Ollama](https://ollama.com), then `ollama pull nomic-embed-text` | yes, best quality — needs Ollama running |
| **Keyword fallback** | `hashtf` | none | no — matches shared words only; used automatically if the download fails |

**New installs get the built-in semantic backend.** The first time it is used it
downloads a model file to `~/.continuity/models/`. If you are offline on first
run, Continuity falls back to keyword matching for that session and tells you so.

**Existing installs are never switched automatically.** If you have been running
Ollama, you keep running Ollama. Continuity looks at what your database was
already built with and keeps using it. Nothing migrates behind your back, and it
will never quietly downgrade you to a weaker engine.

## Check what you are using

```bash
continuity embedder status
```

```
  config [embedder].backend: model2vec
  resolved active embedder:  model2vec:potion-retrieval-32M:512
  corpus declared identity:  model2vec:potion-retrieval-32M:512
  match:                     yes
  running server embedder:   model2vec:potion-retrieval-32M:512
  running server locked:     no
```

The line that matters is **`match`**. It compares the engine Continuity is set to
use against the engine your existing memories were built with. You want `yes`.

## Why a mismatch turns search off

Two different engines produce numbers that are not comparable — like comparing a
temperature in Celsius against one in Fahrenheit without converting. Scoring
across them does not produce bad results so much as meaningless ones.

So when Continuity notices a mismatch, **it disables search and says so** rather
than returning answers it cannot stand behind. You will see:

```
vector identity mismatch: the corpus was embedded with ollama:nomic-embed-text:768
but the active embedder is model2vec:potion-retrieval-32M:512. Search is disabled
to avoid comparing across vector spaces.
```

While search is off, everything else keeps working — memories are still captured,
the tree still browses, your profile still loads. New memories are stored without
search entries and get filled in once you resolve the mismatch. **Nothing is
lost.**

The usual way to end up here is editing the setting by hand, or setting the
`CONTINUITY_EMBEDDER` environment variable. Simply installing Ollama does **not**
do this — Continuity matches whatever your memories were built with and will not
switch you on its own.

## Switching backends

There is exactly one command that changes engines *and* rebuilds your existing
memories to match:

```bash
continuity embedder use ollama       # or: model2vec, hashtf
continuity restart
```

It checks the new engine actually works before changing anything, takes a safety
copy of your database into `~/.continuity/snapshots/`, rebuilds every memory's
search entry, and saves the setting. On a large memory tree this takes a few
minutes.

Confirm it worked — you want `match: yes` again:

```bash
continuity embedder status
```

That safety copy is a full database copy and nothing expires it. Once you are
satisfied, reclaim the space with `continuity snapshot list` and
`continuity snapshot prune`.

> **Editing `config.toml` by hand does not do this.** It changes which engine
> Continuity picks on next start, but leaves your existing memories built with
> the old one — which produces exactly the mismatch above. Use `embedder use`.

## Which should you pick?

**Stay on the default** unless you have a reason. It needs no service, no
configuration, and finds meaning rather than keywords.

**Move to Ollama** if search quality matters more to you than running one more
background service. On our test set it is meaningfully better at finding a
memory from a paraphrase.

**The keyword fallback** is a safety net, not a choice — it exists so a fresh
install works offline. It cannot find a memory unless your query shares actual
words with it.

---

**Next:** [Keeping it healthy](keeping-it-healthy.md)
**See also:** [Configuration](configuration.md) · [Troubleshooting](troubleshooting.md) · [Vector identity](../advanced/vector-identity.md)
