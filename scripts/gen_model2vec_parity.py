#!/usr/bin/env python3
"""gen_model2vec_parity.py — regenerate the model2vec parity fixture.

Generates internal/engine/testdata/model2vec/parity.json from the REAL
model2vec Python package, so the Go WordPiece tokenizer + mean-pooling
reimplementation can be checked byte-exact against ground truth.

Usage:
    pip install model2vec
    python3 scripts/gen_model2vec_parity.py

This is an offline, on-demand generator — it is NOT run as part of `go test`.
The committed fixture (internal/engine/testdata/model2vec/parity.json) is
what the Go test actually reads, so this script only needs to be re-run if
the battery of texts changes or the pinned model revision changes.
"""

import json
import os
import sys

from model2vec import StaticModel

MODEL_NAME = "minishlab/potion-retrieval-32M"

# Battery of texts covering: a corpus-representative sentence, plain words,
# subword-triggering words (uncommon compounds not in vocab as whole tokens),
# punctuation-heavy queries, PII in two formats (hyphen vs space groups),
# mixed case, accented Latin, CJK, and degenerate/empty inputs.
BATTERY = [
    "devbox",
    "snapshotting",
    "kubernetes",
    "unsandboxed",
    "what are the two git remotes?",
    "origin/main",
    "555-123-4567",
    "555 123 4567",
    "DevBox VS devbox",
    "café résumé",
    "snapshot 快照 test",
    "",
    "   ",
    "!!!",
    "Always use devbox run for go commands in labdns",
]


def main():
    out_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "internal", "engine", "testdata", "model2vec",
    )
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "parity.json")

    print(f"Loading {MODEL_NAME} via model2vec...", file=sys.stderr)
    model = StaticModel.from_pretrained(MODEL_NAME)

    # Sanity: these must all be absent for the "no runtime reweight" assumption
    # the Go loader hard-asserts on. Fail loudly if a future model version adds
    # any of them, so the fixture is never generated silently out of step with
    # the Go loader's fail-closed assertion.
    assert model.weights is None, "weights present — Go loader assumption violated"
    assert model.token_mapping is None, "token_mapping present — Go loader assumption violated"
    assert model.vocabulary_quantization is None, "vocabulary_quantization present — Go loader assumption violated"
    assert model.unk_token_id == 1, f"unexpected unk_token_id: {model.unk_token_id}"
    assert model.dim == 512, f"unexpected dim: {model.dim}"

    fixture = {}
    for text in BATTERY:
        ids = model.tokenize([text])[0]
        # model2vec's tokenize() already strips [UNK] (see model2vec source:
        # StaticModel.tokenize calls the HF tokenizer then filters unk_token_id
        # before returning) — so these ids are POST-FILTER, matching what the Go
        # embedder must produce after its own UNK removal.
        ids = [int(i) for i in ids if i != model.unk_token_id]
        vec = model.encode([text])[0].tolist()
        fixture[text] = {"ids": ids, "vec": vec}
        print(f"  {text!r:50s} -> {len(ids)} ids", file=sys.stderr)

    with open(out_path, "w") as f:
        json.dump(fixture, f, indent=0)

    print(f"Wrote {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
