# AGENTS.md

Guidance for coding agents working in this repository. The
[README](README.md) is the **normative spec** for the vector format
(placeholders, matchers, generated data, runner outcomes) — read it before
touching vectors or the schema.

## What this repo is

Language-independent S3 API compatibility **test vectors** (JSON), a JSON
Schema defining them, and four installable packages (npm / PyPI / Go module /
crates.io) that ship the parsed vectors plus a deterministic test-data
generator. It deliberately contains **no test runner**: no assertion engine, no
matcher evaluation, no placeholder interpolation, no HTTP. Do not add runner
logic anywhere, especially not to `packages/`.

## Repo map

```
vectors/<group>.json     CANONICAL vector data (one file per feature group)
schema/vector.schema.json  structure (draft 2020-12); README covers semantics
scripts/validate.js      schema + lint validation      (node scripts/validate.js)
scripts/datagen.js       normative data-generator reference (--self-test)
scripts/sync-packages.js copies vectors+schema into packages, stamps versions
packages/{js,python,go,rust}/  the four distributions (lockstep version)
packages/VERSION         THE version (single source; stamped everywhere by sync)
docs/                    conversion provenance, releasing, results reporting
.conversion/             HISTORICAL conversion/dedup tooling and logs — do not
                         update it for format changes; it records how the corpus
                         was built (pre-migration formats) and never re-runs
resources/               cloned upstream test suites (source material) — read-only
```

## Golden rules

1. **`vectors/*.json` is the single source of truth.** The copies under
   `packages/*/data/`, `packages/{go,rust}/vectors/` and every
   `schema/vector.schema.json` inside packages are **generated** — never edit
   them by hand. After changing canonical vectors or the schema, run
   `node scripts/sync-packages.js`; CI fails on drift via `--check`.
2. **Vector ids are permanent.** `<group>-<NNNN>`, prefix = the vector's `group`
   (which equals the filename stem). Never
   reuse or renumber; deletions leave gaps; a new vector takes the next number
   in its file. Consumers key skip-lists on these ids.
3. **Keyed unions everywhere.** Steps: `{"$operation": {...}}` /
   `{"$http": {...}}` (op steps have `name`). Data specs: `$prng` / `$pattern` /
   `$slice`. Prerequisites: `$bucket` / `$object` / `$credential`. Content
   descriptors: plain string / `{"$data": name}` / `{"$base64": ...}`. Exactly
   one union key per object.
4. **Tags**: exactly one tier tag per vector (schema-enforced). Tier = max tier
   of the **step** operations (prerequisites excluded): tier-1 = core object
   ops, tier-2 = CreateBucket/DeleteBucket/ListBuckets, tier-3 = everything
   else. Plus the group tag and a `source:` provenance tag.
5. **Five datagen implementations must behave identically**:
   `scripts/datagen.js` (normative) and the ports in packages/js, python, go,
   rust. Any algorithm change must land in all five plus their shared
   check-value fixtures (prng seed "test" block hashes; CRCs of "123456789";
   md5("AAAAA")), and their shared invariants: a range equals the same window of
   the whole dataset, a stream concatenates to the same bytes, and a digest is the
   same at any chunk size. `sync-packages.js` does **not** copy the datagen
   sources — they are hand-maintained ports, so those suites are the only thing
   that detects a divergent one.
   Bodies ≥ 1 KiB in vectors use `data` specs, never inline strings; never
   hardcode digests of `$prng` data — use `${data.<name>.<field>}`.
6. **A format/schema change is a five-place change**: schema + README prose +
   the corpus (write an idempotent migration script, verify counts before ==
   after) + all four package models + their tests. Go's `DisallowUnknownFields`
   decode and Rust's `deny_unknown_fields` serde models are deliberate drift
   detectors — if they fail after your change, the models are out of sync with
   the corpus, not the other way around.
7. **`signing` vectors embed the published AWS SigV4 test-suite dummy
   credentials** (`AKIDEXAMPLE` / `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`).
   They are public documentation constants, not leaked secrets — do not redact
   or rotate them.
8. **`resources/` is upstream source material** (ceph/s3-tests, msst-s3, the
   AWS SigV4 suite, storage-test). Never modify it; vectors cite it via
   `source` permalinks pinned to specific commits.

## Commands

```sh
# corpus (run after ANY vector/schema change)
(cd scripts && npm ci)                # once; installs ajv for the validator
node scripts/validate.js              # schema + lint (ids, handles, ${...} refs)
node scripts/validate.js --digests    # + materialize all datasets (~4 min)
node scripts/datagen.js --self-test
node scripts/sync-packages.js         # propagate to packages
node scripts/sync-packages.js --check # drift gate (CI runs this)

# packages (each has a schemaSha256-integrity + strict-decode test)
(cd packages/js && npm test)                                # node:test, no deps
(cd packages/python && python3 -m unittest discover -s tests)
(cd packages/go && go vet ./... && go test ./...)
(cd packages/rust && cargo test && cargo test --no-default-features)
```

Notes: the Python corpus test caps dataset sizes (pure-python CRC is slow) —
`S3VECTORS_FULL=1` lifts that cap. No suite materializes a dataset over 64 MiB
whatever the env: those run to gigabytes, `validate.js` requires the vector to
carry the `large` tag, and the suites spot-check them with ranged reads (the ends
plus a window straddling each 32-bit boundary) against an independent statement of
the formula. Rust tests rely on `[profile.test]
opt-level = 2` in its Cargo.toml; don't remove it (hashing 2 GiB unoptimized
takes ~100 s).

## Common tasks

- **Add or fix a vector**: edit `vectors/<group>.json` (next free id + matching `group`, correct
  tier/group/source tags; follow README examples) → `node scripts/validate.js`
  → `node scripts/sync-packages.js` → run the four package suites.
- **Release**: bump `packages/VERSION`, sync, commit to `main` —
  `.github/workflows/release.yml` verifies, tags `vX.Y.Z` **and**
  `packages/go/vX.Y.Z` (the Go subdir module needs the path-prefixed tag),
  publishes via OIDC trusted publishing (crates.io last — immutable), and
  creates a GitHub Release. Details + first-publish bootstrap:
  [docs/releasing.md](docs/releasing.md).
- **CI**: `.github/workflows/ci.yml` runs the full command list above on every
  PR/push to main. Keep it green; there are no flaky tests — a failure is real.

## Style

- JSON: 2-space indent, trailing newline; vector files are written by scripts
  with stable key order — match it when hand-editing.
- JS is ESM in `packages/js`, CommonJS in `scripts/` (repo tooling). Keep
  dependencies minimal and deliberate: js/python/go have zero runtime deps;
  rust has only serde + serde_json unconditionally (hash crates sit behind the
  default `datagen` feature); `scripts/` uses ajv (dev-only). Don't add more.
- Keep package public APIs in lockstep across languages: `groups` / `load(group)`
  / `all()` / `manifest()` and `datagen.generate` / `generateRange` / `dataSize` /
  `derived`. The streaming form is language-shaped, so it is the one place the
  names differ: JS `generateStream` → a web `ReadableStream`, Python
  `generate_stream` → `Iterator[bytes]`, Go `NewReader`/`NewRangeReader` →
  `*Reader` (`io.Reader` + `io.Seeker`), Rust `Reader::new`/`Reader::range` →
  `impl Read`. JS and Python take a chunk size; Go and Rust take theirs from the
  caller's buffer.
