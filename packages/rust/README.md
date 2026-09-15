# cloud-portable-s3vectors

Language-independent [S3 API compatibility test vectors](https://github.com/cloud-portable/s3vectors),
embedded and parsed, importable per feature group or all at once, plus the
deterministic test-data generator (feature `datagen`, on by default). The crate
version identifies the corpus snapshot — the same version ships for JS, Python,
Go and Rust.

```sh
cargo add cloud-portable-s3vectors
# vectors only, no datagen deps:
cargo add cloud-portable-s3vectors --no-default-features
```

## Usage

```rust
use cloud_portable_s3vectors as s3v;

// everything
for file in s3v::all() {
    for v in &file.vectors { /* ... */ }
}

// a single group (lazy-parsed, cached)
let mp = s3v::group("multipart").unwrap();

// deterministic payloads + the digest values ${data.<name>.<field>} resolve to
use s3v::datagen::{generate, generate_range, derived, DerivedField, Reader};
let s3v::Vector::Api(v) = &mp.vectors[0] else { unreachable!() };
let data = v.data.as_ref().unwrap();
let part1 = generate(data, "part1")?;                       // Vec<u8>
let etag = derived(data, "big", DerivedField::Etag)?;       // "\"<md5hex>\""

// datasets are seekable: read a window, or stream one too big to hold
let window = generate_range(data, "big", 1024, 256)?;       // Vec<u8>
let reader = Reader::new(data, "big")?;                     // impl std::io::Read
```

`s3v::manifest()` reports the embedded corpus version, per-group counts and the
schema checksum.

## Notes

- Normative semantics — placeholder grammar, matcher semantics, prerequisite and
  runner-outcome rules — live in the
  [repository README](https://github.com/cloud-portable/s3vectors#readme).
- Models are strict (`deny_unknown_fields`); matcher-valued fields stay
  `serde_json::Value` — evaluating them is a runner concern.
- The `signing` group embeds the **published dummy credentials from the AWS SigV4
  test suite** (`AKIDEXAMPLE` / `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`) —
  public documentation constants, not secrets.

## What this crate is NOT

No test framework, no assertion helpers, no matcher engine, no placeholder
interpolation, no HTTP, no SigV4 signing. Those are runner concerns.

## License

Apache-2.0 OR MIT
