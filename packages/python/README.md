# cloud-portable-s3vectors

Language-independent [S3 API compatibility test vectors](https://github.com/cloud-portable/s3vectors),
parsed and importable per feature group, plus the deterministic test-data
generator. The package version identifies the corpus snapshot — the same
version ships for JS, Python, Go and Rust.

```sh
pip install cloud-portable-s3vectors
```

## Usage

```python
import cloud_portable_s3vectors as s3v

# everything
for file in s3v.load_all():
    for vector in file["vectors"]:
        ...

# a single group (lazy-loaded, cached — treat results as read-only)
mp = s3v.load("multipart")

# deterministic payloads + the digest values ${data.<name>.<field>} resolve to
from cloud_portable_s3vectors import datagen

v = next(v for v in mp["vectors"] if v["id"] == "multipart-0001")
part1 = datagen.generate(v["data"], "part1")       # bytes
etag = datagen.derived(v["data"], "big", "etag")   # '"<md5hex>"'

# datasets are seekable: read a window, or stream one too big to hold
window = datagen.generate_range(v["data"], "big", 1024, 256)   # bytes
for chunk in datagen.generate_stream(v["data"], "big"):        # Iterator[bytes]
    ...
```

`s3v.manifest()` reports the corpus version, per-group counts and the schema
checksum. The package is fully typed (`py.typed`, TypedDicts).

## Notes

- Normative semantics — placeholder grammar, matcher semantics, prerequisite and
  runner-outcome rules — live in the
  [repository README](https://github.com/cloud-portable/s3vectors#readme).
- Zero dependencies. CRC-32C and CRC-64/NVME use pure-Python tables, which are
  slow on multi-megabyte datasets — fine for test fixtures, but don't put them
  in a hot loop.
- The `signing` group embeds the **published dummy credentials from the AWS SigV4
  test suite** (`AKIDEXAMPLE` / `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`) —
  public documentation constants, not secrets.

## What this package is NOT

No test framework, no assertion helpers, no matcher engine, no placeholder
interpolation, no HTTP, no SigV4 signing. Those are runner concerns.

## License

Apache-2.0 OR MIT
