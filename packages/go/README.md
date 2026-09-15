# s3vectors (Go)

Language-independent [S3 API compatibility test vectors](https://github.com/cloud-portable/s3vectors),
embedded and parsed, importable per feature group or all at once, plus the
deterministic test-data generator. The module version identifies the corpus
snapshot — the same version ships for JS, Python, Go and Rust.

```sh
go get github.com/cloud-portable/s3vectors/packages/go
```

## Usage

```go
import (
    s3vectors "github.com/cloud-portable/s3vectors/packages/go"
    "github.com/cloud-portable/s3vectors/packages/go/datagen"
)

// everything
files, err := s3vectors.All()

// a single group (lazy-parsed, cached — treat results as read-only)
mp, err := s3vectors.Group("multipart")

// deterministic payloads + the digest values ${data.<name>.<field>} resolve to
v := &mp.Vectors[0]
part1, err := datagen.Generate(v.Data, "part1")        // []byte
etag, err := datagen.Derived(v.Data, "big", "etag")    // "\"<md5hex>\""

// datasets are seekable: read a window, or stream one too big to hold
window, err := datagen.GenerateRange(v.Data, "big", 1024, 256)  // []byte
r, err := datagen.NewReader(v.Data, "big")                      // io.Reader + io.Seeker
```

`s3vectors.Manifest()` reports the embedded corpus version, per-group counts and
the schema checksum.

## Notes

- Normative semantics — placeholder grammar, matcher semantics, prerequisite and
  runner-outcome rules — live in the
  [repository README](https://github.com/cloud-portable/s3vectors#readme).
- Matcher-valued fields (`Expect.Response`, `Expect.Headers` values, step
  `Params` values, …) are `json.RawMessage`: their evaluation semantics belong
  to runners, not this module.
- The `signing` group embeds the **published dummy credentials from the AWS SigV4
  test suite** (`AKIDEXAMPLE` / `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`) —
  public documentation constants, not secrets.
- Stdlib-only; no dependencies.

## What this module is NOT

No test framework, no assertion helpers, no matcher engine, no placeholder
interpolation, no HTTP, no SigV4 signing. Those are runner concerns.

## License

Apache-2.0 OR MIT
