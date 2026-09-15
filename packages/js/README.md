# @cloud-portable/s3vectors

Language-independent [S3 API compatibility test vectors](https://github.com/cloud-portable/s3vectors),
parsed and importable per feature group, plus the deterministic test-data generator.
The package version identifies the corpus snapshot — the same version number ships
for JS, Python, Go and Rust.

```sh
npm install @cloud-portable/s3vectors
```

## Usage

```js
// everything
import { all, load, groups, manifest } from '@cloud-portable/s3vectors'
for (const file of all()) {
  for (const vector of file.vectors) { /* ... */ }
}

// a single group (lazy-loaded, cached)
const multipart = load('multipart')

// or the raw JSON directly
import signing from '@cloud-portable/s3vectors/groups/signing.json' with { type: 'json' }
```

Vectors that need large payloads declare deterministic datasets instead of inlining
bytes. The `datagen` module materializes them and computes the digest values that
`${data.<name>.<field>}` placeholders resolve to:

```js
import { generate, generateRange, generateStream, derived } from '@cloud-portable/s3vectors/datagen'

const v = multipart.vectors.find(v => v.id === 'multipart-0001')
const part1 = generate(v.data, 'part1')          // Buffer
const md5 = derived(v.data, 'big', 'md5')        // lowercase hex
const etag = derived(v.data, 'big', 'etag')      // '"<md5hex>"'
```

Datasets are seekable. Read a window without materializing the rest, or stream a
dataset too big to hold in memory — the stream is a web `ReadableStream`, so it can
go straight into `fetch` as a request body:

```js
const window = generateRange(v.data, 'big', 1024, 256)   // Buffer, 256 bytes
const body = generateStream(v.data, 'big')               // ReadableStream<Buffer>
await fetch(url, { method: 'PUT', body, duplex: 'half' })
```

TypeScript types for the full vector model are included.

## Notes

- Normative semantics — placeholder grammar, matcher semantics, prerequisite and
  runner-outcome rules — live in the
  [repository README](https://github.com/cloud-portable/s3vectors#readme). This
  package intentionally does not restate them.
- The `signing` group embeds the **published dummy credentials from the AWS SigV4
  test suite** (`AKIDEXAMPLE` / `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`). They
  are public documentation constants, not secrets — allowlist them in secret
  scanners.

## What this package is NOT

No test framework, no assertion library, no matcher engine, no placeholder
interpolation, no HTTP, no SigV4 signing. Those are runner concerns — build your
runner however you like and consume the vectors from here.

## License

Apache-2.0 OR MIT
