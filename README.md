# S3 Compatibility Test Vectors

Language-independent test vectors for S3 API compatibility, per
[cloud-portable/storage#12](https://github.com/cloud-portable/storage/issues/12).
Vectors are JSON, formally defined by [`schema/vector.schema.json`](schema/vector.schema.json)
(JSON Schema draft 2020-12). This README is the normative companion for the parts a
schema cannot express: the placeholder grammar, the generated-data algorithm, matcher
semantics and runner outcome semantics.

```
├── schema
│   └── vector.schema.json    the schema
├── vectors
│   └── <group>.json          one file per feature group, each { "vectors": [...] }
├── packages
│   ├── js                    npm  @cloud-portable/s3vectors
│   ├── python                PyPI cloud-portable-s3vectors
│   ├── go                    github.com/cloud-portable/s3vectors/packages/go
│   └── rust                  crates.io cloud-portable-s3vectors
├── scripts
│   ├── datagen.js            reference data generator        (node datagen.js --self-test)
│   ├── validate.js           schema + lint validation        (node validate.js)
│   └── sync-packages.js      sync vectors into packages/     (--check for drift)
└── docs
    ├── conversion-report.md  provenance: what was converted, excluded and merged
    └── releasing.md          lockstep release checklist for the packages
```

## Language packages

The corpus ships as installable packages so runner authors never touch this
repo's layout: each package exposes the parsed vectors (per group or all at
once) plus a port of the deterministic data generator — and deliberately
nothing else (no assertions, no matcher engine, no HTTP). See each package's
README for usage:

| Language | Install | Package |
|---|---|---|
| JavaScript | `npm i @cloud-portable/s3vectors` | [packages/js](packages/js) |
| Python | `pip install cloud-portable-s3vectors` | [packages/python](packages/python) |
| Go | `go get github.com/cloud-portable/s3vectors/packages/go` | [packages/go](packages/go) |
| Rust | `cargo add cloud-portable-s3vectors` | [packages/rust](packages/rust) |

The package version identifies the corpus snapshot and is identical across all
four. Each package also ships `schema/vector.schema.json` alongside its data so
the vector files' relative `$schema` links resolve. Vector JSON inside
`packages/` is synchronized from `vectors/` by `node scripts/sync-packages.js`
— never edit it by hand.

## Vector basics

Every vector has:

- **`id`** — stable, of the form `<group>-<NNNN>` (e.g. `multipart-0007`). Numbers are
  monotonic per group and are never reused or renumbered; use ids in runner skip-lists.
- **`group`** — the feature group this vector belongs to; equals the id prefix and the
  name of the file (`<group>.json`) that carries it.
- **`kind`** — `"api"` (a server round-trip test) or `"signing"` (an offline SigV4
  signing-algorithm test, from the AWS SigV4 test suite).
- **`title`** / optional **`description`** — human-readable, for debugging.
- **`tags`** — overlapping categories. Exactly one **tier tag** is required:
  - `tier-1` — exercises only [Tier 1: Core](https://github.com/cloud-portable/storage/blob/main/tier-1.yaml) operations
  - `tier-2` — additionally exercises [Tier 2: Control Plane](https://github.com/cloud-portable/storage/blob/main/tier-2.yaml) operations
  - `tier-3` — everything else

  A vector's tier is the *maximum* tier required by its **step** operations
  (prerequisites are excluded — they are provisioned out-of-band by the runner).
  Raw-HTTP steps are classified by the operation they emulate. Signing vectors are
  `tier-3`: the tiers are defined over the S3 operation lists and signing is not an
  operation (signing vectors also never execute against the server under test).

  Other conventional tags: the feature group (`multipart`, `versioning`, …), provenance
  (`source:ceph-s3-tests`, `source:msst-s3`, `source:aws-sigv4-suite`,
  `source:storage-test`, `source:aws-docs` for a vector written from the AWS API
  reference rather than converted from a suite), `large` for a vector whose data
  runs to gigabytes (the 5 GiB copy-source limit needs a source over 5 GiB), so a
  routine run can skip it by tag, quirk markers, and free-form compliance overlays
  (`soc2`). `large` is required on any vector declaring a dataset over 64 MiB, and
  the validator enforces it: the package test suites skip generating a dataset that
  big, so the tag is what keeps a gigabyte-scale vector from losing that coverage
  silently.
  Quirk markers share the `quirk:` prefix and flag behavior a general-purpose AWS S3
  endpoint does not reproduce, so a target tracking AWS filters them by prefix (see
  `quirk:*` filtering in the runner packages): `quirk:not-aws` (a non-AWS implementation
  deviates from AWS S3 semantics), `quirk:directory-bucket` (real AWS behavior, but only
  on S3 Express One Zone directory buckets — e.g. the `x-amz-if-match-size` and
  `x-amz-if-match-last-modified-time` conditional-delete headers), and
  `quirk:us-east-1-legacy` (us-east-1 legacy CreateBucket behavior a raw request
  cannot get elsewhere: a bare CreateBucket with no LocationConstraint succeeds,
  and recreating your own bucket returns 200).
- **`source`** — optional URL (typically a github permalink with line number) of the
  original test this vector was converted from, so conversion errors can be checked
  when a vector fails.

## `api` vectors

```jsonc
{
  "id": "object-crud-0042",
  "group": "object-crud",
  "kind": "api",
  "title": "GetObject on a missing key returns NoSuchKey",
  "tags": ["tier-1", "object-crud", "errors", "source:storage-test"],
  "source": "https://github.com/olizilla/storage-test/blob/main/tests.json#L173",
  "prerequisites": [{ "$bucket": { "handle": "b1" } }],
  "steps": [
    {
      "$operation": {
        "name": "GetObject",
        "params": { "Bucket": "${res.b1.name}", "Key": "missing-ghost-file.bin" },
        "expect": { "status": 404, "error": "NoSuchKey" }
      }
    }
  ]
}
```

### Prerequisites

Conditions the runner must establish **before step 1**. If a prerequisite cannot be
established, the vector's outcome is **`blocked`** — distinct from `fail` — so a broken
`CreateBucket` doesn't masquerade as hundreds of broken object tests. A prerequisite is
a **keyed union**: an object with exactly one of the keys `$bucket`, `$object` or
`$credential`. Each declares a `handle`; steps reference resource attributes as
`${res.<handle>.<attr>}`.

| key | fields | attributes |
|---|---|---|
| `$bucket` | `handle`, `versioning?` (`Enabled`/`Suspended`), `objectLock?` (bool) | `name` (runner-chosen) |
| `$object` | `handle`, `bucket` (a `$bucket` handle), `key`, `body?`, `contentType?`, `metadata?` | `key`, `etag`, `versionId` |
| `$credential` | `handle` (a second, distinct identity) | `accessKeyId`, `canonicalId`, `displayName` |

The primary identity `main` always exists and needs no prerequisite. Cleanup/teardown
is entirely the runner's responsibility.

### Steps

Steps run strictly sequentially; a failing step aborts the remaining steps of that
vector only. A step is a **keyed union**: an object with exactly one of the keys
`$operation` or `$http` (consistent with the `$data`/`$base64` discriminators).

**`$operation` step** — the default. `name` is the exact AWS S3 API operation name;
`params` uses the AWS API model member names (as in the SDKs):

```jsonc
{
  "$operation": {
    "name": "UploadPart",
    "params": { "Bucket": "${res.b1.name}", "Key": "k", "UploadId": "${cap.uploadId}",
                "PartNumber": 1, "Body": { "$data": "part1" } },
    "identity": "main",                  // optional; default "main"
    "presign": { "expiresIn": 300 },     // optional; execute via a runner-minted presigned URL
    "capture": { "etag1": "ETag" },      // optional; save response values for later steps
    "expect": { }                        // optional; omitted = the step must simply succeed
  }
}
```

**`$http` step** — raw-HTTP escape hatch for wire-level and malformed-request tests:

```jsonc
{
  "$http": {
    "method": "PUT",
    "path": "/${res.b1.name}/k",
    "query": { "partNumber": "1" },
    "headers": { "content-md5": "not-valid-base64" },
    "body": "hello",
    "sign": true,                        // default true: runner SigV4-signs with the step identity.
                                         // false: send byte-literal (malformed-auth tests).
    "expect": { "status": 400, "error": "InvalidDigest" }
  }
}
```

Vectors never contain live credentials; signing is always performed by the runner with
runner-supplied credentials.

- **`identity`** on any step: `"main"` (default) | `"anonymous"` (unsigned request) |
  `"invalid"` (well-formed signature, unknown access key) | the handle of a
  `credential` prerequisite.
- **`capture`**: map of name → path into the parsed API-model response (`$operation`
  steps) or into `{status, headers}` (`$http` steps). Path grammar:
  `ident ("." ident | "[" digits "]")*`, e.g. `UploadId`, `Contents[0].Key`,
  `headers.etag`. Captured values are available to **later** steps as `${cap.<name>}`.

### Placeholders

```
placeholder = "${" namespace "." path "}"
namespace   = "env" | "res" | "cap" | "data"
```

Interpolation applies to every JSON string value inside `prerequisites` and `steps` of
`api` vectors (and nowhere in `signing` vectors). Namespaces:

- `env` — runner context: `${env.endpoint}`, `${env.region}`. Secrets are never
  referenceable.
- `res` — prerequisite resource attributes: `${res.b1.name}`.
- `cap` — values captured by earlier steps: `${cap.uploadId}`.
- `data` — derived values of declared datasets (see below): `${data.big.md5}`.

**Escaping:** write `$${` to emit a literal `${`. Any other `$` is literal as-is
(`"cost: $5"` needs no escaping). An unresolvable placeholder is a vector-definition
error: the runner must error, never send the raw text. Placeholders always substitute
to strings.

### Generated data

Large payloads are declared, not inlined, under `data` (name → spec):

```jsonc
"data": {
  "big":   { "$prng": { "seed": "multipart-0001/big", "size": 10485760 } },
  "part1": { "$slice": { "of": "big", "offset": 0, "length": 5242880 } },
  "aaa":   { "$pattern": { "pattern": "A", "size": 5242880 } }
}
```

Each dataset is a **keyed union**: an object with exactly one of the keys `$prng`,
`$pattern` or `$slice`.

- **`$prng`** — the byte stream is SHA-256 in counter mode (normative; reproducible
  byte-for-byte in any language):

  ```
  block(i) = SHA256( UTF8(seed) || BE64(i) )     BE64 = 8-byte big-endian counter, i = 0,1,2,…
  stream   = block(0) || block(1) || …
  data     = stream[0 : size]
  ```

  Seed convention: `"<vector-id>/<name>"` (guarantees distinct data per vector).
- **`$pattern`** — the pattern bytes (`pattern` UTF-8, or `patternBase64`) repeated and
  truncated to `size`.
- **`$slice`** — a byte range `[offset, offset+length)` of another `$prng`/`$pattern`
  entry. Chained slices are not allowed.

Datasets are referenced two ways:

1. As bytes, via the **content descriptor** `{ "$data": "part1" }` — usable wherever a
   body is expected (params, object prerequisites, expected bodies). The other content
   descriptor forms are a plain string (UTF-8) and `{ "$base64": "…" }` for small
   inline binary.
2. As derived string values in placeholders:
   `${data.<name>.size}`, `.md5` (lowercase hex), `.etag` (the MD5 hex wrapped in
   quotes — a single-part S3 ETag), `.sha256` (hex), `.sha256B64`, `.sha1B64`,
   `.crc32B64`, `.crc32cB64`, `.crc64nvmeB64` (base64 forms as used by
   `x-amz-checksum-*`). Slices support all derived values — that's how per-part
   checksums and ranged-GET digests are written. Multipart *composite* ETags have no
   derived value; assert them with a pattern, e.g. `{ "$matches": "-2\"$" }`.

`scripts/datagen.js` is the reference implementation (`--self-test` includes
independently computed check values).

### Expectations

```jsonc
"expect": {
  "status": 206,                                   // exact HTTP status
  "error": "PreconditionFailed",                   // S3 error code; or { "code", "message" }
  "headers": { "content-range": "bytes 0-4/10",    // lowercase names → matchers
               "x-amz-request-id": { "$exists": true } },
  "response": { "ContentLength": 5,                // $operation steps only: subset match
                "Contents": [ { "Key": "a" } ] },  //   against the parsed API-model response
  "body": { "$data": "part1" }                     // exact bytes, or digest assertion
}
```

All keys optional. Presence of `error` means the step is *expected* to fail; an
omitted/empty `expect` means the step must succeed (no error, 2xx).

**Matcher semantics** (uniform everywhere a matcher appears):

- scalar literal → exact equality
- object literal (no `$`-prefixed keys) → recursive **subset** match: listed fields
  must match, extra actual fields are ignored
- array literal → exact length AND ordered element-wise match (this is the
  list-ordering assertion)
- assertion object (ALL keys `$`-prefixed): `{"$exists": true}`, `{"$absent": true}`,
  `{"$eq": v}` (escape hatch for literals that would parse as assertions),
  `{"$ne": v}` (scalar inequality, after placeholder interpolation — e.g. "this ETag
  differs from the captured one"), `{"$matches": "regex"}` (unanchored; patterns use
  the portable subset valid in both ECMA-262 and RE2 — no lookaheads, lookbehinds or
  backreferences — so native regex engines work in every language, including Go's
  `regexp` and Rust's `regex`), `{"$length": n}` (arrays/strings),
  `{"$contains": matcher}` (some array element matches — unordered membership).
  When an assertion object has multiple `$`-keys, ALL of them must hold (AND) —
  e.g. `{"$ne": "${cap.singleEtag}", "$matches": "-"}`.

`body` is either a content descriptor (exact byte equality) or a digest assertion
`{"$size": n, "$md5": "hex", "$sha256": "hex"}` (any subset, ANDed).

## `signing` vectors

Offline AWS SigV4 signing-algorithm tests (converted from the
[AWS SigV4 test suite](resources/aws-sig-v4-test-suite)). No server round-trip, no
placeholder interpolation. The embedded credentials are the suite's published dummy
values — never real secrets.

```jsonc
{
  "id": "signing-0011",
  "group": "signing",
  "kind": "signing",
  "title": "SigV4: query parameters sorted by key, case-sensitive",
  "tags": ["tier-3", "signing", "source:aws-sigv4-suite"],
  "request": {
    "method": "GET",
    "uri": "/?Param2=value2&Param1=value1",          // raw request-target, UN-normalized
    "headers": [["Host", "example.amazonaws.com"],   // ordered [name, value] pairs:
                ["X-Amz-Date", "20150830T123600Z"]], //   duplicates/order/multiline are under test
    "body": ""                                       // optional
  },
  "credentials": { "accessKeyId": "AKIDEXAMPLE",
                   "secretAccessKey": "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" },
  "scope": { "dateTime": "20150830T123600Z", "region": "us-east-1", "service": "service" },
  "expect": {
    "canonicalRequest": "GET\n/\nParam1=value1&Param2=value2\n…",
    "stringToSign": "AWS4-HMAC-SHA256\n20150830T123600Z\n…",
    "authorization": "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/…, Signature=…",
    "signedRequest": "GET /?Param2=value2&Param1=value1 HTTP/1.1\n…"   // optional
  }
}
```

Note (from the AWS suite): for Amazon **S3** specifically, URI paths must NOT be
normalized when canonicalizing; the `normalize-path` vectors test the generic
(non-S3) rules.

## Runner outcome semantics

Four outcomes per vector:

- **`pass`** — all prerequisites established, all steps met their expectations
- **`fail`** — a step violated its expectations
- **`blocked`** — a prerequisite could not be established (the vector was not runnable;
  look for the failure in the vectors covering that prerequisite's operations)
- **`skipped`** — excluded by an id or tag filter

These outcomes map cleanly onto standard test-report formats (JUnit XML, CTRF,
TAP), so existing CI integrations and HTML report generators work out of the
box — see [docs/reporting.md](docs/reporting.md) for the recommended mappings.

## Out of scope (v1)

Not expressible by design: concurrency/race tests; time-dependent behavior (lifecycle
*execution*, object-lock retention elapse, presigned-URL expiry, sleeps); server- or
account-side provisioning beyond the three prerequisite types (KMS keys, STS/IAM,
replication targets, notification destinations, logging targets, S3 Select, access
points); POST-policy (browser form) uploads; `aws-chunked` streaming signing as a
first-class feature; presigned-URL negative tests (expired/tampered); composite
multipart ETag as a derived data value; cross-vector shared prerequisites and
teardown.

## Worked example: multipart with capture + generated data

```json
{
  "id": "multipart-0001",
  "group": "multipart",
  "kind": "api",
  "title": "Two-part multipart upload with full and ranged read-back",
  "tags": ["tier-1", "multipart", "source:msst-s3"],
  "prerequisites": [{ "$bucket": { "handle": "b1" } }],
  "data": {
    "big":   { "$prng":  { "seed": "multipart-0001/big", "size": 10485760 } },
    "part1": { "$slice": { "of": "big", "offset": 0,       "length": 5242880 } },
    "part2": { "$slice": { "of": "big", "offset": 5242880, "length": 5242880 } }
  },
  "steps": [
    { "$operation": {
        "name": "CreateMultipartUpload",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin" },
        "capture": { "uploadId": "UploadId" } } },
    { "$operation": {
        "name": "UploadPart",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin",
                    "UploadId": "${cap.uploadId}", "PartNumber": 1, "Body": { "$data": "part1" } },
        "capture": { "etag1": "ETag" },
        "expect": { "response": { "ETag": "${data.part1.etag}" } } } },
    { "$operation": {
        "name": "UploadPart",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin",
                    "UploadId": "${cap.uploadId}", "PartNumber": 2, "Body": { "$data": "part2" } },
        "capture": { "etag2": "ETag" } } },
    { "$operation": {
        "name": "CompleteMultipartUpload",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin",
                    "MultipartUpload": { "Parts": [
                      { "PartNumber": 1, "ETag": "${cap.etag1}" },
                      { "PartNumber": 2, "ETag": "${cap.etag2}" } ] } },
        "expect": { "response": { "Key": "mp/two-parts.bin", "ETag": { "$matches": "-2\"$" } } } } },
    { "$operation": {
        "name": "GetObject",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin" },
        "expect": { "status": 200,
                    "response": { "ContentLength": 10485760 },
                    "body": { "$size": 10485760, "$md5": "${data.big.md5}" } } } },
    { "$operation": {
        "name": "GetObject",
        "params": { "Bucket": "${res.b1.name}", "Key": "mp/two-parts.bin",
                    "Range": "bytes=5242880-10485759" },
        "expect": { "status": 206,
                    "headers": { "content-range": "bytes 5242880-10485759/10485760" },
                    "body": { "$data": "part2" } } } }
  ]
}
```

## Validation

```
cd scripts
npm install
node validate.js             # schema + lint (ids, handles, placeholders, refs)
node validate.js --digests   # also materialize datasets / verify derived values
node datagen.js --self-test  # data-generation reference check values
```
