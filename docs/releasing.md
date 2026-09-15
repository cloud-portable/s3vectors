# Releasing the language packages

All four packages (npm, PyPI, crates.io, Go module) release in **lockstep**: one
version number identifies the corpus snapshot. `packages/VERSION` is the only
place the version is authored; everything else is stamped by the sync script.

Version bumps: adding vectors = minor; adding public API surface = minor; fixing
a vector's expectation = patch or minor by judgment; schema/model breaking change
= major (note: a 2.0 requires the Go module path to become `.../packages/go/v2`).

## Normal path (automated)

Releases are automated by [`.github/workflows/release.yml`](../.github/workflows/release.yml),
which fires when `packages/VERSION` changes on `main`:

1. Edit `packages/VERSION`.
2. `node scripts/sync-packages.js` (copies vectors + manifest, stamps versions).
3. Commit and merge to `main`.

The workflow then: verifies everything (corpus checks + all four test suites) →
pushes tags `vX.Y.Z` **and** `packages/go/vX.Y.Z` (the path-prefixed tag *is*
the Go module release) → publishes npm and PyPI → publishes crates.io last
(it is immutable) → creates a GitHub Release. If the tag already exists the
workflow no-ops, so re-runs (via *Run workflow* / `workflow_dispatch`) are safe;
npm/PyPI tolerate re-publishing an existing version (`skip-existing` on PyPI).

### Publishing auth: OIDC trusted publishing (no tokens in secrets)

One-time setup per registry, tied to repo `cloud-portable/s3vectors` and
workflow `release.yml`:

| Registry | Where | When |
|---|---|---|
| PyPI | Project → Publishing → add a **pending** trusted publisher for `cloud-portable-s3vectors` | Before the first release — pending publishers work for the very first publish |
| npm | Package settings → Trusted publisher → GitHub Actions | **After** the first publish (npm requires the package to exist) |
| crates.io | Crate → Settings → Trusted Publishing | **After** the first publish (crates.io requires the crate to exist) |

**First-publish bootstrap:** the very first `npm publish --access public` and
`cargo publish` must be run manually (commands below), then configure the
trusted publishers so every subsequent release is hands-off. Go needs no
registry setup ever.

## Manual fallback

The full checklist, equivalent to what the workflow does:

1. Edit `packages/VERSION` → `node scripts/sync-packages.js` → commit.
2. Verify:

   ```sh
   node scripts/validate.js
   node scripts/datagen.js --self-test
   node scripts/sync-packages.js --check
   (cd packages/js && npm test)
   (cd packages/python && python3 -m unittest discover -s tests)
   (cd packages/go && go test ./...)
   (cd packages/rust && cargo test && cargo test --no-default-features)
   ```

3. Tag **twice** — the Go module requires a path-prefixed tag:

   ```sh
   git tag vX.Y.Z
   git tag packages/go/vX.Y.Z
   git push origin main vX.Y.Z packages/go/vX.Y.Z
   ```

4. Publish (crates.io last — it is immutable):

   ```sh
   (cd packages/js && npm publish --access public)
   (cd packages/python && python3 -m build && twine upload dist/*)   # or: uv build && uv publish
   (cd packages/rust && cargo package --list | grep -q vectors/manifest.json && cargo publish)
   ```

   Optionally warm the Go module proxy:

   ```sh
   GOPROXY=proxy.golang.org go list -m github.com/cloud-portable/s3vectors/packages/go@vX.Y.Z
   ```

## Notes

- `npm pack --dry-run` / `cargo package --list` show exactly what ships; the
  vector data directories must be included.
- crates.io and pkg.go.dev license detectors may not recognize the
  Permissive-License-Stack prose in `LICENSE.md`; the SPDX expression
  `Apache-2.0 OR MIT` is declared in every manifest. If pkg.go.dev refuses to
  render docs, add standard-text `LICENSE-APACHE` / `LICENSE-MIT` files.
