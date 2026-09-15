import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { groups, load, all, manifest } from '../index.js'
import { generate, derived, DERIVED_FIELDS } from '../datagen.js'

// Independently computed check values (shasum/md5/CRC catalog); shared by all
// four language ports of the datagen reference.
const BLOCK0 = 'b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b'
const BLOCK1 = '64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d'

const SPECS = {
  t40: { $prng: { seed: 'test', size: 40 } },
  t32: { $prng: { seed: 'test', size: 32 } },
  t10: { $prng: { seed: 'test', size: 10 } },
  aaa: { $pattern: { pattern: 'A', size: 5 } },
  abc: { $pattern: { pattern: 'abc', size: 8 } },
  bin: { $pattern: { patternBase64: '3q2+7w==', size: 6 } },
  sl: { $slice: { of: 't40', offset: 30, length: 6 } },
  chain: { $slice: { of: 'sl', offset: 0, length: 1 } },
  over: { $slice: { of: 't10', offset: 8, length: 8 } },
  check: { $pattern: { pattern: '123456789', size: 9 } }
}

test('datagen check values', () => {
  assert.equal(generate(SPECS, 't32').toString('hex'), BLOCK0)
  assert.equal(generate(SPECS, 't40').toString('hex'), BLOCK0 + BLOCK1.slice(0, 16))
  assert.equal(generate(SPECS, 't10').toString('hex'), BLOCK0.slice(0, 20))
  assert.equal(generate(SPECS, 'aaa').toString('utf8'), 'AAAAA')
  assert.equal(generate(SPECS, 'abc').toString('utf8'), 'abcabcab')
  assert.equal(generate(SPECS, 'bin').toString('hex'), 'deadbeefdead')
  assert.equal(generate(SPECS, 'sl').toString('hex'), (BLOCK0 + BLOCK1.slice(0, 16)).slice(60, 72))

  assert.equal(derived(SPECS, 'aaa', 'md5'), 'f6a6263167c92de8644ac998b3c4e4d1')
  assert.equal(derived(SPECS, 'aaa', 'etag'), '"f6a6263167c92de8644ac998b3c4e4d1"')
  assert.equal(derived(SPECS, 'aaa', 'size'), '5')
  // CRC catalog check values over ASCII "123456789": 0xCBF43926 / 0xE3069283 / 0xAE8B14860A799888
  assert.equal(derived(SPECS, 'check', 'crc32B64'), Buffer.from('cbf43926', 'hex').toString('base64'))
  assert.equal(derived(SPECS, 'check', 'crc32cB64'), Buffer.from('e3069283', 'hex').toString('base64'))
  assert.equal(derived(SPECS, 'check', 'crc64nvmeB64'), Buffer.from('ae8b14860a799888', 'hex').toString('base64'))
  for (const f of DERIVED_FIELDS) derived(SPECS, 'sl', f)
})

test('datagen error cases', () => {
  assert.throws(() => generate(SPECS, 'nope'), /unknown dataset/)
  assert.throws(() => derived(SPECS, 'aaa', 'sha512'), /unknown derived data field/)
  assert.throws(() => generate(SPECS, 'chain'), /chained slices/)
  assert.throws(() => generate(SPECS, 'over'), /exceeds/)
})

test('shipped schema resolves and matches manifest.schemaSha256', () => {
  // Vector files carry "$schema": "../schema/vector.schema.json" — the schema
  // must ship at that location relative to data/, byte-identical to canonical.
  const schema = readFileSync(new URL('../data/../schema/vector.schema.json', import.meta.url))
  assert.equal(createHash('sha256').update(schema).digest('hex'), manifest.schemaSha256)
})

test('manifest agreement', () => {
  assert.equal(groups.length, manifest.groups.length)
  let total = 0
  for (const entry of manifest.groups) {
    const file = load(entry.group)
    for (const v of file.vectors) assert.equal(v.group, entry.group)
    assert.equal(file.vectors.length, entry.count)
    total += file.vectors.length
  }
  assert.equal(total, manifest.total)
  assert.throws(() => load('no-such-group'), /unknown group/)
})

test('root equals union of groups; ids unique and group-prefixed', () => {
  const files = all()
  assert.deepEqual(files.map(f => f.vectors[0].group), [...groups])
  const ids = new Set()
  for (const file of files) {
    for (const v of file.vectors) {
      assert.ok(!ids.has(v.id), `duplicate id ${v.id}`)
      ids.add(v.id)
      assert.ok(v.id.startsWith(`${v.group}-`), `${v.id} not prefixed with ${v.group}`)
    }
  }
  assert.equal(ids.size, manifest.total)
})

test('vector shape smoke', () => {
  for (const file of all()) {
    for (const v of file.vectors) {
      assert.ok(v.kind === 'api' || v.kind === 'signing', `${v.id}: kind`)
      assert.ok(v.title.length > 0)
      assert.equal(v.tags.filter(t => /^tier-[123]$/.test(t)).length, 1, `${v.id}: tier tags`)
      if (v.kind === 'api') {
        assert.ok(v.steps.length > 0, `${v.id}: steps`)
        for (const s of v.steps) {
          assert.ok(('$operation' in s) !== ('$http' in s), `${v.id}: step must have exactly one of $operation/$http`)
          assert.equal(Object.keys(s).length, 1, `${v.id}: step must have a single union key`)
        }
        for (const p of v.prerequisites ?? []) {
          assert.equal(Object.keys(p).length, 1, `${v.id}: prerequisite must have a single union key`)
          assert.ok('$bucket' in p || '$object' in p || '$credential' in p, `${v.id}: prerequisite union key`)
        }
        for (const [name, spec] of Object.entries(v.data ?? {})) {
          assert.equal(Object.keys(spec).length, 1, `${v.id}/${name}: data spec must have a single union key`)
          assert.ok('$prng' in spec || '$pattern' in spec || '$slice' in spec, `${v.id}/${name}: data union key`)
        }
      } else {
        assert.ok(v.expect.authorization.length > 0, `${v.id}: authorization`)
      }
    }
  }
})

test('full-corpus datagen pass', () => {
  // Every non-slice dataset up to GENERATE_CAP must materialize (slices only
  // re-read parent bytes, and their bounds are validated by the corpus linter).
  // Above the cap a dataset runs to gigabytes — the corpus linter requires those
  // to be tagged `large` — and no unit test allocates that much; the algorithms
  // are size-independent and pinned by the check values above. Derived fields —
  // each of which regenerates its dataset internally — are exercised only where
  // the regenerated bytes are <= 1 MiB.
  const GENERATE_CAP = 64 * 1024 * 1024
  const DERIVED_CAP = 1024 * 1024
  for (const file of all()) {
    for (const v of file.vectors) {
      if (v.kind !== 'api' || !v.data) continue
      for (const [name, spec] of Object.entries(v.data)) {
        let parentSize
        if (spec.$slice) {
          const parent = v.data[spec.$slice.of]
          parentSize = (parent.$prng ?? parent.$pattern).size
        } else {
          const inner = spec.$prng ?? spec.$pattern
          parentSize = inner.size
          if (inner.size <= GENERATE_CAP) {
            const bytes = generate(v.data, name)
            assert.equal(bytes.length, inner.size, `${v.id}/${name}: size`)
          }
        }
        if (parentSize <= DERIVED_CAP) {
          for (const f of DERIVED_FIELDS) derived(v.data, name, f)
        }
      }
    }
  }
})
