import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { groups, load, all, manifest } from '../index.js'
import { generate, generateRange, generateStream, dataSize, derived, DERIVED_FIELDS, CHUNK_SIZE, __derivedChunked } from '../datagen.js'

// Independently computed check values (shasum/md5/CRC catalog); shared by all
// four language ports of the datagen reference.
const BLOCK0 = 'b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b'
const BLOCK1 = '64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d'

const STREAM40 = BLOCK0 + BLOCK1.slice(0, 16)

const SPECS = {
  t96: { $prng: { seed: 'test', size: 96 } },
  t40: { $prng: { seed: 'test', size: 40 } },
  t32: { $prng: { seed: 'test', size: 32 } },
  t10: { $prng: { seed: 'test', size: 10 } },
  aaa: { $pattern: { pattern: 'A', size: 5 } },
  abc: { $pattern: { pattern: 'abc', size: 8 } },
  abc100: { $pattern: { pattern: 'abc', size: 100 } },
  bin: { $pattern: { patternBase64: '3q2+7w==', size: 6 } },
  zero: { $pattern: { pattern: 'A', size: 0 } },
  sl: { $slice: { of: 't40', offset: 30, length: 6 } },
  psl: { $slice: { of: 'abc100', offset: 7, length: 20 } },
  chain: { $slice: { of: 'sl', offset: 0, length: 1 } },
  over: { $slice: { of: 't10', offset: 8, length: 8 } },
  nopat: { $pattern: { size: 4 } },
  check: { $pattern: { pattern: '123456789', size: 9 } }
}

// Every dataset above that a range/stream test sweeps over.
const SWEEP = ['t96', 't40', 't32', 't10', 'aaa', 'abc', 'abc100', 'bin', 'zero', 'sl', 'psl']

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
  assert.throws(() => generate(SPECS, 'nopat'), /neither pattern nor patternBase64/)

  // The spec is resolved before any byte work, so an empty range still reports
  // a bad spec rather than returning empty.
  assert.throws(() => generateRange(SPECS, 'chain', 0, 0), /chained slices/)
  assert.throws(() => generateRange(SPECS, 't10', 8, 8), /exceeds/)
  assert.throws(() => generateRange(SPECS, 't40', 41, 0), /exceeds/)
  assert.throws(() => generateRange(SPECS, 't40', 0, 41), /exceeds/)
  assert.throws(() => generateRange(SPECS, 'sl', 4, 4), /exceeds/) // bound is the slice, not the parent
  assert.throws(() => generateRange(SPECS, 't40', -1, 1), /invalid range/)
  // Streams validate eagerly: the error comes from the call, not the first read.
  assert.throws(() => generateStream(SPECS, 'over'), /exceeds/)
  assert.throws(() => generateStream(SPECS, 't40', { chunkSize: 0 }), /invalid chunkSize/)
})

test('datagen ranged reads', () => {
  const hex = (n, o, l) => generateRange(SPECS, n, o, l).toString('hex')
  assert.equal(hex('t40', 30, 6), STREAM40.slice(60, 72)) // crosses the 32-byte block boundary
  assert.equal(hex('t40', 24, 16), STREAM40.slice(48, 80)) // starts mid-block, crosses
  assert.equal(hex('t40', 36, 4), STREAM40.slice(72, 80)) // trailing partial block
  assert.equal(hex('t96', 0, 32), BLOCK0)
  assert.equal(hex('t96', 32, 32), BLOCK1) // seek to an exact block boundary
  assert.equal(hex('sl', 2, 3), STREAM40.slice(64, 70)) // ranged read of a slice
  assert.equal(generateRange(SPECS, 'abc', 4, 3).toString('utf8'), 'bca') // mid-phase
  assert.equal(generateRange(SPECS, 'abc100', 98, 2).toString('utf8'), 'ca') // tail, phase 98 % 3 == 2
  // A slice of a pattern starts mid-period: a port that dropped the slice's base
  // offset would return 'abc' here and still pass every whole-dataset check.
  assert.equal(generateRange(SPECS, 'psl', 0, 3).toString('utf8'), 'bca')
  assert.equal(generate(SPECS, 'psl').toString('utf8'), 'bcabcabcabcabcabcabc')
  assert.equal(generateRange(SPECS, 't40', 40, 0).length, 0) // empty read at EOF is legal
  assert.equal(dataSize(SPECS, 'sl'), 6)
  assert.equal(dataSize(SPECS, 'zero'), 0)
})

test('a range equals the same window of the whole dataset', () => {
  // generate() walks from zero; a range divides to find its start block and
  // phase, so these are genuinely different code paths. Exhaustive: the
  // fixtures are tiny and boundary bugs only show at specific offsets.
  for (const name of SWEEP) {
    const full = generate(SPECS, name)
    for (let o = 0; o <= full.length; o++) {
      for (let l = 0; l <= full.length - o; l++) {
        assert.deepEqual(generateRange(SPECS, name, o, l), full.subarray(o, o + l), `${name} [${o}, ${o + l})`)
      }
    }
  }
})

test('a stream concatenates to the same bytes', async () => {
  for (const chunkSize of [1, 7, 32, 1000]) {
    for (const name of SWEEP) {
      const full = generate(SPECS, name)
      const parts = []
      for await (const chunk of generateStream(SPECS, name, { chunkSize })) parts.push(chunk)
      assert.deepEqual(Buffer.concat(parts), full, `${name}/${chunkSize}`)
      assert.equal(parts.length, Math.ceil(full.length / chunkSize))
      assert.ok(parts.every(c => c.length > 0 && c.length <= chunkSize))
    }
  }
  // A stream of a sub-range matches the range.
  const parts = []
  for await (const c of generateStream(SPECS, 't96', { offset: 30, length: 40, chunkSize: 9 })) parts.push(c)
  assert.deepEqual(Buffer.concat(parts), generateRange(SPECS, 't96', 30, 40))
})

test('a digest does not depend on how the data was chunked', () => {
  // chunkSize 1 makes every byte a chunk boundary, which is what catches a CRC
  // register that gets its init or xor-out applied per chunk instead of once.
  for (const name of ['zero', 'check', 'aaa', 'abc100', 't96', 'sl', 'psl', 'bin']) {
    for (const f of DERIVED_FIELDS) {
      const want = derived(SPECS, name, f)
      for (const chunkSize of [1, 7, 32, 1000, CHUNK_SIZE]) {
        assert.equal(__derivedChunked(SPECS, name, f, chunkSize), want, `${name}.${f}/${chunkSize}`)
      }
    }
  }
  assert.equal(derived(SPECS, 'zero', 'md5'), 'd41d8cd98f00b204e9800998ecf8427e')
  assert.equal(derived(SPECS, 'zero', 'size'), '0')
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

// An independent statement of the normative formula, used to check windows of a
// dataset too big to materialize. Deliberately not written in terms of generate().
function expectedWindow (spec, offset, length) {
  const out = Buffer.alloc(length)
  if (spec.$pattern) {
    const d = spec.$pattern
    const pat = d.pattern !== undefined ? Buffer.from(d.pattern, 'utf8') : Buffer.from(d.patternBase64, 'base64')
    for (let k = 0; k < length; k++) out[k] = pat[(offset + k) % pat.length]
    return out
  }
  const counter = Buffer.alloc(8)
  for (let i = Math.floor(offset / 32); i <= Math.floor((offset + length - 1) / 32); i++) {
    counter.writeBigUInt64BE(BigInt(i))
    const block = createHash('sha256').update(Buffer.from(spec.$prng.seed, 'utf8')).update(counter).digest()
    const blkStart = i * 32
    const lo = Math.max(offset, blkStart) - blkStart
    const hi = Math.min(offset + length, blkStart + 32) - blkStart
    block.copy(out, blkStart + lo - offset, lo, hi)
  }
  return out
}

// Read bounded windows of a dataset that is too big to hold: the ends, and a
// window straddling each 32-bit boundary inside it. The straddles are the only
// thing in the suite that can catch a seek that truncates an offset to 32 bits.
function spotCheck (data, name, size, streamSpec, sliceBase) {
  const W = 64 * 1024
  const at = o => sliceBase + o
  const win = (o, l) => {
    const got = generateRange(data, name, o, l)
    assert.equal(got.length, l, `${name} [${o}, ${o + l}): length`)
    assert.deepEqual(got, expectedWindow(streamSpec, at(o), l), `${name} [${o}, ${o + l})`)
  }
  win(0, Math.min(W, size))
  if (size > W) win(size - W, W)
  if (size > 0) win(size - 1, 1)
  for (const b of [2 ** 31, 2 ** 32]) {
    if (b - 32 > 0 && b + 32 <= size) win(b - 32, 64)
  }
  assert.equal(generateRange(data, name, size, 0).length, 0)
  assert.throws(() => generateRange(data, name, size, 1), /exceeds/)
  assert.equal(derived(data, name, 'size'), String(size))
}

test('full-corpus datagen pass', () => {
  // Every non-slice dataset up to GENERATE_CAP materializes in full. Above the
  // cap a dataset runs to gigabytes — the corpus linter requires those to be
  // tagged `large` — so instead of holding one we read bounded windows and check
  // them against an independent statement of the formula (spotCheck above).
  //
  // A whole-materialization regression here will not raise a clean error: Node's
  // Buffer limit is far above these sizes, so it shows up as an OOM or a hang.
  //
  // Derived fields still run only below DERIVED_CAP. Chunking made them bounded
  // in memory, not in time, and each field re-reads the dataset — so never loop
  // DERIVED_FIELDS over an oversized dataset. `size` alone is O(1) and is
  // asserted by spotCheck.
  const GENERATE_CAP = 64 * 1024 * 1024 // keep equal to LARGE_DATA_BYTES in scripts/validate.js
  const DERIVED_CAP = 1024 * 1024
  for (const file of all()) {
    for (const v of file.vectors) {
      if (v.kind !== 'api' || !v.data) continue
      for (const [name, spec] of Object.entries(v.data)) {
        let parentSize
        if (spec.$slice) {
          const parent = v.data[spec.$slice.of]
          parentSize = (parent.$prng ?? parent.$pattern).size
          if (parentSize > GENERATE_CAP) {
            spotCheck(v.data, name, spec.$slice.length, parent, spec.$slice.offset)
          }
        } else {
          const inner = spec.$prng ?? spec.$pattern
          parentSize = inner.size
          if (inner.size <= GENERATE_CAP) {
            const bytes = generate(v.data, name)
            assert.equal(bytes.length, inner.size, `${v.id}/${name}: size`)
          } else {
            spotCheck(v.data, name, inner.size, spec, 0)
          }
        }
        if (parentSize <= DERIVED_CAP) {
          for (const f of DERIVED_FIELDS) derived(v.data, name, f)
        }
      }
    }
  }
})
