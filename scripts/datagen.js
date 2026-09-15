// Reference implementation of the vector generated-data algorithm and its
// derived digest values. Normative prose lives in README.md ("Generated data").
//
// Usage:
//   node scripts/datagen.js --self-test
//   (also required as a module by scripts/validate.js)

'use strict'

const crypto = require('node:crypto')

// Bytes per chunk when streaming or digesting. A multiple of the 32-byte prng
// block, so chunk boundaries never split a block.
const CHUNK_SIZE = 1024 * 1024

// --- generation -------------------------------------------------------------

// A dataset resolved to the stream it reads from. `base` is the absolute offset
// of the dataset's byte 0 within that stream, which is what makes a $slice free:
// a slice is its parent with a starting offset. The seed/pattern bytes are
// decoded once here, not per chunk.
function resolve (specs, name) {
  const spec = specs[name]
  if (!spec) throw new Error(`unknown dataset: ${name}`)
  if (spec.$prng) {
    return { seed: Buffer.from(spec.$prng.seed, 'utf8'), pat: null, base: 0, length: spec.$prng.size }
  }
  if (spec.$pattern) {
    const d = spec.$pattern
    let pat
    if (d.pattern !== undefined) pat = Buffer.from(d.pattern, 'utf8')
    else if (d.patternBase64 !== undefined) pat = Buffer.from(d.patternBase64, 'base64')
    else throw new Error(`dataset '${name}': neither pattern nor patternBase64`)
    if (pat.length === 0) throw new Error('empty pattern')
    return { seed: null, pat, base: 0, length: d.size }
  }
  if (spec.$slice) {
    const d = spec.$slice
    const parent = specs[d.of]
    if (!parent) throw new Error(`slice '${name}' references unknown dataset '${d.of}'`)
    if (parent.$slice) throw new Error(`slice '${name}' references slice '${d.of}' (chained slices are not allowed)`)
    const src = resolve(specs, d.of) // validates the parent; generates nothing
    if (d.offset > src.length || d.length > src.length - d.offset) {
      throw new Error(`slice '${name}' [${d.offset}, ${d.offset + d.length}) exceeds '${d.of}' size ${src.length}`)
    }
    return { seed: src.seed, pat: src.pat, base: src.base + d.offset, length: d.length }
  }
  throw new Error(`unknown data kind: ${JSON.stringify(Object.keys(spec))}`)
}

// Written so no intermediate can overflow: never `offset + length > size`.
function checkRange (name, size, offset, length) {
  if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length < 0) {
    throw new Error(`invalid range [${offset}, ${length}) for dataset '${name}'`)
  }
  if (offset > size || length > size - offset) {
    throw new Error(`range [${offset}, ${offset + length}) exceeds dataset '${name}' size ${size}`)
  }
}

// Write `n` bytes of `src`, starting at `offset` within the dataset, into `dst`.
// The only windowing code; every public entry point goes through it.
function readInto (src, offset, dst, n) {
  if (n === 0) return // (abs + n - 1) would underflow below
  const abs = src.base + offset

  if (src.pat) {
    // byte N of the stream is pattern[N % L], so a range starts at phase abs % L
    const L = src.pat.length
    const first = Math.min(n, L)
    const phase = abs % L
    for (let k = 0; k < first; k++) dst[k] = src.pat[(phase + k) % L]
    for (let filled = first; filled < n;) {
      const m = Math.min(filled, n - filled)
      dst.copy(dst, filled, 0, m)
      filled += m
    }
    return
  }

  // block(i) = SHA256(UTF8(seed) || BE64(i)); stream = block(0) || block(1) || ...
  const counter = Buffer.alloc(8)
  const lastBlk = Math.floor((abs + n - 1) / 32)
  for (let i = Math.floor(abs / 32); i <= lastBlk; i++) {
    counter.writeBigUInt64BE(BigInt(i))
    const block = crypto.createHash('sha256').update(src.seed).update(counter).digest()
    const blkStart = i * 32
    const lo = Math.max(abs, blkStart) - blkStart // head trim, nonzero on the first block only
    const hi = Math.min(abs + n, blkStart + 32) - blkStart // clamped to the range end, not the dataset size
    block.copy(dst, blkStart + lo - abs, lo, hi)
  }
}

// Materialize one named dataset from a vector's `data` map.
function generate (specs, name) {
  const src = resolve(specs, name)
  const out = Buffer.alloc(src.length)
  readInto(src, 0, out, src.length)
  return out
}

// Materialize `[offset, offset+length)` of a dataset without materializing the rest.
function generateRange (specs, name, offset, length) {
  const src = resolve(specs, name)
  checkRange(name, src.length, offset, length)
  const out = Buffer.alloc(length)
  readInto(src, offset, out, length)
  return out
}

// A bounded-memory byte stream over a dataset (or a range of one). Every chunk is
// `chunkSize` bytes except the last; a zero-length range yields no chunks.
function generateStream (specs, name, opts = {}) {
  const src = resolve(specs, name)
  const offset = opts.offset ?? 0
  const length = opts.length ?? (Number.isInteger(offset) && offset >= 0 ? src.length - offset : 0)
  checkRange(name, src.length, offset, length)
  const chunkSize = opts.chunkSize ?? CHUNK_SIZE
  if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
    throw new Error(`invalid chunkSize ${chunkSize} for dataset '${name}'`)
  }
  let pos = 0
  return new ReadableStream({
    pull (controller) {
      if (pos >= length) {
        controller.close()
        return
      }
      const n = Math.min(chunkSize, length - pos)
      const chunk = Buffer.alloc(n)
      readInto(src, offset + pos, chunk, n)
      pos += n
      controller.enqueue(chunk)
    }
  })
}

// The dataset's declared length in bytes, without generating it.
function dataSize (specs, name) {
  return resolve(specs, name).length
}

// --- checksums ---------------------------------------------------------------

function makeCrc32Table (poly) {
  const table = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ poly : c >>> 1
    table[n] = c >>> 0
  }
  return table
}

const CRC32_TABLE = makeCrc32Table(0xEDB88320)
const CRC32C_TABLE = makeCrc32Table(0x82F63B78)

// The update functions carry the raw register, so a digest can be accumulated
// across chunks: seed it with the all-ones init once, xor it out once at the end.
function crc32Update (c, buf, table) {
  for (let i = 0; i < buf.length; i++) c = table[(c ^ buf[i]) & 0xFF] ^ (c >>> 8)
  return c >>> 0
}

function crc32 (buf, table) {
  return (crc32Update(0xFFFFFFFF, buf, table) ^ 0xFFFFFFFF) >>> 0
}

// CRC-64/NVME: reflected poly 0x9A6C9329AC4BC9B5, init/xorout all-ones.
const CRC64_TABLE = (() => {
  const poly = 0x9A6C9329AC4BC9B5n
  const table = new BigUint64Array(256)
  for (let n = 0; n < 256; n++) {
    let c = BigInt(n)
    for (let k = 0; k < 8; k++) c = c & 1n ? (c >> 1n) ^ poly : c >> 1n
    table[n] = c
  }
  return table
})()

function crc64Update (c, buf) {
  for (let i = 0; i < buf.length; i++) {
    c = CRC64_TABLE[Number((c ^ BigInt(buf[i])) & 0xFFn)] ^ (c >> 8n)
  }
  return c
}

function crc64nvme (buf) {
  return crc64Update(0xFFFFFFFFFFFFFFFFn, buf) ^ 0xFFFFFFFFFFFFFFFFn
}

function u32ToBase64 (v) {
  const b = Buffer.alloc(4)
  b.writeUInt32BE(v)
  return b.toString('base64')
}

function u64ToBase64 (v) {
  const b = Buffer.alloc(8)
  b.writeBigUInt64BE(v)
  return b.toString('base64')
}

// --- derived values (the ${data.<name>.<field>} placeholders) ----------------

const DERIVED_FIELDS = [
  'size', 'md5', 'etag', 'sha256', 'sha256B64', 'sha1B64', 'crc32B64', 'crc32cB64', 'crc64nvmeB64'
]

// Digests are computed over chunks so peak memory is one chunk, whatever the
// dataset size. `size` reads no bytes at all. Note this bounds memory, not time:
// every field re-reads the dataset, so do not loop DERIVED_FIELDS over a
// multi-gigabyte dataset.
function derivedChunked (specs, name, field, chunkSize) {
  const src = resolve(specs, name)
  if (field === 'size') return String(src.length)

  let hash = null
  let table = null
  let crc = 0xFFFFFFFF
  let crc64 = 0xFFFFFFFFFFFFFFFFn
  switch (field) {
    case 'md5': case 'etag': hash = crypto.createHash('md5'); break
    case 'sha256': case 'sha256B64': hash = crypto.createHash('sha256'); break
    case 'sha1B64': hash = crypto.createHash('sha1'); break
    case 'crc32B64': table = CRC32_TABLE; break
    case 'crc32cB64': table = CRC32C_TABLE; break
    case 'crc64nvmeB64': break
    default: throw new Error(`unknown derived data field: ${field}`)
  }

  const buf = Buffer.alloc(Math.min(chunkSize, src.length) || 1)
  for (let pos = 0; pos < src.length; pos += chunkSize) {
    const n = Math.min(chunkSize, src.length - pos)
    const chunk = buf.subarray(0, n)
    readInto(src, pos, chunk, n)
    if (hash) hash.update(chunk)
    else if (table) crc = crc32Update(crc, chunk, table)
    else crc64 = crc64Update(crc64, chunk)
  }

  switch (field) {
    case 'md5': return hash.digest('hex')
    case 'etag': return `"${hash.digest('hex')}"`
    case 'sha256': return hash.digest('hex')
    case 'sha256B64': return hash.digest('base64')
    case 'sha1B64': return hash.digest('base64')
    case 'crc32B64': case 'crc32cB64': return u32ToBase64((crc ^ 0xFFFFFFFF) >>> 0)
    default: return u64ToBase64(crc64 ^ 0xFFFFFFFFFFFFFFFFn)
  }
}

function derived (specs, name, field) {
  return derivedChunked(specs, name, field, CHUNK_SIZE)
}

module.exports = {
  generate,
  generateRange,
  generateStream,
  dataSize,
  derived,
  DERIVED_FIELDS,
  CHUNK_SIZE,
  __derivedChunked: derivedChunked
}

// --- self-test ----------------------------------------------------------------

async function selfTest () {
  const assert = require('node:assert')

  // prng blocks cross-checked against `printf 'test\0...' | shasum -a 256`
  const specs = {
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
    psl: { $slice: { of: 'abc100', offset: 7, length: 20 } }
  }
  const block0 = 'b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b'
  const block1 = '64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d'
  const stream40 = block0 + block1.slice(0, 16)
  assert.strictEqual(generate(specs, 't32').toString('hex'), block0)
  assert.strictEqual(generate(specs, 't40').toString('hex'), stream40)
  assert.strictEqual(generate(specs, 't10').toString('hex'), block0.slice(0, 20))
  assert.strictEqual(generate(specs, 'aaa').toString('utf8'), 'AAAAA')
  assert.strictEqual(generate(specs, 'abc').toString('utf8'), 'abcabcab')
  assert.strictEqual(generate(specs, 'bin').toString('hex'), 'deadbeefdead')
  assert.strictEqual(generate(specs, 'sl').toString('hex'), stream40.slice(60, 72))
  assert.strictEqual(generate(specs, 'zero').length, 0)

  // Ranged reads: every expectation is a window of the pinned block hashes.
  const hexRange = (n, o, l) => generateRange(specs, n, o, l).toString('hex')
  assert.strictEqual(hexRange('t40', 30, 6), stream40.slice(60, 72)) // crosses the block boundary
  assert.strictEqual(hexRange('t40', 24, 16), stream40.slice(48, 80)) // starts mid-block, crosses
  assert.strictEqual(hexRange('t40', 36, 4), stream40.slice(72, 80)) // trailing partial block
  assert.strictEqual(hexRange('t96', 0, 32), block0)
  assert.strictEqual(hexRange('t96', 32, 32), block1) // seek to an exact block boundary
  assert.strictEqual(hexRange('sl', 2, 3), stream40.slice(64, 70)) // ranged read of a slice
  assert.strictEqual(generateRange(specs, 'abc', 4, 3).toString('utf8'), 'bca') // mid-phase
  assert.strictEqual(generateRange(specs, 'abc100', 98, 2).toString('utf8'), 'ca') // tail, phase 98 % 3 == 2
  // A slice of a pattern starts mid-period: a port that dropped the slice's base
  // offset would return 'abc' here and still pass every whole-dataset check.
  assert.strictEqual(generateRange(specs, 'psl', 0, 3).toString('utf8'), 'bca')
  assert.strictEqual(generate(specs, 'psl').toString('utf8'), 'bcabcabcabcabcabcabc')
  assert.strictEqual(generateRange(specs, 't40', 40, 0).length, 0)
  assert.strictEqual(dataSize(specs, 'sl'), 6)

  // A range is exactly the corresponding window of the whole dataset. generate()
  // walks from zero; a range divides to find its start block and phase, so these
  // are genuinely different code paths.
  for (const name of ['t96', 't40', 't32', 't10', 'aaa', 'abc', 'abc100', 'bin', 'zero', 'sl', 'psl']) {
    const full = generate(specs, name)
    for (let o = 0; o <= full.length; o++) {
      for (let l = 0; l <= full.length - o; l++) {
        assert.deepStrictEqual(generateRange(specs, name, o, l), full.subarray(o, o + l), `${name} [${o}, ${o + l})`)
      }
    }
  }

  // Streaming concatenates to the same bytes, in chunks of the requested size.
  for (const chunkSize of [1, 7, 32, 1000]) {
    for (const name of ['t96', 't40', 'abc100', 'zero', 'sl', 'psl']) {
      const full = generate(specs, name)
      const sizes = []
      const parts = []
      for await (const chunk of generateStream(specs, name, { chunkSize })) {
        sizes.push(chunk.length)
        parts.push(chunk)
      }
      assert.deepStrictEqual(Buffer.concat(parts), full, `stream ${name}/${chunkSize}`)
      assert.ok(sizes.every(s => s > 0 && s <= chunkSize), `stream ${name}/${chunkSize} chunk sizes`)
      assert.strictEqual(sizes.length, Math.ceil(full.length / chunkSize))
    }
  }

  // Errors: spec validation runs before any byte work, even for an empty range.
  const bad = { ...specs, chain: { $slice: { of: 'sl', offset: 0, length: 1 } }, over: { $slice: { of: 't10', offset: 8, length: 8 } }, nopat: { $pattern: { size: 4 } } }
  assert.throws(() => generate(bad, 'nope'), /unknown dataset/)
  assert.throws(() => generate(bad, 'chain'), /chained slices/)
  assert.throws(() => generate(bad, 'over'), /exceeds/)
  assert.throws(() => generate(bad, 'nopat'), /neither pattern nor patternBase64/)
  assert.throws(() => generateRange(bad, 'chain', 0, 0), /chained slices/)
  assert.throws(() => generateRange(bad, 't10', 8, 8), /exceeds/)
  assert.throws(() => generateRange(bad, 't40', 41, 0), /exceeds/)
  assert.throws(() => generateRange(bad, 't40', 0, 41), /exceeds/)
  assert.throws(() => generateRange(bad, 'sl', 4, 4), /exceeds/)
  assert.throws(() => generateRange(bad, 't40', -1, 1), /invalid range/)
  assert.throws(() => generateStream(bad, 'over'), /exceeds/)

  // Standard CRC check values for the ASCII string "123456789".
  const check = Buffer.from('123456789', 'ascii')
  assert.strictEqual(crc32(check, CRC32_TABLE).toString(16), 'cbf43926')
  assert.strictEqual(crc32(check, CRC32C_TABLE).toString(16), 'e3069283')
  assert.strictEqual(crc64nvme(check).toString(16), 'ae8b14860a799888')

  // Derived values: MD5("AAAAA") etc. computed independently.
  assert.strictEqual(derived(specs, 'aaa', 'md5'), 'f6a6263167c92de8644ac998b3c4e4d1')
  assert.strictEqual(derived(specs, 'aaa', 'etag'), '"f6a6263167c92de8644ac998b3c4e4d1"')
  assert.strictEqual(derived(specs, 'aaa', 'size'), '5')
  assert.strictEqual(derived(specs, 'zero', 'md5'), 'd41d8cd98f00b204e9800998ecf8427e')
  assert.strictEqual(derived(specs, 'zero', 'size'), '0')
  for (const f of DERIVED_FIELDS) derived(specs, 'sl', f) // all fields derivable on slices

  // A digest must not depend on how the dataset was cut into chunks. chunkSize 1
  // makes every byte a boundary, which is what catches a per-chunk xor-out.
  const chk = { ...specs, check: { $pattern: { pattern: '123456789', size: 9 } } }
  for (const name of ['zero', 'check', 'aaa', 'abc100', 't96', 'sl', 'psl', 'bin']) {
    for (const f of DERIVED_FIELDS) {
      const want = derived(chk, name, f)
      for (const chunkSize of [1, 7, 32, 1000, CHUNK_SIZE]) {
        assert.strictEqual(derivedChunked(chk, name, f, chunkSize), want, `${name}.${f}/${chunkSize}`)
      }
    }
  }
  assert.strictEqual(derivedChunked(chk, 'check', 'crc32B64', 1), u32ToBase64(0xcbf43926))

  console.log('datagen self-test: OK')
}

if (require.main === module) {
  if (process.argv.includes('--self-test')) {
    selfTest().catch(e => {
      console.error(e)
      process.exit(1)
    })
  } else {
    console.error('usage: node scripts/datagen.js --self-test')
    process.exit(1)
  }
}
