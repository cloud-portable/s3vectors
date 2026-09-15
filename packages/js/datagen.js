// Reference implementation of the vector generated-data algorithm and its
// derived digest values. Normative prose: the repository README, "Generated data".
import { createHash } from 'node:crypto'

/** Bytes per chunk when streaming or digesting. A multiple of the 32-byte prng block. */
export const CHUNK_SIZE = 1024 * 1024

// A dataset resolved to the stream it reads from. `base` is the absolute offset of
// the dataset's byte 0 within that stream, which is what makes a $slice free: a
// slice is its parent with a starting offset. Seed/pattern bytes are decoded once.
function resolve (specs, name) {
  const spec = specs[name]
  if (!spec) throw new Error(`unknown dataset: ${name}`)
  if (spec.$prng) {
    const seeded = createHash('sha256').update(Buffer.from(spec.$prng.seed, 'utf8'))
    return { seeded, pat: null, base: 0, length: spec.$prng.size }
  }
  if (spec.$pattern) {
    const d = spec.$pattern
    let pat
    if (d.pattern !== undefined) pat = Buffer.from(d.pattern, 'utf8')
    else if (d.patternBase64 !== undefined) pat = Buffer.from(d.patternBase64, 'base64')
    else throw new Error(`dataset '${name}': neither pattern nor patternBase64`)
    if (pat.length === 0) throw new Error('empty pattern')
    return { seeded: null, pat, base: 0, length: d.size }
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
    return { seeded: src.seeded, pat: src.pat, base: src.base + d.offset, length: d.length }
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
    const block = src.seeded.copy().update(counter).digest()
    const blkStart = i * 32
    const lo = Math.max(abs, blkStart) - blkStart // head trim, nonzero on the first block only
    const hi = Math.min(abs + n, blkStart + 32) - blkStart // clamped to the range end, not the dataset size
    block.copy(dst, blkStart + lo - abs, lo, hi)
  }
}

/**
 * Materialize one named dataset from a vector's `data` map.
 * @param {Record<string, object>} specs the vector's `data` map
 * @param {string} name
 * @returns {Buffer}
 */
export function generate (specs, name) {
  const src = resolve(specs, name)
  const out = Buffer.alloc(src.length)
  readInto(src, 0, out, src.length)
  return out
}

/**
 * Materialize `[offset, offset+length)` of a dataset without materializing the rest.
 * @param {Record<string, object>} specs the vector's `data` map
 * @param {string} name
 * @param {number} offset first byte of the dataset to produce
 * @param {number} length bytes to produce
 * @returns {Buffer}
 */
export function generateRange (specs, name, offset, length) {
  const src = resolve(specs, name)
  checkRange(name, src.length, offset, length)
  const out = Buffer.alloc(length)
  readInto(src, offset, out, length)
  return out
}

/**
 * A bounded-memory byte stream over a dataset (or a range of one) — the only way
 * to read a dataset larger than the platform's maximum allocation. Every chunk is
 * `chunkSize` bytes except the last; a zero-length range yields no chunks. The
 * spec is validated eagerly, so errors throw from this call, never mid-stream.
 * @param {Record<string, object>} specs the vector's `data` map
 * @param {string} name
 * @param {{ offset?: number, length?: number, chunkSize?: number }} [opts]
 * @returns {ReadableStream<Buffer>}
 */
export function generateStream (specs, name, opts = {}) {
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

/**
 * The dataset's declared length in bytes, without generating it.
 * @param {Record<string, object>} specs the vector's `data` map
 * @param {string} name
 * @returns {number}
 */
export function dataSize (specs, name) {
  return resolve(specs, name).length
}

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

/** The fields available as `${data.<name>.<field>}` placeholders. */
export const DERIVED_FIELDS = Object.freeze([
  'size', 'md5', 'etag', 'sha256', 'sha256B64', 'sha1B64', 'crc32B64', 'crc32cB64', 'crc64nvmeB64'
])

// Digests are computed over chunks so peak memory is one chunk, whatever the
// dataset size. `size` reads no bytes at all. Note this bounds memory, not time:
// every field re-reads the dataset, so do not loop DERIVED_FIELDS over a
// multi-gigabyte dataset. Exported unlisted in datagen.d.ts: the chunk size is a
// test seam, not public API.
export function __derivedChunked (specs, name, field, chunkSize) {
  const src = resolve(specs, name)
  if (field === 'size') return String(src.length)

  let hash = null
  let table = null
  let crc = 0xFFFFFFFF
  let crc64 = 0xFFFFFFFFFFFFFFFFn
  switch (field) {
    case 'md5': case 'etag': hash = createHash('md5'); break
    case 'sha256': case 'sha256B64': hash = createHash('sha256'); break
    case 'sha1B64': hash = createHash('sha1'); break
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

/**
 * Compute a derived string value of a dataset (what a `${data.<name>.<field>}`
 * placeholder resolves to). Computed in bounded memory, whatever the dataset size.
 * @param {Record<string, object>} specs the vector's `data` map
 * @param {string} name
 * @param {string} field one of DERIVED_FIELDS
 * @returns {string}
 */
export function derived (specs, name, field) {
  return __derivedChunked(specs, name, field, CHUNK_SIZE)
}
