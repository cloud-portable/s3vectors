// Validate every vectors/*.json file against schema/vector.schema.json and a
// set of lint rules the schema cannot express (cross-references, ordering).
//
// Usage:
//   node scripts/validate.js [--digests] [files...]
//
//   --digests  also materialize every dataset referenced by a ${data.*}
//              placeholder and check the derived value is computable
//              (slower; generates the data)

'use strict'

const fs = require('node:fs')
const path = require('node:path')
const Ajv = require('ajv/dist/2020')
const addFormats = require('ajv-formats')
const datagen = require('./datagen')

const ROOT = path.join(__dirname, '..')
const SCHEMA_PATH = path.join(ROOT, 'schema', 'vector.schema.json')

const ENV_VARS = new Set(['endpoint', 'region'])
const RESERVED_IDENTITIES = new Set(['main', 'anonymous', 'invalid'])
const RESOURCE_ATTRS = {
  bucket: new Set(['name']),
  object: new Set(['key', 'etag', 'versionId']),
  credential: new Set(['accessKeyId', 'canonicalId', 'displayName'])
}
const DERIVED_FIELDS = new Set(datagen.DERIVED_FIELDS)

// A dataset above this materializes into gigabytes. The package datagen tests
// never hold one — they spot-check it with ranged reads instead — so the vector
// has to declare the cost with a `large` tag. Keep this equal to the suites'
// 64 MiB spot-check threshold (Python's own speed caps are separate and lower).
const LARGE_DATA_BYTES = 64 * 1024 * 1024

const errors = []
function fail (file, vectorId, msg) {
  errors.push(`${file}${vectorId ? ` [${vectorId}]` : ''}: ${msg}`)
}

// Find ${...} occurrences in a string, honoring the `$${` escape for a literal `${`.
function placeholders (str) {
  const out = []
  const re = /\$\$\{|\$\{([^}]*)\}/g
  let m
  while ((m = re.exec(str)) !== null) {
    if (m[0] === '$${') continue // escaped literal ${
    out.push(m[1])
  }
  return out
}

// Walk every string value in a JSON subtree, calling fn(str, jsonPath).
function walkStrings (node, fn, p = '') {
  if (typeof node === 'string') fn(node, p)
  else if (Array.isArray(node)) node.forEach((v, i) => walkStrings(v, fn, `${p}[${i}]`))
  else if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) walkStrings(v, fn, p ? `${p}.${k}` : k)
  }
}

// Lint $matches regex patterns: must stay in the portable subset valid in both
// ECMA-262 and RE2 (no lookarounds, no backreferences), so native regex engines
// work in every runner language.
function lintMatchers (file, id, node, p = '') {
  if (Array.isArray(node)) { node.forEach((v, i) => lintMatchers(file, id, v, `${p}[${i}]`)); return }
  if (!node || typeof node !== 'object') return
  for (const [k, v] of Object.entries(node)) {
    if (k === '$matches' && typeof v === 'string') {
      if (/\(\?=|\(\?!|\(\?<=|\(\?<!/.test(v)) {
        fail(file, id, `${p}: $matches uses a lookaround (not portable to RE2): ${JSON.stringify(v)}`)
      }
      if (/\\[1-9]/.test(v)) {
        fail(file, id, `${p}: $matches uses a backreference (not portable to RE2): ${JSON.stringify(v)}`)
      }
      try {
        // eslint-disable-next-line no-new
        new RegExp(v.replace(/\$\{[^}]*\}/g, 'x'))
      } catch (e) {
        fail(file, id, `${p}: $matches is not a valid regex: ${e.message}`)
      }
    } else {
      lintMatchers(file, id, v, p ? `${p}.${k}` : k)
    }
  }
}

// Collect {"$data": name} references in a subtree.
function dataRefs (node, out = []) {
  if (Array.isArray(node)) node.forEach(v => dataRefs(v, out))
  else if (node && typeof node === 'object') {
    if (typeof node.$data === 'string' && Object.keys(node).length === 1) out.push(node.$data)
    else for (const v of Object.values(node)) dataRefs(v, out)
  }
  return out
}

// Unwrap a prerequisite keyed union -> { type, body } (type without the $).
function prereqParts (p) {
  for (const type of ['bucket', 'object', 'credential']) {
    if (p[`$${type}`]) return { type, body: p[`$${type}`] }
  }
  return { type: undefined, body: {} }
}

function lintApiVector (file, v, opts) {
  const prereqs = (v.prerequisites || []).map(prereqParts)
  const data = v.data || {}
  const resTypes = new Map(prereqs.map(p => [p.body.handle, p.type]))

  if (resTypes.size !== prereqs.length) fail(file, v.id, 'duplicate prerequisite handles')

  for (const p of prereqs) {
    if (p.type === 'object' && resTypes.get(p.body.bucket) !== 'bucket') {
      fail(file, v.id, `$object prerequisite '${p.body.handle}' references '${p.body.bucket}' which is not a $bucket handle`)
    }
  }

  // data specs: slice targets and bounds (sizes are known without generating)
  for (const [name, spec] of Object.entries(data)) {
    if (!spec.$slice) continue
    const s = spec.$slice
    const parent = data[s.of]
    if (!parent) fail(file, v.id, `slice '${name}' references unknown dataset '${s.of}'`)
    else if (parent.$slice) fail(file, v.id, `slice '${name}' references slice '${s.of}' (chained slices not allowed)`)
    else if (s.offset + s.length > (parent.$prng ?? parent.$pattern).size) fail(file, v.id, `slice '${name}' exceeds bounds of '${s.of}'`)
  }

  // gigabyte-scale datasets must advertise themselves (the package datagen
  // tests skip them, and a runner needs the tag to budget the run)
  const oversized = Object.entries(data)
    .filter(([, spec]) => !spec.$slice && (spec.$prng ?? spec.$pattern).size > LARGE_DATA_BYTES)
    .map(([name]) => name)
  if (oversized.length > 0 && !v.tags.includes('large')) {
    fail(file, v.id, `dataset(s) ${oversized.join(', ')} exceed ${LARGE_DATA_BYTES} bytes: tag the vector 'large'`)
  }

  // $data content-descriptor references
  for (const ref of dataRefs([prereqs, v.steps])) {
    if (!data[ref]) fail(file, v.id, `{"$data": "${ref}"} references undeclared dataset`)
  }

  const credHandles = prereqs.filter(p => p.type === 'credential').map(p => p.body.handle)
  const captured = new Set()
  const usedData = new Set()

  v.steps.forEach((step, i) => {
    const where = `step ${i + 1}`
    const isHttp = '$http' in step
    const body = step.$operation ?? step.$http ?? {}

    if (body.identity && !RESERVED_IDENTITIES.has(body.identity) && !credHandles.includes(body.identity)) {
      fail(file, v.id, `${where}: identity '${body.identity}' is not main/anonymous/invalid or a credential handle`)
    }
    if (isHttp && body.expect && body.expect.response) {
      fail(file, v.id, `${where}: expect.response is only valid on $operation steps`)
    }
    if (isHttp && body.capture) {
      for (const [name, cp] of Object.entries(body.capture)) {
        const head = cp.split(/[.[]/)[0]
        if (head !== 'status' && head !== 'headers') {
          fail(file, v.id, `${where}: capture '${name}' path must start with 'status' or 'headers' on $http steps`)
        }
      }
    }

    // placeholder references, honoring step order for ${cap.*}
    walkStrings(step, (str, p) => {
      for (const ph of placeholders(str)) {
        const segs = ph.split('.')
        const ns = segs[0]
        if (ns === 'env') {
          if (segs.length !== 2 || !ENV_VARS.has(segs[1])) fail(file, v.id, `${where} (${p}): unknown env placeholder \${${ph}}`)
        } else if (ns === 'res') {
          const [, handle, attr, ...rest] = segs
          const type = resTypes.get(handle)
          if (!type || rest.length > 0 || !RESOURCE_ATTRS[type] || !RESOURCE_ATTRS[type].has(attr)) {
            fail(file, v.id, `${where} (${p}): unresolvable resource placeholder \${${ph}}`)
          }
        } else if (ns === 'cap') {
          if (segs.length !== 2 || !captured.has(segs[1])) {
            fail(file, v.id, `${where} (${p}): \${${ph}} not captured by an earlier step`)
          }
        } else if (ns === 'data') {
          const [, name, field, ...rest] = segs
          if (rest.length > 0 || !data[name] || !DERIVED_FIELDS.has(field)) {
            fail(file, v.id, `${where} (${p}): invalid data placeholder \${${ph}}`)
          } else usedData.add(name)
        } else {
          fail(file, v.id, `${where} (${p}): unknown placeholder namespace in \${${ph}}`)
        }
      }
    })

    for (const name of Object.keys(body.capture || {})) captured.add(name)
  })

  if (opts.digests) {
    for (const name of usedData) {
      for (const field of DERIVED_FIELDS) {
        try { datagen.derived(data, name, field) } catch (e) {
          fail(file, v.id, `dataset '${name}' derived field '${field}': ${e.message}`)
        }
      }
    }
  }
}

function main () {
  const args = process.argv.slice(2)
  const opts = { digests: args.includes('--digests') }
  let files = args.filter(a => !a.startsWith('--'))
  if (files.length === 0) {
    const dir = path.join(ROOT, 'vectors')
    files = fs.existsSync(dir)
      ? fs.readdirSync(dir).filter(f => f.endsWith('.json')).map(f => path.join('vectors', f)).sort()
      : []
  }
  if (files.length === 0) {
    console.error('no vector files found')
    process.exit(1)
  }

  const ajv = new Ajv({ allErrors: true, strictTypes: false })
  addFormats(ajv)
  const validate = ajv.compile(JSON.parse(fs.readFileSync(SCHEMA_PATH, 'utf8')))

  const seenIds = new Map() // id -> file
  let vectorCount = 0

  for (const rel of files) {
    const file = path.isAbsolute(rel) ? rel : path.join(ROOT, rel)
    const relative = path.relative(ROOT, file)
    const relName = relative.startsWith('..') ? file : relative
    let doc
    try {
      doc = JSON.parse(fs.readFileSync(file, 'utf8'))
    } catch (e) {
      fail(relName, null, `invalid JSON: ${e.message}`)
      continue
    }

    if (!validate(doc)) {
      for (const err of validate.errors.slice(0, 20)) {
        fail(relName, null, `schema: ${err.instancePath || '/'} ${err.message}`)
      }
      continue // lint assumes schema-valid shapes
    }

    const expectedGroup = path.basename(file, '.json')

    for (const v of doc.vectors) {
      vectorCount++
      if (v.group !== expectedGroup) fail(relName, v.id, `group '${v.group}' does not match filename`)
      if (!v.id.startsWith(`${v.group}-`)) fail(relName, v.id, `id prefix does not match group '${v.group}'`)
      if (seenIds.has(v.id)) fail(relName, v.id, `duplicate id (also in ${seenIds.get(v.id)})`)
      seenIds.set(v.id, relName)
      if (v.kind === 'api') {
        lintApiVector(relName, v, opts)
        lintMatchers(relName, v.id, v.steps, 'steps')
      }
    }
  }

  if (errors.length > 0) {
    for (const e of errors) console.error(`ERROR ${e}`)
    console.error(`\n${errors.length} error(s) across ${files.length} file(s)`)
    process.exit(1)
  }
  console.log(`OK: ${vectorCount} vectors in ${files.length} file(s)${opts.digests ? ' (digests verified)' : ''}`)
}

main()
