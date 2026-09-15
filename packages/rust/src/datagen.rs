//! Rust port of the vector generated-data reference: materialize a vector's
//! named datasets and compute the derived digest strings that
//! `${data.<name>.<field>}` placeholders resolve to.
//!
//! Datasets are seekable: [`generate_range`] and [`Reader`] read a window
//! without materializing the rest, and [`derived`] digests in chunks, so a
//! multi-gigabyte dataset needs only a chunk of memory.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use crc::Crc;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest as _, Sha256};

use crate::model::DataSpec;

/// The fields available as `${data.<name>.<field>}` placeholders.
pub const DERIVED_FIELDS: [DerivedField; 9] = [
    DerivedField::Size,
    DerivedField::Md5,
    DerivedField::Etag,
    DerivedField::Sha256,
    DerivedField::Sha256B64,
    DerivedField::Sha1B64,
    DerivedField::Crc32B64,
    DerivedField::Crc32cB64,
    DerivedField::Crc64NvmeB64,
];

/// A derived digest field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedField {
    Size,
    Md5,
    Etag,
    Sha256,
    Sha256B64,
    Sha1B64,
    Crc32B64,
    Crc32cB64,
    Crc64NvmeB64,
}

impl DerivedField {
    /// The field name as written in placeholders (`crc32cB64`, ...).
    pub fn as_str(&self) -> &'static str {
        match self {
            DerivedField::Size => "size",
            DerivedField::Md5 => "md5",
            DerivedField::Etag => "etag",
            DerivedField::Sha256 => "sha256",
            DerivedField::Sha256B64 => "sha256B64",
            DerivedField::Sha1B64 => "sha1B64",
            DerivedField::Crc32B64 => "crc32B64",
            DerivedField::Crc32cB64 => "crc32cB64",
            DerivedField::Crc64NvmeB64 => "crc64nvmeB64",
        }
    }
}

impl std::str::FromStr for DerivedField {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        DERIVED_FIELDS
            .into_iter()
            .find(|f| f.as_str() == s)
            .ok_or_else(|| Error::UnknownField(s.to_string()))
    }
}

/// Datagen error.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    UnknownDataset(String),
    UnknownField(String),
    ChainedSlice {
        name: String,
        of: String,
    },
    SliceOutOfRange {
        name: String,
        of: String,
    },
    BadPattern(String),
    /// A requested `[offset, offset + length)` lies outside the dataset.
    RangeOutOfBounds {
        name: String,
        offset: u64,
        length: u64,
        size: u64,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownDataset(name) => write!(f, "unknown dataset: {name}"),
            Error::UnknownField(field) => write!(f, "unknown derived data field: {field}"),
            Error::ChainedSlice { name, of } => {
                write!(f, "slice {name:?} references slice {of:?} (chained slices are not allowed)")
            }
            Error::SliceOutOfRange { name, of } => {
                write!(f, "slice {name:?} exceeds bounds of {of:?}")
            }
            Error::BadPattern(name) => write!(f, "dataset {name:?} has an invalid pattern"),
            Error::RangeOutOfBounds {
                name,
                offset,
                length,
                size,
            } => write!(
                f,
                "range [{offset}, {}) exceeds dataset {name:?} size {size}",
                offset.saturating_add(*length)
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Bytes per chunk when streaming or digesting. A multiple of the 32-byte prng
/// block, so a chunk boundary never splits a block.
pub const CHUNK_SIZE: usize = 1 << 20;

/// A dataset resolved to the stream it reads from.
///
/// `base` is the absolute offset of the dataset's byte 0 within that stream,
/// which is what makes a `$slice` free: a slice is its parent with a starting
/// offset. Seed and pattern bytes are decoded once, here, not per chunk.
#[derive(Debug, Clone)]
struct Source {
    seed: Option<Vec<u8>>,
    pat: Option<Vec<u8>>,
    base: u64,
    length: u64,
}

fn resolve(specs: &BTreeMap<String, DataSpec>, name: &str) -> Result<Source, Error> {
    let spec = specs
        .get(name)
        .ok_or_else(|| Error::UnknownDataset(name.to_string()))?;
    match spec {
        DataSpec::Prng(d) => Ok(Source {
            seed: Some(d.seed.as_bytes().to_vec()),
            pat: None,
            base: 0,
            length: d.size,
        }),
        DataSpec::Pattern(d) => {
            let pat = match (&d.pattern, &d.pattern_base64) {
                (Some(p), _) => p.as_bytes().to_vec(),
                (None, Some(b64)) => BASE64
                    .decode(b64)
                    .map_err(|_| Error::BadPattern(name.to_string()))?,
                (None, None) => return Err(Error::BadPattern(name.to_string())),
            };
            if pat.is_empty() {
                return Err(Error::BadPattern(name.to_string()));
            }
            Ok(Source {
                seed: None,
                pat: Some(pat),
                base: 0,
                length: d.size,
            })
        }
        DataSpec::Slice(d) => {
            let parent = specs
                .get(&d.of)
                .ok_or_else(|| Error::UnknownDataset(d.of.clone()))?;
            if matches!(parent, DataSpec::Slice(_)) {
                return Err(Error::ChainedSlice {
                    name: name.to_string(),
                    of: d.of.clone(),
                });
            }
            // Validates the parent and decodes its pattern; generates nothing.
            let src = resolve(specs, &d.of)?;
            if d.offset > src.length || d.length > src.length - d.offset {
                return Err(Error::SliceOutOfRange {
                    name: name.to_string(),
                    of: d.of.clone(),
                });
            }
            Ok(Source {
                seed: src.seed,
                pat: src.pat,
                base: src.base + d.offset,
                length: d.length,
            })
        }
    }
}

// Written so no intermediate can overflow: never `offset + length > size`.
fn check_range(name: &str, size: u64, offset: u64, length: u64) -> Result<(), Error> {
    if offset > size || length > size - offset {
        return Err(Error::RangeOutOfBounds {
            name: name.to_string(),
            offset,
            length,
            size,
        });
    }
    Ok(())
}

/// Write `dst.len()` bytes of `src`, starting at `offset` within the dataset.
///
/// The only windowing code; every public entry point goes through it.
fn read_into(src: &Source, offset: u64, dst: &mut [u8]) {
    let n = dst.len() as u64;
    if n == 0 {
        return; // (abs + n - 1) would underflow below
    }
    let abs = src.base + offset;

    if let Some(pat) = &src.pat {
        // byte N of the stream is pat[N % L], so a range starts at phase abs % L
        let l = pat.len();
        let first = usize::min(dst.len(), l);
        let phase = (abs % l as u64) as usize;
        for (k, slot) in dst[..first].iter_mut().enumerate() {
            *slot = pat[(phase + k) % l];
        }
        let mut filled = first;
        while filled < dst.len() {
            let m = usize::min(filled, dst.len() - filled);
            dst.copy_within(..m, filled);
            filled += m;
        }
        return;
    }

    // block(i) = SHA256(UTF8(seed) || BE64(i)); stream = block(0) || block(1) || ...
    let seed = src.seed.as_deref().unwrap_or(&[]);
    for i in (abs / 32)..=((abs + n - 1) / 32) {
        let mut h = Sha256::new();
        h.update(seed);
        h.update(i.to_be_bytes());
        let block = h.finalize();
        let blk_start = i * 32;
        let lo = u64::max(abs, blk_start) - blk_start; // head trim, first block only
        let hi = u64::min(abs + n, blk_start + 32) - blk_start; // clamped to the range end
        let at = (blk_start + lo - abs) as usize;
        dst[at..at + (hi - lo) as usize].copy_from_slice(&block[lo as usize..hi as usize]);
    }
}

// A length that must fit this platform's addressable memory. Keeps offsets u64
// throughout and casts only the (bounded) allocation length.
fn to_usize(name: &str, length: u64, size: u64) -> Result<usize, Error> {
    usize::try_from(length).map_err(|_| Error::RangeOutOfBounds {
        name: name.to_string(),
        offset: 0,
        length,
        size,
    })
}

/// Materialize one named dataset from a vector's `data` map.
pub fn generate(specs: &BTreeMap<String, DataSpec>, name: &str) -> Result<Vec<u8>, Error> {
    let src = resolve(specs, name)?;
    let mut out = vec![0u8; to_usize(name, src.length, src.length)?];
    read_into(&src, 0, &mut out);
    Ok(out)
}

/// Materialize `[offset, offset + length)` without materializing the rest.
pub fn generate_range(
    specs: &BTreeMap<String, DataSpec>,
    name: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, Error> {
    let src = resolve(specs, name)?;
    check_range(name, src.length, offset, length)?;
    let mut out = vec![0u8; to_usize(name, length, src.length)?];
    read_into(&src, offset, &mut out);
    Ok(out)
}

/// The dataset's declared length in bytes, without generating it.
pub fn size(specs: &BTreeMap<String, DataSpec>, name: &str) -> Result<u64, Error> {
    Ok(resolve(specs, name)?.length)
}

/// A bounded-memory [`Read`] over a dataset, or a range of one — the only way to
/// read a dataset larger than this platform's addressable memory.
///
/// The spec is resolved eagerly at construction, so every error surfaces from
/// [`Reader::new`] / [`Reader::range`] and [`Read::read`] never fails. The reader
/// owns its resolved source, so it is independent of the specs map and `Send`.
#[derive(Debug, Clone)]
pub struct Reader {
    src: Source,
    offset: u64,
    length: u64,
    pos: u64,
}

impl Reader {
    /// A reader over the whole dataset.
    pub fn new(specs: &BTreeMap<String, DataSpec>, name: &str) -> Result<Self, Error> {
        let src = resolve(specs, name)?;
        let length = src.length;
        Ok(Reader {
            src,
            offset: 0,
            length,
            pos: 0,
        })
    }

    /// A reader over `[offset, offset + length)` of the dataset.
    pub fn range(
        specs: &BTreeMap<String, DataSpec>,
        name: &str,
        offset: u64,
        length: u64,
    ) -> Result<Self, Error> {
        let src = resolve(specs, name)?;
        check_range(name, src.length, offset, length)?;
        Ok(Reader {
            src,
            offset,
            length,
            pos: 0,
        })
    }

    /// Total bytes this reader will produce.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Whether this reader will produce no bytes.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.length - self.pos;
        if remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let n = usize::min(buf.len(), remaining.try_into().unwrap_or(usize::MAX));
        read_into(&self.src, self.offset + self.pos, &mut buf[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

// `static`, not `const`: `Crc::digest()` borrows self, and a const would be
// materialized as a temporary that drops at the end of the statement (E0716).
static CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
static CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
static CRC64NVME: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_NVME);

/// Digest a dataset in chunks, so peak memory is one chunk at any size.
///
/// This bounds memory, not time: every field re-reads the dataset, so do not
/// loop over [`DERIVED_FIELDS`] for a multi-gigabyte dataset. `Size` reads nothing.
fn derived_chunked(
    specs: &BTreeMap<String, DataSpec>,
    name: &str,
    field: DerivedField,
    chunk_size: usize,
) -> Result<String, Error> {
    let src = resolve(specs, name)?;
    if field == DerivedField::Size {
        return Ok(src.length.to_string());
    }

    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut sha1 = Sha1::new();
    let mut crc32 = CRC32.digest();
    let mut crc32c = CRC32C.digest();
    let mut crc64 = CRC64NVME.digest();

    // Derive the buffer from chunk_size, and narrow src.length only when it is the
    // smaller of the two: a multi-gigabyte dataset digests fine on a 32-bit target
    // because only one chunk is ever allocated.
    let cap = if src.length < chunk_size as u64 {
        src.length as usize
    } else {
        chunk_size
    }
    .max(1);
    let mut buf = vec![0u8; cap];
    let mut pos = 0u64;
    while pos < src.length {
        let n = u64::min(chunk_size as u64, src.length - pos) as usize;
        let chunk = &mut buf[..n];
        read_into(&src, pos, chunk);
        match field {
            DerivedField::Md5 | DerivedField::Etag => md5.update(&*chunk),
            DerivedField::Sha256 | DerivedField::Sha256B64 => sha256.update(&*chunk),
            DerivedField::Sha1B64 => sha1.update(&*chunk),
            DerivedField::Crc32B64 => crc32.update(chunk),
            DerivedField::Crc32cB64 => crc32c.update(chunk),
            DerivedField::Crc64NvmeB64 => crc64.update(chunk),
            DerivedField::Size => unreachable!("returned above"),
        }
        pos += n as u64;
    }

    Ok(match field {
        DerivedField::Size => unreachable!("returned above"),
        DerivedField::Md5 => hex(&md5.finalize()),
        DerivedField::Etag => format!("\"{}\"", hex(&md5.finalize())),
        DerivedField::Sha256 => hex(&sha256.finalize()),
        DerivedField::Sha256B64 => BASE64.encode(sha256.finalize()),
        DerivedField::Sha1B64 => BASE64.encode(sha1.finalize()),
        DerivedField::Crc32B64 => BASE64.encode(crc32.finalize().to_be_bytes()),
        DerivedField::Crc32cB64 => BASE64.encode(crc32c.finalize().to_be_bytes()),
        DerivedField::Crc64NvmeB64 => BASE64.encode(crc64.finalize().to_be_bytes()),
    })
}

/// Compute the string a `${data.<name>.<field>}` placeholder resolves to.
/// Computed in bounded memory, whatever the dataset size.
pub fn derived(
    specs: &BTreeMap<String, DataSpec>,
    name: &str,
    field: DerivedField,
) -> Result<String, Error> {
    derived_chunked(specs, name, field, CHUNK_SIZE)
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> BTreeMap<String, DataSpec> {
        serde_json::from_str(
            r#"{
            "t96":    {"$prng": {"seed": "test", "size": 96}},
            "aaa":    {"$pattern": {"pattern": "A", "size": 5}},
            "abc100": {"$pattern": {"pattern": "abc", "size": 100}},
            "bin":    {"$pattern": {"patternBase64": "3q2+7w==", "size": 6}},
            "zero":   {"$pattern": {"pattern": "A", "size": 0}},
            "sl":     {"$slice": {"of": "t96", "offset": 30, "length": 6}},
            "psl":    {"$slice": {"of": "abc100", "offset": 7, "length": 20}},
            "check":  {"$pattern": {"pattern": "123456789", "size": 9}}
        }"#,
        )
        .unwrap()
    }

    /// A digest must not depend on how the dataset was cut into chunks.
    ///
    /// `chunk_size` 1 makes every byte a chunk boundary, which is what catches a
    /// checksum whose init or xor-out is applied per chunk instead of once. Lives
    /// in the crate because `derived_chunked` is private.
    #[test]
    fn digest_does_not_depend_on_chunking() {
        let specs = specs();
        for name in ["zero", "check", "aaa", "abc100", "t96", "sl", "psl", "bin"] {
            for field in DERIVED_FIELDS {
                let want = derived(&specs, name, field).unwrap();
                for chunk_size in [1usize, 7, 32, 1000, CHUNK_SIZE] {
                    assert_eq!(
                        derived_chunked(&specs, name, field, chunk_size).unwrap(),
                        want,
                        "{name}.{}/{chunk_size}",
                        field.as_str()
                    );
                }
            }
        }
        // The CRC catalog check values must survive a 9-way split.
        assert_eq!(
            derived_chunked(&specs, "check", DerivedField::Crc32B64, 1).unwrap(),
            BASE64.encode(0xcbf4_3926u32.to_be_bytes())
        );
        assert_eq!(
            derived_chunked(&specs, "check", DerivedField::Crc32cB64, 1).unwrap(),
            BASE64.encode(0xe306_9283u32.to_be_bytes())
        );
        assert_eq!(
            derived_chunked(&specs, "check", DerivedField::Crc64NvmeB64, 1).unwrap(),
            BASE64.encode(0xae8b_1486_0a79_9888u64.to_be_bytes())
        );
    }
}
