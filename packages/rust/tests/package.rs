#![cfg(feature = "datagen")]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;

use cloud_portable_s3vectors as s3v;
use s3v::datagen::{
    derived, generate, generate_range, size as data_size, DerivedField, Error, Reader,
    DERIVED_FIELDS,
};
use s3v::{DataSpec, Step, Vector};

// Independently computed check values shared by all four language ports.
const BLOCK0: &str = "b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b";
const BLOCK1: &str = "64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64_of_hex(h: &str) -> String {
    use base64::Engine as _;
    let raw: Vec<u8> = (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
        .collect();
    base64::engine::general_purpose::STANDARD.encode(raw)
}

fn check_specs() -> BTreeMap<String, DataSpec> {
    serde_json::from_str(
        r#"{
        "t96":    {"$prng": {"seed": "test", "size": 96}},
        "t40":    {"$prng": {"seed": "test", "size": 40}},
        "t32":    {"$prng": {"seed": "test", "size": 32}},
        "t10":    {"$prng": {"seed": "test", "size": 10}},
        "aaa":    {"$pattern": {"pattern": "A", "size": 5}},
        "abc":    {"$pattern": {"pattern": "abc", "size": 8}},
        "abc100": {"$pattern": {"pattern": "abc", "size": 100}},
        "bin":    {"$pattern": {"patternBase64": "3q2+7w==", "size": 6}},
        "zero":   {"$pattern": {"pattern": "A", "size": 0}},
        "sl":     {"$slice": {"of": "t40", "offset": 30, "length": 6}},
        "psl":    {"$slice": {"of": "abc100", "offset": 7, "length": 20}},
        "chain":  {"$slice": {"of": "sl", "offset": 0, "length": 1}},
        "over":   {"$slice": {"of": "t10", "offset": 8, "length": 8}},
        "nopat":  {"$pattern": {"size": 4}},
        "check":  {"$pattern": {"pattern": "123456789", "size": 9}}
    }"#,
    )
    .unwrap()
}

/// Every dataset a range/stream test sweeps over.
const SWEEP: [&str; 11] = [
    "t96", "t40", "t32", "t10", "aaa", "abc", "abc100", "bin", "zero", "sl", "psl",
];

fn stream40() -> String {
    format!("{BLOCK0}{}", &BLOCK1[..16])
}

#[test]
fn datagen_ranged_reads() {
    let specs = check_specs();
    let s40 = stream40();
    let r = |n: &str, o: u64, l: u64| hex(&generate_range(&specs, n, o, l).unwrap());
    assert_eq!(r("t40", 30, 6), s40[60..72]); // crosses the 32-byte block boundary
    assert_eq!(r("t40", 24, 16), s40[48..80]); // starts mid-block, crosses
    assert_eq!(r("t40", 36, 4), s40[72..80]); // trailing partial block
    assert_eq!(r("t96", 0, 32), BLOCK0);
    assert_eq!(r("t96", 32, 32), BLOCK1); // seek to an exact block boundary
    assert_eq!(r("sl", 2, 3), s40[64..70]); // ranged read of a slice
    assert_eq!(generate_range(&specs, "abc", 4, 3).unwrap(), b"bca"); // mid-phase
    assert_eq!(generate_range(&specs, "abc100", 98, 2).unwrap(), b"ca"); // phase 98 % 3 == 2

    // A slice of a pattern starts mid-period: a port that dropped the slice's base
    // offset would return b"abc" here and still pass every whole-dataset check.
    assert_eq!(generate_range(&specs, "psl", 0, 3).unwrap(), b"bca");
    assert_eq!(generate(&specs, "psl").unwrap(), b"bcabcabcabcabcabcabc");
    assert!(generate_range(&specs, "t40", 40, 0).unwrap().is_empty()); // empty read at EOF
    assert_eq!(data_size(&specs, "sl").unwrap(), 6);
    assert_eq!(data_size(&specs, "zero").unwrap(), 0);
}

#[test]
fn range_equals_window_of_whole() {
    // generate() walks from zero; a range divides to find its start block and
    // phase, so these are genuinely different code paths. Exhaustive: the
    // fixtures are tiny and boundary bugs only show at specific offsets.
    let specs = check_specs();
    for name in SWEEP {
        let full = generate(&specs, name).unwrap();
        for offset in 0..=full.len() as u64 {
            for length in 0..=(full.len() as u64 - offset) {
                let got = generate_range(&specs, name, offset, length).unwrap();
                let want = &full[offset as usize..(offset + length) as usize];
                assert_eq!(got, want, "{name} [{offset}, {})", offset + length);
            }
        }
    }
}

#[test]
fn reader_produces_the_same_bytes() {
    let specs = check_specs();
    for buf_size in [1usize, 7, 32, 1000] {
        for name in SWEEP {
            let full = generate(&specs, name).unwrap();
            let mut r = Reader::new(&specs, name).unwrap();
            assert_eq!(r.len(), full.len() as u64);
            assert_eq!(r.is_empty(), full.is_empty());
            let mut got = Vec::new();
            let mut buf = vec![0u8; buf_size];
            loop {
                let n = r.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            assert_eq!(got, full, "{name}/{buf_size}");
            assert_eq!(r.read(&mut buf).unwrap(), 0, "{name}: read past EOF");
        }
    }

    // A zero-length buffer reads nothing and is not an error.
    let mut r = Reader::new(&specs, "t40").unwrap();
    assert_eq!(r.read(&mut []).unwrap(), 0);

    // A range reader matches the range.
    let mut rr = Reader::range(&specs, "t96", 30, 40).unwrap();
    let mut got = Vec::new();
    rr.read_to_end(&mut got).unwrap();
    assert_eq!(got, generate_range(&specs, "t96", 30, 40).unwrap());
}

#[test]
fn digest_does_not_depend_on_chunking() {
    // Chunk invariance is proven inside the crate (see the #[cfg(test)] module in
    // src/datagen.rs) because derived_chunked is private. Here we pin the values
    // an empty dataset must produce, which is the edge the chunked fold can drop.
    let specs = check_specs();
    assert_eq!(
        derived(&specs, "zero", DerivedField::Md5).unwrap(),
        "d41d8cd98f00b204e9800998ecf8427e"
    );
    assert_eq!(derived(&specs, "zero", DerivedField::Size).unwrap(), "0");
    assert_eq!(
        derived(&specs, "zero", DerivedField::Crc32B64).unwrap(),
        b64_of_hex("00000000")
    );
    assert_eq!(
        derived(&specs, "zero", DerivedField::Crc64NvmeB64).unwrap(),
        b64_of_hex("0000000000000000")
    );
}

#[test]
fn datagen_check_values() {
    let specs = check_specs();
    let stream40 = format!("{BLOCK0}{}", &BLOCK1[..16]);
    assert_eq!(hex(&generate(&specs, "t32").unwrap()), BLOCK0);
    assert_eq!(hex(&generate(&specs, "t40").unwrap()), stream40);
    assert_eq!(hex(&generate(&specs, "t10").unwrap()), &BLOCK0[..20]);
    assert_eq!(generate(&specs, "aaa").unwrap(), b"AAAAA");
    assert_eq!(generate(&specs, "abc").unwrap(), b"abcabcab");
    assert_eq!(hex(&generate(&specs, "bin").unwrap()), "deadbeefdead");
    assert_eq!(hex(&generate(&specs, "sl").unwrap()), &stream40[60..72]);

    assert_eq!(
        derived(&specs, "aaa", DerivedField::Md5).unwrap(),
        "f6a6263167c92de8644ac998b3c4e4d1"
    );
    assert_eq!(
        derived(&specs, "aaa", DerivedField::Etag).unwrap(),
        "\"f6a6263167c92de8644ac998b3c4e4d1\""
    );
    assert_eq!(derived(&specs, "aaa", DerivedField::Size).unwrap(), "5");
    // CRC catalog check values over ASCII "123456789"
    assert_eq!(
        derived(&specs, "check", DerivedField::Crc32B64).unwrap(),
        b64_of_hex("cbf43926")
    );
    assert_eq!(
        derived(&specs, "check", DerivedField::Crc32cB64).unwrap(),
        b64_of_hex("e3069283")
    );
    assert_eq!(
        derived(&specs, "check", DerivedField::Crc64NvmeB64).unwrap(),
        b64_of_hex("ae8b14860a799888")
    );
    for f in DERIVED_FIELDS {
        derived(&specs, "sl", f).unwrap();
    }
}

#[test]
fn datagen_error_cases() {
    let specs = check_specs();
    assert!(generate(&specs, "nope").is_err());
    assert!("sha512".parse::<DerivedField>().is_err());
    assert!(matches!(
        generate(&specs, "chain"),
        Err(s3v::datagen::Error::ChainedSlice { .. })
    ));
    assert!(matches!(
        generate(&specs, "over"),
        Err(s3v::datagen::Error::SliceOutOfRange { .. })
    ));
    assert!(matches!(
        generate(&specs, "nopat"),
        Err(Error::BadPattern(_))
    ));

    // The spec is resolved before any byte work, so an empty range still reports
    // a bad spec rather than returning empty.
    assert!(matches!(
        generate_range(&specs, "chain", 0, 0),
        Err(Error::ChainedSlice { .. })
    ));
    for (name, offset, length) in [("t10", 8, 8), ("t40", 41, 0), ("t40", 0, 41), ("sl", 4, 4)] {
        assert!(
            matches!(
                generate_range(&specs, name, offset, length),
                Err(Error::RangeOutOfBounds { .. })
            ),
            "{name} [{offset}, {})",
            offset + length
        );
    }
    // The message keeps the "exceeds" wording the other ports match on.
    let err = generate_range(&specs, "t10", 8, 8).unwrap_err();
    assert!(err.to_string().contains("exceeds"), "{err}");

    // Readers validate eagerly: the error comes from the constructor.
    assert!(matches!(
        Reader::new(&specs, "over"),
        Err(Error::SliceOutOfRange { .. })
    ));
    assert!(matches!(
        Reader::range(&specs, "t40", 0, 41),
        Err(Error::RangeOutOfBounds { .. })
    ));
}

#[test]
fn shipped_schema_matches_manifest() {
    use sha2::{Digest as _, Sha256};
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/schema/vector.schema.json");
    let schema = std::fs::read(path).expect("shipped schema missing");
    let sum = Sha256::digest(&schema);
    let hex: String = sum.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, s3v::manifest().schema_sha256);
}

#[test]
fn manifest_agreement() {
    let m = s3v::manifest();
    assert!(!m.version.is_empty());
    assert_eq!(m.groups.len(), s3v::groups().count());
    let mut total = 0;
    for entry in &m.groups {
        let f = s3v::group(&entry.group).expect("manifest group loads");
        for v in &f.vectors {
            assert_eq!(v.group(), entry.group, "{}", v.id());
        }
        assert_eq!(f.vectors.len(), entry.count, "{}", entry.group);
        total += f.vectors.len();
    }
    assert_eq!(total, m.total);
    assert!(s3v::group("no-such-group").is_none());
}

#[test]
fn root_equals_union_of_groups() {
    let names: Vec<&str> = s3v::groups().collect();
    let files: Vec<_> = s3v::all().collect();
    assert_eq!(files.len(), names.len());
    let mut ids = BTreeSet::new();
    for (file, name) in files.iter().zip(&names) {
        for v in &file.vectors {
            assert_eq!(&v.group(), name, "{}", v.id());
            assert!(ids.insert(v.id().to_string()), "duplicate id {}", v.id());
            assert!(
                v.id().starts_with(&format!("{}-", v.group())),
                "{} not prefixed with {}",
                v.id(),
                v.group()
            );
        }
    }
    assert_eq!(ids.len(), s3v::manifest().total);
}

#[test]
fn vector_shape_smoke() {
    // deny_unknown_fields on the models makes loading itself strict; here we
    // assert cross-field invariants.
    for file in s3v::all() {
        for v in &file.vectors {
            assert!(!v.title().is_empty(), "{}: title", v.id());
            let tiers = v
                .tags()
                .iter()
                .filter(|t| matches!(t.as_str(), "tier-1" | "tier-2" | "tier-3"))
                .count();
            assert_eq!(tiers, 1, "{}: tier tags", v.id());
            match v {
                Vector::Api(api) => {
                    assert!(!api.steps.is_empty(), "{}: steps", api.id);
                    for s in &api.steps {
                        // externally-tagged decode guarantees exactly one variant
                        match s {
                            Step::Operation(op) => assert!(!op.name.is_empty()),
                            Step::Http(h) => assert!(!h.method.is_empty()),
                        }
                    }
                }
                Vector::Signing(sig) => {
                    assert!(!sig.expect.authorization.is_empty(), "{}", sig.id);
                }
            }
        }
    }
}

#[test]
fn full_corpus_datagen_pass() {
    // Every non-slice dataset up to GENERATE_CAP materializes in full. Above the
    // cap a dataset runs to gigabytes — the corpus linter requires those to carry
    // the `large` tag — so instead of holding one we read bounded windows and
    // check them against an independent statement of the formula (spot_check).
    //
    // Derived fields still run only below DERIVED_CAP: chunking made them bounded
    // in memory, not in time, and each field re-reads the dataset. `Size` alone is
    // O(1) and is asserted by spot_check.
    const GENERATE_CAP: u64 = 1 << 26; // keep equal to LARGE_DATA_BYTES in scripts/validate.js
    const DERIVED_CAP: u64 = 1 << 20;
    for file in s3v::all() {
        for v in &file.vectors {
            let Vector::Api(api) = v else { continue };
            let Some(data) = &api.data else { continue };
            for (name, spec) in data {
                let parent_size = match spec {
                    DataSpec::Slice(d) => {
                        let parent = &data[&d.of];
                        if parent.size() > GENERATE_CAP {
                            spot_check(data, name, d.length, parent, d.offset);
                        }
                        parent.size()
                    }
                    _ => {
                        if spec.size() <= GENERATE_CAP {
                            let bytes = generate(data, name)
                                .unwrap_or_else(|e| panic!("{}/{name}: {e}", api.id));
                            assert_eq!(bytes.len() as u64, spec.size(), "{}/{name}", api.id);
                        } else {
                            spot_check(data, name, spec.size(), spec, 0);
                        }
                        spec.size()
                    }
                };
                if parent_size <= DERIVED_CAP {
                    for field in DERIVED_FIELDS {
                        derived(data, name, field)
                            .unwrap_or_else(|e| panic!("{}/{name}.{}: {e}", api.id, field.as_str()));
                    }
                }
            }
        }
    }
}

/// An independent statement of the normative formula, used to check windows of a
/// dataset too big to materialize. Deliberately not written in terms of generate().
fn expected_window(spec: &DataSpec, offset: u64, length: u64) -> Vec<u8> {
    use sha2::{Digest as _, Sha256};
    let mut out = vec![0u8; length as usize];
    match spec {
        DataSpec::Pattern(d) => {
            use base64::Engine as _;
            let pat: Vec<u8> = match (&d.pattern, &d.pattern_base64) {
                (Some(p), _) => p.as_bytes().to_vec(),
                (None, Some(b64)) => base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .unwrap(),
                (None, None) => panic!("pattern with neither field"),
            };
            for (k, slot) in out.iter_mut().enumerate() {
                *slot = pat[((offset + k as u64) % pat.len() as u64) as usize];
            }
        }
        DataSpec::Prng(d) => {
            for i in (offset / 32)..=((offset + length - 1) / 32) {
                let mut h = Sha256::new();
                h.update(d.seed.as_bytes());
                h.update(i.to_be_bytes());
                let block = h.finalize();
                let blk_start = i * 32;
                let lo = u64::max(offset, blk_start) - blk_start;
                let hi = u64::min(offset + length, blk_start + 32) - blk_start;
                let at = (blk_start + lo - offset) as usize;
                out[at..at + (hi - lo) as usize].copy_from_slice(&block[lo as usize..hi as usize]);
            }
        }
        DataSpec::Slice(_) => panic!("expected_window takes the parent spec"),
    }
    out
}

/// Read bounded windows of a dataset too big to hold. The 32-bit straddles are
/// the only thing in the suite that can catch a seek that truncates an offset to
/// 32 bits.
fn spot_check(
    data: &BTreeMap<String, DataSpec>,
    name: &str,
    size: u64,
    stream_spec: &DataSpec,
    slice_base: u64,
) {
    const W: u64 = 64 * 1024;
    let win = |offset: u64, length: u64| {
        let got = generate_range(data, name, offset, length)
            .unwrap_or_else(|e| panic!("{name} [{offset}, {}): {e}", offset + length));
        assert_eq!(
            got.len() as u64,
            length,
            "{name} [{offset}, {})",
            offset + length
        );
        assert_eq!(
            got,
            expected_window(stream_spec, slice_base + offset, length),
            "{name} [{offset}, {})",
            offset + length
        );
    };
    win(0, u64::min(W, size));
    if size > W {
        win(size - W, W);
    }
    if size > 0 {
        win(size - 1, 1);
    }
    for b in [1u64 << 31, 1u64 << 32] {
        if b > 32 && b + 32 <= size {
            win(b - 32, 64);
        }
    }
    assert!(generate_range(data, name, size, 0).unwrap().is_empty());
    assert!(matches!(
        generate_range(data, name, size, 1),
        Err(Error::RangeOutOfBounds { .. })
    ));
    assert_eq!(
        derived(data, name, DerivedField::Size).unwrap(),
        size.to_string()
    );
}
