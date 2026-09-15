#![cfg(feature = "datagen")]

use std::collections::{BTreeMap, BTreeSet};

use cloud_portable_s3vectors as s3v;
use s3v::datagen::{derived, generate, DerivedField, DERIVED_FIELDS};
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
        "t40":   {"$prng": {"seed": "test", "size": 40}},
        "t32":   {"$prng": {"seed": "test", "size": 32}},
        "t10":   {"$prng": {"seed": "test", "size": 10}},
        "aaa":   {"$pattern": {"pattern": "A", "size": 5}},
        "abc":   {"$pattern": {"pattern": "abc", "size": 8}},
        "bin":   {"$pattern": {"patternBase64": "3q2+7w==", "size": 6}},
        "sl":    {"$slice": {"of": "t40", "offset": 30, "length": 6}},
        "chain": {"$slice": {"of": "sl", "offset": 0, "length": 1}},
        "over":  {"$slice": {"of": "t10", "offset": 8, "length": 8}},
        "check": {"$pattern": {"pattern": "123456789", "size": 9}}
    }"#,
    )
    .unwrap()
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
    // Every non-slice dataset up to GENERATE_CAP must materialize (slice bounds
    // are corpus-linted). A dataset above the cap runs to gigabytes and no unit
    // test allocates that much; the corpus linter requires those to carry the
    // `large` tag, and the algorithms are size-independent and pinned by check
    // values above. Derived fields regenerate their dataset internally, so
    // exercise them only where the regenerated bytes are small.
    const GENERATE_CAP: u64 = 1 << 26;
    const DERIVED_CAP: u64 = 1 << 20;
    for file in s3v::all() {
        for v in &file.vectors {
            let Vector::Api(api) = v else { continue };
            let Some(data) = &api.data else { continue };
            for (name, spec) in data {
                let parent_size = match spec {
                    DataSpec::Slice(d) => data[&d.of].size(),
                    _ => {
                        if spec.size() <= GENERATE_CAP {
                            let bytes = generate(data, name)
                                .unwrap_or_else(|e| panic!("{}/{name}: {e}", api.id));
                            assert_eq!(bytes.len() as u64, spec.size(), "{}/{name}", api.id);
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
