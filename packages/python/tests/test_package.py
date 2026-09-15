"""Package self-tests. Run from packages/python:

    PYTHONPATH=src python3 -m unittest discover -s tests

Set S3VECTORS_FULL=1 to run derived-field checks over every dataset the suite
materializes; gigabyte-scale datasets (tagged `large`) are skipped either way.
"""

import base64
import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import cloud_portable_s3vectors as s3v  # noqa: E402
from cloud_portable_s3vectors import datagen  # noqa: E402

# Independently computed check values shared by all four language ports.
BLOCK0 = "b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b"
BLOCK1 = "64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d"

SPECS = {
    "t40": {"$prng": {"seed": "test", "size": 40}},
    "t32": {"$prng": {"seed": "test", "size": 32}},
    "t10": {"$prng": {"seed": "test", "size": 10}},
    "aaa": {"$pattern": {"pattern": "A", "size": 5}},
    "abc": {"$pattern": {"pattern": "abc", "size": 8}},
    "bin": {"$pattern": {"patternBase64": "3q2+7w==", "size": 6}},
    "sl": {"$slice": {"of": "t40", "offset": 30, "length": 6}},
    "chain": {"$slice": {"of": "sl", "offset": 0, "length": 1}},
    "over": {"$slice": {"of": "t10", "offset": 8, "length": 8}},
    "check": {"$pattern": {"pattern": "123456789", "size": 9}},
}


def b64_of_hex(h: str) -> str:
    return base64.b64encode(bytes.fromhex(h)).decode("ascii")


class TestDatagen(unittest.TestCase):
    def test_check_values(self):
        stream40 = BLOCK0 + BLOCK1[:16]
        self.assertEqual(datagen.generate(SPECS, "t32").hex(), BLOCK0)
        self.assertEqual(datagen.generate(SPECS, "t40").hex(), stream40)
        self.assertEqual(datagen.generate(SPECS, "t10").hex(), BLOCK0[:20])
        self.assertEqual(datagen.generate(SPECS, "aaa"), b"AAAAA")
        self.assertEqual(datagen.generate(SPECS, "abc"), b"abcabcab")
        self.assertEqual(datagen.generate(SPECS, "bin").hex(), "deadbeefdead")
        self.assertEqual(datagen.generate(SPECS, "sl").hex(), stream40[60:72])

        self.assertEqual(datagen.derived(SPECS, "aaa", "md5"), "f6a6263167c92de8644ac998b3c4e4d1")
        self.assertEqual(datagen.derived(SPECS, "aaa", "etag"), '"f6a6263167c92de8644ac998b3c4e4d1"')
        self.assertEqual(datagen.derived(SPECS, "aaa", "size"), "5")
        # CRC catalog check values over ASCII "123456789"
        self.assertEqual(datagen.derived(SPECS, "check", "crc32B64"), b64_of_hex("cbf43926"))
        self.assertEqual(datagen.derived(SPECS, "check", "crc32cB64"), b64_of_hex("e3069283"))
        self.assertEqual(datagen.derived(SPECS, "check", "crc64nvmeB64"), b64_of_hex("ae8b14860a799888"))
        for field in datagen.DERIVED_FIELDS:
            datagen.derived(SPECS, "sl", field)

    def test_error_cases(self):
        with self.assertRaises(KeyError):
            datagen.generate(SPECS, "nope")
        with self.assertRaises(KeyError):
            datagen.derived(SPECS, "aaa", "sha512")
        with self.assertRaisesRegex(ValueError, "chained slices"):
            datagen.generate(SPECS, "chain")
        with self.assertRaisesRegex(ValueError, "exceeds"):
            datagen.generate(SPECS, "over")


class TestCorpus(unittest.TestCase):
    def test_shipped_schema_matches_manifest(self):
        # Vector files carry "$schema": "../schema/vector.schema.json" — the
        # schema must ship at that location, matching the manifest hash.
        import hashlib
        from importlib import resources

        schema = (resources.files("cloud_portable_s3vectors") / "schema" / "vector.schema.json").read_bytes()
        self.assertEqual(hashlib.sha256(schema).hexdigest(), s3v.manifest()["schemaSha256"])

    def test_manifest_agreement(self):
        m = s3v.manifest()
        total = 0
        for entry in m["groups"]:
            file = s3v.load(entry["group"])
            for v in file["vectors"]:
                self.assertEqual(v["group"], entry["group"], v["id"])
            self.assertEqual(len(file["vectors"]), entry["count"], entry["group"])
            total += len(file["vectors"])
        self.assertEqual(total, m["total"])
        with self.assertRaises(KeyError):
            s3v.load("no-such-group")

    def test_root_equals_union_of_groups(self):
        files = s3v.load_all()
        self.assertEqual([f["vectors"][0]["group"] for f in files], list(s3v.GROUPS))
        ids = set()
        for file in files:
            for v in file["vectors"]:
                self.assertNotIn(v["id"], ids, f"duplicate id {v['id']}")
                ids.add(v["id"])
                self.assertTrue(v["id"].startswith(v["group"] + "-"), v["id"])
        self.assertEqual(len(ids), s3v.manifest()["total"])

    def test_vector_shape_smoke(self):
        tier = re.compile(r"^tier-[123]$")
        for file in s3v.load_all():
            for v in file["vectors"]:
                self.assertIn(v["kind"], ("api", "signing"), v["id"])
                self.assertTrue(v["title"], v["id"])
                self.assertEqual(sum(1 for t in v["tags"] if tier.match(t)), 1, v["id"])
                if v["kind"] == "api":
                    self.assertTrue(v["steps"], v["id"])
                    for s in v["steps"]:
                        self.assertNotEqual("$operation" in s, "$http" in s, v["id"])
                        self.assertEqual(len(s), 1, v["id"])
                    for p in v.get("prerequisites", []):
                        self.assertEqual(len(p), 1, v["id"])
                        self.assertTrue(
                            "$bucket" in p or "$object" in p or "$credential" in p, v["id"]
                        )
                    for name, spec in v.get("data", {}).items():
                        self.assertEqual(len(spec), 1, f"{v['id']}/{name}")
                        self.assertTrue(
                            "$prng" in spec or "$pattern" in spec or "$slice" in spec,
                            f"{v['id']}/{name}",
                        )
                else:
                    self.assertTrue(v["expect"]["authorization"], v["id"])

    def test_full_corpus_datagen(self):
        # Every non-slice dataset up to the cap must materialize (slice bounds
        # are corpus-linted). Derived fields regenerate their dataset
        # internally, so they run only where the regenerated bytes are small —
        # pure-Python CRC is slow. S3VECTORS_FULL=1 lifts those speed caps to
        # the ceiling, but never past it: above the ceiling a dataset runs to
        # gigabytes (the corpus linter requires those to be tagged `large`) and
        # no unit test materializes one.
        ceiling = 64 * 1024 * 1024
        full = os.environ.get("S3VECTORS_FULL") == "1"
        generate_cap = ceiling if full else 8 * 1024 * 1024
        derived_cap = ceiling if full else 64 * 1024
        for file in s3v.load_all():
            for v in file["vectors"]:
                if v["kind"] != "api" or not v.get("data"):
                    continue
                data = v["data"]
                for name, spec in data.items():
                    if "$slice" in spec:
                        p = data[spec["$slice"]["of"]]
                        parent = (p.get("$prng") or p["$pattern"])["size"]
                    else:
                        size = (spec.get("$prng") or spec["$pattern"])["size"]
                        parent = size
                        if size <= generate_cap:
                            self.assertEqual(len(datagen.generate(data, name)), size, f"{v['id']}/{name}")
                    if parent <= derived_cap:
                        for field in datagen.DERIVED_FIELDS:
                            datagen.derived(data, name, field)


if __name__ == "__main__":
    unittest.main()
