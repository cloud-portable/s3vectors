"""Package self-tests. Run from packages/python:

    PYTHONPATH=src python3 -m unittest discover -s tests

Set S3VECTORS_FULL=1 to run derived-field checks over every dataset the suite
materializes; gigabyte-scale datasets (tagged `large`) are never materialized,
they are spot-checked with ranged reads instead.
"""

import base64
import hashlib
import os
import re
import struct
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import cloud_portable_s3vectors as s3v  # noqa: E402
from cloud_portable_s3vectors import datagen  # noqa: E402

# Independently computed check values shared by all four language ports.
BLOCK0 = "b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b"
BLOCK1 = "64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d"

STREAM40 = BLOCK0 + BLOCK1[:16]

SPECS = {
    "t96": {"$prng": {"seed": "test", "size": 96}},
    "t40": {"$prng": {"seed": "test", "size": 40}},
    "t32": {"$prng": {"seed": "test", "size": 32}},
    "t10": {"$prng": {"seed": "test", "size": 10}},
    "aaa": {"$pattern": {"pattern": "A", "size": 5}},
    "abc": {"$pattern": {"pattern": "abc", "size": 8}},
    "abc100": {"$pattern": {"pattern": "abc", "size": 100}},
    "bin": {"$pattern": {"patternBase64": "3q2+7w==", "size": 6}},
    "zero": {"$pattern": {"pattern": "A", "size": 0}},
    "sl": {"$slice": {"of": "t40", "offset": 30, "length": 6}},
    "psl": {"$slice": {"of": "abc100", "offset": 7, "length": 20}},
    "chain": {"$slice": {"of": "sl", "offset": 0, "length": 1}},
    "over": {"$slice": {"of": "t10", "offset": 8, "length": 8}},
    "nopat": {"$pattern": {"size": 4}},
    "check": {"$pattern": {"pattern": "123456789", "size": 9}},
}

# Every dataset a range/stream test sweeps over.
SWEEP = ["t96", "t40", "t32", "t10", "aaa", "abc", "abc100", "bin", "zero", "sl", "psl"]


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
        with self.assertRaises(KeyError):
            datagen.generate(SPECS, "nopat")

        # The spec is resolved before any byte work, so an empty range still
        # reports a bad spec rather than returning empty.
        with self.assertRaisesRegex(ValueError, "chained slices"):
            datagen.generate_range(SPECS, "chain", 0, 0)
        for name, offset, length in [
            ("t10", 8, 8), ("t40", 41, 0), ("t40", 0, 41), ("sl", 4, 4),
        ]:
            with self.assertRaisesRegex(ValueError, "exceeds"):
                datagen.generate_range(SPECS, name, offset, length)
        with self.assertRaisesRegex(ValueError, "invalid range"):
            datagen.generate_range(SPECS, "t40", -1, 1)

        # Streams validate eagerly: the error comes from the call, not the first
        # next(). A generator function here would defer all of this.
        with self.assertRaisesRegex(ValueError, "exceeds"):
            datagen.generate_stream(SPECS, "over")
        with self.assertRaisesRegex(ValueError, "invalid chunk_size"):
            datagen.generate_stream(SPECS, "t40", chunk_size=0)

    def test_ranged_reads(self):
        self.assertEqual(datagen.generate_range(SPECS, "t40", 30, 6).hex(), STREAM40[60:72])
        self.assertEqual(datagen.generate_range(SPECS, "t40", 24, 16).hex(), STREAM40[48:80])
        self.assertEqual(datagen.generate_range(SPECS, "t40", 36, 4).hex(), STREAM40[72:80])
        self.assertEqual(datagen.generate_range(SPECS, "t96", 0, 32).hex(), BLOCK0)
        self.assertEqual(datagen.generate_range(SPECS, "t96", 32, 32).hex(), BLOCK1)
        self.assertEqual(datagen.generate_range(SPECS, "sl", 2, 3).hex(), STREAM40[64:70])
        self.assertEqual(datagen.generate_range(SPECS, "abc", 4, 3), b"bca")
        self.assertEqual(datagen.generate_range(SPECS, "abc100", 98, 2), b"ca")
        # A slice of a pattern starts mid-period: a port that dropped the slice's
        # base offset would return b"abc" here and still pass every whole check.
        self.assertEqual(datagen.generate_range(SPECS, "psl", 0, 3), b"bca")
        self.assertEqual(datagen.generate(SPECS, "psl"), b"bcabcabcabcabcabcabc")
        self.assertEqual(datagen.generate_range(SPECS, "t40", 40, 0), b"")
        self.assertEqual(datagen.data_size(SPECS, "sl"), 6)
        self.assertEqual(datagen.data_size(SPECS, "zero"), 0)

    def test_range_equals_window_of_whole(self):
        # generate() walks from zero; a range divides to find its start block and
        # phase, so these are genuinely different code paths. Exhaustive: the
        # fixtures are tiny and boundary bugs only show at specific offsets.
        for name in SWEEP:
            full = datagen.generate(SPECS, name)
            for offset in range(len(full) + 1):
                for length in range(len(full) - offset + 1):
                    self.assertEqual(
                        datagen.generate_range(SPECS, name, offset, length),
                        full[offset:offset + length],
                        f"{name} [{offset}, {offset + length})",
                    )

    def test_stream_concatenates_to_the_same_bytes(self):
        for chunk_size in (1, 7, 32, 1000):
            for name in SWEEP:
                full = datagen.generate(SPECS, name)
                parts = list(datagen.generate_stream(SPECS, name, chunk_size=chunk_size))
                self.assertEqual(b"".join(parts), full, f"{name}/{chunk_size}")
                self.assertEqual(len(parts), -(-len(full) // chunk_size))
                self.assertTrue(all(0 < len(p) <= chunk_size for p in parts))
        parts = list(datagen.generate_stream(SPECS, "t96", offset=30, length=40, chunk_size=9))
        self.assertEqual(b"".join(parts), datagen.generate_range(SPECS, "t96", 30, 40))

    def test_digest_does_not_depend_on_chunking(self):
        # chunk_size 1 makes every byte a chunk boundary, which is what catches a
        # CRC register whose init or xor-out is applied per chunk instead of once.
        for name in ("zero", "check", "aaa", "abc100", "t96", "sl", "psl", "bin"):
            for field in datagen.DERIVED_FIELDS:
                want = datagen.derived(SPECS, name, field)
                for chunk_size in (1, 7, 32, 1000, datagen.CHUNK_SIZE):
                    self.assertEqual(
                        datagen._derived_chunked(SPECS, name, field, chunk_size),
                        want,
                        f"{name}.{field}/{chunk_size}",
                    )
        self.assertEqual(datagen.derived(SPECS, "zero", "md5"), "d41d8cd98f00b204e9800998ecf8427e")
        self.assertEqual(datagen.derived(SPECS, "zero", "size"), "0")


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

    def _expected_window(self, spec, offset, length):
        """An independent statement of the normative formula.

        Used to check windows of a dataset too big to materialize; deliberately
        not written in terms of generate().
        """
        if "$pattern" in spec:
            d = spec["$pattern"]
            pat = d["pattern"].encode() if "pattern" in d else base64.b64decode(d["patternBase64"])
            return bytes(pat[(offset + k) % len(pat)] for k in range(length))
        seed = spec["$prng"]["seed"].encode()
        out = bytearray()
        for i in range(offset // 32, (offset + length - 1) // 32 + 1):
            block = hashlib.sha256(seed + struct.pack(">Q", i)).digest()
            start = i * 32
            out += block[max(offset, start) - start:min(offset + length, start + 32) - start]
        return bytes(out)

    def _spot_check(self, data, name, size, stream_spec, slice_base):
        """Read bounded windows of a dataset too big to hold.

        The 32-bit straddles are the only thing in the suite that can catch a
        seek that truncates an offset to 32 bits.
        """
        W = 64 * 1024

        def win(offset, length):
            got = datagen.generate_range(data, name, offset, length)
            self.assertEqual(len(got), length, f"{name} [{offset}, {offset + length}): length")
            self.assertEqual(
                got,
                self._expected_window(stream_spec, slice_base + offset, length),
                f"{name} [{offset}, {offset + length})",
            )

        win(0, min(W, size))
        if size > W:
            win(size - W, W)
        if size > 0:
            win(size - 1, 1)
        for b in (2 ** 31, 2 ** 32):
            if b - 32 > 0 and b + 32 <= size:
                win(b - 32, 64)
        self.assertEqual(datagen.generate_range(data, name, size, 0), b"")
        with self.assertRaisesRegex(ValueError, "exceeds"):
            datagen.generate_range(data, name, size, 1)
        self.assertEqual(datagen.derived(data, name, "size"), str(size))

    def test_full_corpus_datagen(self):
        # Every non-slice dataset up to the cap materializes in full. Above the
        # ceiling a dataset runs to gigabytes (the corpus linter requires those
        # to be tagged `large`), so instead of holding one we read bounded
        # windows and check them against an independent statement of the
        # formula. S3VECTORS_FULL=1 lifts the speed caps to the ceiling but
        # never past it — the spot check covers what lies above.
        #
        # Derived fields still run only below derived_cap: chunking made them
        # bounded in memory, not in time, and each field re-reads the dataset.
        # `size` alone is O(1) and is asserted by the spot check.
        ceiling = 64 * 1024 * 1024  # keep equal to LARGE_DATA_BYTES in scripts/validate.js
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
                        d = spec["$slice"]
                        p = data[d["of"]]
                        parent = (p.get("$prng") or p["$pattern"])["size"]
                        if parent > ceiling:
                            self._spot_check(data, name, d["length"], p, d["offset"])
                    else:
                        size = (spec.get("$prng") or spec["$pattern"])["size"]
                        parent = size
                        if size <= generate_cap:
                            self.assertEqual(len(datagen.generate(data, name)), size, f"{v['id']}/{name}")
                        elif size > ceiling:
                            self._spot_check(data, name, size, spec, 0)
                    if parent <= derived_cap:
                        for field in datagen.DERIVED_FIELDS:
                            datagen.derived(data, name, field)


if __name__ == "__main__":
    unittest.main()
