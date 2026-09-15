"""Python port of the vector generated-data reference.

Materializes a vector's named datasets and computes the derived digest strings
that ``${data.<name>.<field>}`` placeholders resolve to. Stdlib-only: CRC-32C
and CRC-64/NVME use pure-Python tables (slow on large datasets; the digest
algorithms are size-independent).

Datasets are seekable: :func:`generate_range` and :func:`generate_stream` read a
window without materializing the rest, and :func:`derived` digests in chunks, so
a multi-gigabyte dataset needs only a chunk of memory.
"""

from __future__ import annotations

import base64
import hashlib
import struct
import zlib
from collections.abc import Iterator, Mapping
from typing import Any

__all__ = [
    "DERIVED_FIELDS", "CHUNK_SIZE", "generate", "generate_range",
    "generate_stream", "data_size", "derived",
]

#: The fields available as ``${data.<name>.<field>}`` placeholders.
DERIVED_FIELDS: tuple[str, ...] = (
    "size", "md5", "etag", "sha256", "sha256B64", "sha1B64",
    "crc32B64", "crc32cB64", "crc64nvmeB64",
)

#: Bytes per chunk when streaming or digesting. A multiple of the 32-byte prng block.
CHUNK_SIZE: int = 1 << 20


class _Source:
    """A dataset resolved to the stream it reads from.

    ``base`` is the absolute offset of the dataset's byte 0 within that stream,
    which is what makes a ``$slice`` free: a slice is its parent with a starting
    offset. Seed and pattern bytes are decoded once, here, not per chunk.
    """

    __slots__ = ("seed", "pat", "base", "length")

    def __init__(self, seed: bytes | None, pat: bytes | None, base: int, length: int) -> None:
        self.seed = seed
        self.pat = pat
        self.base = base
        self.length = length


def _resolve(specs: Mapping[str, Mapping[str, Any]], name: str) -> _Source:
    spec = specs.get(name)
    if spec is None:
        raise KeyError(f"unknown dataset: {name}")
    if "$prng" in spec:
        d = spec["$prng"]
        return _Source(d["seed"].encode("utf-8"), None, 0, d["size"])
    if "$pattern" in spec:
        d = spec["$pattern"]
        if "pattern" in d:
            pat = d["pattern"].encode("utf-8")
        elif "patternBase64" in d:
            pat = base64.b64decode(d["patternBase64"])
        else:
            raise KeyError(f"dataset {name!r}: neither pattern nor patternBase64")
        if not pat:
            raise ValueError("empty pattern")
        return _Source(None, pat, 0, d["size"])
    if "$slice" in spec:
        d = spec["$slice"]
        parent = specs.get(d["of"])
        if parent is None:
            raise KeyError(f"slice {name!r} references unknown dataset {d['of']!r}")
        if "$slice" in parent:
            raise ValueError(f"slice {name!r} references slice {d['of']!r} (chained slices are not allowed)")
        src = _resolve(specs, d["of"])  # validates the parent; generates nothing
        offset, length = d["offset"], d["length"]
        if offset > src.length or length > src.length - offset:
            raise ValueError(
                f"slice {name!r} [{offset}, {offset + length}) exceeds {d['of']!r} size {src.length}"
            )
        return _Source(src.seed, src.pat, src.base + offset, length)
    raise ValueError(f"unknown data kind: {sorted(spec)}")


def _check_range(name: str, size: int, offset: int, length: int) -> None:
    # Written so no intermediate can overflow: never ``offset + length > size``.
    if not isinstance(offset, int) or not isinstance(length, int) or isinstance(offset, bool) or isinstance(length, bool):
        raise ValueError(f"invalid range offset {offset!r} length {length!r} for dataset {name!r}")
    if offset < 0 or length < 0:
        raise ValueError(f"invalid range offset {offset} length {length} for dataset {name!r}")
    if offset > size or length > size - offset:
        raise ValueError(f"range [{offset}, {offset + length}) exceeds dataset {name!r} size {size}")


def _read_range(src: _Source, offset: int, n: int) -> bytes:
    """Produce ``n`` bytes of ``src`` starting at ``offset`` within the dataset.

    The only windowing code; every public entry point goes through it.
    """
    if n == 0:  # (abs + n - 1) would go negative below
        return b""
    abs_off = src.base + offset

    if src.pat is not None:
        # byte N of the stream is pattern[N % L], so a range starts at phase abs % L
        pat = src.pat
        L = len(pat)
        phase = abs_off % L
        rotated = pat[phase:] + pat[:phase]
        return (rotated * (n // L + 1))[:n]

    # block(i) = SHA256(UTF8(seed) || BE64(i)); stream = block(0) || block(1) || ...
    # Filled in place through a memoryview: accumulating one bytes object per
    # 32-byte block costs several times the payload in peak memory.
    seeded = hashlib.sha256(src.seed)
    out = bytearray(n)
    mv = memoryview(out)
    for i in range(abs_off // 32, (abs_off + n - 1) // 32 + 1):
        h = seeded.copy()
        h.update(struct.pack(">Q", i))
        block = h.digest()
        blk_start = i * 32
        lo = max(abs_off, blk_start) - blk_start  # head trim, nonzero on the first block only
        hi = min(abs_off + n, blk_start + 32) - blk_start  # clamped to the range end, not the dataset size
        at = blk_start + lo - abs_off
        if hi - lo == 32:
            mv[at:at + 32] = block  # whole block, no slice of the digest
        else:
            mv[at:at + (hi - lo)] = block[lo:hi]
    mv.release()
    return bytes(out)


def generate(specs: Mapping[str, Mapping[str, Any]], name: str) -> bytes:
    """Materialize one named dataset from a vector's ``data`` map."""
    src = _resolve(specs, name)
    return _read_range(src, 0, src.length)


def generate_range(
    specs: Mapping[str, Mapping[str, Any]], name: str, offset: int, length: int
) -> bytes:
    """Materialize ``[offset, offset+length)`` without materializing the rest."""
    src = _resolve(specs, name)
    _check_range(name, src.length, offset, length)
    return _read_range(src, offset, length)


def generate_stream(
    specs: Mapping[str, Mapping[str, Any]],
    name: str,
    *,
    offset: int = 0,
    length: int | None = None,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[bytes]:
    """Iterate a dataset (or a range of one) in ``chunk_size`` pieces.

    Every chunk is ``chunk_size`` bytes except the last; a zero-length range
    yields no chunks. This function deliberately contains no ``yield``: a
    generator function would defer every check below to the first ``next()``,
    making Python the only port that validates lazily.
    """
    src = _resolve(specs, name)
    if length is None:
        _check_range(name, src.length, offset, 0)
        length = src.length - offset
    _check_range(name, src.length, offset, length)
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError(f"invalid chunk_size {chunk_size!r} for dataset {name!r}")
    return _iter_range(src, offset, length, chunk_size)


def _iter_range(src: _Source, offset: int, length: int, chunk_size: int) -> Iterator[bytes]:
    pos = 0
    while pos < length:
        n = min(chunk_size, length - pos)
        yield _read_range(src, offset + pos, n)
        pos += n


def data_size(specs: Mapping[str, Mapping[str, Any]], name: str) -> int:
    """The dataset's declared length in bytes, without generating it."""
    return _resolve(specs, name).length


def _make_crc_table(poly: int, width: int) -> list[int]:
    mask = (1 << width) - 1
    table = []
    for n in range(256):
        c = n
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        table.append(c & mask)
    return table


_CRC32C_TABLE = _make_crc_table(0x82F63B78, 32)
# CRC-64/NVME: reflected poly, init/xorout all-ones.
_CRC64NVME_TABLE = _make_crc_table(0x9A6C9329AC4BC9B5, 64)


def _crc_update(table: list[int], mask: int, reg: int, data: bytes) -> int:
    """Fold ``data`` into a raw CRC register.

    Takes and returns the *raw* register, so a digest can be accumulated across
    chunks: seed it with the all-ones init once and xor it out once at the end.
    (``zlib.crc32`` below uses the other convention — it takes and returns the
    *finalized* value and chains directly from 0.)
    """
    for b in data:
        reg = table[(reg ^ b) & 0xFF] ^ (reg >> 8)
    return reg & mask


def _derived_chunked(
    specs: Mapping[str, Mapping[str, Any]], name: str, field: str, chunk_size: int
) -> str:
    """Digest a dataset in chunks, so peak memory is one chunk at any size.

    This bounds memory, not time: every field re-reads the dataset, so do not
    loop over DERIVED_FIELDS for a multi-gigabyte dataset. ``size`` reads nothing.
    """
    src = _resolve(specs, name)
    if field == "size":
        return str(src.length)

    hasher = None
    table = mask = None
    reg = 0
    crc32_val = 0
    if field in ("md5", "etag"):
        hasher = hashlib.md5()
    elif field in ("sha256", "sha256B64"):
        hasher = hashlib.sha256()
    elif field == "sha1B64":
        hasher = hashlib.sha1()
    elif field == "crc32B64":
        pass
    elif field == "crc32cB64":
        table, mask, reg = _CRC32C_TABLE, 0xFFFFFFFF, 0xFFFFFFFF
    elif field == "crc64nvmeB64":
        table, mask, reg = _CRC64NVME_TABLE, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF
    else:
        raise KeyError(f"unknown derived data field: {field}")

    pos = 0
    while pos < src.length:
        n = min(chunk_size, src.length - pos)
        chunk = _read_range(src, pos, n)
        if hasher is not None:
            hasher.update(chunk)
        elif table is not None:
            reg = _crc_update(table, mask, reg, chunk)
        else:
            crc32_val = zlib.crc32(chunk, crc32_val)
        pos += n

    if field == "md5":
        return hasher.hexdigest()
    if field == "etag":
        return f'"{hasher.hexdigest()}"'
    if field == "sha256":
        return hasher.hexdigest()
    if field == "sha256B64":
        return base64.b64encode(hasher.digest()).decode("ascii")
    if field == "sha1B64":
        return base64.b64encode(hasher.digest()).decode("ascii")
    if field == "crc32B64":
        return base64.b64encode(struct.pack(">I", crc32_val & 0xFFFFFFFF)).decode("ascii")
    if field == "crc32cB64":
        return base64.b64encode(struct.pack(">I", reg ^ mask)).decode("ascii")
    return base64.b64encode(struct.pack(">Q", reg ^ mask)).decode("ascii")


def derived(specs: Mapping[str, Mapping[str, Any]], name: str, field: str) -> str:
    """Compute the string a ``${data.<name>.<field>}`` placeholder resolves to."""
    return _derived_chunked(specs, name, field, CHUNK_SIZE)
