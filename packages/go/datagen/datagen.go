// Package datagen is the Go port of the vector generated-data reference:
// it materializes a vector's named datasets and computes the derived digest
// strings that ${data.<name>.<field>} placeholders resolve to.
//
// Datasets are seekable: GenerateRange and NewReader read a window without
// materializing the rest, and Derived digests in chunks, so a multi-gigabyte
// dataset needs only a chunk of memory.
package datagen

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"strconv"

	s3vectors "github.com/cloud-portable/s3vectors/packages/go"
)

// DerivedFields lists the fields available as ${data.<name>.<field>} placeholders.
var DerivedFields = []string{
	"size", "md5", "etag", "sha256", "sha256B64", "sha1B64", "crc32B64", "crc32cB64", "crc64nvmeB64",
}

// ChunkSize is the bytes per chunk used when streaming or digesting. A multiple
// of the 32-byte prng block, so a chunk boundary never splits a block.
const ChunkSize = 1 << 20

var (
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	// CRC-64/NVME reflected polynomial; Go's crc64 applies the all-ones
	// init/xorout of the NVME parameterization internally.
	crc64Table = crc64.MakeTable(0x9A6C9329AC4BC9B5)
)

// source is a dataset resolved to the stream it reads from. base is the absolute
// offset of the dataset's byte 0 within that stream, which is what makes a $slice
// free: a slice is its parent with a starting offset. Seed and pattern bytes are
// decoded once, here, not per chunk.
type source struct {
	seed   []byte // $prng
	pat    []byte // $pattern
	base   int64
	length int64
}

func resolve(specs map[string]s3vectors.DataSpec, name string) (source, error) {
	spec, ok := specs[name]
	if !ok {
		return source{}, fmt.Errorf("unknown dataset: %s", name)
	}
	switch {
	case spec.Prng != nil:
		return source{seed: []byte(spec.Prng.Seed), length: spec.Prng.Size}, nil
	case spec.Pattern != nil:
		d := spec.Pattern
		var pat []byte
		switch {
		case d.Pattern != nil:
			pat = []byte(*d.Pattern)
		case d.PatternBase64 != nil:
			raw, err := base64.StdEncoding.DecodeString(*d.PatternBase64)
			if err != nil {
				return source{}, fmt.Errorf("dataset %s: bad patternBase64: %w", name, err)
			}
			pat = raw
		default:
			return source{}, fmt.Errorf("dataset %q: neither pattern nor patternBase64", name)
		}
		if len(pat) == 0 {
			return source{}, fmt.Errorf("empty pattern")
		}
		return source{pat: pat, length: d.Size}, nil
	case spec.Slice != nil:
		d := spec.Slice
		parent, ok := specs[d.Of]
		if !ok {
			return source{}, fmt.Errorf("slice %q references unknown dataset %q", name, d.Of)
		}
		if parent.Slice != nil {
			return source{}, fmt.Errorf("slice %q references slice %q (chained slices are not allowed)", name, d.Of)
		}
		src, err := resolve(specs, d.Of) // validates the parent; generates nothing
		if err != nil {
			return source{}, err
		}
		if d.Offset > src.length || d.Length > src.length-d.Offset {
			return source{}, fmt.Errorf("slice %q [%d, %d) exceeds %q size %d",
				name, d.Offset, d.Offset+d.Length, d.Of, src.length)
		}
		src.base += d.Offset
		src.length = d.Length
		return src, nil
	default:
		return source{}, fmt.Errorf("dataset %s: no $prng/$pattern/$slice key", name)
	}
}

// checkRange is written so no intermediate can overflow: never offset+length > size.
func checkRange(name string, size, offset, length int64) error {
	if offset < 0 || length < 0 {
		return fmt.Errorf("invalid range offset %d length %d for dataset %q", offset, length, name)
	}
	if offset > size || length > size-offset {
		return fmt.Errorf("range [%d, %d) exceeds dataset %q size %d", offset, offset+length, name, size)
	}
	return nil
}

// readInto writes n bytes of src, starting at offset within the dataset, into dst.
// It is the only windowing code; every exported entry point goes through it.
func readInto(src source, offset int64, dst []byte) {
	n := int64(len(dst))
	if n == 0 { // (abs + n - 1) would underflow below
		return
	}
	abs := src.base + offset

	if src.pat != nil {
		// byte N of the stream is pat[N % L], so a range starts at phase abs % L
		L := int64(len(src.pat))
		first := n
		if first > L {
			first = L
		}
		phase := abs % L
		for k := int64(0); k < first; k++ {
			dst[k] = src.pat[(phase+k)%L]
		}
		for filled := first; filled < n; {
			m := filled
			if m > n-filled {
				m = n - filled
			}
			copy(dst[filled:filled+m], dst[:m])
			filled += m
		}
		return
	}

	// block(i) = SHA256(UTF8(seed) || BE64(i)); stream = block(0) || block(1) || ...
	var counter [8]byte
	var digest [sha256.Size]byte // Sum appends here, so no per-block allocation
	h := sha256.New()            // reset per block, so the state is allocated once
	for i := abs / 32; i <= (abs+n-1)/32; i++ {
		binary.BigEndian.PutUint64(counter[:], uint64(i))
		h.Reset()
		h.Write(src.seed)
		h.Write(counter[:])
		block := h.Sum(digest[:0])
		blkStart := i * 32
		lo := blkStart // head trim, nonzero on the first block only
		if abs > lo {
			lo = abs
		}
		hi := blkStart + 32 // clamped to the range end, not the dataset size
		if abs+n < hi {
			hi = abs + n
		}
		copy(dst[lo-abs:hi-abs], block[lo-blkStart:hi-blkStart])
	}
}

// Generate materializes one named dataset from a vector's data map.
func Generate(specs map[string]s3vectors.DataSpec, name string) ([]byte, error) {
	src, err := resolve(specs, name)
	if err != nil {
		return nil, err
	}
	out := make([]byte, src.length)
	readInto(src, 0, out)
	return out, nil
}

// GenerateRange materializes [offset, offset+length) of a named dataset without
// materializing the rest.
func GenerateRange(specs map[string]s3vectors.DataSpec, name string, offset, length int64) ([]byte, error) {
	src, err := resolve(specs, name)
	if err != nil {
		return nil, err
	}
	if err := checkRange(name, src.length, offset, length); err != nil {
		return nil, err
	}
	out := make([]byte, length)
	readInto(src, offset, out)
	return out, nil
}

// Size reports the dataset's declared length in bytes, without generating it.
func Size(specs map[string]s3vectors.DataSpec, name string) (int64, error) {
	src, err := resolve(specs, name)
	return src.length, err
}

// Reader is a bounded-memory io.Reader over a dataset, or a range of one — the
// only way to read a dataset larger than the largest slice that can be allocated.
// It resolves the spec eagerly at construction and holds no reference to the
// specs map afterwards, so Read never fails.
//
// Reader also implements io.Seeker: the underlying stream is seekable, so a
// consumer that rewinds (an HTTP client retrying a request body, say) costs
// nothing and never needs to buffer.
type Reader struct {
	src    source
	offset int64
	length int64
	pos    int64
}

// NewReader returns a Reader over the whole dataset.
func NewReader(specs map[string]s3vectors.DataSpec, name string) (*Reader, error) {
	src, err := resolve(specs, name)
	if err != nil {
		return nil, err
	}
	return &Reader{src: src, length: src.length}, nil
}

// NewRangeReader returns a Reader over [offset, offset+length) of the dataset.
func NewRangeReader(specs map[string]s3vectors.DataSpec, name string, offset, length int64) (*Reader, error) {
	src, err := resolve(specs, name)
	if err != nil {
		return nil, err
	}
	if err := checkRange(name, src.length, offset, length); err != nil {
		return nil, err
	}
	return &Reader{src: src, offset: offset, length: length}, nil
}

// Size reports the total bytes this Reader will produce.
func (r *Reader) Size() int64 { return r.length }

func (r *Reader) Read(p []byte) (int, error) {
	if r.pos >= r.length {
		return 0, io.EOF
	}
	n := int64(len(p))
	if rem := r.length - r.pos; n > rem {
		n = rem
	}
	if n == 0 {
		return 0, nil
	}
	readInto(r.src, r.offset+r.pos, p[:n])
	r.pos += n
	return int(n), nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	pos := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		pos += r.pos
	case io.SeekEnd:
		pos += r.length
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if pos < 0 {
		return 0, fmt.Errorf("negative position %d", pos)
	}
	r.pos = pos
	return pos, nil
}

// derivedChunked digests a dataset in chunks, so peak memory is one chunk at any
// size. This bounds memory, not time: every field re-reads the dataset, so do not
// loop over DerivedFields for a multi-gigabyte dataset. "size" reads nothing.
func derivedChunked(specs map[string]s3vectors.DataSpec, name, field string, chunkSize int64) (string, error) {
	src, err := resolve(specs, name)
	if err != nil {
		return "", err
	}
	if field == "size" {
		return strconv.FormatInt(src.length, 10), nil
	}

	var h hash.Hash
	switch field {
	case "md5", "etag":
		h = md5.New()
	case "sha256", "sha256B64":
		h = sha256.New()
	case "sha1B64":
		h = sha1.New()
	case "crc32B64":
		h = crc32.New(crc32.IEEETable)
	case "crc32cB64":
		h = crc32.New(crc32cTable)
	case "crc64nvmeB64":
		h = crc64.New(crc64Table)
	default:
		return "", fmt.Errorf("unknown derived data field: %s", field)
	}

	bufSize := chunkSize
	if bufSize > src.length {
		bufSize = src.length
	}
	buf := make([]byte, bufSize)
	for pos := int64(0); pos < src.length; pos += chunkSize {
		n := chunkSize
		if rem := src.length - pos; n > rem {
			n = rem
		}
		readInto(src, pos, buf[:n])
		h.Write(buf[:n]) // hash.Hash.Write never returns an error
	}
	sum := h.Sum(nil)

	switch field {
	case "md5":
		return hex.EncodeToString(sum), nil
	case "etag":
		return `"` + hex.EncodeToString(sum) + `"`, nil
	case "sha256":
		return hex.EncodeToString(sum), nil
	default:
		return base64.StdEncoding.EncodeToString(sum), nil
	}
}

// Derived computes the string a ${data.<name>.<field>} placeholder resolves to.
// Computed in bounded memory, whatever the dataset size.
func Derived(specs map[string]s3vectors.DataSpec, name, field string) (string, error) {
	return derivedChunked(specs, name, field, ChunkSize)
}
