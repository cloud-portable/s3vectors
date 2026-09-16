package datagen

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"

	s3vectors "github.com/cloud-portable/s3vectors/packages/go"
)

// Independently computed check values shared by all four language ports.
const (
	block0 = "b8cc3d1fcf7818feab07f224263256110eeb3b576a94ef8e7e439b48fc77998b"
	block1 = "64a3a04c326aae7efd121f8468df1ac90ead2ece1e952353903cbcb6ae47618d"
)

func str(s string) *string { return &s }

var stream40 = block0 + block1[:16]

var specs = map[string]s3vectors.DataSpec{
	"t96":    {Prng: &s3vectors.PrngData{Seed: "test", Size: 96}},
	"t40":    {Prng: &s3vectors.PrngData{Seed: "test", Size: 40}},
	"t32":    {Prng: &s3vectors.PrngData{Seed: "test", Size: 32}},
	"t10":    {Prng: &s3vectors.PrngData{Seed: "test", Size: 10}},
	"aaa":    {Pattern: &s3vectors.PatternData{Pattern: str("A"), Size: 5}},
	"abc":    {Pattern: &s3vectors.PatternData{Pattern: str("abc"), Size: 8}},
	"abc100": {Pattern: &s3vectors.PatternData{Pattern: str("abc"), Size: 100}},
	"bin":    {Pattern: &s3vectors.PatternData{PatternBase64: str("3q2+7w=="), Size: 6}},
	"zero":   {Pattern: &s3vectors.PatternData{Pattern: str("A"), Size: 0}},
	"sl":     {Slice: &s3vectors.SliceData{Of: "t40", Offset: 30, Length: 6}},
	"psl":    {Slice: &s3vectors.SliceData{Of: "abc100", Offset: 7, Length: 20}},
	"chain":  {Slice: &s3vectors.SliceData{Of: "sl", Offset: 0, Length: 1}},
	"over":   {Slice: &s3vectors.SliceData{Of: "t10", Offset: 8, Length: 8}},
	"nopat":  {Pattern: &s3vectors.PatternData{Size: 4}},
	"check":  {Pattern: &s3vectors.PatternData{Pattern: str("123456789"), Size: 9}},
}

// Every dataset a range/stream test sweeps over.
var sweep = []string{"t96", "t40", "t32", "t10", "aaa", "abc", "abc100", "bin", "zero", "sl", "psl"}

func mustGenerate(t *testing.T, name string) []byte {
	t.Helper()
	b, err := Generate(specs, name)
	if err != nil {
		t.Fatalf("Generate(%s): %v", name, err)
	}
	return b
}

func mustDerived(t *testing.T, name, field string) string {
	t.Helper()
	s, err := Derived(specs, name, field)
	if err != nil {
		t.Fatalf("Derived(%s, %s): %v", name, field, err)
	}
	return s
}

func b64OfHex(t *testing.T, h string) string {
	t.Helper()
	raw, err := hex.DecodeString(h)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestCheckValues(t *testing.T) {
	stream40 := block0 + block1[:16]
	cases := []struct{ name, wantHex string }{
		{"t32", block0},
		{"t40", stream40},
		{"t10", block0[:20]},
		{"bin", "deadbeefdead"},
		{"sl", stream40[60:72]},
	}
	for _, c := range cases {
		if got := hex.EncodeToString(mustGenerate(t, c.name)); got != c.wantHex {
			t.Errorf("%s = %s, want %s", c.name, got, c.wantHex)
		}
	}
	if got := string(mustGenerate(t, "aaa")); got != "AAAAA" {
		t.Errorf("aaa = %q", got)
	}
	if got := string(mustGenerate(t, "abc")); got != "abcabcab" {
		t.Errorf("abc = %q", got)
	}

	derivedCases := []struct{ name, field, want string }{
		{"aaa", "md5", "f6a6263167c92de8644ac998b3c4e4d1"},
		{"aaa", "etag", `"f6a6263167c92de8644ac998b3c4e4d1"`},
		{"aaa", "size", "5"},
		// CRC catalog check values over ASCII "123456789"
		{"check", "crc32B64", b64OfHex(t, "cbf43926")},
		{"check", "crc32cB64", b64OfHex(t, "e3069283")},
		{"check", "crc64nvmeB64", b64OfHex(t, "ae8b14860a799888")},
	}
	for _, c := range derivedCases {
		if got := mustDerived(t, c.name, c.field); got != c.want {
			t.Errorf("derived(%s, %s) = %s, want %s", c.name, c.field, got, c.want)
		}
	}
	for _, f := range DerivedFields {
		mustDerived(t, "sl", f)
	}
}

func TestErrorCases(t *testing.T) {
	cases := []struct {
		run  func() error
		want string
	}{
		{func() error { _, err := Generate(specs, "nope"); return err }, "unknown dataset"},
		{func() error { _, err := Derived(specs, "aaa", "sha512"); return err }, "unknown derived data field"},
		{func() error { _, err := Generate(specs, "chain"); return err }, "chained slices"},
		{func() error { _, err := Generate(specs, "over"); return err }, "exceeds"},
		{func() error { _, err := Generate(specs, "nopat"); return err }, "neither pattern nor patternBase64"},
		// The spec is resolved before any byte work, so an empty range still
		// reports a bad spec rather than returning empty.
		{func() error { _, err := GenerateRange(specs, "chain", 0, 0); return err }, "chained slices"},
		{func() error { _, err := GenerateRange(specs, "t10", 8, 8); return err }, "exceeds"},
		{func() error { _, err := GenerateRange(specs, "t40", 41, 0); return err }, "exceeds"},
		{func() error { _, err := GenerateRange(specs, "t40", 0, 41); return err }, "exceeds"},
		// The bound is the slice's own length, not the parent's.
		{func() error { _, err := GenerateRange(specs, "sl", 4, 4); return err }, "exceeds"},
		{func() error { _, err := GenerateRange(specs, "t40", -1, 1); return err }, "invalid range"},
		// Readers validate eagerly: the error comes from the constructor.
		{func() error { _, err := NewReader(specs, "over"); return err }, "exceeds"},
		{func() error { _, err := NewRangeReader(specs, "t40", 0, 41); return err }, "exceeds"},
	}
	for _, c := range cases {
		err := c.run()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("want error containing %q, got %v", c.want, err)
		}
	}
}

func mustRange(t *testing.T, name string, offset, length int64) []byte {
	t.Helper()
	b, err := GenerateRange(specs, name, offset, length)
	if err != nil {
		t.Fatalf("GenerateRange(%s, %d, %d): %v", name, offset, length, err)
	}
	return b
}

func TestRangedReads(t *testing.T) {
	cases := []struct {
		name           string
		offset, length int64
		wantHex        string
	}{
		{"t40", 30, 6, stream40[60:72]},  // crosses the 32-byte block boundary
		{"t40", 24, 16, stream40[48:80]}, // starts mid-block, crosses
		{"t40", 36, 4, stream40[72:80]},  // trailing partial block
		{"t96", 0, 32, block0},
		{"t96", 32, 32, block1},       // seek to an exact block boundary
		{"sl", 2, 3, stream40[64:70]}, // ranged read of a slice
		{"abc", 4, 3, "626361"},       // "bca", mid-phase
		{"abc100", 98, 2, "6361"},     // "ca", tail, phase 98 % 3 == 2
		// A slice of a pattern starts mid-period: a port that dropped the slice's
		// base offset would return "abc" here and still pass every whole check.
		{"psl", 0, 3, "626361"}, // "bca"
		{"t40", 40, 0, ""},      // empty read at EOF is legal
	}
	for _, c := range cases {
		if got := hex.EncodeToString(mustRange(t, c.name, c.offset, c.length)); got != c.wantHex {
			t.Errorf("GenerateRange(%s, %d, %d) = %s, want %s", c.name, c.offset, c.length, got, c.wantHex)
		}
	}
	for _, c := range []struct {
		name string
		want int64
	}{{"sl", 6}, {"zero", 0}, {"t96", 96}} {
		got, err := Size(specs, c.name)
		if err != nil || got != c.want {
			t.Errorf("Size(%s) = %d, %v; want %d", c.name, got, err, c.want)
		}
	}
}

func TestRangeEqualsWindowOfWhole(t *testing.T) {
	// Generate walks from zero; a range divides to find its start block and
	// phase, so these are genuinely different code paths. Exhaustive: the
	// fixtures are tiny and boundary bugs only show at specific offsets.
	for _, name := range sweep {
		full := mustGenerate(t, name)
		for offset := int64(0); offset <= int64(len(full)); offset++ {
			for length := int64(0); length <= int64(len(full))-offset; length++ {
				got := mustRange(t, name, offset, length)
				if !bytes.Equal(got, full[offset:offset+length]) {
					t.Fatalf("%s [%d, %d): got %x, want %x", name, offset, offset+length,
						got, full[offset:offset+length])
				}
			}
		}
	}
}

func TestReaderProducesTheSameBytes(t *testing.T) {
	for _, bufSize := range []int{1, 7, 32, 1000} {
		for _, name := range sweep {
			full := mustGenerate(t, name)
			r, err := NewReader(specs, name)
			if err != nil {
				t.Fatalf("NewReader(%s): %v", name, err)
			}
			if r.Size() != int64(len(full)) {
				t.Errorf("%s: Size() = %d, want %d", name, r.Size(), len(full))
			}
			var got []byte
			buf := make([]byte, bufSize)
			for {
				n, err := r.Read(buf)
				got = append(got, buf[:n]...)
				if err == io.EOF {
					if n != 0 {
						t.Errorf("%s: Read returned (%d, io.EOF) together", name, n)
					}
					break
				}
				if err != nil {
					t.Fatalf("%s: Read: %v", name, err)
				}
			}
			if !bytes.Equal(got, full) {
				t.Errorf("%s/%d: got %x, want %x", name, bufSize, got, full)
			}
			// Reading again at EOF keeps reporting EOF.
			if n, err := r.Read(buf); n != 0 || err != io.EOF {
				t.Errorf("%s: read past EOF = (%d, %v)", name, n, err)
			}
		}
	}

	// A zero-length buffer is not EOF.
	r, err := NewReader(specs, "t40")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Errorf("Read(nil) = (%d, %v), want (0, nil)", n, err)
	}

	// A range reader matches the range, and Seek rewinds it.
	rr, err := NewRangeReader(specs, "t96", 30, 40)
	if err != nil {
		t.Fatal(err)
	}
	want := mustRange(t, "t96", 30, 40)
	got, err := io.ReadAll(rr)
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("range reader = %x, %v; want %x", got, err, want)
	}
	if _, err := rr.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	again, err := io.ReadAll(rr)
	if err != nil || !bytes.Equal(again, want) {
		t.Errorf("after Seek = %x, %v; want %x", again, err, want)
	}
}

func TestDigestDoesNotDependOnChunking(t *testing.T) {
	// chunkSize 1 makes every byte a chunk boundary, which is what catches a
	// checksum whose init or xor-out is applied per chunk instead of once.
	for _, name := range []string{"zero", "check", "aaa", "abc100", "t96", "sl", "psl", "bin"} {
		for _, field := range DerivedFields {
			want := mustDerived(t, name, field)
			for _, chunkSize := range []int64{1, 7, 32, 1000, ChunkSize} {
				got, err := derivedChunked(specs, name, field, chunkSize)
				if err != nil || got != want {
					t.Errorf("derivedChunked(%s, %s, %d) = %q, %v; want %q", name, field, chunkSize, got, err, want)
				}
			}
		}
	}
	if got := mustDerived(t, "zero", "md5"); got != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Errorf("md5 of empty = %s", got)
	}
	if got := mustDerived(t, "zero", "size"); got != "0" {
		t.Errorf("size of empty = %s", got)
	}
}

// expectedWindow is an independent statement of the normative formula, used to
// check windows of a dataset too big to materialize. Deliberately not written in
// terms of Generate.
func expectedWindow(t *testing.T, spec s3vectors.DataSpec, offset, length int64) []byte {
	t.Helper()
	if length == 0 { // offset + length - 1 would go negative below
		return nil
	}
	out := make([]byte, length)
	if spec.Pattern != nil {
		var pat []byte
		if spec.Pattern.Pattern != nil {
			pat = []byte(*spec.Pattern.Pattern)
		} else {
			raw, err := base64.StdEncoding.DecodeString(*spec.Pattern.PatternBase64)
			if err != nil {
				t.Fatal(err)
			}
			pat = raw
		}
		for k := int64(0); k < length; k++ {
			out[k] = pat[(offset+k)%int64(len(pat))]
		}
		return out
	}
	var counter [8]byte
	for i := offset / 32; i <= (offset+length-1)/32; i++ {
		binary.BigEndian.PutUint64(counter[:], uint64(i))
		h := sha256.New()
		h.Write([]byte(spec.Prng.Seed))
		h.Write(counter[:])
		block := h.Sum(nil)
		blkStart := i * 32
		lo := blkStart
		if offset > lo {
			lo = offset
		}
		hi := blkStart + 32
		if offset+length < hi {
			hi = offset + length
		}
		copy(out[lo-offset:hi-offset], block[lo-blkStart:hi-blkStart])
	}
	return out
}

// spotCheck reads bounded windows of a dataset too big to hold. The 32-bit
// straddles are the only thing in the suite that can catch a seek that truncates
// an offset to 32 bits.
func spotCheck(t *testing.T, data map[string]s3vectors.DataSpec, name string, size int64, streamSpec s3vectors.DataSpec, sliceBase int64) {
	t.Helper()
	const w = 64 * 1024
	win := func(offset, length int64) {
		got, err := GenerateRange(data, name, offset, length)
		if err != nil {
			t.Fatalf("%s [%d, %d): %v", name, offset, offset+length, err)
		}
		if int64(len(got)) != length {
			t.Fatalf("%s [%d, %d): length %d", name, offset, offset+length, len(got))
		}
		if want := expectedWindow(t, streamSpec, sliceBase+offset, length); !bytes.Equal(got, want) {
			t.Fatalf("%s [%d, %d): got %x, want %x", name, offset, offset+length, got, want)
		}
	}
	head := int64(w)
	if size < head {
		head = size
	}
	win(0, head)
	if size > w {
		win(size-w, w)
	}
	if size > 0 {
		win(size-1, 1)
	}
	for _, b := range []int64{1 << 31, 1 << 32} {
		if b-32 > 0 && b+32 <= size {
			win(b-32, 64)
		}
	}
	if got, err := GenerateRange(data, name, size, 0); err != nil || len(got) != 0 {
		t.Errorf("%s: empty read at EOF = %x, %v", name, got, err)
	}
	if _, err := GenerateRange(data, name, size, 1); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("%s: read past end = %v", name, err)
	}
	if got, err := Derived(data, name, "size"); err != nil || got != strconv.FormatInt(size, 10) {
		t.Errorf("%s: Derived size = %q, %v", name, got, err)
	}
}

func TestFullCorpusDatagen(t *testing.T) {
	// Every non-slice dataset up to generateCap materializes in full. Above the
	// cap a dataset runs to gigabytes — the corpus linter requires those to
	// carry the large tag — so instead of holding one we read bounded windows
	// and check them against an independent statement of the formula.
	//
	// Derived fields still run only below derivedCap: chunking made them bounded
	// in memory, not in time, and each field re-reads the dataset. "size" alone
	// is O(1) and is asserted by spotCheck.
	const generateCap = 1 << 26 // keep equal to LARGE_DATA_BYTES in scripts/validate.js
	const derivedCap = 1 << 20
	all, err := s3vectors.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range all {
		for i := range f.Vectors {
			v := &f.Vectors[i]
			if !v.IsAPI() || len(v.Data) == 0 {
				continue
			}
			for name, spec := range v.Data {
				var parentSize int64
				if spec.Slice != nil {
					parent := v.Data[spec.Slice.Of]
					if parent.Prng != nil {
						parentSize = parent.Prng.Size
					} else if parent.Pattern != nil {
						parentSize = parent.Pattern.Size
					}
					if parentSize > generateCap {
						spotCheck(t, v.Data, name, spec.Slice.Length, parent, spec.Slice.Offset)
					}
				} else {
					if spec.Prng != nil {
						parentSize = spec.Prng.Size
					} else if spec.Pattern != nil {
						parentSize = spec.Pattern.Size
					}
					if parentSize <= generateCap {
						b, err := Generate(v.Data, name)
						if err != nil {
							t.Fatalf("%s/%s: %v", v.ID, name, err)
						}
						if int64(len(b)) != parentSize {
							t.Fatalf("%s/%s: size %d != %d", v.ID, name, len(b), parentSize)
						}
					} else {
						spotCheck(t, v.Data, name, parentSize, spec, 0)
					}
				}
				if parentSize <= derivedCap {
					for _, field := range DerivedFields {
						if _, err := Derived(v.Data, name, field); err != nil {
							t.Fatalf("%s/%s.%s: %v", v.ID, name, field, err)
						}
					}
				}
			}
		}
	}
}

func TestGenerateDoesNotAllocatePerBlock(t *testing.T) {
	// The prng digest is written into a caller-owned array: heap-allocating it
	// per 32-byte block cost ~32k allocations and a byte of garbage per byte
	// generated. One allocation for the output buffer is expected.
	specs := map[string]s3vectors.DataSpec{
		"d": {Prng: &s3vectors.PrngData{Seed: "alloc", Size: 1 << 20}},
	}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	if _, err := Generate(specs, "d"); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&m1)
	if got := m1.Mallocs - m0.Mallocs; got > 8 {
		t.Errorf("Generate made %d allocations for 1 MiB of prng; want a handful", got)
	}
}
