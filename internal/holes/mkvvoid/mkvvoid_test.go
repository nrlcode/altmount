package mkvvoid

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/mkvbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holeReader serves a file's bytes and fails the test on any read that
// touches the hole: Layout must decide from the bytes outside it.
type holeReader struct {
	t      *testing.T
	b      []byte
	hs, he int64
	// other are further ranges answered with ErrHole.
	other [][2]int64
	err   error // returned for every read when set
}

func (r *holeReader) ReadAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if off < r.he && r.hs < end {
		r.t.Fatalf("read [%d,%d) touches the hole [%d,%d)", off, end, r.hs, r.he)
	}
	if r.err != nil {
		return 0, r.err
	}
	for _, o := range r.other {
		if off < o[1] && o[0] < end {
			return 0, ErrHole
		}
	}
	return copy(p, r.b[off:]), nil
}

func layoutOf(t *testing.T, b []byte, hs, he int64) Result {
	t.Helper()
	res, err := Layout(&holeReader{t: t, b: b, hs: hs, he: he}, int64(len(b)), hs, he)
	require.NoError(t, err)
	return res
}

// damage returns b with [hs, he) zeroed and, when res.OK, the Void header
// written at res.Offset.
func damage(b []byte, hs, he int64, res Result) []byte {
	out := append([]byte(nil), b...)
	clear(out[hs:he])
	if res.OK {
		h := Header(res.Size)
		copy(out[res.Offset:], h[:])
	}
	return out
}

// childrenOK walks the cluster [start, end) of a damaged copy and reports
// whether every child parses and the sizes land exactly on end, the way a
// demuxer walks it.
func childrenOK(b []byte, cluster mkvbuild.Element) bool {
	e, ok := parseElem(b, int(cluster.Start))
	if !ok {
		return false
	}
	p := e.data
	for p < int(cluster.End) {
		c, ok := parseElem(b, p)
		if !ok || c.unknown {
			return false
		}
		p = c.data + int(c.size)
	}
	return p == int(cluster.End)
}

func TestVintRoundTripEveryWidth(t *testing.T) {
	for w := 1; w <= 8; w++ {
		maxV := int64(1)<<(7*w) - 2 // all-ones is reserved for "unknown"
		for _, v := range []int64{0, 1, maxV / 2, maxV} {
			enc := EncodeVint(v, w)
			require.Len(t, enc, w)
			got, n, unknown, ok := readVint(enc)
			require.True(t, ok)
			assert.False(t, unknown)
			assert.Equal(t, w, n)
			assert.Equal(t, v, got, "width %d", w)
		}
		allOnes := make([]byte, w)
		allOnes[0] = 0xFF >> (w - 1)
		for i := 1; i < w; i++ {
			allOnes[i] = 0xFF
		}
		_, n, unknown, ok := readVint(allOnes)
		require.True(t, ok)
		assert.True(t, unknown, "all-ones %d-byte vint is the unknown size", w)
		assert.Equal(t, w, n)
	}
	_, _, _, ok := readVint([]byte{0x00, 0x01})
	assert.False(t, ok, "a zero first byte is not a vint")
	_, _, _, ok = readVint([]byte{0x40})
	assert.False(t, ok, "a truncated vint is rejected")

	h := Header(0x0102030405)
	assert.Equal(t, []byte{0xEC, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05}, h[:])
	v, n, _, ok := readVint(h[1:])
	require.True(t, ok)
	assert.Equal(t, 8, n)
	assert.EqualValues(t, 0x0102030405, v)
}

func TestReadID(t *testing.T) {
	for _, c := range []struct {
		b  []byte
		id uint32
		n  int
	}{
		{[]byte{0xA3}, 0xA3, 1},
		{[]byte{0x42, 0x86}, 0x4286, 2},
		{[]byte{0x2A, 0xD7, 0xB1}, 0x2AD7B1, 3},
		{[]byte{0x1F, 0x43, 0xB6, 0x75}, 0x1F43B675, 4},
	} {
		id, n, ok := readID(c.b)
		require.True(t, ok)
		assert.Equal(t, c.id, id)
		assert.Equal(t, c.n, n)
	}
	_, _, ok := readID([]byte{0x08, 1, 2, 3, 4})
	assert.False(t, ok, "IDs are at most 4 bytes")
	_, _, ok = readID([]byte{0x00})
	assert.False(t, ok)
}

// basicFile is a two-cluster file. The first cluster holds blocks of known
// size so holes can be placed relative to them.
func basicFile(crc bool) (*mkvbuild.File, []mkvbuild.Element) {
	f := mkvbuild.New(1)
	f.StartCluster(0, crc)
	var blocks []mkvbuild.Element
	for i := range 40 {
		blocks = append(blocks, f.SimpleBlock(1+i%2, 3000+(i*37)%500))
	}
	f.EndCluster()
	f.StartCluster(5000, crc)
	for i := range 10 {
		f.SimpleBlock(1+i%2, 2000)
	}
	f.EndCluster()
	return f, blocks
}

func TestLayout_VoidSkipsToNextBlock(t *testing.T) {
	for _, crc := range []bool{false, true} {
		f, blocks := basicFile(crc)
		b := f.Bytes()
		// Hole from the middle of block 10 to the middle of block 14.
		hs := blocks[10].Start + 1000
		he := blocks[14].Start + 500
		res := layoutOf(t, b, hs, he)
		require.True(t, res.OK, "crc=%v reason=%s", crc, res.Reason)
		assert.Equal(t, blocks[10].End, res.Offset, "the Void starts where the block containing hs ends")
		assert.Equal(t, blocks[15].Start, res.End(), "the Void ends at the first intact block after the hole")
		assert.LessOrEqual(t, res.Offset+HeaderLen, he)

		d := damage(b, hs, he, res)
		assert.True(t, childrenOK(d, f.Clusters[0]), "the damaged cluster must still walk to its end")
		// Nothing outside the hole changed.
		assert.Equal(t, b[:hs], d[:hs])
		assert.Equal(t, b[he:], d[he:])

		z := damage(b, hs, he, Result{})
		assert.False(t, childrenOK(z, f.Clusters[0]), "plain zeros break the walk (what the demuxer sees today)")
	}
}

func TestLayout_HoleOnElementBoundary(t *testing.T) {
	f, blocks := basicFile(false)
	b := f.Bytes()
	hs := blocks[5].Start
	he := blocks[7].Start + 10
	res := layoutOf(t, b, hs, he)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, hs, res.Offset)
	assert.Equal(t, blocks[8].Start, res.End())
}

func TestLayout_WalksBlockGroupVoidAndUnknownChildren(t *testing.T) {
	f := mkvbuild.New(2)
	f.StartCluster(0, true)
	f.SimpleBlock(1, 500)
	f.BlockGroup(2, 700)
	f.Void(100)
	f.Unknown(300)
	f.BlockGroup(1, 800)
	e := f.SimpleBlock(2, 4000) // contains hs
	f.SimpleBlock(1, 4000)      // lost in the hole
	f.BlockGroup(2, 3000)       // straddles he
	next := f.BlockGroup(1, 2000)
	f.SimpleBlock(2, 1000)
	f.Void(50)
	f.SimpleBlock(1, 1000)
	f.EndCluster()
	b := f.Bytes()

	hs := e.Start + 200
	he := next.Start - 100
	res := layoutOf(t, b, hs, he)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, e.End, res.Offset)
	assert.Equal(t, next.Start, res.End(), "a BlockGroup is a valid resume point")
	assert.True(t, childrenOK(damage(b, hs, he, res), f.Clusters[0]))
}

func TestLayout_RejectsFalseBlockHeaderInPayload(t *testing.T) {
	f := mkvbuild.New(3)
	f.StartCluster(0, false)
	for range 5 {
		f.SimpleBlock(1, 2000)
	}
	e := f.SimpleBlock(2, 3000)
	f.SimpleBlock(1, 3000)
	// The block straddling he carries a plausible SimpleBlock header (A3,
	// a size inside the cluster, track 1, room for flags) that does not
	// chain to the cluster end.
	fake := append([]byte{0xA3, 0x88}, make([]byte, 0)...)
	fake = append(fake, []byte{0x81, 0, 0, 0x80, 1, 2, 3, 4}...)
	payload := append(bytes.Repeat([]byte{0x11}, 600), fake...)
	payload = append(payload, bytes.Repeat([]byte{0x22}, 1400)...)
	straddle := f.SimpleBlockPayload(1, payload)
	next := f.SimpleBlock(2, 2000)
	f.SimpleBlock(1, 2000)
	f.EndCluster()
	b := f.Bytes()

	hs := e.Start + 100
	he := straddle.Start + 20 // the fake header lies after he
	fakeAt := int64(bytes.Index(b[straddle.Start:], fake)) + straddle.Start
	require.Greater(t, fakeAt, he)

	c := chainer{b: b[he:f.Clusters[0].End], end: int(f.Clusters[0].End - he), tracks: map[int64]struct{}{1: {}, 2: {}}, memo: map[int]bool{}}
	require.True(t, c.blockOK(int(fakeAt-he)), "the fake header passes the header check alone")

	res := layoutOf(t, b, hs, he)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, next.Start, res.End(), "the false A3 inside the payload must be rejected by chaining")
}

func TestLayout_NoChainFallsBackToClusterEnd(t *testing.T) {
	f := mkvbuild.New(4)
	f.StartCluster(0, false)
	for range 5 {
		f.SimpleBlock(1, 2000)
	}
	e := f.SimpleBlock(2, 3000)
	f.SimpleBlock(1, 3000)
	f.Pad(5000) // garbage: nothing after the hole chains
	f.EndCluster()
	f.StartCluster(100, false)
	f.SimpleBlock(1, 100)
	f.EndCluster()
	b := f.Bytes()
	res := layoutOf(t, b, e.Start+100, e.End+50)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, f.Clusters[0].End, res.End(), "no validated block: the Void runs to the cluster end")
}

// A cluster of unknown size ends wherever the next top-level element
// starts, which may be inside the hole: plain zeros.
func TestLayout_UnknownSizeCluster(t *testing.T) {
	f := mkvbuild.New(5)
	f.StartClusterUnknownSize(0)
	var blocks []mkvbuild.Element
	for range 12 {
		blocks = append(blocks, f.SimpleBlock(1, 2500))
	}
	f.EndCluster()
	f.StartCluster(5000, false)
	f.SimpleBlock(2, 1000)
	f.EndCluster()
	b := f.Bytes()
	res := layoutOf(t, b, blocks[3].Start+10, blocks[6].Start+10)
	assert.False(t, res.OK)
	assert.Equal(t, "unknown-size cluster", res.Reason)
}

func TestLayout_SkipsFalseClusterIDBeforeHole(t *testing.T) {
	f := mkvbuild.New(6)
	f.StartCluster(0, false)
	f.SimpleBlock(1, 1000)
	// A Cluster ID inside a payload, not followed by a valid timestamp.
	f.SimpleBlockPayload(2, append([]byte{0x1F, 0x43, 0xB6, 0x75, 0x81, 0x00}, make([]byte, 500)...))
	e := f.SimpleBlock(1, 3000)
	f.SimpleBlock(2, 3000)
	next := f.SimpleBlock(1, 3000)
	f.EndCluster()
	b := f.Bytes()
	res := layoutOf(t, b, e.Start+100, next.Start-50)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, next.Start, res.End())
}

func TestLayout_Fallbacks(t *testing.T) {
	f, blocks := basicFile(false)
	cues := f.Cues(4000)
	b := f.Bytes()
	tracksEnd := f.Clusters[0].Start // the head ends where the first cluster starts

	cases := []struct {
		name   string
		hs, he int64
		want   string
	}{
		{"block spans the hole", blocks[3].Start + 100, blocks[3].Start + 900, "block spans"},
		{"header does not fit", blocks[3].End - 5, blocks[3].End + 3, "does not fit"},
		{"hole in the file head", 40, 80, "head"},
		{"hole covers the first cluster header", tracksEnd - 2, tracksEnd + 30, "head"},
		{"hole crosses the cluster end", f.Clusters[0].End - 1000, f.Clusters[0].End + 1000, "cluster end"},
		{"hole in cues", cues.Start + 100, cues.Start + 2000, "not inside a cluster"},
		{"E header straddles hs", blocks[3].Start + 2, blocks[5].Start, "straddles"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := layoutOf(t, b, c.hs, c.he)
			assert.False(t, res.OK)
			assert.Contains(t, res.Reason, c.want)
		})
	}
}

func TestLayout_NotMatroska(t *testing.T) {
	f, blocks := basicFile(false)
	b := f.Bytes()
	i := bytes.Index(b, []byte("matroska"))
	copy(b[i:], "mp4fake!")
	res := layoutOf(t, b, blocks[3].Start+10, blocks[6].Start)
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "doctype")
}

func TestLayout_OtherHoleInWindowIsDefiniteZeros(t *testing.T) {
	f, blocks := basicFile(false)
	b := f.Bytes()
	hs, he := blocks[10].Start+100, blocks[12].Start
	r := &holeReader{t: t, b: b, hs: hs, he: he, other: [][2]int64{{blocks[4].Start, blocks[5].Start}}}
	res, err := Layout(r, int64(len(b)), hs, he)
	require.NoError(t, err, "a known hole in the window is a content answer, not a failure")
	assert.False(t, res.OK)
}

func TestLayout_ReadErrorIsUnknown(t *testing.T) {
	f, blocks := basicFile(false)
	b := f.Bytes()
	boom := errors.New("pool timeout")
	hs, he := blocks[10].Start+100, blocks[12].Start
	_, err := Layout(&holeReader{t: t, b: b, hs: hs, he: he, err: boom}, int64(len(b)), hs, he)
	assert.ErrorIs(t, err, boom, "a failed read must not be reported as a definite answer")
}

func TestLayout_ClusterOverBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~70 MB")
	}
	f := mkvbuild.New(7)
	f.StartCluster(0, false)
	f.SimpleBlock(1, MaxNeighbourBytes+(4<<20))
	e := f.SimpleBlock(2, 3000)
	f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	f.EndCluster()
	b := f.Bytes()
	res := layoutOf(t, b, e.Start+100, e.End+100)
	assert.False(t, res.OK)
	assert.Contains(t, res.Reason, "budget")
}

func TestEligible(t *testing.T) {
	assert.True(t, Eligible("a/b.MKV"))
	assert.True(t, Eligible("x.webm"))
	assert.False(t, Eligible("x.mp4"))
	assert.False(t, Eligible("x.mkv.part"))
}
