package mkvvoid

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/kipsilabs/altmount/internal/testsupport/mkvbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Review cases: inputs where an earlier version served a Void that a demuxer
// would misread. Each must now be plain zeros, or the true boundary.

// Unknown-size clusters end wherever the next top-level element starts: a
// Cues element, a cluster header inside the hole, or one split across the
// forward reads. All plain zeros.
func TestLayout_UnknownSizeClusterCases(t *testing.T) {
	t.Run("ends at top-level Cues", func(t *testing.T) {
		f := mkvbuild.New(23)
		f.StartClusterUnknownSize(0)
		e := f.SimpleBlock(1, 3000)
		partial := f.SimpleBlock(1, 3000)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		f.Cues(1000)
		f.StartCluster(5000, false)
		f.SimpleBlock(1, 1000)
		f.EndCluster()
		b := f.Bytes()
		res := layoutOf(t, b, e.Start+100, partial.Start+100)
		assert.False(t, res.OK, "Void would cross the cluster end into Cues")
		assert.Equal(t, "unknown-size cluster", res.Reason)
	})
	t.Run("next cluster header inside the hole", func(t *testing.T) {
		f := mkvbuild.New(25)
		f.StartClusterUnknownSize(0)
		e := f.SimpleBlock(1, 3000)
		f.EndCluster()
		f.StartCluster(5000, false)
		partial := f.SimpleBlock(1, 3000)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		f.StartCluster(10000, false)
		f.SimpleBlock(1, 1000)
		f.EndCluster()
		b := f.Bytes()
		res := layoutOf(t, b, e.Start+100, partial.Start+100)
		assert.False(t, res.OK, "the hole covers the next cluster's header and timestamp")
	})
	t.Run("next cluster header across a forward read boundary", func(t *testing.T) {
		f := mkvbuild.New(91)
		f.StartClusterUnknownSize(0)
		e := f.SimpleBlock(1, 3000)
		blk := f.SimpleBlock(1, 3000)
		hs, he := e.Start+100, blk.Start+100
		f.SimpleBlockTotal(1, int(he+backChunk-4-f.Len()))
		f.EndCluster()
		f.StartCluster(5000, false)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		f.StartCluster(10000, false)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		b := f.Bytes()
		res := layoutOf(t, b, hs, he)
		assert.False(t, res.OK)
		assert.Equal(t, "unknown-size cluster", res.Reason)
	})
}

// A known-size cluster whose header (ID, size, CRC-32, Timestamp) is split
// between two backward reads at any point is still found: candidates are
// validated against every byte up to the hole.
func TestLayout_ClusterHeaderSplitAcrossBackwardReads(t *testing.T) {
	// Header: ID 4 + size 8 + CRC-32 6 + Timestamp 17 = 35 bytes.
	for _, split := range []int64{2, 6, 14, 25, 34} {
		f := mkvbuild.New(40)
		f.StartCluster(0, true)
		c0 := f.Clusters[0].Start
		hs := c0 + split + backChunk
		f.SimpleBlockTotal(1, int(hs-500-f.Len()))
		e := f.SimpleBlock(1, 1000)
		require.True(t, e.Start < hs && hs < e.End)
		var after []mkvbuild.Element
		for range 6 {
			after = append(after, f.SimpleBlock(2, 1000))
		}
		f.EndCluster()
		b := f.Bytes()
		he := hs + 3000
		res := layoutOf(t, b, hs, he)
		require.True(t, res.OK, "split %d: %s", split, res.Reason)
		assert.Equal(t, e.End, res.Offset, "split %d", split)
		for _, x := range after {
			if x.Start >= he {
				assert.Equal(t, x.Start, res.End(), "split %d", split)
				break
			}
		}
	}
}

// A forged block header in the payload of the block the hole cuts, with a
// known track and valid flags, sized to reach the cluster end: it chains,
// and so does the real next block inside its span. Ambiguous: plain zeros.
func TestLayout_FalseBlockSwallowingIntactBlocksIsAmbiguous(t *testing.T) {
	f := mkvbuild.New(22)
	f.StartCluster(0, false)
	e := f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	partial := f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	f.EndCluster()
	b := f.Bytes()
	fakeAt := partial.Start + 500
	copy(b[fakeAt:], []byte{0xA3})
	copy(b[fakeAt+1:], EncodeVint(f.Clusters[0].End-fakeAt-9, 8))
	copy(b[fakeAt+9:], []byte{0x81, 0, 0, 0x80})
	res := layoutOf(t, b, e.Start+100, partial.Start+100)
	assert.False(t, res.OK, "the forged block would swallow every intact block after it")
	assert.Equal(t, "ambiguous block boundary after the hole", res.Reason)
}

// fakeTail builds a cluster where the block the hole cuts ends with a forged
// SimpleBlock (body as given) that ends exactly where that block does, so it
// chains; the real boundary is the next block.
func fakeTail(t *testing.T, seed uint64, body []byte) (b []byte, hs, he, real int64) {
	t.Helper()
	f := mkvbuild.New(seed)
	f.StartCluster(0, false)
	e := f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	fake := append([]byte{0xA3}, EncodeVint(int64(len(body)), 8)...)
	fake = append(fake, body...)
	damaged := f.SimpleBlockPayload(1, append(bytes.Repeat([]byte{0x22}, 600), fake...))
	next := f.SimpleBlock(1, 3000)
	f.SimpleBlock(2, 3000)
	f.EndCluster()
	return f.Bytes(), e.Start + 100, damaged.Start + 50, next.Start
}

func TestLayout_RejectsForgedBlockFlagsAndLacing(t *testing.T) {
	filler := bytes.Repeat([]byte{0x11}, 1000)
	cases := map[string][]byte{
		// All three reserved SimpleBlock flag bits set.
		"reserved flags": append([]byte{0x81, 0, 0, 0x70}, filler...),
		// EBML lacing, 2 frames, first frame 16382 bytes in a 1000-byte body.
		"EBML lace too big": append([]byte{0x81, 0, 0, 0x86, 0x01, 0x7F, 0xFE}, filler...),
		// Xiph lacing, 2 frames, a size of 255*5+0 run past nothing left.
		"Xiph lace too big": append([]byte{0x81, 0, 0, 0x82, 0x01}, bytes.Repeat([]byte{0xFF}, 1000)...),
		// Fixed-size lacing, 3 frames over 1000 bytes.
		"fixed lace uneven": append([]byte{0x81, 0, 0, 0x84, 0x02}, filler...),
		// Fixed-size lacing that declares a single frame (Matroska forbids it).
		"fixed lace one frame": append([]byte{0x81, 0, 0, 0x84, 0x00}, filler...),
		// Xiph and EBML lacing that declare a single frame.
		"Xiph lace one frame": append([]byte{0x81, 0, 0, 0x82, 0x00}, filler...),
		"EBML lace one frame": append([]byte{0x81, 0, 0, 0x86, 0x00, 0x81}, filler...),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			b, hs, he, real := fakeTail(t, 92, body)
			res := layoutOf(t, b, hs, he)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, real, res.End(), "the forged block must be rejected")
		})
	}
}

// Real laced blocks still count as boundaries.
func TestLayout_AcceptsValidLacedBlocks(t *testing.T) {
	cases := map[string][]byte{
		// 3 frames: 300 (255+45), 100, and 50 left.
		"Xiph": append([]byte{0x02, 0xFF, 45, 100}, bytes.Repeat([]byte{0x33}, 450)...),
		// 3 frames: 300, then a difference of -200 (signed vint: 8191-200) = 100, and 50 left.
		"EBML": append(append(append([]byte{0x02}, EncodeVint(300, 2)...), EncodeVint(7991, 2)...), bytes.Repeat([]byte{0x33}, 450)...),
		// 4 frames of 100.
		"fixed": append([]byte{0x03}, bytes.Repeat([]byte{0x33}, 400)...),
		// Lace values have no "unknown" sentinel: 2 frames, the all-ones
		// 1-byte vint is a first size of 127, and 50 left.
		"EBML all-ones size": append([]byte{0x01, 0xFF}, bytes.Repeat([]byte{0x33}, 177)...),
		// 3 frames: 100, then the all-ones 1-byte difference +64 = 164, and 50 left.
		"EBML all-ones difference": append([]byte{0x02, 0x40, 100, 0xFF}, bytes.Repeat([]byte{0x33}, 314)...),
	}
	flags := map[string]byte{"Xiph": 0x82, "EBML": 0x86, "fixed": 0x84, "EBML all-ones size": 0x86, "EBML all-ones difference": 0x86}
	for name, lace := range cases {
		t.Run(name, func(t *testing.T) {
			f := mkvbuild.New(93)
			f.StartCluster(0, false)
			e := f.SimpleBlock(1, 3000)
			damaged := f.SimpleBlock(1, 3000)
			laced := f.SimpleBlockFlags(2, flags[name], lace)
			f.SimpleBlock(1, 3000)
			f.EndCluster()
			b := f.Bytes()
			res := layoutOf(t, b, e.Start+100, damaged.Start+50)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, laced.Start, res.End())
		})
	}
}

// The backward scan keeps what it has read in one buffer that doubles,
// instead of re-copying everything on every read: a ~60 MiB scan allocates a
// small multiple of 60 MiB, not ~1 GiB.
func TestLayout_BackwardScanCopiesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a ~60 MB fixture")
	}
	f := mkvbuild.New(27)
	f.StartCluster(0, false)
	f.SimpleBlock(1, 60<<20)
	e := f.SimpleBlock(1, 4000)
	next := f.SimpleBlock(1, 4000)
	f.SimpleBlock(1, 1000)
	f.EndCluster()
	b := f.Bytes()
	r := bytes.NewReader(b)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	res, err := Layout(r, int64(len(b)), e.Start+100, next.Start+100)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.True(t, res.OK, res.Reason)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("60 MiB backward scan allocated %.1f MiB", float64(allocated)/(1<<20))
	assert.Less(t, allocated, uint64(200<<20))
}

// A valid laced block later in the cluster must not break the chain from
// the real first block after the hole (an all-ones lace value read as
// "unknown" made every earlier candidate fail to chain, so the Void ran on
// to the laced block and swallowed intact blocks).
func TestLayout_AllOnesLaceValueKeepsEarlierBoundary(t *testing.T) {
	cases := map[string][]byte{
		"first size 127": append([]byte{0x02, 0xFF, 0xBF}, bytes.Repeat([]byte{0x33}, 450)...),
		"difference +64": append([]byte{0x02, 0xE4, 0xFF}, bytes.Repeat([]byte{0x33}, 450)...),
	}
	for name, lace := range cases {
		t.Run(name, func(t *testing.T) {
			f := mkvbuild.New(93)
			f.StartCluster(0, false)
			e := f.SimpleBlock(1, 3000)
			cut := f.SimpleBlock(1, 3000)
			first := f.SimpleBlock(1, 3000)
			f.SimpleBlockFlags(2, 0x86, lace)
			f.SimpleBlock(1, 3000)
			f.EndCluster()
			b := f.Bytes()
			res := layoutOf(t, b, e.Start+100, cut.Start+50)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, first.Start, res.End())
		})
	}
}

// The same valid lace must not hide a forged candidate's ambiguity: the
// real blocks inside the forged one's span chain, so it is plain zeros.
func TestLayout_AllOnesLaceValueKeepsAmbiguityVisible(t *testing.T) {
	f := mkvbuild.New(95)
	f.StartCluster(0, false)
	e := f.SimpleBlock(1, 3000)
	cut := f.SimpleBlock(1, 3000)
	f.SimpleBlock(1, 3000)
	f.SimpleBlockFlags(2, 0x86, append([]byte{0x02, 0xE4, 0xFF}, bytes.Repeat([]byte{0x33}, 450)...))
	after := f.SimpleBlock(1, 3000)
	f.EndCluster()
	b := f.Bytes()
	fake := cut.Start + 500
	copy(b[fake:], append(append([]byte{0xA3}, EncodeVint(after.Start-fake-9, 8)...), 0x81, 0, 0, 0x80))
	res := layoutOf(t, b, e.Start+100, cut.Start+50)
	assert.False(t, res.OK)
	assert.Equal(t, "ambiguous block boundary after the hole", res.Reason)
}

// A BlockGroup whose first child is a CRC-32 (allowed, and first when
// present) is a valid block: as the first block after the hole it is the
// boundary, and later in the cluster it does not break the chain from the
// real first block.
func TestLayout_BlockGroupWithCRC(t *testing.T) {
	t.Run("first after the hole", func(t *testing.T) {
		f := mkvbuild.New(97)
		f.StartCluster(0, false)
		e := f.SimpleBlock(1, 3000)
		cut := f.SimpleBlock(1, 3000)
		g := f.BlockGroupCRC(2, 3000)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		res := layoutOf(t, f.Bytes(), e.Start+100, cut.Start+50)
		require.True(t, res.OK, res.Reason)
		assert.Equal(t, g.Start, res.End())
	})
	t.Run("later in the cluster", func(t *testing.T) {
		f := mkvbuild.New(98)
		f.StartCluster(0, false)
		e := f.SimpleBlock(1, 3000)
		cut := f.SimpleBlock(1, 3000)
		first := f.SimpleBlock(1, 3000)
		f.BlockGroupCRC(2, 3000)
		f.SimpleBlock(1, 3000)
		f.EndCluster()
		res := layoutOf(t, f.Bytes(), e.Start+100, cut.Start+50)
		require.True(t, res.OK, res.Reason)
		assert.Equal(t, first.Start, res.End())
	})
}

// A BlockGroup's children come in any order: BlockDuration, ReferenceBlock
// or a CRC-32 may come before the Block (RFC 9559 section 10.4). Only a group
// with no Block, or a child before the Block that overruns the group, is
// rejected.
func TestLayout_BlockGroupChildOrder(t *testing.T) {
	var (
		refBlock = []byte{0xFB, 0x81, 0xF0}       // ReferenceBlock -16
		duration = []byte{0x9B, 0x81, 0x20}       // BlockDuration 32
		crc      = []byte{0xBF, 0x84, 0, 0, 0, 0} // CRC-32
		overrun  = []byte{0x9B, 0x4F, 0xFF, 0x20} // BlockDuration declaring 4095 bytes, past the group
	)
	valid := map[string][][]byte{
		"ReferenceBlock before Block":                  {refBlock, nil},
		"BlockDuration before Block":                   {duration, nil},
		"CRC-32, BlockDuration, ReferenceBlock, Block": {append(append(append([]byte(nil), crc...), duration...), refBlock...), nil},
		"ReferenceBlock after Block":                   {nil, refBlock},
	}
	for name, kids := range valid {
		t.Run(name+", first after the hole", func(t *testing.T) {
			f := mkvbuild.New(101)
			f.StartCluster(0, false)
			e := f.SimpleBlock(1, 3000)
			cut := f.SimpleBlock(1, 3000)
			g := f.BlockGroupWith(2, 3000, kids[0], kids[1])
			f.SimpleBlock(1, 3000)
			f.EndCluster()
			res := layoutOf(t, f.Bytes(), e.Start+100, cut.Start+50)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, g.Start, res.End())
		})
		t.Run(name+", later in the cluster", func(t *testing.T) {
			f := mkvbuild.New(102)
			f.StartCluster(0, false)
			e := f.SimpleBlock(1, 3000)
			cut := f.SimpleBlock(1, 3000)
			first := f.SimpleBlock(1, 3000)
			f.BlockGroupWith(2, 3000, kids[0], kids[1])
			f.SimpleBlock(1, 3000)
			f.EndCluster()
			res := layoutOf(t, f.Bytes(), e.Start+100, cut.Start+50)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, first.Start, res.End())
		})
	}
	invalid := map[string]struct {
		n             int
		before, after []byte
	}{
		"no Block":                             {-1, append(append([]byte(nil), duration...), refBlock...), nil},
		"a child before the Block overruns it": {3000, overrun, nil},
	}
	for name, g := range invalid {
		t.Run(name+": rejected", func(t *testing.T) {
			f := mkvbuild.New(103)
			f.StartCluster(0, false)
			e := f.SimpleBlock(1, 3000)
			cut := f.SimpleBlock(1, 3000)
			f.BlockGroupWith(2, g.n, g.before, g.after)
			next := f.SimpleBlock(1, 3000)
			f.SimpleBlock(1, 3000)
			f.EndCluster()
			res := layoutOf(t, f.Bytes(), e.Start+100, cut.Start+50)
			require.True(t, res.OK, res.Reason)
			assert.Equal(t, next.Start, res.End(), "the invalid group is not a boundary")
		})
	}
}

// setTrackNumber rewrites the TrackNumber of track from to to in place
// (same width, one byte), leaving the CRC-32 elements as they are.
func setTrackNumber(t *testing.T, b []byte, from, to uint64) {
	t.Helper()
	hdr, ok := parseElem(b, 0)
	require.True(t, ok)
	seg, ok := parseElem(b, hdr.data+int(hdr.size))
	require.True(t, ok)
	found := false
	for p := seg.data; p < len(b); {
		el, ok := parseElem(b, p)
		require.True(t, ok)
		if el.id == idTracks {
			for q := el.data; q < el.data+int(el.size); {
				entry, _ := parseElem(b, q)
				if entry.id == idTrackEntry {
					for j := entry.data; j < entry.data+int(entry.size); {
						c, _ := parseElem(b, j)
						if c.id == idTrackNumber && beUint(b[c.data:c.data+int(c.size)]) == from {
							b[c.data+int(c.size)-1] = byte(to)
							found = true
						}
						j = c.data + int(c.size)
					}
				}
				q = entry.data + int(entry.size)
			}
			break
		}
		p = el.data + int(el.size)
	}
	require.True(t, found, "fixture: track %d in Tracks", from)
}

// Track numbers are plain numbers: the 1-byte vint 0xFF is track 127, not
// "unknown". A block on track 127 later in the cluster must not break the
// chain from the real first block after the hole.
func TestLayout_Track127IsATrack(t *testing.T) {
	f := mkvbuild.New(93)
	f.StartCluster(0, false)
	e := f.SimpleBlock(1, 3000)
	cut := f.SimpleBlock(1, 3000)
	first := f.SimpleBlock(1, 3000)
	trk := f.SimpleBlock(2, 3000)
	f.SimpleBlock(1, 3000)
	f.EndCluster()
	b := f.Bytes()
	setTrackNumber(t, b, 2, 127)
	el, _ := parseElem(b, int(trk.Start))
	b[el.data] = 0xFF
	res := layoutOf(t, b, e.Start+100, cut.Start+50)
	require.True(t, res.OK, res.Reason)
	assert.Equal(t, first.Start, res.End())
}
