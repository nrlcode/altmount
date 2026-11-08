// Package mkvbuild writes small synthetic Matroska files for tests: an EBML
// header, a Segment with Info and Tracks, and clusters of SimpleBlocks,
// BlockGroups and other children whose exact byte positions the test controls
// and can look up afterwards.
package mkvbuild

import (
	"encoding/binary"
	"math/rand/v2"
)

// Element is one cluster child the builder wrote, by absolute offsets.
type Element struct {
	ID         uint32
	Start, End int64 // [Start, End)
	Track      int   // for blocks
}

// File is a Matroska file under construction.
type File struct {
	b        []byte
	segSize  int // offset of the Segment's 8-byte size field
	segData  int
	cluster  int // offset of the open cluster's 8-byte size field, -1 if none
	rng      *rand.Rand
	Elements []Element
	Clusters []Element
}

// New starts a file with tracks 1 and 2. crc writes a CRC-32 (BF) child as
// the first child of each cluster, like ffmpeg's muxer does by default.
func New(seed uint64) *File {
	f := &File{cluster: -1, rng: rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))}
	// EBML header.
	f.elem([]byte{0x1A, 0x45, 0xDF, 0xA3}, concat(
		uintElem([]byte{0x42, 0x86}, 1),         // EBMLVersion
		strElem([]byte{0x42, 0x82}, "matroska"), // DocType
		uintElem([]byte{0x42, 0x87}, 4),         // DocTypeVersion
	))
	// Segment, size patched in Bytes().
	f.b = append(f.b, 0x18, 0x53, 0x80, 0x67)
	f.segSize = len(f.b)
	f.b = append(f.b, make([]byte, 8)...)
	f.segData = len(f.b)
	f.elem([]byte{0x15, 0x49, 0xA9, 0x66}, uintElem([]byte{0x2A, 0xD7, 0xB1}, 1000000)) // Info/TimestampScale
	// Tracks.
	f.elem([]byte{0x16, 0x54, 0xAE, 0x6B}, concat(
		f.trackEntry(1, 1),
		f.trackEntry(2, 2),
	))
	return f
}

func (f *File) trackEntry(num, typ uint64) []byte {
	return elemBytes([]byte{0xAE}, concat(
		uintElem([]byte{0xD7}, num),
		uintElem([]byte{0x73, 0xC5}, num), // TrackUID
		uintElem([]byte{0x83}, typ),
	))
}

// Len is the current file length.
func (f *File) Len() int64 { return int64(len(f.b)) }

// StartCluster opens a known-size cluster (size patched by EndCluster).
func (f *File) StartCluster(ts uint64, crc bool) {
	start := len(f.b)
	f.b = append(f.b, 0x1F, 0x43, 0xB6, 0x75)
	f.cluster = len(f.b)
	f.b = append(f.b, make([]byte, 8)...)
	f.Clusters = append(f.Clusters, Element{ID: 0x1F43B675, Start: int64(start)})
	if crc {
		f.b = append(f.b, 0xBF, 0x84, 0, 0, 0, 0)
	}
	f.b = append(f.b, uintElem([]byte{0xE7}, ts)...)
}

// StartClusterUnknownSize opens a cluster whose size is the reserved
// "unknown" value; it ends where the next cluster starts.
func (f *File) StartClusterUnknownSize(ts uint64) {
	f.StartCluster(ts, false)
	copy(f.b[f.cluster:], []byte{0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.cluster = -2
}

// EndCluster patches the open cluster's size (unknown-size clusters keep
// theirs).
func (f *File) EndCluster() {
	if f.cluster == -2 {
		f.Clusters[len(f.Clusters)-1].End = int64(len(f.b))
		f.cluster = -1
		return
	}
	size := uint64(len(f.b) - f.cluster - 8)
	putVint8(f.b[f.cluster:], size)
	f.Clusters[len(f.Clusters)-1].End = int64(len(f.b))
	f.cluster = -1
}

// SimpleBlock writes a SimpleBlock with a random payload of n bytes.
func (f *File) SimpleBlock(track int, n int) Element {
	return f.SimpleBlockPayload(track, f.random(n))
}

// SimpleBlockPayload writes a SimpleBlock carrying payload.
func (f *File) SimpleBlockPayload(track int, payload []byte) Element {
	return f.SimpleBlockFlags(track, 0x80, payload)
}

// SimpleBlockFlags writes a SimpleBlock with the given flags byte; payload
// is everything after it (lace data included).
func (f *File) SimpleBlockFlags(track int, flags byte, payload []byte) Element {
	body := concat([]byte{0x80 | byte(track), 0, 0, flags}, payload)
	return f.child(0xA3, []byte{0xA3}, body, track)
}

// SimpleBlockTotal writes a SimpleBlock whose whole element (header
// included) is exactly total bytes long. total must be at least 14.
func (f *File) SimpleBlockTotal(track int, total int) Element {
	// ID 1 + 8-byte size + 4-byte block header.
	return f.SimpleBlock(track, total-1-8-4)
}

// BlockGroup writes a BlockGroup holding one Block with an n-byte payload.
func (f *File) BlockGroup(track int, n int) Element {
	blk := elemBytes([]byte{0xA1}, concat([]byte{0x80 | byte(track), 0, 0, 0}, f.random(n)))
	return f.child(0xA0, []byte{0xA0}, blk, track)
}

// BlockGroupCRC writes a BlockGroup whose first child is a CRC-32 (value
// zero; demuxers do not check it by default), then a Block with an n-byte
// payload.
func (f *File) BlockGroupCRC(track int, n int) Element {
	return f.BlockGroupWith(track, n, []byte{0xBF, 0x84, 0, 0, 0, 0}, nil)
}

// BlockGroupWith writes a BlockGroup holding the raw child bytes before, a
// Block with an n-byte payload (none when n < 0), then the raw bytes after.
func (f *File) BlockGroupWith(track int, n int, before, after []byte) Element {
	var blk []byte
	if n >= 0 {
		blk = elemBytes([]byte{0xA1}, concat([]byte{0x80 | byte(track), 0, 0, 0}, f.random(n)))
	}
	return f.child(0xA0, []byte{0xA0}, concat(before, blk, after), track)
}

// Void writes a Void child with an n-byte payload.
func (f *File) Void(n int) Element { return f.child(0xEC, []byte{0xEC}, make([]byte, n), 0) }

// Unknown writes a child with an ID the walker does not know (0xAF).
func (f *File) Unknown(n int) Element { return f.child(0xAF, []byte{0xAF}, f.random(n), 0) }

// Pad writes random bytes directly (no element), to break structure.
func (f *File) Pad(n int) { f.b = append(f.b, f.random(n)...) }

// Cues writes a top-level Cues element with an n-byte body.
func (f *File) Cues(n int) Element {
	start := len(f.b)
	f.elem([]byte{0x1C, 0x53, 0xBB, 0x6B}, f.random(n))
	return Element{ID: 0x1C53BB6B, Start: int64(start), End: int64(len(f.b))}
}

func (f *File) child(id uint32, idb, body []byte, track int) Element {
	start := len(f.b)
	f.elem(idb, body)
	e := Element{ID: id, Start: int64(start), End: int64(len(f.b)), Track: track}
	f.Elements = append(f.Elements, e)
	return e
}

// Bytes returns the finished file (the Segment size patched).
func (f *File) Bytes() []byte {
	out := append([]byte(nil), f.b...)
	putVint8(out[f.segSize:], uint64(len(out)-f.segData))
	return out
}

// ElementAt returns the cluster child containing off.
func (f *File) ElementAt(off int64) (Element, bool) {
	for _, e := range f.Elements {
		if e.Start <= off && off < e.End {
			return e, true
		}
	}
	return Element{}, false
}

func (f *File) random(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(f.rng.Uint32())
	}
	return out
}

func (f *File) elem(id, body []byte) { f.b = append(f.b, elemBytes(id, body)...) }

// elemBytes encodes an element with an 8-byte size vint.
func elemBytes(id, body []byte) []byte {
	out := append([]byte(nil), id...)
	var sz [8]byte
	putVint8(sz[:], uint64(len(body)))
	out = append(out, sz[:]...)
	return append(out, body...)
}

func uintElem(id []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return elemBytes(id, b[:])
}

func strElem(id []byte, s string) []byte { return elemBytes(id, []byte(s)) }

func putVint8(b []byte, v uint64) {
	binary.BigEndian.PutUint64(b[:8], v)
	b[0] = 0x01
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
