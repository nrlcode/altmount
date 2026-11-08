// Package mkvvoid computes where an EBML Void element goes inside a
// zero-filled hole of a Matroska/WebM file, so a demuxer skips the damaged
// bytes and resumes at the next intact block instead of dropping the rest of
// the cluster.
//
// When playback zero-fills a missing segment, a demuxer walking a Cluster's
// children reads 0x00 as the next element ID, rejects it, and resyncs at the
// next Cluster: everything the cluster held after the hole is lost (seconds of
// audio and video for ~0.7 MB of missing bytes). A Void (ID 0xEC) tells the
// demuxer to skip its payload unread. Writing only a Void header (9 bytes)
// inside the hole's zeros, sized to end exactly at the next intact block,
// changes nothing outside the hole: the intact bytes after the hole become
// Void payload unchanged, and the cluster's declared size stays correct
// because Void is a legal cluster child.
//
// Layout only reads bytes outside the hole, through a byte budget, and answers
// "plain zeros" whenever it is not certain. The package is pure: no network,
// no logging.
package mkvvoid

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// EBML / Matroska element IDs used here.
const (
	idEBML        = 0x1A45DFA3
	idDocType     = 0x4282
	idSegment     = 0x18538067
	idCluster     = 0x1F43B675
	idTracks      = 0x1654AE6B
	idTrackEntry  = 0xAE
	idTrackNumber = 0xD7
	idTimestamp   = 0xE7
	idSimpleBlock = 0xA3
	idBlockGroup  = 0xA0
	idBlock       = 0xA1
	idVoid        = 0xEC
	idCRC32       = 0xBF
)

// clusterChildren are the IDs accepted while chaining element sizes from a
// candidate block to the cluster end.
var clusterChildren = map[uint32]struct{}{
	idTimestamp:   {},
	idSimpleBlock: {},
	idBlockGroup:  {},
	0xAB:          {}, // PrevSize
	0xA7:          {}, // Position
	idVoid:        {},
	idCRC32:       {},
	0x5854:        {}, // SilentTracks
}

var clusterIDBytes = []byte{0x1F, 0x43, 0xB6, 0x75}

const (
	// HeaderLen is the size of the Void header Layout places: the ID byte
	// plus an 8-byte size vint, fixed so the header length never depends on
	// the size it encodes.
	HeaderLen = 9

	// MaxNeighbourBytes caps the bytes read around one hole: from the
	// enclosing cluster's start up to the hole, plus from the hole's end up
	// to the cluster's end. Together that is about one cluster. Matroska
	// muxers cut clusters at about 5 s (mkvmerge's default, ffmpeg's
	// cluster_time_limit), which is ~62 MB for a 100 Mb/s UHD remux, so
	// 64 MiB covers the clusters real releases carry; a larger cluster gets
	// plain zeros.
	MaxNeighbourBytes = 64 << 20

	// maxHeadBytes caps the bytes read from the file head (EBML header,
	// top-level element headers, the Tracks body) to learn the track
	// numbers. Tracks is a few KB in practice.
	maxHeadBytes = 4 << 20

	// backChunk is the backward-scan read size.
	backChunk = 2 << 20

	// maxTopLevel bounds the top-level elements walked before the first
	// Cluster.
	maxTopLevel = 4096
)

// ErrHole is what a reader returns (possibly wrapped) for bytes that lie in a
// known missing range. Layout treats it as a definite "plain zeros" answer,
// not as a transient failure.
var ErrHole = errors.New("mkvvoid: bytes are in a known hole")

// Result is Layout's answer for one hole.
type Result struct {
	// OK reports whether a Void header should be written. When false the
	// hole is served as plain zeros and Reason says why.
	OK bool
	// Offset is the file offset of the Void header (inside the hole).
	Offset int64
	// Size is the Void's data size: the element ends at
	// Offset + HeaderLen + Size, the first intact element boundary at or
	// after the hole's end.
	Size int64
	// Reason names the fallback when OK is false.
	Reason string
}

// End is the file offset where the Void element ends.
func (r Result) End() int64 { return r.Offset + HeaderLen + r.Size }

// Eligible reports whether a file name is a Matroska/WebM container.
func Eligible(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".webm":
		return true
	}
	return false
}

// Header returns the Void header for a data size: 0xEC, then the size as an
// 8-byte vint (0x01 marker byte, 7 value bytes).
func Header(size int64) [HeaderLen]byte {
	var h [HeaderLen]byte
	h[0] = idVoid
	h[1] = 0x01
	for i := 0; i < 7; i++ {
		h[8-i] = byte(size >> (8 * i))
	}
	return h
}

// fallback is a definite "plain zeros" answer.
type fallback string

func (f fallback) Error() string { return string(f) }

// Layout decides where the Void header for the hole [hs, he) goes in a file
// of the given size, reading only bytes outside the hole through r. It is
// the plan's algorithm, ported from the spike's void.py:
//
//  1. Find the enclosing Cluster by scanning backward from hs for the
//     Cluster ID, validated by its size vint and a first child of Cluster
//     Timestamp (an optional CRC-32 child may come first). A cluster of
//     unknown size gets plain zeros: where it ends is itself a guess.
//  2. Walk the cluster's children from its data start to the element E that
//     contains hs.
//  3. If E ends at or after he, no Void is needed: plain zeros.
//  4. Otherwise the Void starts at end(E) and runs to the first
//     SimpleBlock/BlockGroup at or after he that passes the block checks
//     (known track, reserved flag bits clear, lace sizes that fit) and whose
//     element-size chain lands exactly on the cluster end; with no such
//     block, to the cluster end. If another such block starts inside the
//     chosen one, the boundary is ambiguous: plain zeros.
//  5. The 9-byte header must fit inside the hole.
//
// The error is non-nil only when a read failed for a reason other than the
// file's content (network, timeout): the answer is then unknown and must not
// be persisted. Every content-based fallback returns OK=false with a nil
// error.
func Layout(r io.ReaderAt, size, hs, he int64) (Result, error) {
	l := &layout{r: r, size: size, hs: hs, he: he}
	res, err := l.run()
	if err != nil {
		var fb fallback
		if errors.As(err, &fb) || errors.Is(err, ErrHole) {
			return Result{Reason: err.Error()}, nil
		}
		return Result{}, err
	}
	return res, nil
}

type layout struct {
	r          io.ReaderAt
	size       int64
	hs, he     int64
	headRead   int64
	neighbours int64
	tracks     map[int64]struct{}
}

// read returns n bytes at off. Reads touching the hole, past the file, or
// over budget are definite fallbacks.
func (l *layout) read(off, n int64, head bool) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := l.charge(off, n, head); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	if err := l.fill(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}

// charge checks a read of n bytes at off and counts it against its budget.
func (l *layout) charge(off, n int64, head bool) error {
	if off < 0 || off+n > l.size {
		return fallback("read past end of file")
	}
	if off < l.he && l.hs < off+n {
		return fallback("needs bytes inside the hole")
	}
	if head {
		l.headRead += n
		if l.headRead > maxHeadBytes {
			return fallback("file head exceeds read budget")
		}
	} else {
		l.neighbours += n
		if l.neighbours > MaxNeighbourBytes {
			return fallback("cluster exceeds read budget")
		}
	}
	return nil
}

// fill reads len(buf) bytes at off.
func (l *layout) fill(buf []byte, off int64) error {
	got, err := l.r.ReadAt(buf, off)
	if got == len(buf) {
		return nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// readHeader reads up to 12 bytes (the longest ID + size) at off, never
// crossing into the hole or past the file.
func (l *layout) readHeader(off int64) ([]byte, error) {
	end := min(off+12, l.size)
	if off < l.hs {
		end = min(end, l.hs)
	}
	return l.read(off, end-off, true)
}

func (l *layout) run() (Result, error) {
	if l.hs < 0 || l.he <= l.hs || l.he > l.size {
		return Result{}, fallback("invalid hole")
	}
	firstCluster, err := l.readHead()
	if err != nil {
		var fb fallback
		if errors.As(err, &fb) {
			return Result{}, fallback("file head: " + fb.Error())
		}
		return Result{}, err
	}
	if l.hs < firstCluster+4 {
		return Result{}, fallback("hole touches the file head")
	}

	// 1. Enclosing cluster, by backward scan.
	back, backStart, cds, cend, err := l.findCluster(firstCluster)
	if err != nil {
		return Result{}, err
	}
	if cend < 0 {
		// Its end is wherever the next top-level element starts, which may
		// be inside the hole. ffmpeg and mkvmerge write known sizes.
		return Result{}, fallback("unknown-size cluster")
	}
	if cend <= l.hs {
		return Result{}, fallback("hole is not inside a cluster")
	}
	if l.he > cend {
		return Result{}, fallback("hole crosses the cluster end")
	}

	// 2. Walk the children to E, the element containing hs.
	vs, err := walkTo(back, backStart, cds, l.hs)
	if err != nil {
		return Result{}, err
	}
	// 3. E spans the hole: the demuxer reads one corrupt block and resumes.
	if vs >= l.he {
		return Result{}, fallback("block spans the hole")
	}
	if vs+HeaderLen > l.he {
		return Result{}, fallback("void header does not fit in the hole")
	}

	// 4. First validated boundary at or after he.
	fwd, err := l.read(l.he, cend-l.he, false)
	if err != nil {
		return Result{}, err
	}
	ve, err := l.firstBoundary(fwd, cend)
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Offset: vs, Size: ve - vs - HeaderLen}, nil
}

// readHead checks the EBML DocType, collects the track numbers from Tracks,
// and returns the offset of the first Cluster.
func (l *layout) readHead() (int64, error) {
	h, err := l.readHeader(0)
	if err != nil {
		return 0, err
	}
	e, ok := parseElem(h, 0)
	if !ok || e.id != idEBML || e.unknown || e.size > 4096 {
		return 0, fallback("not an EBML file")
	}
	body, err := l.read(int64(e.data), e.size, true)
	if err != nil {
		return 0, err
	}
	if !matroskaDocType(body) {
		return 0, fallback("not a matroska or webm doctype")
	}
	segAt := int64(e.data) + e.size
	h, err = l.readHeader(segAt)
	if err != nil {
		return 0, err
	}
	s, ok := parseElem(h, 0)
	if !ok || s.id != idSegment {
		return 0, fallback("no segment")
	}
	segData := segAt + int64(s.data)
	segEnd := l.size
	if !s.unknown {
		segEnd = min(segEnd, segData+s.size)
	}

	p := segData
	for range maxTopLevel {
		if p >= segEnd {
			break
		}
		h, err := l.readHeader(p)
		if err != nil {
			return 0, err
		}
		t, ok := parseElem(h, 0)
		if !ok {
			return 0, fallback("bad top-level element")
		}
		if t.id == idCluster {
			if len(l.tracks) == 0 {
				return 0, fallback("no tracks before the first cluster")
			}
			return p, nil
		}
		if t.unknown {
			return 0, fallback("unknown-size top-level element")
		}
		if t.id == idTracks {
			body, err := l.read(p+int64(t.data), t.size, true)
			if err != nil {
				return 0, err
			}
			l.tracks = trackNumbers(body)
		}
		p += int64(t.data) + t.size
	}
	return 0, fallback("no cluster found")
}

// matroskaDocType reports whether an EBML header body names matroska/webm.
func matroskaDocType(body []byte) bool {
	for q := 0; q < len(body); {
		e, ok := parseElem(body, q)
		if !ok || e.unknown || int64(e.data)+e.size > int64(len(body)) {
			return false
		}
		if e.id == idDocType {
			v := string(bytes.TrimRight(body[e.data:e.data+int(e.size)], "\x00"))
			return v == "matroska" || v == "webm"
		}
		q = e.data + int(e.size)
	}
	return false
}

// trackNumbers collects TrackEntry/TrackNumber values from a Tracks body.
func trackNumbers(body []byte) map[int64]struct{} {
	out := map[int64]struct{}{}
	for q := 0; q < len(body); {
		e, ok := parseElem(body, q)
		if !ok || e.unknown || int64(e.data)+e.size > int64(len(body)) {
			break
		}
		if e.id == idTrackEntry {
			entry := body[e.data : e.data+int(e.size)]
			for r := 0; r < len(entry); {
				c, ok := parseElem(entry, r)
				if !ok || c.unknown || int64(c.data)+c.size > int64(len(entry)) {
					break
				}
				if c.id == idTrackNumber && c.size >= 1 && c.size <= 8 {
					out[int64(beUint(entry[c.data:c.data+int(c.size)]))] = struct{}{}
				}
				r = c.data + int(c.size)
			}
		}
		q = e.data + int(e.size)
	}
	return out
}

// findCluster scans backward from hs for the nearest valid Cluster. It
// returns the bytes [backStart, hs) it read, the cluster's data start, and
// its end (-1 when the cluster has unknown size).
//
// The bytes read so far stay contiguous up to hs, so a candidate is always
// validated against everything after it (a header split across two reads is
// complete). They live at the end of one buffer that doubles when it runs
// out of room, so the bytes copied stay linear in the bytes read.
func (l *layout) findCluster(firstCluster int64) ([]byte, int64, int64, int64, error) {
	lo := max(firstCluster, l.hs-MaxNeighbourBytes)
	var buf []byte // buf[off:] holds [backStart, hs)
	off := 0
	backStart := l.hs
	for backStart > lo {
		start := max(lo, backStart-backChunk)
		n := int(backStart - start)
		if err := l.charge(start, int64(n), false); err != nil {
			return nil, 0, 0, 0, err
		}
		if off < n {
			have := len(buf) - off
			grown := make([]byte, min(max(2*len(buf), have+n), int(l.hs-lo)))
			copy(grown[len(grown)-have:], buf[off:])
			buf, off = grown, len(grown)-have
		}
		if err := l.fill(buf[off-n:off], start); err != nil {
			return nil, 0, 0, 0, err
		}
		off -= n
		back := buf[off:]
		prev := backStart
		backStart = start
		// Candidates whose ID starts in the new chunk (an ID straddling the
		// old boundary starts in the new chunk too), nearest first.
		limit := int(prev-backStart) + 3
		limit = min(limit, len(back))
		for i := limit - 4; i >= 0; i-- {
			if i+4 > len(back) || !bytes.Equal(back[i:i+4], clusterIDBytes) {
				continue
			}
			if cds, cend, ok := validCluster(back, i, l.size-backStart); ok {
				return back, backStart, backStart + int64(cds), shiftEnd(cend, backStart), nil
			}
		}
	}
	return nil, 0, 0, 0, fallback("no cluster before the hole within budget")
}

func shiftEnd(end, base int64) int64 {
	if end < 0 {
		return -1
	}
	return base + end
}

// validCluster validates a Cluster at b[p:]: a known or unknown size, then an
// optional CRC-32 child, then a Cluster Timestamp of at most 8 bytes, all
// inside b. limit is the largest end (relative to b) the cluster may claim.
// It returns the data start and the end (-1 for unknown size), relative to b.
func validCluster(b []byte, p int, limit int64) (int, int64, bool) {
	e, ok := parseElem(b, p)
	if !ok || e.id != idCluster {
		return 0, 0, false
	}
	end := int64(-1)
	if !e.unknown {
		end = int64(e.data) + e.size
		if end > limit {
			return 0, 0, false
		}
	}
	q := e.data
	c, ok := parseElem(b, q)
	if ok && c.id == idCRC32 {
		if c.unknown || c.size != 4 {
			return 0, 0, false
		}
		q = c.data + 4
		c, ok = parseElem(b, q)
	}
	if !ok || c.id != idTimestamp || c.unknown || c.size > 8 || c.data+int(c.size) > len(b) {
		return 0, 0, false
	}
	if end >= 0 && int64(c.data)+c.size > end {
		return 0, 0, false
	}
	return e.data, end, true
}

// walkTo walks cluster children from cds (absolute) to the element that
// contains hs, using back (bytes [backStart, hs)). It returns vs: end(E), or
// hs itself when an element boundary falls exactly on hs.
func walkTo(back []byte, backStart, cds, hs int64) (int64, error) {
	q := cds
	for {
		if q == hs {
			return hs, nil
		}
		if q > hs {
			return 0, fallback("walk overshot the hole")
		}
		e, ok := parseElem(back, int(q-backStart))
		if !ok {
			// The header is malformed, or its bytes run into the hole.
			return 0, fallback("element header straddles the hole start or is malformed")
		}
		if e.unknown {
			return 0, fallback("unknown-size cluster child")
		}
		end := backStart + int64(e.data) + e.size
		if end > hs {
			return end, nil
		}
		q = end
	}
}

// firstBoundary returns ve: the first SimpleBlock/BlockGroup at or after he
// that passes the block checks and whose element chain lands exactly on cend,
// else cend. fwd holds [he, cend).
//
// A forged block header inside some block's payload can pass every check and
// chain to the end too. When a second passing candidate starts inside the
// first one's span, the two disagree about which bytes are blocks: the answer
// is plain zeros.
func (l *layout) firstBoundary(fwd []byte, cend int64) (int64, error) {
	c := chainer{b: fwd, end: len(fwd), tracks: l.tracks, memo: map[int]bool{}}
	first, firstEnd := -1, len(fwd)
	for i := 0; i < firstEnd; i++ {
		if fwd[i] != idSimpleBlock && fwd[i] != idBlockGroup {
			continue
		}
		if !c.blockOK(i) || !c.chains(i) {
			continue
		}
		if first >= 0 {
			return 0, fallback("ambiguous block boundary after the hole")
		}
		e, _ := parseElem(fwd, i)
		first, firstEnd = i, e.data+int(e.size)
	}
	if first < 0 {
		return cend, nil
	}
	return l.he + int64(first), nil
}

type chainer struct {
	b      []byte
	end    int
	tracks map[int64]struct{}
	memo   map[int]bool
}

// Reserved flag bits that must be clear: SimpleBlock keeps bits 1-3 reserved,
// Block bits 0-3 (Matroska spec, Block structure).
const (
	simpleBlockReserved = 0x70
	blockReserved       = 0xF0
)

// blockOK validates a SimpleBlock/BlockGroup at p: known size inside the
// cluster, then the block itself (see blockBodyOK).
func (c *chainer) blockOK(p int) bool {
	e, ok := parseElem(c.b, p)
	if !ok || e.unknown || int64(e.data)+e.size > int64(c.end) {
		return false
	}
	switch e.id {
	case idSimpleBlock:
		return c.blockBodyOK(c.b[e.data:e.data+int(e.size)], simpleBlockReserved)
	case idBlockGroup:
		// The group's children come in any order (a CRC-32 first, then
		// e.g. BlockDuration or ReferenceBlock before the Block; RFC 9559
		// section 10.4), so walk them generically: each must have a valid
		// ID and a known size inside the group, until the Block.
		gend := int64(e.data) + e.size
		for q := e.data; int64(q) < gend; {
			ch, ok := parseElem(c.b, q)
			if !ok || ch.unknown || int64(ch.data)+ch.size > gend {
				return false
			}
			if ch.id == idBlock {
				return c.blockBodyOK(c.b[ch.data:ch.data+int(ch.size)], blockReserved)
			}
			q = ch.data + int(ch.size)
		}
		return false
	}
	return false
}

// blockBodyOK validates a block's body: a track number from Tracks, the
// 2-byte timestamp, flags with the reserved bits clear, and frame data the
// lacing mode can split (lace sizes that fit, with a last frame left).
func (c *chainer) blockBodyOK(b []byte, reserved byte) bool {
	tn, tl, ok := numVint(b) // a number: 0xFF is track 127
	if !ok {
		return false
	}
	if _, known := c.tracks[tn]; !known {
		return false
	}
	h := tl + 3
	if h > len(b) {
		return false
	}
	flags := b[h-1]
	if flags&reserved != 0 {
		return false
	}
	return lacesOK(b[h:], (flags>>1)&3)
}

// lacesOK checks a block's frame data for its lacing mode (0 none, 1 Xiph,
// 2 fixed-size, 3 EBML): a laced block must declare at least two frames
// (Matroska forbids lacing a single frame), and the declared lace sizes must
// leave at least one byte for the last frame, as libavformat requires.
func lacesOK(d []byte, lacing byte) bool {
	if lacing == 0 {
		return len(d) > 0
	}
	if len(d) < 2 {
		return false
	}
	frames := int(d[0]) + 1
	if frames < 2 {
		return false
	}
	d = d[1:]
	var total int64
	q := 0
	switch lacing {
	case 1: // Xiph: each size is a run of 255s plus a final byte
		for range frames - 1 {
			for {
				if q >= len(d) {
					return false
				}
				v := d[q]
				q++
				total += int64(v)
				if v != 255 {
					break
				}
			}
		}
	case 2: // fixed-size: the data splits evenly
		return len(d)%frames == 0
	case 3: // EBML: the first size, then signed differences
		size, w, ok := numVint(d)
		if !ok {
			return false
		}
		q, total = w, size
		for range frames - 2 {
			if total >= int64(len(d)) {
				return false
			}
			v, w, ok := numVint(d[q:])
			if !ok {
				return false
			}
			q += w
			size += v - (int64(1)<<(7*w-1) - 1)
			if size < 0 {
				return false
			}
			total += size
		}
	}
	return total < int64(len(d)-q)
}

// chains follows element sizes from p and reports whether they land exactly
// on the cluster end. Results are memoized per position, so scanning every
// candidate byte stays linear.
func (c *chainer) chains(p int) bool {
	var path []int
	result := false
	for {
		if p == c.end {
			result = true
			break
		}
		if r, seen := c.memo[p]; seen {
			result = r
			break
		}
		if p > c.end {
			break
		}
		e, ok := parseElem(c.b, p)
		if !ok || e.unknown {
			break
		}
		if _, child := clusterChildren[e.id]; !child {
			break
		}
		if (e.id == idSimpleBlock || e.id == idBlockGroup) && !c.blockOK(p) {
			break
		}
		path = append(path, p)
		next := int64(e.data) + e.size
		if next > int64(c.end) {
			break
		}
		p = int(next)
	}
	for _, q := range path {
		c.memo[q] = result
	}
	return result
}

// elem is a parsed element header; data is the data start relative to the
// parsed slice.
type elem struct {
	id      uint32
	data    int
	size    int64
	unknown bool
}

// parseElem parses the element header at b[p:]. ok is false when the ID or
// size vint is malformed or runs past b.
func parseElem(b []byte, p int) (elem, bool) {
	if p < 0 || p >= len(b) {
		return elem{}, false
	}
	id, il, ok := readID(b[p:])
	if !ok {
		return elem{}, false
	}
	sz, sl, unknown, ok := readVint(b[p+il:])
	if !ok {
		return elem{}, false
	}
	return elem{id: id, data: p + il + sl, size: sz, unknown: unknown}, true
}

// readID reads an EBML element ID (1-4 bytes, marker bits kept).
func readID(b []byte) (uint32, int, bool) {
	if len(b) == 0 || b[0] == 0 {
		return 0, 0, false
	}
	n := 1
	for b[0]&(0x80>>(n-1)) == 0 {
		n++
	}
	if n > 4 || n > len(b) {
		return 0, 0, false
	}
	var id uint32
	for i := 0; i < n; i++ {
		id = id<<8 | uint32(b[i])
	}
	return id, n, true
}

// readVint reads an EBML size vint (1-8 bytes, marker removed). unknown
// reports the all-ones reserved value.
func readVint(b []byte) (int64, int, bool, bool) {
	v, n, ok := numVint(b)
	if !ok {
		return 0, 0, false, false
	}
	if v == int64(1)<<(7*n)-1 {
		return 0, n, true, true
	}
	return v, n, false, true
}

// numVint reads a vint as a plain number (1-8 bytes, marker removed). Only
// element sizes reserve all ones for "unknown" (readVint); a vint that is a
// number has no such value: a block's track number (0xFF is track 127, RFC
// 9559 section 10.2), EBML lace sizes (0xFF is 127) and lace differences
// (0xFF is +64, section 10.3.3).
func numVint(b []byte) (int64, int, bool) {
	if len(b) == 0 || b[0] == 0 {
		return 0, 0, false
	}
	n := 1
	for b[0]&(0x80>>(n-1)) == 0 {
		n++
	}
	if n > len(b) {
		return 0, 0, false
	}
	v := uint64(b[0]) & (0xFF >> n)
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return int64(v), n, true
}

// EncodeVint encodes v as an EBML size vint of the given width (1-8). It
// panics on a value that does not fit; for tests and fixture builders.
func EncodeVint(v int64, width int) []byte {
	if width < 1 || width > 8 || v < 0 || uint64(v) >= (uint64(1)<<(7*width))-1 {
		panic(fmt.Sprintf("mkvvoid: %d does not fit a %d-byte vint", v, width))
	}
	out := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		out[i] = byte(v)
		v >>= 8
	}
	out[0] |= 0x80 >> (width - 1)
	return out
}

func beUint(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v
}
