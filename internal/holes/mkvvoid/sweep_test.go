package mkvvoid

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSweepRealFiles checks the parser against real Matroska files. It runs
// only when ALTMOUNT_MKV_SWEEP_DIR names a directory of .mkv/.mka/.webm
// files (which may be truncated: an incomplete last cluster is skipped):
//
//  1. every SimpleBlock and BlockGroup in every complete cluster must pass
//     the block checks, and every cluster must chain from its first child to
//     its end; each rejection is logged with its offset and bytes;
//  2. Layout runs at sweepHoles hole positions of sweepHoleLen bytes spread
//     through the complete clusters, and the outcomes are tabulated;
//  3. with ffprobe installed, up to sweepProbes of the Void cases per file
//     are demuxed: the Void may lose at most the frames of the blocks that
//     overlap the hole, and no more than zero-fill.
//
// Damaged copies go to t.TempDir (TMPDIR) one at a time and are removed
// after each probe.
func TestSweepRealFiles(t *testing.T) {
	dir := os.Getenv("ALTMOUNT_MKV_SWEEP_DIR")
	if dir == "" {
		t.Skip("set ALTMOUNT_MKV_SWEEP_DIR to a directory of Matroska files")
	}
	var files []string
	for _, pat := range []string{"*.mkv", "*.mka", "*.webm"} {
		m, err := filepath.Glob(filepath.Join(dir, pat))
		require.NoError(t, err)
		files = append(files, m...)
	}
	sort.Strings(files)
	require.NotEmpty(t, files, "no Matroska files in %s", dir)
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) { sweepFile(t, f) })
	}
}

const (
	sweepHoles      = 50
	sweepHoleLen    = 716800
	sweepProbes     = 5
	sweepMaxReports = 40
)

type sweepCluster struct {
	start, data, end int64
	blocks           []block
}

func sweepFile(t *testing.T, path string) {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	tracks, clusters, notes := sweepWalk(b)
	for _, n := range notes {
		t.Logf("walk: %s", n)
	}
	require.NotEmpty(t, tracks, "no tracks")
	require.NotEmpty(t, clusters, "no complete cluster")

	// 1. Block checks and chains.
	var simple, groups, rejected, broken int
	orders := map[string]int{} // BlockGroup child IDs, in order
	report := func(format string, args ...any) {
		if rejected+broken <= sweepMaxReports {
			t.Errorf(format, args...)
		}
	}
	for _, cl := range clusters {
		c := chainer{b: b, end: int(cl.end), tracks: tracks, memo: map[int]bool{}}
		for _, x := range cl.blocks {
			e, _ := parseElem(b, int(x.start))
			if e.id == idSimpleBlock {
				simple++
			} else {
				groups++
				orders[groupChildren(b, e)]++
			}
			if !c.blockOK(int(x.start)) {
				rejected++
				report("rejected %s at %d: %s; bytes %s", elemName(e.id), x.start, sweepWhy(b, e, tracks), sweepHex(b, x.start, x.end))
			}
		}
		if !c.chains(int(cl.data)) {
			broken++
			p, why := sweepChainBreak(&c, int(cl.data))
			report("cluster at %d does not chain to its end %d: %s at %d; bytes %s", cl.start, cl.end, why, p, sweepHex(b, int64(p), cl.end))
		}
	}
	t.Logf("blocks: %d clusters, %d SimpleBlocks, %d BlockGroups, %d rejected; %d clusters do not chain; tracks %v",
		len(clusters), simple, groups, rejected, broken, sortedTracks(tracks))
	for o, n := range orders {
		t.Logf("blocks: %5d BlockGroups with children %s", n, o)
	}

	// 2. Layout outcomes.
	lo, hi := clusters[0].start, clusters[len(clusters)-1].end
	outcomes := map[string]int{}
	var voids []sweepHole
	for i := range sweepHoles {
		hs := lo + (hi-lo-sweepHoleLen)*int64(i+1)/int64(sweepHoles+1)
		he := hs + sweepHoleLen
		res, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
		switch {
		case err != nil:
			outcomes["zeros: "+err.Error()]++
		case !res.OK:
			outcomes["zeros: "+res.Reason]++
		default:
			outcomes["header"]++
			voids = append(voids, sweepHole{hs, he, res})
		}
	}
	keys := make([]string, 0, len(outcomes))
	for k := range outcomes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return outcomes[keys[i]] > outcomes[keys[j]] || outcomes[keys[i]] == outcomes[keys[j]] && keys[i] < keys[j]
	})
	for _, k := range keys {
		t.Logf("layout: %3d  %s", outcomes[k], k)
	}

	// 3. Demux a few Void cases.
	if len(voids) == 0 || !haveFFprobe() {
		t.Logf("probe: skipped (%d Void cases, ffprobe %v)", len(voids), haveFFprobe())
		return
	}
	orig := packets(t, path)
	tmp := t.TempDir()
	n := min(sweepProbes, len(voids))
	for k := range n {
		h := voids[k*len(voids)/n]
		frames := overlapFrames(b, clusters, h.hs, h.he)
		zeroLost := sweepLoss(t, tmp, b, h.hs, h.he, Result{}, orig)
		voidLost := sweepLoss(t, tmp, b, h.hs, h.he, h.res, orig)
		t.Logf("probe: hole [%d,%d) Void [%d,%d): packets lost zero-fill %d, Void %d; frames in blocks overlapping the hole %d",
			h.hs, h.he, h.res.Offset, h.res.End(), zeroLost, voidLost, frames)
		if voidLost > frames || voidLost > zeroLost {
			t.Errorf("probe: hole at %d: the Void lost %d packets (overlapping frames %d, zero-fill %d)", h.hs, voidLost, frames, zeroLost)
		}
	}
}

type sweepHole struct {
	hs, he int64
	res    Result
}

// sweepWalk walks the file's top level: the track numbers, and each complete
// cluster with its blocks. It stops at the first element that does not fit
// in the file (the truncation point) or does not parse.
func sweepWalk(b []byte) (map[int64]struct{}, []sweepCluster, []string) {
	var notes []string
	e, ok := parseElem(b, 0)
	if !ok || e.id != idEBML {
		return nil, nil, []string{"not EBML"}
	}
	s, ok := parseElem(b, e.data+int(e.size))
	if !ok || s.id != idSegment {
		return nil, nil, []string{"no Segment"}
	}
	segEnd := int64(len(b))
	if !s.unknown {
		segEnd = min(segEnd, int64(s.data)+s.size)
	}
	tracks := map[int64]struct{}{}
	var clusters []sweepCluster
	for p := int64(s.data); p < segEnd; {
		top, ok := parseElem(b, int(p))
		if !ok {
			notes = append(notes, fmt.Sprintf("top-level element at %d does not parse; stop", p))
			break
		}
		if top.unknown {
			notes = append(notes, fmt.Sprintf("unknown-size %s at %d; stop", elemName(top.id), p))
			break
		}
		end := int64(top.data) + top.size
		if end > int64(len(b)) {
			notes = append(notes, fmt.Sprintf("%s at %d ends at %d, past the %d bytes read: truncated, skipped", elemName(top.id), p, end, len(b)))
			break
		}
		switch top.id {
		case idTracks:
			tracks = trackNumbers(b[top.data:end])
		case idCluster:
			cl := sweepCluster{start: p, data: int64(top.data), end: end}
			for q := int64(top.data); q < end; {
				c, ok := parseElem(b, int(q))
				if !ok || c.unknown || int64(c.data)+c.size > end {
					notes = append(notes, fmt.Sprintf("cluster at %d: child at %d does not parse or overruns", p, q))
					break
				}
				ce := int64(c.data) + c.size
				if c.id == idSimpleBlock || c.id == idBlockGroup {
					cl.blocks = append(cl.blocks, block{q, ce})
				}
				q = ce
			}
			clusters = append(clusters, cl)
		}
		p = end
	}
	return tracks, clusters, notes
}

// sweepWhy names the first block check a rejected element fails.
func sweepWhy(b []byte, e elem, tracks map[int64]struct{}) string {
	body, reserved := b[e.data:e.data+int(e.size)], byte(simpleBlockReserved)
	if e.id == idBlockGroup {
		var kids []string
		var blk []byte
		gend := e.data + int(e.size)
		for q := e.data; q < gend; {
			c, ok := parseElem(b, q)
			if !ok || c.unknown || c.data+int(c.size) > gend {
				kids = append(kids, fmt.Sprintf("bad child at +%d", q-e.data))
				break
			}
			kids = append(kids, fmt.Sprintf("%X", c.id))
			if c.id == idBlock && blk == nil {
				blk = b[c.data : c.data+int(c.size)]
			}
			q = c.data + int(c.size)
		}
		if blk == nil {
			return "group children " + strings.Join(kids, ",") + ": no Block"
		}
		body, reserved = blk, blockReserved
	}
	tn, tl, ok := numVint(body)
	if !ok {
		return "bad track vint"
	}
	if _, known := tracks[tn]; !known {
		return fmt.Sprintf("track %d not in Tracks", tn)
	}
	if tl+3 > len(body) {
		return "body shorter than the block header"
	}
	flags := body[tl+2]
	if flags&reserved != 0 {
		return fmt.Sprintf("reserved flag bits set (flags %02x)", flags)
	}
	lacing := (flags >> 1) & 3
	if !lacesOK(body[tl+3:], lacing) {
		return fmt.Sprintf("lacing %d does not parse (%d data bytes)", lacing, len(body)-tl-3)
	}
	return "unclassified"
}

// sweepChainBreak follows a cluster's children from p and names the first
// element the chain stops at.
func sweepChainBreak(c *chainer, p int) (int, string) {
	for p < c.end {
		e, ok := parseElem(c.b, p)
		if !ok || e.unknown {
			return p, "element does not parse or has unknown size"
		}
		if _, child := clusterChildren[e.id]; !child {
			return p, fmt.Sprintf("child ID %X not in the chain set", e.id)
		}
		if (e.id == idSimpleBlock || e.id == idBlockGroup) && !c.blockOK(p) {
			return p, elemName(e.id) + " rejected"
		}
		next := int64(e.data) + e.size
		if next > int64(c.end) {
			return p, "child overruns the cluster"
		}
		p = int(next)
	}
	return p, "chain passes the cluster end"
}

// groupChildren lists a BlockGroup's child IDs in order.
func groupChildren(b []byte, e elem) string {
	var ids []string
	for q := e.data; q < e.data+int(e.size); {
		c, ok := parseElem(b, q)
		if !ok || c.unknown {
			ids = append(ids, "?")
			break
		}
		name := fmt.Sprintf("%X", c.id)
		switch c.id {
		case idBlock:
			name = "Block"
		case idCRC32:
			name = "CRC-32"
		case 0x9B:
			name = "BlockDuration"
		case 0xFB:
			name = "ReferenceBlock"
		case 0x75A1:
			name = "BlockAdditions"
		case 0x75A2:
			name = "DiscardPadding"
		}
		ids = append(ids, name)
		q = c.data + int(c.size)
	}
	return strings.Join(ids, ",")
}

// overlapFrames counts the frames of the blocks that overlap [hs, he).
func overlapFrames(b []byte, clusters []sweepCluster, hs, he int64) int {
	n := 0
	for _, cl := range clusters {
		if cl.end <= hs || cl.start >= he {
			continue
		}
		for _, x := range cl.blocks {
			if x.end <= hs || x.start >= he {
				continue
			}
			n += blockFrames(b, x)
		}
	}
	return n
}

// blockFrames is the number of frames a SimpleBlock/BlockGroup holds.
func blockFrames(b []byte, x block) int {
	e, _ := parseElem(b, int(x.start))
	body := b[e.data : e.data+int(e.size)]
	if e.id == idBlockGroup {
		body = nil
		for q := e.data; q < e.data+int(e.size); {
			c, ok := parseElem(b, q)
			if !ok {
				break
			}
			if c.id == idBlock {
				body = b[c.data : c.data+int(c.size)]
				break
			}
			q = c.data + int(c.size)
		}
	}
	_, tl, ok := numVint(body)
	if !ok || tl+4 > len(body) || (body[tl+2]>>1)&3 == 0 {
		return 1
	}
	return int(body[tl+3]) + 1
}

func sweepLoss(t *testing.T, dir string, b []byte, hs, he int64, res Result, orig map[string]int) int {
	t.Helper()
	p := filepath.Join(dir, "damaged.mkv")
	require.NoError(t, os.WriteFile(p, damage(b, hs, he, res), 0o600))
	defer os.Remove(p)
	return lost(orig, packets(t, p))
}

func haveFFprobe() bool {
	_, err := exec.LookPath("ffprobe")
	return err == nil
}

func sweepHex(b []byte, from, to int64) string {
	to = min(to, from+32, int64(len(b)))
	return hex.EncodeToString(b[from:to])
}

func sortedTracks(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func elemName(id uint32) string {
	switch id {
	case idSimpleBlock:
		return "SimpleBlock"
	case idBlockGroup:
		return "BlockGroup"
	case idCluster:
		return "Cluster"
	case idTracks:
		return "Tracks"
	}
	return fmt.Sprintf("element %X", id)
}
