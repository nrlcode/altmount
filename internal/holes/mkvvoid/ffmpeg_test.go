package mkvvoid

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// block is a SimpleBlock/BlockGroup of a real file, by absolute offsets.
type block struct{ start, end int64 }

// clustersOf walks a whole Matroska file: each cluster's [start, end) and
// the blocks it holds.
func clustersOf(t *testing.T, b []byte) ([][2]int64, [][]block) {
	t.Helper()
	e, ok := parseElem(b, 0)
	require.True(t, ok)
	s, ok := parseElem(b, e.data+int(e.size))
	require.True(t, ok)
	require.EqualValues(t, idSegment, s.id)
	segEnd := len(b)
	if !s.unknown {
		segEnd = s.data + int(s.size)
	}
	var clusters [][2]int64
	var blocks [][]block
	for p := s.data; p < segEnd; {
		top, ok := parseElem(b, p)
		require.True(t, ok)
		require.False(t, top.unknown)
		end := top.data + int(top.size)
		if top.id == idCluster {
			clusters = append(clusters, [2]int64{int64(p), int64(end)})
			var bl []block
			for q := top.data; q < end; {
				c, ok := parseElem(b, q)
				require.True(t, ok)
				ce := c.data + int(c.size)
				if c.id == idSimpleBlock || c.id == idBlockGroup {
					bl = append(bl, block{int64(q), int64(ce)})
				}
				q = ce
			}
			blocks = append(blocks, bl)
		}
		p = end
	}
	return clusters, blocks
}

// packets returns "stream,pts" -> count from ffprobe -show_packets.
func packets(t *testing.T, path string) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "quiet", "-show_entries", "packet=stream_index,pts_time",
		"-of", "csv=p=0", path).Output()
	require.NoError(t, err)
	m := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			m[line]++
		}
	}
	return m
}

func lost(orig, damaged map[string]int) int {
	n := 0
	for k, c := range orig {
		if d := damaged[k]; d < c {
			n += c - d
		}
	}
	return n
}

// TestFFmpegAcceptance_VoidKeepsRestOfCluster is the plan's acceptance
// experiment in small: an A/V MKV from ffmpeg (default muxer settings, so
// clusters carry a CRC-32 first child), one hole in the middle of a cluster,
// and two damaged copies. With plain zeros ffmpeg's demuxer drops the rest of
// the cluster; with the Void it loses at most the blocks that overlap the
// hole. A forged laced block header planted after the hole (in the payload
// of the block the hole cuts) must not move the boundary.
func TestFFmpegAcceptance_VoidKeepsRestOfCluster(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mkv")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	gen := exec.CommandContext(ctx, "ffmpeg", "-nostdin", "-v", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=s=320x240:r=24:d=16",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:d=16",
		"-map", "0:v", "-map", "1:a",
		"-c:v", "libx264", "-preset", "ultrafast", "-b:v", "3M", "-g", "120", "-keyint_min", "120", "-sc_threshold", "0",
		"-c:a", "ac3", "-b:a", "192k",
		"-cluster_time_limit", "5000", "-cluster_size_limit", "100000000",
		src)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg could not build the fixture: %v %s", err, out)
	}
	b, err := os.ReadFile(src)
	require.NoError(t, err)

	clusters, blocks := clustersOf(t, b)
	require.GreaterOrEqual(t, len(clusters), 3, "expected ~5 s clusters")
	ci := 1 // a full cluster in the middle of the file
	cl, bl := clusters[ci], blocks[ci]
	ce, _ := parseElem(b, int(cl[0]))
	first, _ := parseElem(b, ce.data)
	require.EqualValues(t, idCRC32, first.id, "ffmpeg's default muxer writes a CRC-32 first child")
	holeLen := int64(64 << 10)

	var hs, he int64
	var res Result
	for _, frac := range []int64{40, 45, 50, 55, 60, 35, 30} {
		hs = cl[0] + (cl[1]-cl[0])*frac/100
		he = hs + holeLen
		res, err = Layout(bytes.NewReader(b), int64(len(b)), hs, he)
		require.NoError(t, err)
		if res.OK {
			break
		}
	}
	require.True(t, res.OK, "no mid-cluster hole position got a Void layout: %s", res.Reason)
	require.Less(t, he, cl[1], "the hole must sit inside the cluster")

	overlap, after := 0, 0
	for _, x := range bl {
		if x.start < he && hs < x.end && !(x.start <= hs && hs < x.end) {
			overlap++ // overlaps the hole, not counting E (served, corrupt)
		}
		if x.start >= he {
			after++
		}
	}

	zero := filepath.Join(dir, "zero.mkv")
	void := filepath.Join(dir, "void.mkv")
	require.NoError(t, os.WriteFile(zero, damage(b, hs, he, Result{}), 0o600))
	require.NoError(t, os.WriteFile(void, damage(b, hs, he, res), 0o600))

	orig := packets(t, src)
	zeroLost := lost(orig, packets(t, zero))
	voidLost := lost(orig, packets(t, void))
	t.Logf("cluster %d [%d,%d) with %d blocks; hole [%d,%d) %d B; Void at %d size %d (%d intact bytes after the hole become payload)",
		ci, cl[0], cl[1], len(bl), hs, he, holeLen, res.Offset, res.Size, res.End()-he)
	t.Logf("packets lost: zero-fill %d, void %d; blocks overlapping the hole (not E) %d, blocks after the hole in its cluster %d",
		zeroLost, voidLost, overlap, after)

	require.LessOrEqual(t, voidLost, overlap,
		"with the Void the demuxer may lose only the blocks overlapping the hole")
	require.GreaterOrEqual(t, zeroLost, after,
		"with plain zeros the demuxer drops the rest of the cluster (the behaviour this fixes)")
	require.Greater(t, zeroLost, voidLost)

	// A SimpleBlock header 16 bytes after the hole that claims EBML lacing
	// with a first frame bigger than the block, sized to end exactly at the
	// real boundary: it chains, but libavformat rejects its lace sizes ("Error
	// parsing frame sizes") and drops the rest of the cluster.
	fakeAt := he + 16
	body := append([]byte{0x81, 0, 0, 0x86, 0x01, 0x7F, 0xFE}, make([]byte, res.End()-fakeAt-9-7)...)
	forged := append([]byte(nil), b...)
	copy(forged[fakeAt:], append(append([]byte{idSimpleBlock}, EncodeVint(int64(len(body)), 8)...), body...))
	forgedRes, err := Layout(bytes.NewReader(forged), int64(len(forged)), hs, he)
	require.NoError(t, err)
	forgedSrc := filepath.Join(dir, "forged.mkv")
	forgedVoid := filepath.Join(dir, "forged-void.mkv")
	require.NoError(t, os.WriteFile(forgedSrc, forged, 0o600))
	require.NoError(t, os.WriteFile(forgedVoid, damage(forged, hs, he, forgedRes), 0o600))
	forgedLost := lost(packets(t, forgedSrc), packets(t, forgedVoid))
	t.Logf("forged laced block at %d: Void ends at %d (real boundary %d), packets lost %d", fakeAt, forgedRes.End(), res.End(), forgedLost)
	require.Equal(t, res, forgedRes, "the forged laced block must not become the boundary")
	require.LessOrEqual(t, forgedLost, overlap)
}

// needFFmpeg skips when the ffmpeg or ffprobe executable is missing.
func needFFmpeg(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed", tool)
		}
	}
}

// ffmpegMKV runs ffmpeg with args and the output path appended, and returns
// the file's bytes. A failure fails the test.
func ffmpegMKV(t *testing.T, out string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffmpeg", append(append([]string{"-nostdin", "-v", "error", "-y"}, args...), out)...)
	if o, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg could not build the fixture: %v %s", err, o)
	}
	b, err := os.ReadFile(out)
	require.NoError(t, err)
	return b
}

// lossWith writes b damaged by res and returns the (stream, pts) packets
// ffprobe loses against b itself.
func lossWith(t *testing.T, dir, name string, b []byte, hs, he int64, res Result, orig map[string]int) int {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, damage(b, hs, he, res), 0o600))
	return lost(orig, packets(t, p))
}

// Real AAC frames from an ffmpeg file, packed into one valid EBML-laced
// block whose lace difference is the all-ones vint (+64). ffmpeg decodes it.
// The layout must still end at the first block after the hole: reading the
// lace value as "unknown" broke every chain through that block and moved
// the boundary past it (ffprobe lost 49 packets instead of 17).
func TestFFmpegLace_ValidAllOnesDifferenceInAACKeepsBoundary(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mkv")
	b := ffmpegMKV(t, src,
		"-f", "lavfi", "-i", "testsrc2=s=320x240:r=24:d=16",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:d=16",
		"-map", "0:v", "-map", "1:a",
		"-c:v", "libx264", "-preset", "ultrafast", "-b:v", "3M", "-g", "120", "-sc_threshold", "0",
		"-c:a", "aac", "-b:a", "128k",
		"-cluster_time_limit", "5000", "-cluster_size_limit", "100000000")
	clusters, blocks := clustersOf(t, b)
	require.GreaterOrEqual(t, len(clusters), 3)

	// Two AAC frame payloads whose sizes differ by exactly 64.
	bySize := map[int][]byte{}
	for _, bl := range blocks {
		for _, x := range bl {
			el, ok := parseElem(b, int(x.start))
			if !ok || el.id != idSimpleBlock {
				continue
			}
			tn, tl, _, ok := readVint(b[el.data:])
			if !ok || tn != 2 || el.size < int64(tl+4) {
				continue
			}
			bySize[int(el.size)-tl-3] = b[el.data+tl+3 : el.data+int(el.size)]
		}
	}
	var a, c []byte
	for n, p := range bySize {
		if q, ok := bySize[n+64]; ok && n >= 64 && n < 16000 && (a == nil || n < len(a)) {
			a, c = p, q
		}
	}
	require.NotNil(t, a, "fixture: two AAC frames 64 bytes apart")
	a, c = append([]byte(nil), a...), append([]byte(nil), c...)

	cl, bl := clusters[1], blocks[1]
	hs := cl[0] + (cl[1]-cl[0])*40/100
	he := hs + 64<<10
	want, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
	require.NoError(t, err)
	require.True(t, want.OK, want.Reason)

	// Replace blocks well after the boundary with the laced block (3 frames:
	// a, a+64, a), padding the rest of the replaced span with a Void.
	var target int64
	for i, x := range bl {
		if x.start < want.End()+100000 {
			continue
		}
		el, ok := parseElem(b, int(x.start))
		if !ok || el.id != idSimpleBlock {
			continue
		}
		_, tl, _, _ := readVint(b[el.data:])
		body := append([]byte{0x82, b[el.data+tl], b[el.data+tl+1], 0x86, 2}, EncodeVint(int64(len(a)), 2)...)
		body = append(body, 0xFF)
		body = append(append(append(body, a...), c...), a...)
		block := append(append([]byte{idSimpleBlock}, EncodeVint(int64(len(body)), 3)...), body...)
		j := i
		for j < len(bl) && bl[j].end-x.start < int64(len(block)+3) {
			j++
		}
		if j == len(bl) {
			continue
		}
		rem := bl[j].end - x.start - int64(len(block))
		if rem < 3 || rem > 16385 {
			continue
		}
		copy(b[x.start:], block)
		void := append([]byte{idVoid}, EncodeVint(rem-3, 2)...)
		copy(b[x.start+int64(len(block)):], void)
		clear(b[x.start+int64(len(block))+int64(len(void)) : bl[j].end])
		target = x.start
		break
	}
	require.NotZero(t, target, "fixture: room for the laced block")
	laced := filepath.Join(dir, "laced.mkv")
	require.NoError(t, os.WriteFile(laced, b, 0o600))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if o, err := exec.CommandContext(ctx, "ffmpeg", "-nostdin", "-v", "error", "-xerror", "-i", laced,
		"-map", "0:a:0", "-f", "null", "-").CombinedOutput(); err != nil {
		t.Fatalf("fixture: ffmpeg must decode the laced block: %v %s", err, o)
	}

	got, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
	require.NoError(t, err)
	orig := packets(t, laced)
	wantLost := lossWith(t, dir, "want.mkv", b, hs, he, want, orig)
	gotLost := lossWith(t, dir, "got.mkv", b, hs, he, got, orig)
	t.Logf("AAC frames %d,%d,%d laced at %d; boundary %d (want %d); packets lost %d (want %d)",
		len(a), len(c), len(a), target, got.End(), want.End(), gotLost, wantLost)
	require.Equal(t, want, got, "a valid laced block later in the cluster must not move the boundary")
}

// An ffmpeg PCM file with one block relaced into three valid frames of 100,
// 164 and 1780 bytes (the +64 difference is the all-ones vint). ffprobe
// reads all three; the layout must still resume at the first block after
// the hole, not after the laced block.
func TestFFmpegLace_ValidAllOnesDifferenceInPCMKeepsBoundary(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "pcm.mkv")
	b := ffmpegMKV(t, src, "-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono:d=2", "-c:a", "pcm_s16le",
		"-cluster_time_limit", "5000")
	_, blocks := clustersOf(t, b)
	bl := blocks[0]
	require.GreaterOrEqual(t, len(bl), 15, "fixture: one cluster of PCM blocks")
	target := bl[10]
	e, _ := parseElem(b, int(target.start))
	_, tl, _, _ := readVint(b[e.data:])
	h := e.data + tl + 3
	require.Equal(t, int64(2048), e.size-int64(tl+3), "fixture: 2048-byte PCM blocks")
	b[h-1] = 0x86
	copy(b[h:], []byte{0x02, 0x40, 100, 0xFF})
	laced := filepath.Join(dir, "laced.mkv")
	require.NoError(t, os.WriteFile(laced, b, 0o600))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "packet=size", "-of", "csv=p=0", laced).Output()
	require.NoError(t, err)
	for _, size := range []string{"100", "164", "1780"} {
		require.Contains(t, strings.Fields(string(out)), size, "fixture: ffprobe reads the %s-byte frame", size)
	}

	hs, he := bl[1].start+100, bl[2].start+100
	res, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
	require.NoError(t, err)
	require.True(t, res.OK, res.Reason)
	t.Logf("laced block at %d; boundary %d (first intact block %d)", target.start, res.End(), bl[3].start)
	require.Equal(t, bl[3].start, res.End())
}

// avMKV is the acceptance test's A/V source: H.264 + AC3, ~5 s clusters
// with ffmpeg's default CRC-32 children.
func avMKV(t *testing.T, dir string) []byte {
	t.Helper()
	return ffmpegMKV(t, filepath.Join(dir, "src.mkv"),
		"-f", "lavfi", "-i", "testsrc2=s=320x240:r=24:d=16",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:d=16",
		"-map", "0:v", "-map", "1:a",
		"-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-b:v", "3M", "-g", "120", "-sc_threshold", "0",
		"-c:a", "ac3", "-b:a", "192k",
		"-cluster_time_limit", "5000", "-cluster_size_limit", "100000000")
}

// fixMasterCRC recomputes a master element's leading CRC-32 child, if any,
// after its body was edited.
func fixMasterCRC(b []byte, master elem) {
	first, ok := parseElem(b, master.data)
	if !ok || first.id != idCRC32 || first.size != 4 {
		return
	}
	binary.LittleEndian.PutUint32(b[first.data:first.data+4], crc32.ChecksumIEEE(b[first.data+4:master.data+int(master.size)]))
}

// midHole is the acceptance test's hole: 64 KiB at 40 % of cluster 1, and
// its layout on the unedited file.
func midHole(t *testing.T, b []byte, clusters [][2]int64) (hs, he int64, want Result) {
	t.Helper()
	require.GreaterOrEqual(t, len(clusters), 3, "expected ~5 s clusters")
	cl := clusters[1]
	hs = cl[0] + (cl[1]-cl[0])*40/100
	he = hs + 64<<10
	want, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
	require.NoError(t, err)
	require.True(t, want.OK, want.Reason)
	return hs, he, want
}

// The audio track renumbered to 127, so every audio block's track vint is
// 0xFF. ffprobe reads the file; the layout must still end at the first
// block after the hole (reading 0xFF as "unknown" broke every chain through
// an audio block: 167 packets lost instead of 14).
func TestFFmpegTrack127KeepsBoundary(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	b := avMKV(t, dir)
	clusters, blocks := clustersOf(t, b)
	hs, he, want := midHole(t, b, clusters)

	setTrackNumber(t, b, 2, 127)
	changed := 0
	for _, bl := range blocks {
		for _, x := range bl {
			if el, _ := parseElem(b, int(x.start)); el.id == idSimpleBlock && b[el.data] == 0x82 {
				b[el.data] = 0xFF
				changed++
			}
		}
	}
	require.Positive(t, changed)
	for _, c := range clusters {
		cl, _ := parseElem(b, int(c[0]))
		fixMasterCRC(b, cl)
	}
	hdr, _ := parseElem(b, 0)
	seg, _ := parseElem(b, hdr.data+int(hdr.size))
	for p := seg.data; p < len(b); {
		el, ok := parseElem(b, p)
		require.True(t, ok)
		if el.id == idTracks {
			for q := el.data; q < el.data+int(el.size); {
				entry, _ := parseElem(b, q)
				fixMasterCRC(b, entry)
				q = entry.data + int(entry.size)
			}
			fixMasterCRC(b, el)
			break
		}
		p = el.data + int(el.size)
	}
	f := filepath.Join(dir, "track127.mkv")
	require.NoError(t, os.WriteFile(f, b, 0o600))
	orig := packets(t, f)
	require.NotEmpty(t, orig)

	got, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
	require.NoError(t, err)
	wantLost := lossWith(t, dir, "want.mkv", b, hs, he, want, orig)
	gotLost := lossWith(t, dir, "got.mkv", b, hs, he, got, orig)
	t.Logf("%d audio blocks on track 127; boundary %d (want %d); packets lost %d (want %d)", changed, got.End(), want.End(), gotLost, wantLost)
	require.Equal(t, want, got)
}

// groupFile replaces a SimpleBlock on track 2 well after the hole's
// boundary with a BlockGroup holding the same frame, its children laid out
// by kids from the Block element, padding the rest of the slot with a Void.
func groupFile(t *testing.T, b []byte, bl []block, after int64, kids func(blk []byte) []byte) ([]byte, int64) {
	t.Helper()
	b = bytes.Clone(b)
	for i, x := range bl {
		if x.start < after || i+1 >= len(bl) {
			continue
		}
		e, ok := parseElem(b, int(x.start))
		if !ok || e.id != idSimpleBlock || b[e.data] != 0x82 {
			continue
		}
		body := append([]byte(nil), b[e.data:e.data+int(e.size)]...)
		body[3] &= 0x0F // Block flags: keyframe/discardable bits are SimpleBlock-only
		blk := append(append([]byte{idBlock}, EncodeVint(int64(len(body)), 3)...), body...)
		gb := kids(blk)
		group := append(append([]byte{idBlockGroup}, EncodeVint(int64(len(gb)), 3)...), gb...)
		end := bl[i+1].end
		rem := end - x.start - int64(len(group))
		if rem < 4 || rem > 2097150 {
			continue
		}
		copy(b[x.start:], group)
		copy(b[x.start+int64(len(group)):], append([]byte{idVoid}, EncodeVint(rem-4, 3)...))
		clear(b[x.start+int64(len(group))+4 : end])
		return b, x.start
	}
	t.Fatal("fixture: no room for the BlockGroup")
	return nil, 0
}

// withCRC prefixes children with a CRC-32 over them, as ffmpeg writes it.
func withCRC(children []byte) []byte {
	crc := []byte{idCRC32, 0x84, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(crc[2:], crc32.ChecksumIEEE(children))
	return append(crc, children...)
}

// A BlockGroup later in the cluster, with its children in any order the
// spec allows (RFC 9559 section 10.4), must not break the chain from the
// real first block after the hole. Before the fix a BlockDuration before
// the Block moved the boundary past the group and lost 35 packets, not 14.
func TestFFmpegBlockGroupKeepsBoundary(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := avMKV(t, dir)
	clusters, blocks := clustersOf(t, src)
	hs, he, want := midHole(t, src, clusters)
	duration := []byte{0x9B, 0x81, 32} // BlockDuration: 32 ticks, one AC3 frame
	cases := []struct {
		name string
		kids func(blk []byte) []byte
	}{
		{"CRC-32, Block", func(blk []byte) []byte { return withCRC(blk) }},
		{"BlockDuration, Block", func(blk []byte) []byte { return append(bytes.Clone(duration), blk...) }},
		{"CRC-32, BlockDuration, Block", func(blk []byte) []byte { return withCRC(append(bytes.Clone(duration), blk...)) }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, target := groupFile(t, src, blocks[1], want.End()+100000, tc.kids)
			c, _ := parseElem(b, int(clusters[1][0]))
			fixMasterCRC(b, c)
			f := filepath.Join(dir, fmt.Sprintf("group%d.mkv", i))
			require.NoError(t, os.WriteFile(f, b, 0o600))
			orig := packets(t, f)
			require.NotEmpty(t, orig)
			if i == 2 {
				// Control: the same group with the Block first demuxes to the
				// same packets, so the order is valid to ffmpeg too.
				ctl, _ := groupFile(t, src, blocks[1], want.End()+100000, func(blk []byte) []byte { return withCRC(append(bytes.Clone(blk), duration...)) })
				fixMasterCRC(ctl, c)
				cf := filepath.Join(dir, "control.mkv")
				require.NoError(t, os.WriteFile(cf, ctl, 0o600))
				require.Equal(t, orig, packets(t, cf), "child order does not change the packets")
			}

			got, err := Layout(bytes.NewReader(b), int64(len(b)), hs, he)
			require.NoError(t, err)
			wantLost := lossWith(t, dir, fmt.Sprintf("want%d.mkv", i), b, hs, he, want, orig)
			gotLost := lossWith(t, dir, fmt.Sprintf("got%d.mkv", i), b, hs, he, got, orig)
			t.Logf("BlockGroup (%s) at %d; boundary %d (want %d); packets lost %d (want %d)", tc.name, target, got.End(), want.End(), gotLost, wantLost)
			require.Equal(t, want, got)
		})
	}
}
