package nzbfilesystem

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/holes/mkvvoid"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/mkvbuild"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/kipsilabs/altmount/internal/usenet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const voidSegSize = 4096

// voidFixture is a synthetic MKV cut into voidSegSize segments. One big
// cluster of ~1000-byte blocks covers segments 1-159, so a hole can be placed
// on any of them, and a two-segment run stays inside the padding caps.
func voidFixture(t *testing.T) []byte {
	t.Helper()
	f := mkvbuild.New(11)
	f.StartCluster(0, true)
	for f.Len() < 160*voidSegSize {
		f.SimpleBlock(1+int(f.Len()/1000)%2, 1000+int(f.Len()%97))
	}
	f.EndCluster()
	f.StartCluster(5000, true)
	for range 20 {
		f.SimpleBlock(1, 1000)
	}
	f.EndCluster()
	return f.Bytes()
}

// straddleFixture places the Void start 4 bytes before the boundary between
// segments k and k+1, so a 9-byte header would be split across two padded
// segments of the run (k, 2): such a layout must be plain zeros.
func straddleFixture(t *testing.T, k int) []byte {
	t.Helper()
	f := mkvbuild.New(12)
	f.StartCluster(0, false)
	hs := int64(k * voidSegSize)
	for f.Len() < hs-2000 {
		f.SimpleBlock(1, 1000)
	}
	e := f.SimpleBlockTotal(2, int((hs+voidSegSize-4)-f.Len()))
	require.Equal(t, hs+voidSegSize-4, e.End)
	for f.Len() < int64((k+120)*voidSegSize) {
		f.SimpleBlock(1, 1000)
	}
	f.EndCluster()
	return f.Bytes()
}

func segmentsFor(data []byte) []*metapb.SegmentData {
	n := (len(data) + voidSegSize - 1) / voidSegSize
	out := make([]*metapb.SegmentData, n)
	for i := range out {
		size := min(voidSegSize, len(data)-i*voidSegSize)
		out[i] = &metapb.SegmentData{Id: segments.MessageID(i), SegmentSize: int64(size), StartOffset: 0, EndOffset: int64(size - 1)}
	}
	return out
}

// voidPool serves data's segments, with the hole segments gone.
func voidPool(data []byte, hole ...int) *fakepool.Client {
	fp := fakepool.New()
	missing := map[int]bool{}
	for _, h := range hole {
		missing[h] = true
	}
	for i, s := range segmentsFor(data) {
		if missing[i] {
			fp.SetBehavior(s.Id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
			continue
		}
		start := i * voidSegSize
		fp.SetBehavior(s.Id, fakepool.SegmentBehavior{Bytes: data[start : start+int(s.SegmentSize)]})
	}
	return fp
}

func voidMVF(t *testing.T, fp *fakepool.Client, data []byte, rows []*metapb.HoleRun) *MetadataVirtualFile {
	t.Helper()
	segs := segmentsFor(data)
	mvf := newTestMVF(t, context.Background(), fp, len(segs), voidSegSize, 4)
	mvf.name = "movies/void.mkv"
	mvf.meta.SegmentData = segs
	mvf.meta.FileSize = int64(len(data))
	mvf.meta.KnownHoles = rows
	return mvf
}

func readFrom(t *testing.T, mvf *MetadataVirtualFile, off, n int64) []byte {
	t.Helper()
	buf := make([]byte, n)
	got, err := mvf.ReadAtContext(context.Background(), buf, off)
	require.NoError(t, err)
	require.EqualValues(t, n, got)
	return buf
}

// expected is the bytes every read must see: the file with the hole zeroed
// and the Void header written at the layout's offset.
func expected(data []byte, hs, he int64, res mkvvoid.Result) []byte {
	out := append([]byte(nil), data...)
	clear(out[hs:he])
	if res.OK {
		h := mkvvoid.Header(res.Size)
		copy(out[res.Offset:], h[:])
	}
	return out
}

func layoutFor(t *testing.T, data []byte, hs, he int64) mkvvoid.Result {
	t.Helper()
	res, err := mkvvoid.Layout(bytes.NewReader(data), int64(len(data)), hs, he)
	require.NoError(t, err)
	require.True(t, res.OK, res.Reason)
	return res
}

// Served bytes are a pure function of the file and the hole set: a read that
// starts in the middle of the hole, even inside the Void header, returns
// exactly what a sequential read returns there.
func TestVoidPad_MidHoleRangeReadEqualsSequential(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	want := expected(data, hs, he, res)

	fp := voidPool(data, k)
	seq := readFrom(t, voidMVF(t, fp, data, rows), 0, int64(len(data)))
	require.Equal(t, want, seq, "sequential read: zeros plus the Void header, everything else intact")
	assert.Zero(t, fp.PerMessageCalls(segments.MessageID(k)), "a known hole is not fetched")

	for _, off := range []int64{res.Offset + 3, res.Offset, hs, hs + 1, res.Offset + mkvvoid.HeaderLen} {
		got := readFrom(t, voidMVF(t, voidPool(data, k), data, rows), off, int64(len(data))-off)
		require.Equal(t, seq[off:], got, "range read from %d", off)
	}
	small := readFrom(t, voidMVF(t, voidPool(data, k), data, rows), res.Offset+2, 5)
	require.Equal(t, seq[res.Offset+2:res.Offset+7], small)
}

// Two consecutive reads split anywhere around the header, ending or starting
// inside it, join to exactly what one read covering both serves.
func TestVoidPad_ReadsSplitAroundTheHeaderJoinToOneRead(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	vs := res.Offset
	start, end := hs-16, vs+32
	one := readFrom(t, voidMVF(t, voidPool(data, k), data, rows), start, end-start)
	require.Equal(t, expected(data, hs, he, res)[start:end], one, "fixture: one read serves the header")

	for split := vs - 2; split <= vs+11; split++ {
		mvf := voidMVF(t, voidPool(data, k), data, rows)
		a := readFrom(t, mvf, start, split-start)
		b := readFrom(t, mvf, split, end-split)
		assert.Equal(t, one, append(a, b...), "split at header offset %d", split-vs)
	}
}

// A run that is not a known hole yet (its stored record expired) is probed,
// confirmed missing, and padded by OnHole: the same layout applies there.
func TestVoidPad_ReconfirmedHoleGetsTheHeader(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Add(-25 * time.Hour).Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	fp := voidPool(data, k)
	got := readFrom(t, voidMVF(t, fp, data, rows), 0, int64(len(data)))
	assert.NotZero(t, fp.PerMessageCalls(segments.MessageID(k)), "the expired hole is asked for again")
	assert.Equal(t, expected(data, hs, he, res), got)
}

// Stored runs that merge into one bigger run in the handle (here (20,1)
// with a layout and an adjacent (21,1)) must not use the old run's layout.
func TestVoidPad_MergedRunServesZeros(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	now := time.Now().Unix()
	rows := []*metapb.HoleRun{
		{StartSegment: k, Count: 1, RecordedAt: now, VoidOffset: res.Offset, VoidSize: res.Size},
		{StartSegment: k + 1, Count: 1, RecordedAt: now},
	}
	got := readFrom(t, voidMVF(t, voidPool(data, k, k+1), data, rows), 0, int64(len(data)))
	assert.Equal(t, expected(data, hs, he+voidSegSize, mkvvoid.Result{}), got)
}

func TestVoidPad_IneligibleFilesStayZeros(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	mvf := voidMVF(t, voidPool(data, k), data, rows)
	mvf.name = "movies/void.mp4"
	got := readFrom(t, mvf, 0, int64(len(data)))
	assert.Equal(t, expected(data, hs, he, mkvvoid.Result{}), got)
}

// writeVoidMeta persists metadata matching voidMVF's segments.
func writeVoidMeta(t *testing.T, ms *metadata.MetadataService, name string, data []byte, rows []*metapb.HoleRun) {
	t.Helper()
	m := ms.CreateFileMetadata(int64(len(data)), "x.nzb", metapb.FileStatus_FILE_STATUS_DEGRADED, segmentsFor(data),
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	m.KnownHoles = rows
	require.NoError(t, ms.WriteFileMetadata(name, m))
}

func noStore() usenet.SegmentStore { return nil }

// The whole lifecycle: a replayed hole without a layout serves plain zeros
// and asks for one; the layouter reads the neighbouring bytes through the
// pool (never the hole), persists the layout, and the next open serves the
// header.
func TestVoidLayouter_ComputesPersistsAndNextOpenServesHeader(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+1)*voidSegSize)
	want := layoutFor(t, data, hs, he)

	_, _, ms := setupStreamHealthEnv(t)
	name := "movies/void.mkv"
	writeVoidMeta(t, ms, name, data, []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Unix()}})
	m, err := ms.ReadFileMetadata(name)
	require.NoError(t, err)

	fp := voidPool(data, k)
	layouter := newVoidLayouter(ms, poolLayoutReaders(newFakePoolManager(fp), noStore))
	first := voidMVF(t, fp, data, m.KnownHoles)
	first.voidLayouter = layouter
	got := readFrom(t, first, 0, int64(len(data)))
	assert.Equal(t, expected(data, hs, he, mkvvoid.Result{}), got, "no layout yet: plain zeros, as before")
	layouter.Close() // drains the request made by the open

	m, err = ms.ReadFileMetadata(name)
	require.NoError(t, err)
	require.Len(t, m.KnownHoles, 1)
	assert.Equal(t, want.Offset, m.KnownHoles[0].VoidOffset)
	assert.Equal(t, want.Size, m.KnownHoles[0].VoidSize)
	assert.Zero(t, fp.PerMessageCalls(segments.MessageID(k)), "neither the read nor the layouter fetches the hole")

	second := voidMVF(t, voidPool(data, k), data, m.KnownHoles)
	assert.Equal(t, expected(data, hs, he, want), readFrom(t, second, 0, int64(len(data))))
}

// A layout that cannot be placed is recorded as -1 so it is not recomputed
// on every open; a computation that fails on the pool records nothing.
func TestVoidLayouter_DefiniteZerosAndFailures(t *testing.T) {
	data := voidFixture(t)
	_, _, ms := setupStreamHealthEnv(t)

	// Segment 0 holds the file head: definite zeros.
	writeVoidMeta(t, ms, "movies/head.mkv", data, []*metapb.HoleRun{{StartSegment: 0, Count: 1, RecordedAt: time.Now().Unix()}})
	// Segment 20 is a hole whose neighbour 19 is gone too, but not yet known.
	writeVoidMeta(t, ms, "movies/flaky.mkv", data, []*metapb.HoleRun{{StartSegment: 20, Count: 1, RecordedAt: time.Now().Unix()}})

	fp := voidPool(data, 0, 19, 20)
	l := newVoidLayouter(ms, poolLayoutReaders(newFakePoolManager(fp), noStore))
	l.request("movies/head.mkv", 0)
	l.request("movies/flaky.mkv", 20)
	l.Close()

	m, err := ms.ReadFileMetadata("movies/head.mkv")
	require.NoError(t, err)
	assert.EqualValues(t, -1, m.KnownHoles[0].VoidOffset, "computed, plain zeros")
	m, err = ms.ReadFileMetadata("movies/flaky.mkv")
	require.NoError(t, err)
	assert.Zero(t, m.KnownHoles[0].VoidOffset, "a failed read is not an answer")
	assert.Len(t, l.retryAt, 1, "the failed run backs off before a retry")
}

type countingLayoutStore struct {
	mu    sync.Mutex
	reads []string
}

func (s *countingLayoutStore) ReadFileMetadata(p string) (*metapb.FileMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, p)
	return nil, nil
}

func (s *countingLayoutStore) SetHoleLayout(string, holes.Run, int64, int64) error { return nil }

// padRecorder asks for a layout after recording a hole only when the event
// says so (a Matroska handle whose ceiling still allows replay).
func TestPadRecorderRequestsVoidLayoutOnlyWhenAsked(t *testing.T) {
	store := &countingLayoutStore{}
	l := newVoidLayouter(store, nil)
	r := newPadRecorder(&fakePadMetadataStore{holesRecorded: make(chan struct{}, 8), statusRecorded: make(chan struct{}, 8)}, nil, nil)
	r.layouts = l
	r.record(padEvent{name: "movies/a.mkv", segIndex: 3, voidLayout: true})
	r.record(padEvent{name: "movies/b.mkv", segIndex: 3})
	r.record(padEvent{name: "movies/a.mp4", segIndex: 3})
	r.Close()
	l.Close()
	assert.Equal(t, []string{"movies/a.mkv"}, store.reads)
}
