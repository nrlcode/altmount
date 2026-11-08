package nzbfilesystem

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/holes/mkvvoid"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Review cases: states in which an earlier version could serve half a Void
// header (garbage size bytes), or did work it could not use.

// lockedPatches is a PatchSource a test can add to while reads run.
type lockedPatches struct {
	mu      sync.Mutex
	patches map[string][]byte
}

func (p *lockedPatches) Get(id string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.patches[id]
	return b, ok
}

func (p *lockedPatches) add(seg int, b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.patches == nil {
		p.patches = map[string][]byte{}
	}
	p.patches[strings.Trim(segments.MessageID(seg), "<>")] = b
}

func segBytes(data []byte, seg int) []byte {
	return data[seg*voidSegSize : (seg+1)*voidSegSize]
}

// zerosOrHeader asserts the 9 bytes at a header slot are the whole header or
// all zeros, never a mix.
func zerosOrHeader(t *testing.T, got []byte, hdr [mkvvoid.HeaderLen]byte, msg string) {
	t.Helper()
	assert.True(t, string(got) == string(hdr[:]) || string(got) == string(make([]byte, mkvvoid.HeaderLen)),
		"%s: torn header %x (want %x or zeros)", msg, got, hdr)
}

// A layout whose header would cross a segment boundary is computed as plain
// zeros (-1), even though the parser alone would place it.
func TestVoidLayouter_StraddlingHeaderPersistsZeros(t *testing.T) {
	const k = 3
	data := straddleFixture(t, k)
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	require.Equal(t, int64((k+1)*voidSegSize-4), res.Offset, "fixture: the parser puts the header across the boundary")

	_, _, ms := setupStreamHealthEnv(t)
	name := "movies/straddle.mkv"
	writeVoidMeta(t, ms, name, data, []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Unix()}})
	l := newVoidLayouter(ms, poolLayoutReaders(newFakePoolManager(voidPool(data, k, k+1)), noStore))
	l.request(name, k)
	l.Close()
	m, err := ms.ReadFileMetadata(name)
	require.NoError(t, err)
	assert.EqualValues(t, -1, m.KnownHoles[0].VoidOffset)
}

// A straddling layout persisted by an earlier build is ignored at open, so a
// PAR2 patch arriving mid-handle for the second segment cannot combine half
// a header with the article's real bytes (this read an 8 MB Void size out of
// a 5 KB one).
func TestVoidPad_StraddlingLayoutIsNeverServed(t *testing.T) {
	const k = 3
	data := straddleFixture(t, k)
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	boundary := int64((k + 1) * voidSegSize)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}

	got := readFrom(t, voidMVF(t, voidPool(data, k, k+1), data, rows), 0, int64(len(data)))
	assert.Equal(t, expected(data, hs, he, mkvvoid.Result{}), got, "plain zeros")

	mvf := voidMVF(t, voidPool(data, k, k+1), data, rows)
	patches := &lockedPatches{}
	mvf.patchSource = patches
	mvf.holeHooks() // layouts loaded with no patch present
	patches.add(k+1, segBytes(data, k+1))
	got = readFrom(t, mvf, 0, int64(len(data)))
	want := append([]byte(nil), data...)
	clear(want[hs:boundary])
	assert.Equal(t, want, got, "zeros, then the patched segment's real bytes; no header half")
}

// The header lies inside one segment, so whichever segment of the run a
// patch lands on mid-handle, a read of the whole segment serves the header
// whole or not at all. A patch on a later segment (k+1) is served real but
// inside the Void, so a demuxer skips it as it would skip zeros: a known
// limit (see runGate), pinned here.
func TestVoidPad_PatchArrivingMidHandleNeverTearsTheHeader(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	require.Equal(t, k, headerSegment(buildSegmentIndex(segmentsFor(data)), res.Offset), "fixture: header in the run's first segment")
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}

	for _, patched := range []int{k, k + 1} {
		mvf := voidMVF(t, voidPool(data, k, k+1), data, rows)
		patches := &lockedPatches{}
		mvf.patchSource = patches
		mvf.holeHooks()
		patches.add(patched, segBytes(data, patched))
		got := readFrom(t, mvf, 0, int64(len(data)))

		want := expected(data, hs, he, res)
		copy(want[patched*voidSegSize:], segBytes(data, patched))
		assert.Equal(t, want, got, "patch on segment %d", patched)
		if patched == k+1 {
			assert.Greater(t, res.End(), int64(patched*voidSegSize), "the Void spans the patched segment")
		}
	}
}

// An expired run re-confirmed segment by segment: until the whole run is
// back in the handle its layout does not apply, so in either order the
// header segment, read whole, is served with the whole header or as plain
// zeros, never a mix.
func TestVoidPad_ReconfirmOrderNeverTearsTheHeader(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	hdr := mkvvoid.Header(res.Size)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Add(-25 * time.Hour).Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	slot := func(b []byte) []byte { return b[res.Offset-hs : res.Offset-hs+mkvvoid.HeaderLen] }

	for _, order := range [][]int{{k + 1, k, k + 1, k}, {k, k + 1, k}} {
		mvf := voidMVF(t, voidPool(data, k, k+1), data, rows)
		var seen [][]byte
		for _, seg := range order {
			got := readFrom(t, mvf, int64(seg*voidSegSize), voidSegSize)
			if seg == k {
				seen = append(seen, slot(got))
				zerosOrHeader(t, slot(got), hdr, fmt.Sprint("read order ", order))
			}
		}
		if order[0] == k+1 {
			assert.Equal(t, hdr[:], seen[0], "the run was whole when its header segment was first read")
		}
	}
}

// With acceptable_missing_segments_percentage at 0 the handle may not replay
// its stored holes, so no Void would ever be served: no layout is asked for
// and no neighbouring article is fetched. Within the ceiling it is.
func TestVoidLayouts_NotRequestedWhenTheCeilingForbidsReplay(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	want := layoutFor(t, data, int64(k*voidSegSize), int64((k+1)*voidSegSize))

	for _, pct := range []float64{0, 2} {
		t.Run(fmt.Sprint(pct, "%"), func(t *testing.T) {
			cfg := acceptableMissingConfig(pct)
			rows := []*metapb.HoleRun{{StartSegment: k, Count: 1, RecordedAt: time.Now().Unix()}}
			_, _, ms := setupStreamHealthEnv(t)
			name := "movies/ceiling.mkv"
			writeVoidMeta(t, ms, name, data, rows)

			fp := voidPool(data, k)
			mvf := withConfig(voidMVF(t, fp, data, rows), cfg)
			mvf.name = name
			mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()
			l := newVoidLayouter(ms, poolLayoutReaders(newFakePoolManager(fp), noStore))
			mvf.voidLayouter = l
			mvf.holeHooks()
			l.Close()

			m, err := ms.ReadFileMetadata(name)
			require.NoError(t, err)
			if pct == 0 {
				require.False(t, mvf.isKnownHole(k), "fixture: replay forbidden at 0 %%")
				assert.Zero(t, fp.PerMessageCalls(segments.MessageID(0)), "no neighbour reads")
				assert.Zero(t, m.KnownHoles[0].VoidOffset, "no layout computed")
			} else {
				require.True(t, mvf.isKnownHole(k))
				assert.Equal(t, want.Offset, m.KnownHoles[0].VoidOffset, "within the ceiling the layout is computed")
			}
		})
	}
}

// Past the ceiling the stored holes are checked with the providers at open
// (dropRecoveredStoredHoles): the ones that came back are dropped and serve
// their real bytes, and the header goes only into a run this handle still
// holds whole, so a lone padded segment of a two-segment run is plain zeros.
// Within the ceiling the stored run is trusted and gets its header.
func TestVoidPad_StoredHolesPastTheCeilingAreCheckedFirst(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	open := func(pct float64, missing ...int) *MetadataVirtualFile {
		cfg := acceptableMissingConfig(pct)
		mvf := withConfig(voidMVF(t, voidPool(data, missing...), data, rows), cfg)
		mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()
		return mvf
	}

	t.Run("past the ceiling, both came back", func(t *testing.T) {
		assert.Equal(t, data, readFrom(t, open(1), 0, int64(len(data))))
	})
	t.Run("past the ceiling, one came back", func(t *testing.T) {
		want := append([]byte(nil), data...)
		clear(want[hs : hs+voidSegSize])
		assert.Equal(t, want, readFrom(t, open(1, k), 0, int64(len(data))))
	})
	t.Run("within the ceiling", func(t *testing.T) {
		assert.Equal(t, expected(data, hs, he, res), readFrom(t, open(2, k, k+1), 0, int64(len(data))))
	})
}

// runGate: the header of a run goes out only if every segment of the run is
// served as zeros. onHole counts a miss before it decides and keeps the ones
// it failed, so a run of the handle's holes may hold a segment the handle
// will never pad.
func TestVoidPad_RunGate(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	hdr := mkvvoid.Header(res.Size)
	slot := res.Offset - hs
	// Expired rows: the run is not replayed, but its layout is loaded.
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Add(-25 * time.Hour).Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	open := func(pct float64) *MetadataVirtualFile {
		return withConfig(voidMVF(t, voidPool(data, k, k+1), data, rows), acceptableMissingConfig(pct))
	}
	pad := func(mvf *MetadataVirtualFile) []byte {
		p := make([]byte, voidSegSize)
		mvf.holeHooks().PadBytes(k, p)
		return p[slot : slot+mkvvoid.HeaderLen]
	}

	t.Run("a failed segment in the run", func(t *testing.T) {
		mvf := open(1)
		h := mvf.holeHooks()
		require.Equal(t, holes.DecisionPad, h.OnHole(k, segments.MessageID(k)))
		require.Equal(t, holes.DecisionFail, h.OnHole(k+1, segments.MessageID(k+1)), "fixture: two misses pass 1 %%")
		assert.Equal(t, make([]byte, mkvvoid.HeaderLen), pad(mvf), "the run holds a segment onHole failed: zeros")
	})
	t.Run("a run the handle padded whole keeps its header", func(t *testing.T) {
		mvf := open(2)
		h := mvf.holeHooks()
		require.Equal(t, holes.DecisionPad, h.OnHole(k, segments.MessageID(k)))
		require.Equal(t, holes.DecisionPad, h.OnHole(k+1, segments.MessageID(k+1)))
		assert.Equal(t, hdr[:], pad(mvf))
		require.Equal(t, holes.DecisionPad, h.OnHole(k+50, segments.MessageID(k+50)))
		require.Equal(t, holes.DecisionFail, h.OnHole(k+60, segments.MessageID(k+60)), "fixture: four misses pass 2 %%")
		assert.Equal(t, hdr[:], pad(mvf), "produced again, the header matches the zeros already served")
	})
}

// The header segment's pad is decided while the read-ahead's miss on the
// next segment of the run is already counted but not yet decided, and that
// miss then fails the stream: the header must not go out, since a demuxer
// would skip over the failed segment to the Void's end.
func TestVoidPad_HeaderWaitsForTheRunsLaterMisses(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	hs, he := int64(k*voidSegSize), int64((k+2)*voidSegSize)
	res := layoutFor(t, data, hs, he)
	rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Add(-25 * time.Hour).Unix(), VoidOffset: res.Offset, VoidSize: res.Size}}
	mvf := withConfig(voidMVF(t, voidPool(data, k, k+1), data, rows), acceptableMissingConfig(1))
	h := mvf.holeHooks()
	onHole := h.OnHole
	first, second := make(chan struct{}), make(chan struct{})
	h.OnHole = func(idx int, id string) holes.Decision {
		switch idx {
		case k:
			d := onHole(idx, id)
			close(first)
			select {
			case <-second:
			case <-time.After(5 * time.Second):
				t.Error("the read-ahead never reached the second segment")
			}
			return d
		case k + 1:
			select {
			case <-first:
			case <-time.After(5 * time.Second):
				t.Error("the first segment was never decided")
			}
			d := onHole(idx, id)
			close(second)
			return d
		}
		return onHole(idx, id)
	}
	mvf.position, mvf.readAtSharedNext = hs, hs

	got := make([]byte, voidSegSize)
	n, err := mvf.ReadAtContext(mvf.ctx, got, hs)
	require.NoError(t, err)
	require.Equal(t, voidSegSize, n)
	slot := res.Offset - hs
	assert.Equal(t, make([]byte, mkvvoid.HeaderLen), got[slot:slot+mkvvoid.HeaderLen], "no header")
	assert.Equal(t, voidSegSize, bytes.Count(got, []byte{0}), "the first segment is plain zeros")
	mvf.holeMu.Lock()
	run, _ := mvf.holeAcc.RunContaining(k)
	mvf.holeMu.Unlock()
	assert.Equal(t, holes.Run{Start: k, Count: 2}, run, "fixture: the failed miss is in the layout's run")
}

// The open-time request and a pad's request follow one decision: whether
// this handle pads with its holes as they stand. Stored holes past the
// ceiling that are still missing stay counted, so a new miss fails, and
// nothing asks for a layout or reads the neighbours; stored holes that came
// back are dropped, and the new miss pads and asks for its layout.
func TestVoidLayouts_FollowTheHandlesDecision(t *testing.T) {
	data := voidFixture(t)
	const k = 20
	cfg := acceptableMissingConfig(1)
	type opened struct {
		mvf    *MetadataVirtualFile
		ms     *metadata.MetadataService
		fp     *fakepool.Client
		l      *voidLayouter
		events chan padEvent
		name   string
	}
	open := func(t *testing.T, missing ...int) opened {
		rows := []*metapb.HoleRun{{StartSegment: k, Count: 2, RecordedAt: time.Now().Unix()}}
		_, _, ms := setupStreamHealthEnv(t)
		name := "movies/decision.mkv"
		writeVoidMeta(t, ms, name, data, rows)
		fp := voidPool(data, missing...)
		mvf := withConfig(voidMVF(t, fp, data, rows), cfg)
		mvf.name = name
		mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()
		l := newVoidLayouter(ms, poolLayoutReaders(newFakePoolManager(fp), noStore))
		mvf.voidLayouter = l
		events := make(chan padEvent, 8)
		mvf.padRecorder = &padRecorder{ch: events}
		mvf.holeHooks()
		return opened{mvf, ms, fp, l, events, name}
	}

	t.Run("stored holes still missing", func(t *testing.T) {
		o := open(t, k, k+1, k+2)
		require.False(t, o.mvf.isKnownHole(k), "fixture: two stored holes pass 1 %%")
		buf := make([]byte, voidSegSize)
		_, err := o.mvf.ReadAtContext(context.Background(), buf, int64((k+2)*voidSegSize))
		assert.Error(t, err, "the new miss fails the stream")
		o.l.Close()
		assert.Empty(t, o.events, "no pad event, so no layout request")
		assert.Zero(t, o.fp.PerMessageCalls(segments.MessageID(0)), "no neighbour reads")
		m, err := o.ms.ReadFileMetadata(o.name)
		require.NoError(t, err)
		assert.Zero(t, m.KnownHoles[0].VoidOffset, "no layout computed")
	})
	t.Run("stored holes came back", func(t *testing.T) {
		o := open(t, k+50)
		o.l.Close()
		o.mvf.holeMu.Lock()
		assert.Zero(t, o.mvf.holeAcc.Total(), "the recovered stored holes are dropped at open")
		o.mvf.holeMu.Unlock()
		require.Equal(t, holes.DecisionPad, o.mvf.onHole(k+50, segments.MessageID(k+50)))
		ev := <-o.events
		assert.True(t, ev.voidLayout, "the pad asks for its run's layout")
	})
}
