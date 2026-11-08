package nzbfilesystem

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/holes/mkvvoid"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/usenet"
)

// Matroska holes are padded with zeros plus, where the file's structure
// allows, an EBML Void header (see package mkvvoid): the demuxer then skips to
// the next intact block instead of dropping the rest of the cluster.
//
// The header's place depends on bytes around the hole, which may be far from
// what the current read has in hand, and every read of a hole must serve the
// same bytes (rclone caches each range it gets). So the layout is computed
// once per hole run, off the read path, and persisted with the run
// (HoleRun.void_offset/void_size). Reads only ever apply a persisted layout:
// until one exists the hole is plain zeros, exactly as before.

const (
	// voidLayoutTimeout bounds one layout computation, neighbour reads
	// included (at most mkvvoid.MaxNeighbourBytes plus the file head).
	voidLayoutTimeout = 30 * time.Second
	// voidLayoutRetry is how long a computation that failed for a
	// non-content reason (pool error, timeout) waits before it is retried.
	voidLayoutRetry = 10 * time.Minute
	// voidLayoutQueueSize bounds pending requests; overflow is dropped (the
	// next open asks again).
	voidLayoutQueueSize = 64
	// voidLayoutPrefetch is the read-ahead of the neighbour readers.
	voidLayoutPrefetch = 8
	// voidSmallRead is the largest read served through the one-segment
	// cache (element headers during the head walk) instead of a reader of
	// its own.
	voidSmallRead = 64 << 10
)

// voidLayout is a persisted layout for one exact hole run.
type voidLayout struct {
	offset int64 // file offset of the Void header
	size   int64 // Void data size
	seg    int   // the segment holding the whole header
}

// headerSegment returns the segment that holds the whole Void header at off,
// or -1 when the header would cross a segment boundary. Each segment is
// padded, patched or fetched on its own, so a split header could be served
// half (a patch landing mid-handle, a client caching the halves from
// different opens) and its size bytes would then be garbage. Such a layout
// is plain zeros instead.
func headerSegment(idx *segmentOffsetIndex, off int64) int {
	i := idx.findSegmentForOffset(off)
	if i < 0 || off+mkvvoid.HeaderLen > idx.offsets[i]+idx.sizes[i] {
		return -1
	}
	return i
}

// voidEligible reports whether a file's holes may carry a Void header:
// Matroska/WebM files whose bytes are the plain posted bytes and whose
// segment list is complete (no nil entries to build offsets from).
func voidEligible(name string, enc metapb.Encryption, nested, clips int, segs []*metapb.SegmentData) bool {
	if !mkvvoid.Eligible(name) || enc != metapb.Encryption_NONE || nested != 0 || clips != 0 || len(segs) == 0 {
		return false
	}
	for _, s := range segs {
		if s == nil {
			return false
		}
	}
	return true
}

// runBounds is the hole run's file range [hs, he), or ok=false when the run
// lies outside the segment index.
func runBounds(idx *segmentOffsetIndex, run holes.Run) (hs, he int64, ok bool) {
	if idx == nil || run.Start < 0 || run.Count <= 0 || run.Start+run.Count > len(idx.offsets) {
		return 0, 0, false
	}
	last := run.Start + run.Count - 1
	return idx.offsets[run.Start], idx.offsets[last] + idx.sizes[last], true
}

// loadVoidLayouts builds the handle's layout table from the persisted runs,
// keeping only layouts whose header lies inside their own run and none of
// whose segments a PAR2 patch serves (the header assumes every byte of the
// run is a zero). Called from holeHooks while mvf.meta is valid.
func (mvf *MetadataVirtualFile) loadVoidLayouts(idx *segmentOffsetIndex) map[holes.Run]voidLayout {
	out := map[holes.Run]voidLayout{}
	for _, row := range mvf.meta.KnownHoles {
		if row == nil || row.VoidOffset <= 0 || row.VoidSize < 0 {
			continue
		}
		run := holes.Run{Start: int(row.StartSegment), Count: int(row.Count)}
		hs, he, ok := runBounds(idx, run)
		if !ok || row.VoidOffset < hs || row.VoidOffset+mkvvoid.HeaderLen > he ||
			row.VoidOffset+mkvvoid.HeaderLen+row.VoidSize > mvf.meta.FileSize {
			continue
		}
		seg := headerSegment(idx, row.VoidOffset)
		if seg < 0 {
			continue
		}
		if mvf.patchSource != nil && mvf.runPatched(run) {
			continue
		}
		out[run] = voidLayout{offset: row.VoidOffset, size: row.VoidSize, seg: seg}
	}
	return out
}

func (mvf *MetadataVirtualFile) runPatched(run holes.Run) bool {
	segs := mvf.meta.SegmentData
	for i := run.Start; i < run.Start+run.Count && i < len(segs); i++ {
		if mvf.patchLookup(segs[i].Id) != nil {
			return true
		}
	}
	return false
}

// requestVoidLayouts asks the layouter for every live run that has no layout
// yet (replayed holes recorded before this feature, or whose computation has
// not run). It follows the handle's own decision on its holes as opened
// (after dropRecoveredStoredHoles): nothing is asked for when a miss would
// not be padded now (wouldPad), since runGate would then serve no Void and
// the neighbour reads would be wasted. A pad's request (padEvent.voidLayout)
// follows the same decision, taken by onHole on the holes at that moment.
// Called from holeHooks while mvf.meta is valid.
func (mvf *MetadataVirtualFile) requestVoidLayouts(live []holes.Run) {
	if mvf.voidLayouter == nil || len(live) == 0 {
		return
	}
	mvf.holeMu.Lock()
	pads := mvf.wouldPad(mvf.holeAcc)
	mvf.holeMu.Unlock()
	if !pads {
		return
	}
	computed := map[holes.Run]bool{}
	for _, row := range mvf.meta.KnownHoles {
		if row != nil && row.VoidOffset != 0 {
			computed[holes.Run{Start: int(row.StartSegment), Count: int(row.Count)}] = true
		}
	}
	for _, run := range live {
		if !computed[run] {
			mvf.voidLayouter.request(mvf.name, run.Start)
		}
	}
}

// padBytes is the reader's PadBytes hook: when this segment holds the Void
// header of the run it belongs to, it writes the header, addressed by file
// offset (the part inside the payload, when a bounded read cuts the segment
// inside it), so a read that starts or ends mid-hole serves the same bytes
// as a sequential one. A header never crosses a segment (see headerSegment), so a
// segment is served either with its whole header, or as plain zeros (its run
// has changed in this handle, or runGate refuses it), or as patched or
// fetched bytes: never half a header. Runs on download goroutines: reads only
// the hole snapshot, never mvf.meta.
func (mvf *MetadataVirtualFile) padBytes(segIndex int, payload []byte) {
	mvf.holeMu.Lock()
	run, ok := mvf.holeAcc.RunContaining(segIndex)
	var lay voidLayout
	if ok {
		lay, ok = mvf.voidLayouts[run]
	}
	ok = ok && lay.seg == segIndex && mvf.runGate(run)
	mvf.holeMu.Unlock()
	if !ok {
		return
	}
	idx, segs := mvf.holeMeta.segIndex, mvf.holeMeta.segments
	if segIndex < 0 || segIndex >= len(idx.offsets) || segIndex >= len(segs) {
		return
	}
	// File offset -> article byte: the segment's usable bytes start at
	// StartOffset within the article.
	// A bounded read can end inside the header, with the payload cut there:
	// write the header bytes that fall inside it, so two reads split there
	// join to what one read serves.
	j := segs[segIndex].StartOffset + (lay.offset - idx.offsets[segIndex])
	if j < 0 || j >= int64(len(payload)) {
		return
	}
	hdr := mkvvoid.Header(lay.size)
	copy(payload[j:], hdr[:])
}

// runGate reports whether the header of run, a run of mvf.holeAcc whose
// layout this segment holds, may be served. The Void spans the whole run, so
// it is only right when every segment of the run is served as zeros, and the
// header usually goes out with the run's first segment, before the later ones
// are decided. holeAcc is the decision count (onHole adds a miss before it
// decides, and keeps the ones it failed), so it is not a list of padded
// segments; the gate asks instead:
//
//   - whether every segment of the run has been served as padding by this
//     handle (holePadded); a header produced again then matches the zeros
//     the handle already served; or
//   - whether onHole would pad with every segment of the run counted
//     (wouldPad on holeAcc, which holds the whole run already): the
//     lookahead for the segments not yet read.
//
// It does not look at patches: one landing mid-handle for a later run
// segment is served real, but inside the Void, so a demuxer skips it, as it
// would skip zeros. A check that no run segment has a usable patch would go
// here, required in both cases. Caller holds mvf.holeMu.
func (mvf *MetadataVirtualFile) runGate(run holes.Run) bool {
	allPadded := true
	for i := run.Start; i < run.Start+run.Count; i++ {
		if !mvf.holePadded.Has(i) {
			allPadded = false
			break
		}
	}
	return allPadded || mvf.wouldPad(mvf.holeAcc)
}

// voidLayoutStore is the slice of metadata.MetadataService the layouter
// needs; narrowed to an interface so tests can fake it.
type voidLayoutStore interface {
	ReadFileMetadata(virtualPath string) (*metapb.FileMetadata, error)
	SetHoleLayout(virtualPath string, run holes.Run, offset, size int64) error
}

// layoutReaderFactory returns a reader over a file's bytes for one layout
// computation. Reads touching a segment in any stored hole run must fail with
// mkvvoid.ErrHole.
type layoutReaderFactory func(ctx context.Context, meta *metapb.FileMetadata) io.ReaderAt

type voidJob struct {
	name     string
	segIndex int
}

type voidRunKey struct {
	name string
	run  holes.Run
}

// voidLayouter computes and persists Void layouts on one process-lived
// worker, off every read path. Requests are deduplicated while pending;
// computations that fail for a non-content reason back off before retrying.
type voidLayouter struct {
	store   voidLayoutStore
	readers layoutReaderFactory
	timeout time.Duration

	ch      chan voidJob
	mu      sync.Mutex
	pending map[voidJob]struct{}
	retryAt map[voidRunKey]time.Time

	stopCh chan struct{}
	stopWg sync.WaitGroup
}

func newVoidLayouter(store voidLayoutStore, readers layoutReaderFactory) *voidLayouter {
	l := &voidLayouter{
		store:   store,
		readers: readers,
		timeout: voidLayoutTimeout,
		ch:      make(chan voidJob, voidLayoutQueueSize),
		pending: map[voidJob]struct{}{},
		retryAt: map[voidRunKey]time.Time{},
		stopCh:  make(chan struct{}),
	}
	l.stopWg.Add(1)
	go l.run()
	return l
}

// request asks for the layout of the run holding segIndex. Never blocks;
// safe on a nil receiver.
func (l *voidLayouter) request(name string, segIndex int) {
	if l == nil {
		return
	}
	job := voidJob{name: name, segIndex: segIndex}
	l.mu.Lock()
	if _, dup := l.pending[job]; dup {
		l.mu.Unlock()
		return
	}
	l.pending[job] = struct{}{}
	l.mu.Unlock()
	select {
	case l.ch <- job:
	default:
		l.mu.Lock()
		delete(l.pending, job)
		l.mu.Unlock()
	}
}

// Close stops the worker after draining queued requests (tests).
func (l *voidLayouter) Close() {
	close(l.stopCh)
	l.stopWg.Wait()
}

func (l *voidLayouter) run() {
	defer l.stopWg.Done()
	for {
		select {
		case job := <-l.ch:
			l.process(job)
		case <-l.stopCh:
			for {
				select {
				case job := <-l.ch:
					l.process(job)
				default:
					return
				}
			}
		}
	}
}

func (l *voidLayouter) process(job voidJob) {
	defer func() {
		l.mu.Lock()
		delete(l.pending, job)
		l.mu.Unlock()
		if rec := recover(); rec != nil {
			slog.ErrorContext(context.Background(), "Panic computing void layout", "file", job.name, "panic", rec)
		}
	}()

	meta, err := l.store.ReadFileMetadata(job.name)
	if err != nil || meta == nil {
		return
	}
	if !voidEligible(job.name, meta.Encryption, len(meta.NestedSources), len(meta.ClipBoundaries), meta.SegmentData) {
		return
	}
	var row *metapb.HoleRun
	for _, r := range meta.KnownHoles {
		if r != nil && int64(job.segIndex) >= r.StartSegment && int64(job.segIndex) < r.StartSegment+r.Count {
			row = r
			break
		}
	}
	if row == nil || row.VoidOffset != 0 {
		return // not a stored hole, or already computed
	}
	run := holes.Run{Start: int(row.StartSegment), Count: int(row.Count)}
	key := voidRunKey{name: job.name, run: run}
	now := time.Now()
	l.mu.Lock()
	if at, ok := l.retryAt[key]; ok && now.Before(at) {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	idx := buildSegmentIndex(meta.SegmentData)
	hs, he, ok := runBounds(idx, run)
	if !ok {
		return
	}
	size := idx.totalBytes()
	if meta.FileSize > 0 {
		size = min(size, meta.FileSize)
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()
	res, err := mkvvoid.Layout(l.readers(ctx, meta), size, hs, he)
	if err != nil {
		l.mu.Lock()
		if len(l.retryAt) > 4096 {
			clear(l.retryAt)
		}
		l.retryAt[key] = now.Add(voidLayoutRetry)
		l.mu.Unlock()
		slog.DebugContext(ctx, "Void layout not computed, holes stay plain zeros for now",
			"file", job.name, "start_segment", run.Start, "count", run.Count, "error", err)
		return
	}
	offset, voidSize := int64(-1), int64(0)
	if res.OK && headerSegment(idx, res.Offset) < 0 {
		res = mkvvoid.Result{Reason: "void header would cross a segment boundary"}
	}
	if res.OK {
		offset, voidSize = res.Offset, res.Size
	}
	if err := l.store.SetHoleLayout(job.name, run, offset, voidSize); err != nil {
		slog.WarnContext(ctx, "Failed to persist void layout", "file", job.name, "error", err)
		return
	}
	slog.DebugContext(ctx, "Recorded void layout for missing segments",
		"file", job.name, "start_segment", run.Start, "count", run.Count,
		"void", res.OK, "void_offset", offset, "void_size", voidSize, "reason", res.Reason)
}

// poolLayoutReaders reads a file's bytes for layout computation through the
// pool, on the normal (non-priority) lane so it always yields to playback.
// Stored hole segments answer mkvvoid.ErrHole without a fetch.
func poolLayoutReaders(pm pool.Manager, store func() usenet.SegmentStore) layoutReaderFactory {
	return func(ctx context.Context, meta *metapb.FileMetadata) io.ReaderAt {
		segs := meta.SegmentData
		idx := buildSegmentIndex(segs)
		var known holes.Accumulator
		known.Load(metadata.KnownHolesFromProto(meta.KnownHoles))
		var date time.Time
		if meta.ReleaseDate > 0 {
			date = time.Unix(meta.ReleaseDate, 0)
		}
		open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
			first := idx.findSegmentForOffset(start)
			last := idx.findSegmentForOffset(end)
			rg := usenet.NewLazySegmentRange(ctx, start, end, newMetadataSegmentLoader(segs),
				first, idx.getOffsetForSegment(first), last, idx.getOffsetForSegment(last))
			ur, err := usenet.NewUsenetReader(ctx, pm.GetPool, rg, voidLayoutPrefetch, pm, "", store(),
				usenet.WithImportProfile(nil),
				usenet.WithArticleDate(date))
			if err != nil {
				return nil, err
			}
			ur.Start()
			return ur, nil
		}
		return &segmentReaderAt{ctx: ctx, idx: idx, hole: known.Has, open: open, cached: -1}
	}
}

// segmentReaderAt is an io.ReaderAt over a file's segments. Reads touching a
// hole segment fail with mkvvoid.ErrHole; small reads go through a
// one-segment cache, larger ones through a reader of their own.
type segmentReaderAt struct {
	ctx  context.Context
	idx  *segmentOffsetIndex
	hole func(segIndex int) bool
	open func(ctx context.Context, start, end int64) (io.ReadCloser, error)

	mu     sync.Mutex
	cached int
	cache  []byte
}

func (r *segmentReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	end := off + int64(len(p)) - 1
	first := r.idx.findSegmentForOffset(off)
	last := r.idx.findSegmentForOffset(end)
	if first < 0 || last < 0 {
		return 0, io.EOF
	}
	for i := first; i <= last; i++ {
		if r.hole(i) {
			return 0, mkvvoid.ErrHole
		}
	}
	if first == last && len(p) <= voidSmallRead {
		seg, err := r.segment(first)
		if err != nil {
			return 0, err
		}
		return copy(p, seg[off-r.idx.offsets[first]:]), nil
	}
	return r.readRange(p, off, end)
}

func (r *segmentReaderAt) segment(i int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cached == i {
		return r.cache, nil
	}
	buf := make([]byte, r.idx.sizes[i])
	if _, err := r.readRange(buf, r.idx.offsets[i], r.idx.offsets[i]+r.idx.sizes[i]-1); err != nil {
		return nil, err
	}
	r.cached, r.cache = i, buf
	return buf, nil
}

func (r *segmentReaderAt) readRange(p []byte, start, end int64) (int, error) {
	rc, err := r.open(r.ctx, start, end)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return io.ReadFull(rc, p)
}
