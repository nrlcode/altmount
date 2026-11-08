package usenet

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/pool"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
)

// transient_miss_test.go pins the contract for a mid-stream 430 that is not
// actually a missing article (issue #749): providers desync and answer "no
// such article" for a segment that is still there seconds later.

// TestTransientMiss_ReadRecovers is the issue itself: one segment 430s once,
// the article is present on a re-check, and playback must survive.
func TestTransientMiss_ReadRecovers(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 4
		segSize     = 16
		maxPrefetch = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		b := fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)}
		if i == 1 {
			// One transient 430, then the article serves normally.
			b.FailFirst = 1
			b.FailErr = nntppool.ErrArticleNotFound
		}
		fp.SetBehavior(segments.MessageID(i), b)
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, maxPrefetch)
	ur.Start()

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("read failed on a transient miss: %v", err)
	}

	var want []byte
	for i := range segCount {
		want = append(want, segments.Payload(i, segSize)...)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch after recovery: got %d bytes, want %d", len(got), len(want))
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("re-check issued %d priority STATs, want exactly 1", n)
	}
}

// TestTransientMiss_ConfirmedGoneNamesSegment pins what the health pipeline
// needs from a genuine miss: the segment and the verdict. Without SegmentID
// the downstream re-check in nzbfilesystem cannot run at all.
func TestTransientMiss_ConfirmedGoneNamesSegment(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 3
		segSize     = 16
		maxPrefetch = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetBehavior(segments.MessageID(0), fakepool.SegmentBehavior{Bytes: segments.Payload(0, segSize)})
	// Permanently gone: both the body fetch and the re-check say 430.
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Bytes: segments.Payload(2, segSize)})

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newReaderForTest(t, ctx, fp, rg, maxPrefetch)
	ur.Start()

	_, err := io.ReadAll(ur)
	var dce *DataCorruptionError
	if !errors.As(err, &dce) {
		t.Fatalf("want DataCorruptionError, got %v", err)
	}
	if dce.SegmentID != segments.MessageID(1) {
		t.Errorf("SegmentID = %q, want %q", dce.SegmentID, segments.MessageID(1))
	}
	if dce.MissVerdict != MissConfirmed {
		t.Errorf("MissVerdict = %v, want MissConfirmed", dce.MissVerdict)
	}
	// One body attempt (never retried) plus exactly one confirming STAT.
	if n := fp.PerMessageBodyCalls(segments.MessageID(1)); n != 1 {
		t.Errorf("segment 1 issued %d body calls, want 1", n)
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("issued %d priority STATs, want 1", n)
	}
}

// TestTransientMiss_RecheckIsBudgeted keeps a dead release from paying a
// ~1.2s existence check per hole: once a re-checked miss has stood, later
// misses in the reader are not asked about again. Segments 1-7 are held
// until segment 0's miss has stood; misses that are already re-checking
// when the first one stands are TestTransientMiss_ConcurrentDeadRelease.
func TestTransientMiss_RecheckIsBudgeted(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 8
		segSize     = 16
		maxPrefetch = 8
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	release := make(chan struct{})
	gc := &gatedClient{Client: fp, beforeFetch: func(ctx context.Context, id string) {
		if id != segments.MessageID(0) {
			waitGate(ctx, release)
		}
	}}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, nil)
	ur.Start()

	if v := waitMissVerdict(t, ctx, rg, 0); v != MissConfirmed {
		t.Fatalf("segment 0 MissVerdict = %v, want MissConfirmed", v)
	}
	close(release)
	for i := 1; i < segCount; i++ {
		if v := waitMissVerdict(t, ctx, rg, i); v != MissUnverified {
			t.Errorf("segment %d MissVerdict = %v, want MissUnverified (budget spent by segment 0)", i, v)
		}
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("issued %d priority STATs across %d missing segments, want 1", n, segCount)
	}
}

// TestTransientMiss_ImportProfileDoesNotRecheck keeps the import path on its
// own validation: it sweeps availability up front, so a re-check is pure cost.
func TestTransientMiss_ImportProfileDoesNotRecheck(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 3
		segSize     = 16
		maxPrefetch = 3
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	rg := buildEagerRange(ctx, t, segCount, segSize)
	getter := func() (pool.NntpClient, error) { return fp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, maxPrefetch, noopMetrics{}, "import-stream", nil,
		withFlightMap(newFlightMap()), WithImportProfile(nil))
	if err != nil {
		t.Fatalf("NewUsenetReader: %v", err)
	}
	t.Cleanup(func() { _ = ur.Close() })
	ur.Start()
	_, _ = io.ReadAll(ur)
	time.Sleep(150 * time.Millisecond)

	if n := fp.StatCalls(); n != 0 {
		t.Errorf("import profile issued %d existence checks, want 0", n)
	}
}

// gatedClient wraps the fake so a test can order fetches and existence
// checks with channels instead of sleeps. Every hook is optional.
type gatedClient struct {
	*fakepool.Client
	beforeFetch  func(ctx context.Context, id string) // may block
	beforeExists func(ctx context.Context, id string) // may block
	existsErr    func(id string) error                // non-nil replaces the fake's answer
	afterExists  func(id string)
}

func (g *gatedClient) Fetch(ctx context.Context, r nntppool.Req) (*nntppool.ArticleBody, error) {
	if g.beforeFetch != nil {
		g.beforeFetch(ctx, r.MessageID)
	}
	return g.Client.Fetch(ctx, r)
}

func (g *gatedClient) Exists(ctx context.Context, r nntppool.Req) (*nntppool.StatResult, error) {
	if g.beforeExists != nil {
		g.beforeExists(ctx, r.MessageID)
	}
	res, err := g.Client.Exists(ctx, r)
	if g.afterExists != nil {
		defer g.afterExists(r.MessageID)
	}
	if g.existsErr != nil {
		if e := g.existsErr(r.MessageID); e != nil {
			return nil, e
		}
	}
	return res, err
}

// waitGate blocks until gate is closed or ctx ends.
func waitGate(ctx context.Context, gate <-chan struct{}) {
	select {
	case <-gate:
	case <-ctx.Done():
	}
}

// barrier releases its callers only once n of them are waiting at the same
// time, which proves n calls overlapped. broke records a caller whose
// context ended first.
type barrier struct {
	n       int32
	arrived atomic.Int32
	all     chan struct{}
	broke   atomic.Bool
}

func newBarrier(n int) *barrier { return &barrier{n: int32(n), all: make(chan struct{})} }

func (b *barrier) wait(ctx context.Context) {
	if b.arrived.Add(1) == b.n {
		close(b.all)
	}
	select {
	case <-b.all:
	case <-ctx.Done():
		b.broke.Store(true)
	}
}

func newGatedReader(t testing.TB, ctx context.Context, cp pool.NntpClient, rg *segmentRange, maxPrefetch int, hooks *HoleHooks) *UsenetReader {
	t.Helper()
	getter := func() (pool.NntpClient, error) { return cp, nil }
	ur, err := NewUsenetReader(ctx, getter, rg, maxPrefetch, noopMetrics{}, "test-stream", nil,
		WithHoleHooks(hooks), withFlightMap(newFlightMap()))
	if err != nil {
		t.Fatalf("NewUsenetReader: %v", err)
	}
	t.Cleanup(func() { _ = ur.Close() })
	return ur
}

// waitMissErr waits for segment i's download to record its error and
// returns it. It fails the test if the segment ends without a miss
// annotation.
func waitMissErr(t *testing.T, ctx context.Context, rg *segmentRange, i int) error {
	t.Helper()
	for {
		if err := rg.segments[i].GetDownloadError(); err != nil {
			if _, _, ok := MissInfo(err); !ok {
				t.Fatalf("segment %d error %v carries no miss verdict", i, err)
			}
			return err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("segment %d never recorded an error", i)
		case <-time.After(time.Millisecond):
		}
	}
}

func waitMissVerdict(t *testing.T, ctx context.Context, rg *segmentRange, i int) MissVerdict {
	t.Helper()
	_, v, _ := MissInfo(waitMissErr(t, ctx, rg, i))
	return v
}

// TestTransientMiss_BurstAllRecover is the burst seen on a live install: a
// batch of read-ahead fetches 430s at once and every one of those articles is
// present a moment later. A re-check that comes back present must not use up
// the budget, or every miss after the first is padded without being asked.
// The STATs are held until all eight are in flight, so they provably overlap.
func TestTransientMiss_BurstAllRecover(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 8
		segSize     = 16
		maxPrefetch = 8
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{
			Bytes:     segments.Payload(i, segSize),
			FailFirst: 1,
			FailErr:   nntppool.ErrArticleNotFound,
		})
	}
	bar := newBarrier(segCount)
	gc := &gatedClient{Client: fp, beforeExists: func(ctx context.Context, _ string) { bar.wait(ctx) }}

	var onHoleCalls atomic.Int32
	hooks := &HoleHooks{
		OnHole: func(int, string) holes.Decision {
			onHoleCalls.Add(1)
			return holes.DecisionPad
		},
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, hooks)
	ur.Start()

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("read failed on a burst of transient misses: %v", err)
	}
	if bar.broke.Load() {
		t.Errorf("only %d of %d re-checks were in flight together", bar.arrived.Load(), segCount)
	}

	var want []byte
	for i := range segCount {
		want = append(want, segments.Payload(i, segSize)...)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch: a transient miss was zero-filled instead of re-fetched")
	}
	if n := onHoleCalls.Load(); n != 0 {
		t.Errorf("OnHole called %d times for articles that were present, want 0", n)
	}
	if n := fp.StatPriorityCalls(); n != segCount {
		t.Errorf("issued %d priority STATs, want one per miss (%d)", n, segCount)
	}
}

// TestTransientMiss_ConcurrentDeadRelease bounds the dead-release cost when
// every miss is re-checking at once: each STATs once, none twice, and every
// segment still pads as before. The STATs are held until all eight are in
// flight, so none of them can see another's verdict first.
func TestTransientMiss_ConcurrentDeadRelease(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 8
		segSize     = 16
		maxPrefetch = 8
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	fp.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	bar := newBarrier(segCount)
	gc := &gatedClient{Client: fp, beforeExists: func(ctx context.Context, _ string) { bar.wait(ctx) }}

	var onHoleCalls atomic.Int32
	hooks := &HoleHooks{
		OnHole: func(int, string) holes.Decision {
			onHoleCalls.Add(1)
			return holes.DecisionPad
		},
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, hooks)
	ur.Start()

	got, err := io.ReadAll(ur)
	if err != nil {
		t.Fatalf("read failed although every hole was approved for padding: %v", err)
	}
	if bar.broke.Load() {
		t.Errorf("only %d of %d re-checks were in flight together", bar.arrived.Load(), segCount)
	}
	if string(got) != string(make([]byte, segCount*segSize)) {
		t.Errorf("got %d bytes, want %d zero-filled bytes", len(got), segCount*segSize)
	}
	if n := onHoleCalls.Load(); n != segCount {
		t.Errorf("OnHole called %d times, want once per missing segment (%d)", n, segCount)
	}
	if n := fp.StatPriorityCalls(); n != segCount {
		t.Errorf("issued %d priority STATs, want one per miss in flight (%d)", n, segCount)
	}
	for i := range segCount {
		if n := fp.PerMessageBodyCalls(segments.MessageID(i)); n != 1 {
			t.Errorf("segment %d issued %d body calls, want 1 (a confirmed miss is not refetched)", i, n)
		}
	}
}

// TestTransientMiss_UnfetchableUsesBudget is the takedown case: STAT says the
// article exists but every BODY 430s. That miss stands as MissUnfetchable, so
// it pays for the reader's re-check and the next miss is not asked about.
func TestTransientMiss_UnfetchableUsesBudget(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 4
		segSize     = 16
		maxPrefetch = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		b := fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)}
		if i == 1 || i == 2 {
			// STAT answers present (the fake's STAT ignores FailFirst); every
			// BODY answers 430.
			b.FailFirst = 1000
			b.FailErr = nntppool.ErrArticleNotFound
		}
		fp.SetBehavior(segments.MessageID(i), b)
	}
	// Segment 2 is held until segment 1's miss has stood.
	release := make(chan struct{})
	gc := &gatedClient{Client: fp, beforeFetch: func(ctx context.Context, id string) {
		if id == segments.MessageID(2) {
			waitGate(ctx, release)
		}
	}}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, nil)
	ur.Start()

	if v := waitMissVerdict(t, ctx, rg, 1); v != MissUnfetchable {
		t.Fatalf("segment 1 MissVerdict = %v, want MissUnfetchable", v)
	}
	close(release)
	if v := waitMissVerdict(t, ctx, rg, 2); v != MissUnverified {
		t.Errorf("segment 2 MissVerdict = %v, want MissUnverified (budget spent by segment 1)", v)
	}
	// Segment 1: first fetch, STAT present, refetch. Segment 2: one fetch only.
	if n := fp.PerMessageBodyCalls(segments.MessageID(1)); n != 2 {
		t.Errorf("segment 1 issued %d body calls, want 2 (fetch and refetch)", n)
	}
	if n := fp.PerMessageBodyCalls(segments.MessageID(2)); n != 1 {
		t.Errorf("segment 2 issued %d body calls, want 1", n)
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("issued %d priority STATs, want 1", n)
	}
}

// TestTransientMiss_UnresolvedUsesBudget: a re-check the pool cannot answer
// stands as MissUnresolved, keeps the miss a plain not-found for every
// existing branch, and spends the budget like any other miss that stood.
func TestTransientMiss_UnresolvedUsesBudget(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 4
		segSize     = 16
		maxPrefetch = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)})
	}
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	errTransport := errors.New("fake: connection reset during STAT")
	// Segment 2 is held until segment 1's miss has stood.
	release := make(chan struct{})
	gc := &gatedClient{
		Client: fp,
		beforeFetch: func(ctx context.Context, id string) {
			if id == segments.MessageID(2) {
				waitGate(ctx, release)
			}
		},
		existsErr: func(id string) error {
			if id == segments.MessageID(1) {
				return errTransport
			}
			return nil
		},
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, nil)
	ur.Start()

	err := waitMissErr(t, ctx, rg, 1)
	if _, v, _ := MissInfo(err); v != MissUnresolved {
		t.Fatalf("segment 1 MissVerdict = %v, want MissUnresolved", v)
	}
	if !errors.Is(err, nntppool.ErrArticleNotFound) {
		t.Errorf("segment 1 error %v no longer matches ErrArticleNotFound", err)
	}
	if errors.Is(err, errTransport) {
		t.Errorf("segment 1 error %v carries the STAT's transport error instead of the miss", err)
	}
	close(release)
	if v := waitMissVerdict(t, ctx, rg, 2); v != MissUnverified {
		t.Errorf("segment 2 MissVerdict = %v, want MissUnverified (budget spent by segment 1)", v)
	}
	if n := fp.StatPriorityCalls(); n != 1 {
		t.Errorf("issued %d priority STATs, want 1", n)
	}
}

// TestTransientMiss_TransientThenPermanent: a miss that recovers costs
// nothing, so a later real miss in the same reader still gets its re-check
// and reaches the health pipeline as confirmed.
func TestTransientMiss_TransientThenPermanent(t *testing.T) {
	t.Parallel()
	const (
		segCount    = 4
		segSize     = 16
		maxPrefetch = 4
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fp := fakepool.New()
	for i := range segCount {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)})
	}
	fp.SetBehavior(segments.MessageID(1), fakepool.SegmentBehavior{
		Bytes:     segments.Payload(1, segSize),
		FailFirst: 1,
		FailErr:   nntppool.ErrArticleNotFound,
	})
	fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	// Segment 2 is held until segment 1's re-check has answered present.
	release := make(chan struct{})
	var once sync.Once
	gc := &gatedClient{
		Client: fp,
		beforeFetch: func(ctx context.Context, id string) {
			if id == segments.MessageID(2) {
				waitGate(ctx, release)
			}
		},
		afterExists: func(id string) {
			if id == segments.MessageID(1) {
				once.Do(func() { close(release) })
			}
		},
	}

	rg := buildEagerRange(ctx, t, segCount, segSize)
	ur := newGatedReader(t, ctx, gc, rg, maxPrefetch, nil)
	ur.Start()

	got, err := io.ReadAll(ur)
	var dce *DataCorruptionError
	if !errors.As(err, &dce) {
		t.Fatalf("want DataCorruptionError for segment 2, got %v", err)
	}
	if dce.SegmentID != segments.MessageID(2) {
		t.Fatalf("SegmentID = %q, want %q", dce.SegmentID, segments.MessageID(2))
	}
	if dce.MissVerdict != MissConfirmed {
		t.Errorf("segment 2 MissVerdict = %v, want MissConfirmed", dce.MissVerdict)
	}
	want := append(segments.Payload(0, segSize), segments.Payload(1, segSize)...)
	if string(got) != string(want) {
		t.Errorf("got %d bytes before the real miss, want segments 0 and 1 intact", len(got))
	}
	if n := fp.StatPriorityCalls(); n != 2 {
		t.Errorf("issued %d priority STATs, want 2 (one per miss)", n)
	}
}
