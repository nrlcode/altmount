package pool

import (
	"context"
	"testing"
	"time"
)

func TestImportBudget_ZeroCapacityIsNoOp(t *testing.T) {
	b := NewImportBudget()
	for i := range 50 {
		release, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d failed: %v", i, err)
		}
		release()
	}
	b.sem.mu.Lock()
	if b.sem.inFlight != 0 {
		t.Fatalf("disabled budget leaked inFlight=%d", b.sem.inFlight)
	}
	b.sem.mu.Unlock()
}

func TestImportBudget_BlocksAtCapacityAndWakesOnRelease(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(2)

	r1, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	r2, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Errorf("acquire 3: %v", err)
			return
		}
		close(acquired)
		r()
	}()

	select {
	case <-acquired:
		t.Fatal("third Acquire should block at capacity 2")
	case <-time.After(50 * time.Millisecond):
	}

	r1()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("third Acquire did not unblock after release")
	}
	r2()
}

func TestImportBudget_WeightedAcquireIsAtomicAndCancelable(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(4)

	hold, err := b.AcquireN(context.Background(), 3)
	if err != nil {
		t.Fatalf("weighted hold: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan error, 1)
	go func() {
		release, acquireErr := b.AcquireN(ctx, 2)
		if acquireErr == nil {
			release()
		}
		blocked <- acquireErr
	}()

	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return b.sem.inFlight == 3 && len(b.sem.waiters) == 1
	}) {
		t.Fatal("weighted waiter was partially granted instead of queued atomically")
	}
	select {
	case err := <-blocked:
		t.Fatalf("weighted request escaped capacity before release: %v", err)
	default:
	}

	hold()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("weighted request failed after capacity release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("weighted request did not acquire after release")
	}

	holdOne, err := b.AcquireN(context.Background(), 4)
	if err != nil {
		t.Fatalf("fill weighted budget: %v", err)
	}
	canceledCtx, cancelQueued := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, acquireErr := b.AcquireN(canceledCtx, 2)
		canceled <- acquireErr
	}()
	requireQueued := waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	})
	if !requireQueued {
		t.Fatal("cancelable weighted waiter was not queued")
	}
	cancelQueued()
	select {
	case err := <-canceled:
		if err == nil {
			t.Fatal("cancelable weighted request returned no error")
		}
	case <-time.After(time.Second):
		t.Fatal("weighted request did not observe cancellation")
	}
	holdOne()
}

func TestImportBudget_WeightedAcquireHonorsPlaybackHeadroom(t *testing.T) {
	source := &stubStreamSource{}
	b := NewImportBudget()
	b.SetStreamSource(source)
	b.SetCapacity(8)
	source.set(2) // effective capacity is four
	b.NotifyStreamChange()

	hold, err := b.AcquireN(context.Background(), 4)
	if err != nil {
		t.Fatalf("acquire effective playback budget: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan error, 1)
	go func() {
		_, acquireErr := b.AcquireN(ctx, 1)
		blocked <- acquireErr
	}()
	select {
	case err := <-blocked:
		t.Fatalf("weighted work consumed playback headroom: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-blocked; err == nil {
		t.Fatal("blocked playback-headroom request ignored cancellation")
	}
	hold()
}

func TestImportBudget_FlexibleBatchFollowsCapacityShrink(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(4)
	hold, err := b.AcquireN(context.Background(), 4)
	if err != nil {
		t.Fatalf("fill budget: %v", err)
	}

	type result struct {
		release func()
		granted int
		err     error
	}
	done := make(chan result, 1)
	go func() {
		release, granted, acquireErr := b.AcquireUpTo(context.Background(), 4)
		done <- result{release: release, granted: granted, err: acquireErr}
	}()
	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("flexible waiter was not queued")
	}

	b.SetCapacity(2)
	hold()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("flexible acquire after shrink: %v", got.err)
		}
		if got.granted != 2 {
			t.Fatalf("granted = %d, want shrunken capacity 2", got.granted)
		}
		got.release()
	case <-time.After(time.Second):
		t.Fatal("flexible waiter stalled above shrunken capacity")
	}
}

func TestImportBudget_FlexibleHeadDoesNotStarveLaterBodyAfterShrink(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(4)
	hold, err := b.AcquireN(context.Background(), 4)
	if err != nil {
		t.Fatalf("fill budget: %v", err)
	}

	batchGranted := make(chan int, 1)
	batchRelease := make(chan func(), 1)
	go func() {
		release, granted, acquireErr := b.AcquireUpTo(context.Background(), 4)
		if acquireErr != nil {
			batchGranted <- 0
			return
		}
		batchRelease <- release
		batchGranted <- granted
	}()
	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("flexible batch was not queued")
	}

	bodyGranted := make(chan func(), 1)
	go func() {
		release, acquireErr := b.Acquire(context.Background())
		if acquireErr == nil {
			bodyGranted <- release
		}
	}()
	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 2
	}) {
		t.Fatal("later BODY waiter was not queued")
	}

	b.SetCapacity(2)
	hold()
	if granted := <-batchGranted; granted != 2 {
		t.Fatalf("batch grant after shrink = %d, want 2", granted)
	}
	batchDone := <-batchRelease
	batchDone()
	select {
	case release := <-bodyGranted:
		release()
	case <-time.After(time.Second):
		t.Fatal("later BODY waiter was starved behind resized batch")
	}
}

func TestAdaptiveSemaphore_FlexibleCancelUndoesActualDynamicGrant(t *testing.T) {
	s := adaptiveSemaphore{inFlight: 3}
	// The request originally wanted four slots, but a capacity change reduced
	// the atomic grant to two before cancellation won the delivery race.
	w := &waiter{slots: 2, maxSlots: 4, flexible: true}
	s.undoGrantedWaiterLocked(w)
	if s.inFlight != 1 {
		t.Fatalf("inFlight = %d, want unrelated single slot retained", s.inFlight)
	}
}

func TestImportBudget_StreamsShrinkEffectiveCap(t *testing.T) {
	src := &stubStreamSource{}
	b := NewImportBudget()
	b.SetStreamSource(src)
	b.SetCapacity(8)

	// 2 streams -> reserve 2*streamHeadroom=4 -> effective cap 4.
	src.set(2)
	b.NotifyStreamChange()

	var releases []func()
	for i := range 4 {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, r)
	}

	blocked := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(blocked)
			r()
		}
	}()
	select {
	case <-blocked:
		t.Fatal("fifth Acquire should block at effective cap 4 (capacity 8, 2 streams)")
	case <-time.After(50 * time.Millisecond):
	}

	// Streams stop -> effective cap returns to 8, waiter granted.
	src.set(0)
	b.NotifyStreamChange()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after streams ended")
	}
	for _, r := range releases {
		r()
	}
}

func TestImportBudget_FloorOfOneUnderManyStreams(t *testing.T) {
	src := &stubStreamSource{}
	b := NewImportBudget()
	b.SetStreamSource(src)
	b.SetCapacity(4)

	// Reservation would exceed capacity — cap must floor at 1, not 0.
	src.set(100)
	b.NotifyStreamChange()

	r1, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire under floor: %v", err)
	}

	blocked := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(blocked)
			r()
		}
	}()
	select {
	case <-blocked:
		t.Fatal("second Acquire should block at floored cap 1")
	case <-time.After(50 * time.Millisecond):
	}

	r1()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after release at floored cap")
	}
}

func TestImportBudget_SetCapacityGrowWakesWaiters(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(1)

	hold, _ := b.Acquire(context.Background())

	granted := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(granted)
			r()
		}
	}()

	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("waiter never enqueued")
	}

	b.SetCapacity(2)
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after capacity grew")
	}
	hold()
}

func TestImportBudget_ShrinkBelowInFlightBlocksNewGrants(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(3)

	var releases []func()
	for i := range 3 {
		r, err := b.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, r)
	}

	// Shrink to 1 while 3 are in flight — existing tokens drain naturally.
	b.SetCapacity(1)

	granted := make(chan struct{})
	go func() {
		r, err := b.Acquire(context.Background())
		if err == nil {
			close(granted)
			r()
		}
	}()

	// Releasing two still leaves inFlight=1 == cap, so no grant yet.
	releases[0]()
	releases[1]()
	select {
	case <-granted:
		t.Fatal("Acquire granted while inFlight >= shrunken cap")
	case <-time.After(50 * time.Millisecond):
	}

	// Releasing the last one frees a slot under the new cap.
	releases[2]()
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("waiter not granted after inFlight drained below the new cap")
	}
}

func TestImportBudget_CtxCancelWhileQueued(t *testing.T) {
	b := NewImportBudget()
	b.SetCapacity(1)

	hold, _ := b.Acquire(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Acquire(ctx)
		done <- err
	}()

	if !waitFor(time.Second, func() bool {
		b.sem.mu.Lock()
		defer b.sem.mu.Unlock()
		return len(b.sem.waiters) == 1
	}) {
		t.Fatal("waiter never enqueued")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx error on cancel, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after ctx cancellation")
	}

	b.sem.mu.Lock()
	if len(b.sem.waiters) != 0 {
		t.Fatalf("expected 0 waiters after cancel, got %d", len(b.sem.waiters))
	}
	if b.sem.inFlight != 1 {
		t.Fatalf("expected inFlight=1, got %d", b.sem.inFlight)
	}
	b.sem.mu.Unlock()

	hold()
}

func TestImportBudget_CapacitySnapshot(t *testing.T) {
	b := NewImportBudget()
	if got := b.Capacity(); got != 0 {
		t.Fatalf("Capacity() = %d, want 0", got)
	}
	b.SetCapacity(42)
	if got := b.Capacity(); got != 42 {
		t.Fatalf("Capacity() = %d, want 42", got)
	}
	b.SetCapacity(-5)
	if got := b.Capacity(); got != 0 {
		t.Fatalf("Capacity() = %d, want 0 after negative set", got)
	}
}
