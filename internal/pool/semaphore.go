package pool

import (
	"context"
	"errors"
	"sync"
)

var errInvalidSemaphoreWeight = errors.New("admission weight must be positive")

// adaptiveSemaphore is a FIFO counting semaphore whose capacity is computed
// on demand by capLocked, so it can change between acquisitions (config
// updates, stream activity). A computed cap of 0 means "disabled": Acquire is
// a fast-path no-op and any queued waiters are drained.
//
// It uses a FIFO waiter queue with manual select / channel signalling so we
// can support ctx cancellation without dropping wake-ups (the classic
// "lost wakeup" hazard with sync.Cond.Wait + cancellation).
type adaptiveSemaphore struct {
	mu       sync.Mutex
	inFlight int
	waiters  []*waiter
	// capLocked returns the current effective capacity. Called with mu held.
	// 0 means disabled (unlimited, no accounting).
	capLocked func() int
}

type waiter struct {
	// ch is closed (or sent on) exactly once when the waiter is granted a slot.
	// Buffered with capacity 1 so a granter never blocks; on a race with ctx
	// cancellation, the cancelling goroutine drains and forwards the grant to
	// the next waiter to avoid losing the wake-up.
	ch       chan struct{}
	slots    int
	maxSlots int
	flexible bool
}

// Acquire blocks until a slot is available or ctx is cancelled. The returned
// release function MUST be called exactly once when the work is done. When
// the current cap is 0 the call is a fast-path no-op.
func (s *adaptiveSemaphore) Acquire(ctx context.Context) (release func(), err error) {
	return s.AcquireN(ctx, 1)
}

// AcquireN atomically reserves slots from the current dynamic capacity. A
// weighted request is either fully granted or remains queued, preventing the
// partial-token deadlocks that arise from looping over Acquire.
func (s *adaptiveSemaphore) AcquireN(ctx context.Context, slots int) (release func(), err error) {
	release, _, err = s.acquire(ctx, slots, false)
	return release, err
}

// AcquireUpTo atomically reserves as many slots as possible up to maxSlots.
// Unlike AcquireN, a queued request follows a shrinking dynamic capacity so it
// cannot become permanently heavier than the configured pool.
func (s *adaptiveSemaphore) AcquireUpTo(
	ctx context.Context,
	maxSlots int,
) (release func(), granted int, err error) {
	return s.acquire(ctx, maxSlots, true)
}

func (s *adaptiveSemaphore) acquire(
	ctx context.Context,
	slots int,
	flexible bool,
) (release func(), granted int, err error) {
	if slots <= 0 {
		return noopRelease, 0, errInvalidSemaphoreWeight
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	cap := s.capLocked()
	if cap == 0 {
		s.mu.Unlock()
		return noopRelease, slots, nil
	}
	requested := slots
	grant := slots
	if flexible {
		grant = min(requested, cap-s.inFlight)
	}
	if len(s.waiters) == 0 && grant > 0 && s.inFlight+grant <= cap {
		s.inFlight += grant
		s.mu.Unlock()
		return s.releaseOnce(grant), grant, nil
	}

	w := &waiter{
		ch: make(chan struct{}, 1), slots: slots, maxSlots: requested, flexible: flexible,
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		// Granted. inFlight was already incremented by the granter.
		return s.releaseOnce(w.slots), w.slots, nil
	case <-ctx.Done():
		// We may have been granted concurrently. Resolve the race under the
		// lock: if the channel has a pending wake, consume it and forward it
		// to the next waiter; otherwise remove ourselves from the queue.
		s.mu.Lock()
		select {
		case <-w.ch:
			// Already granted. Hand the slot to the next waiter.
			s.undoGrantedWaiterLocked(w)
			s.wakeWaitersLocked()
		default:
			s.removeWaiterLocked(w)
		}
		s.mu.Unlock()
		return noopRelease, 0, ctx.Err()
	}
}

func (s *adaptiveSemaphore) undoGrantedWaiterLocked(w *waiter) {
	if w == nil {
		return
	}
	s.inFlight -= w.slots
	if s.inFlight < 0 {
		s.inFlight = 0
	}
}

// wakeWaitersLocked wakes waiters in FIFO order while there is headroom under
// the current cap. Each wake-up increments inFlight, so callers that receive
// the signal must call their release exactly once.
func (s *adaptiveSemaphore) wakeWaitersLocked() {
	cap := s.capLocked()
	if cap == 0 {
		// Disabled — drain any waiters as free grants; their releases are
		// no-ops on the accounting side because inFlight is decremented on
		// release and re-clamped at 0.
		for _, w := range s.waiters {
			if w.flexible {
				w.slots = w.maxSlots
			}
			select {
			case w.ch <- struct{}{}:
				s.inFlight += w.slots
			default:
			}
		}
		s.waiters = nil
		return
	}

	for len(s.waiters) > 0 {
		w := s.waiters[0]
		if w.flexible {
			available := cap - s.inFlight
			if available <= 0 {
				break
			}
			w.slots = min(w.maxSlots, available)
		}
		if s.inFlight+w.slots > cap {
			break
		}
		s.waiters = s.waiters[1:]
		s.inFlight += w.slots
		// Buffered chan capacity 1 — never blocks.
		w.ch <- struct{}{}
	}
}

func (s *adaptiveSemaphore) removeWaiterLocked(target *waiter) {
	for i, w := range s.waiters {
		if w == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

func (s *adaptiveSemaphore) releaseOnce(slots int) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if s.inFlight > 0 {
				s.inFlight -= slots
				if s.inFlight < 0 {
					s.inFlight = 0
				}
			}
			s.wakeWaitersLocked()
			s.mu.Unlock()
		})
	}
}

func noopRelease() {}
