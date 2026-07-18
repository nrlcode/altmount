package pool

import (
	"context"
	"errors"
	"io"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
)

// Compile against the published FNCORE handover contract through FACORE's
// selected module. These declarations intentionally do not compile against the
// pre-FNCORE-CHG-005 dependency.
var (
	_ func(*nntppool.Client, map[string]nntppool.ProviderQuotaState) error = (*nntppool.Client).RestoreProviderQuotas
	_ NntpClient                                                           = (*facoreGeneration)(nil)
	_ generationClient                                                     = (*facoreGeneration)(nil)
)

type facoreGeneration struct {
	id string

	mu          sync.Mutex
	stats       nntppool.ClientStats
	restored    map[string]nntppool.ProviderQuotaState
	restoreFn   func(map[string]nntppool.ProviderQuotaState) error
	bodySource  <-chan nntppool.BodyResult
	statSource  <-chan nntppool.StatManyResult
	speedGate   <-chan struct{}
	speedDone   func()
	built       chan struct{}
	closeCalls  atomic.Int32
	statCalls   atomic.Int32
	restoreCall atomic.Int32
	speedEnter  chan struct{}
}

func newFACOREGeneration(id string, stats ...nntppool.ProviderStats) *facoreGeneration {
	return &facoreGeneration{
		id:         id,
		stats:      nntppool.ClientStats{Providers: stats},
		built:      make(chan struct{}),
		speedEnter: make(chan struct{}, 1),
	}
}

func (g *facoreGeneration) Body(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	return &nntppool.ArticleBody{ProviderID: g.id}, nil
}

func (g *facoreGeneration) BodyPriority(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	return &nntppool.ArticleBody{ProviderID: g.id}, nil
}

func (g *facoreGeneration) BodyAsync(context.Context, string, io.Writer, ...func(nntppool.YEncMeta)) <-chan nntppool.BodyResult {
	if g.bodySource != nil {
		return g.bodySource
	}
	ch := make(chan nntppool.BodyResult)
	close(ch)
	return ch
}

func (g *facoreGeneration) Stat(context.Context, string) (*nntppool.StatResult, error) {
	g.statCalls.Add(1)
	return &nntppool.StatResult{ProviderID: g.id}, nil
}

func (g *facoreGeneration) StatMany(context.Context, []string, nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	if g.statSource != nil {
		return g.statSource
	}
	ch := make(chan nntppool.StatManyResult)
	close(ch)
	return ch
}

func (g *facoreGeneration) Stats() nntppool.ClientStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	stats := g.stats
	stats.Providers = append([]nntppool.ProviderStats(nil), stats.Providers...)
	return stats
}

func (g *facoreGeneration) SpeedTest(ctx context.Context, _ nntppool.SpeedTestOptions) (*nntppool.SpeedTestResult, error) {
	select {
	case g.speedEnter <- struct{}{}:
	default:
	}
	if g.speedGate != nil {
		select {
		case <-g.speedGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if g.speedDone != nil {
		g.speedDone()
	}
	return &nntppool.SpeedTestResult{}, nil
}

func (g *facoreGeneration) RestoreProviderQuotas(states map[string]nntppool.ProviderQuotaState) error {
	g.restoreCall.Add(1)
	copyOfStates := maps.Clone(states)
	g.mu.Lock()
	g.restored = copyOfStates
	g.mu.Unlock()
	if g.restoreFn != nil {
		return g.restoreFn(copyOfStates)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range g.stats.Providers {
		if state, ok := states[g.stats.Providers[i].ProviderID]; ok {
			g.stats.Providers[i].QuotaUsed = state.Used
			g.stats.Providers[i].QuotaResetAt = state.ResetAt
		}
	}
	return nil
}

func (g *facoreGeneration) Close() error {
	g.closeCalls.Add(1)
	return nil
}

func (g *facoreGeneration) NumProviders() int                   { return len(g.Stats().Providers) }
func (g *facoreGeneration) AddProvider(nntppool.Provider) error { return nil }
func (g *facoreGeneration) RemoveProvider(string) error         { return nil }
func (g *facoreGeneration) ResetProviderQuota(string) error     { return nil }

func facoreManager(t *testing.T, repo StatsRepository, generations ...*facoreGeneration) *manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	m := NewManager(ctx, repo).(*manager)
	var mu sync.Mutex
	next := 0
	m.newClient = func(context.Context, []nntppool.Provider) (generationClient, error) {
		mu.Lock()
		defer mu.Unlock()
		if next == len(generations) {
			return nil, errors.New("unexpected generation build")
		}
		generation := generations[next]
		next++
		close(generation.built)
		return generation, nil
	}
	m.handoverTimeout = 250 * time.Millisecond
	t.Cleanup(func() {
		cancel()
		for _, generation := range generations {
			_ = generation.Close()
		}
	})
	return m
}

func facoreProviders(ids ...string) []nntppool.Provider {
	providers := make([]nntppool.Provider, len(ids))
	for i, id := range ids {
		providers[i] = nntppool.Provider{ID: id, Host: id + ".invalid", Connections: 1}
	}
	return providers
}

func facoreAwait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func facorePending[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s completed early", what)
	case <-time.After(10 * time.Millisecond):
	}
}

func facoreAwaitPaused(t *testing.T, client NntpClient) {
	t.Helper()
	state, ok := client.(interface{ isPaused() bool })
	require.True(t, ok, "stable facade must expose package-private pause state to deterministic tests")
	require.Eventually(t, state.isPaused, time.Second, time.Millisecond, "facade did not pause")
}

func facoreDrain[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out draining %s", what)
			return
		}
	}
}

func TestFACORECHG003StableFacadeAndSynchronousLease(t *testing.T) {
	gate := make(chan struct{})
	old := newFACOREGeneration("old", nntppool.ProviderStats{ProviderID: "shared"})
	old.speedGate = gate
	candidate := newFACOREGeneration("candidate", nntppool.ProviderStats{ProviderID: "shared"})
	m := facoreManager(t, nil, old, candidate)
	require.NoError(t, m.SetProviders(facoreProviders("shared")))
	facade, err := m.GetPool()
	require.NoError(t, err)

	speedDone := make(chan error, 1)
	go func() {
		_, speedErr := facade.SpeedTest(context.Background(), nntppool.SpeedTestOptions{})
		speedDone <- speedErr
	}()
	facoreAwait(t, old.speedEnter, "old SpeedTest admission")

	swapDone := make(chan error, 1)
	go func() { swapDone <- m.SetProviders(facoreProviders("shared")) }()
	facoreAwait(t, candidate.built, "candidate construction")
	facoreAwaitPaused(t, facade)
	duringDone := make(chan error, 1)
	go func() {
		_, statErr := facade.Stat(context.Background(), "during-handover")
		duringDone <- statErr
	}()
	facorePending(t, duringDone, "call admitted during handover")
	facorePending(t, swapDone, "replacement with an active synchronous lease")
	require.Zero(t, old.closeCalls.Load(), "old generation closed while leased")
	require.Zero(t, old.statCalls.Load(), "handover call reached old generation after admissions paused")
	require.Zero(t, candidate.statCalls.Load(), "handover call reached unpublished candidate")

	close(gate)
	require.NoError(t, facoreAwait(t, speedDone, "SpeedTest completion"))
	require.NoError(t, facoreAwait(t, swapDone, "replacement"))
	require.NoError(t, facoreAwait(t, duringDone, "handover call"))
	require.Equal(t, int32(1), old.closeCalls.Load())

	after, err := m.GetPool()
	require.NoError(t, err)
	require.Same(t, facade, after, "GetPool must retain one manager-owned facade")
	stat, err := facade.Stat(context.Background(), "after-handover")
	require.NoError(t, err)
	require.Equal(t, "candidate", stat.ProviderID)
}

func TestFACORECHG003AsyncLeaseLifetimes(t *testing.T) {
	t.Run("BodyAsync source close releases lease", func(t *testing.T) {
		source := make(chan nntppool.BodyResult, 1)
		old := newFACOREGeneration("old", nntppool.ProviderStats{ProviderID: "shared"})
		old.bodySource = source
		candidate := newFACOREGeneration("candidate", nntppool.ProviderStats{ProviderID: "shared"})
		m := facoreManager(t, nil, old, candidate)
		require.NoError(t, m.SetProviders(facoreProviders("shared")))
		facade, _ := m.GetPool()
		out := facade.BodyAsync(context.Background(), "body", io.Discard)
		swapDone := make(chan error, 1)
		go func() { swapDone <- m.SetProviders(facoreProviders("shared")) }()
		facoreAwait(t, candidate.built, "candidate construction")
		facoreAwaitPaused(t, facade)
		facorePending(t, swapDone, "replacement before BodyAsync source close")
		require.Zero(t, old.closeCalls.Load())
		source <- nntppool.BodyResult{Body: &nntppool.ArticleBody{ProviderID: "old"}}
		close(source)
		require.NoError(t, facoreAwait(t, swapDone, "replacement"))
		facoreDrain(t, out, "BodyAsync output")
	})

	t.Run("StatMany cancellation drains before release", func(t *testing.T) {
		source := make(chan nntppool.StatManyResult)
		old := newFACOREGeneration("old", nntppool.ProviderStats{ProviderID: "shared"})
		old.statSource = source
		candidate := newFACOREGeneration("candidate", nntppool.ProviderStats{ProviderID: "shared"})
		m := facoreManager(t, nil, old, candidate)
		require.NoError(t, m.SetProviders(facoreProviders("shared")))
		facade, _ := m.GetPool()
		ctx, cancel := context.WithCancel(context.Background())
		out := facade.StatMany(ctx, []string{"one", "two"}, nntppool.StatManyOptions{Concurrency: 1})
		cancel()

		producerPaused := make(chan struct{})
		finishSource := make(chan struct{})
		go func() {
			source <- nntppool.StatManyResult{MessageID: "one"}
			close(producerPaused)
			<-finishSource
			close(source)
		}()
		facoreAwait(t, producerPaused, "cancelled StatMany source drain")
		swapDone := make(chan error, 1)
		go func() { swapDone <- m.SetProviders(facoreProviders("shared")) }()
		facoreAwait(t, candidate.built, "candidate construction")
		facoreAwaitPaused(t, facade)
		facorePending(t, swapDone, "replacement before drained source close")
		require.Zero(t, old.closeCalls.Load())
		close(finishSource)
		require.NoError(t, facoreAwait(t, swapDone, "replacement"))
		facoreDrain(t, out, "StatMany output")
	})
}

func TestFACORECHG003SettledQuotaTransferUsesCanonicalIDs(t *testing.T) {
	firstReset := time.Unix(1_900_000_000, 111)
	settledReset := time.Unix(1_900_000_100, 222)
	gate := make(chan struct{})
	old := newFACOREGeneration("old",
		nntppool.ProviderStats{Name: "same.invalid+alice", ProviderID: "alice-id", QuotaBytes: 1000, QuotaUsed: 10, QuotaResetAt: firstReset},
		nntppool.ProviderStats{Name: "same.invalid+bob", ProviderID: "bob-id", QuotaBytes: 1000, QuotaUsed: 90, QuotaResetAt: firstReset},
		nntppool.ProviderStats{Name: "removed.invalid", ProviderID: "removed-id", QuotaBytes: 1000, QuotaUsed: 70, QuotaResetAt: firstReset},
	)
	old.speedGate = gate
	old.speedDone = func() {
		old.mu.Lock()
		old.stats.Providers[0].QuotaUsed = 55 // completion charge settles at lease return
		old.stats.Providers[1].QuotaUsed = 0  // reset settles at lease return
		old.stats.Providers[1].QuotaResetAt = settledReset
		old.mu.Unlock()
	}
	candidate := newFACOREGeneration("candidate",
		nntppool.ProviderStats{Name: "renamed.invalid+alice", ProviderID: "alice-id", QuotaBytes: 1000},
		nntppool.ProviderStats{Name: "renamed.invalid+bob", ProviderID: "bob-id", QuotaBytes: 1000},
		nntppool.ProviderStats{Name: "new.invalid", ProviderID: "new-id", QuotaBytes: 1000},
	)
	m := facoreManager(t, nil, old, candidate)
	require.NoError(t, m.SetProviders(facoreProviders("alice-id", "bob-id", "removed-id")))
	facade, _ := m.GetPool()
	leaseDone := make(chan error, 1)
	go func() {
		_, err := facade.SpeedTest(context.Background(), nntppool.SpeedTestOptions{})
		leaseDone <- err
	}()
	facoreAwait(t, old.speedEnter, "quota-mutating lease")
	swapDone := make(chan error, 1)
	go func() { swapDone <- m.SetProviders(facoreProviders("alice-id", "bob-id", "new-id")) }()
	facoreAwait(t, candidate.built, "candidate construction")
	facoreAwaitPaused(t, facade)
	require.Zero(t, candidate.restoreCall.Load(), "quota snapshot taken before old calls settled")
	close(gate)
	require.NoError(t, facoreAwait(t, leaseDone, "quota-mutating lease"))
	require.NoError(t, facoreAwait(t, swapDone, "quota handover"))

	candidate.mu.Lock()
	restored := maps.Clone(candidate.restored)
	candidate.mu.Unlock()
	require.Equal(t, map[string]nntppool.ProviderQuotaState{
		"alice-id": {Used: 55, ResetAt: firstReset},
		"bob-id":   {Used: 0, ResetAt: settledReset},
	}, restored)
}

func TestFACORECHG003RestoreFailureAndTimeoutRollback(t *testing.T) {
	for _, tc := range []struct {
		name         string
		timeout      time.Duration
		failure      error
		holdOldLease bool
	}{
		{name: "failure", failure: errors.New("restore rejected")},
		{name: "timeout", timeout: 15 * time.Millisecond, holdOldLease: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset := time.Unix(1_900_000_000, 1)
			old := newFACOREGeneration("old", nntppool.ProviderStats{ProviderID: "shared", QuotaBytes: 100, QuotaUsed: 25, QuotaResetAt: reset})
			failed := newFACOREGeneration("failed", nntppool.ProviderStats{ProviderID: "shared", QuotaBytes: 100})
			if !tc.holdOldLease {
				failed.restoreFn = func(map[string]nntppool.ProviderQuotaState) error { return tc.failure }
			}
			fresh := newFACOREGeneration("fresh", nntppool.ProviderStats{ProviderID: "shared", QuotaBytes: 100})
			m := facoreManager(t, nil, old, failed, fresh)
			if tc.timeout > 0 {
				m.handoverTimeout = tc.timeout
			}
			require.NoError(t, m.SetProviders(facoreProviders("shared")))
			facade, _ := m.GetPool()
			var leaseGate chan struct{}
			var leaseDone chan error
			if tc.holdOldLease {
				leaseGate = make(chan struct{})
				leaseDone = make(chan error, 1)
				old.speedGate = leaseGate
				go func() {
					_, speedErr := facade.SpeedTest(context.Background(), nntppool.SpeedTestOptions{})
					leaseDone <- speedErr
				}()
				facoreAwait(t, old.speedEnter, "old lease")
			}
			errCh := make(chan error, 1)
			go func() { errCh <- m.SetProviders(facoreProviders("shared")) }()
			facoreAwait(t, failed.built, "failed candidate construction")
			handoverErr := facoreAwait(t, errCh, "failed handover")
			require.Error(t, handoverErr)
			if tc.failure != nil {
				require.ErrorIs(t, handoverErr, tc.failure)
			}
			require.Equal(t, int32(1), failed.closeCalls.Load())
			require.Zero(t, old.closeCalls.Load())
			stat, err := facade.Stat(context.Background(), "rollback")
			require.NoError(t, err)
			require.Equal(t, "old", stat.ProviderID, "new traffic did not resume on old generation")
			if tc.holdOldLease {
				require.Zero(t, failed.restoreCall.Load(), "timed-out handover reached quota restore")
				close(leaseGate)
				require.NoError(t, facoreAwait(t, leaseDone, "old lease release"))
			}

			require.NoError(t, m.SetProviders(facoreProviders("shared")))
			stat, err = facade.Stat(context.Background(), "fresh-attempt")
			require.NoError(t, err)
			require.Equal(t, "fresh", stat.ProviderID)
			fresh.mu.Lock()
			restored := maps.Clone(fresh.restored)
			fresh.mu.Unlock()
			require.Equal(t, map[string]nntppool.ProviderQuotaState{
				"shared": {Used: 25, ResetAt: reset},
			}, restored, "fresh retry did not restore canonical settled quota")
			wantRestoreCalls := int32(1)
			if tc.holdOldLease {
				wantRestoreCalls = 0
			}
			require.Equal(t, wantRestoreCalls, failed.restoreCall.Load(), "failed candidate was replayed")
			require.Equal(t, int32(1), old.closeCalls.Load(), "successful retry did not retire old exactly once")
			require.Equal(t, int32(1), failed.closeCalls.Load(), "failed candidate was closed more than once")
		})
	}
}
