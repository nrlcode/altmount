package pool

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
)

func TestFACORECHG003MetricsUseProviderIDForSameHostAccounts(t *testing.T) {
	now := time.Unix(1_900_001_000, 0)
	started := now.Add(-time.Hour)
	mt := &MetricsTracker{
		startedAt:                started,
		calculationWindow:        2 * time.Minute,
		initialProviderErrors:    map[string]int64{"alice-id": 5, "bob-id": 7},
		initialProviderBytes:     map[string]int64{"alice-id": 100, "bob-id": 200},
		initialProviderStartedAt: map[string]time.Time{"alice-id": started, "bob-id": started},
		samples: []metricsample{{
			timestamp:       now.Add(-time.Minute),
			providerMissing: map[string]int64{"alice-id": 1, "bob-id": 0},
		}},
	}
	stats := nntppool.ClientStats{Providers: []nntppool.ProviderStats{
		{Name: "same.invalid+alice", ProviderID: "alice-id", Errors: 2, BytesConsumed: 20, Missing: 4, AvgSpeed: 2 << 20, QuotaBytes: 100, QuotaUsed: 11},
		{Name: "same.invalid+bob", ProviderID: "bob-id", Errors: 3, BytesConsumed: 30, Missing: 20, AvgSpeed: 3 << 20, QuotaBytes: 200, QuotaUsed: 22},
	}}
	snapshot := mt.getSnapshot(now, stats)

	require.Equal(t, map[string]int64{"alice-id": 7, "bob-id": 10}, snapshot.ProviderErrors)
	require.Equal(t, int64(17), snapshot.TotalErrors)
	require.Equal(t, map[string]int64{"alice-id": 120, "bob-id": 230}, snapshot.ProviderBytes)
	require.Equal(t, map[string]float64{"alice-id": 3, "bob-id": 20}, snapshot.ProviderMissingRates)
	require.Equal(t, map[string]bool{"alice-id": false, "bob-id": true}, snapshot.ProviderMissingWarning)
	require.Equal(t, map[string]float64{"alice-id": 2 << 20, "bob-id": 3 << 20}, snapshot.ProviderSpeeds)
	require.Equal(t, int64(11), snapshot.ProviderQuotas["alice-id"].QuotaUsed)
	require.Equal(t, int64(22), snapshot.ProviderQuotas["bob-id"].QuotaUsed)
	for _, byProvider := range []any{
		snapshot.ProviderErrors, snapshot.ProviderBytes, snapshot.ProviderStartedAt,
		snapshot.ProviderMissingRates, snapshot.ProviderMissingWarning,
		snapshot.ProviderSpeeds, snapshot.ProviderQuotas,
	} {
		require.NotContains(t, byProvider, "same.invalid+alice")
		require.NotContains(t, byProvider, "same.invalid+bob")
	}
}

type facorePersistBlock struct {
	entered chan map[string]int64
	release chan struct{}
}

type facoreStatsRepo struct {
	blocks chan facorePersistBlock
}

func newFACOREStatsRepo() *facoreStatsRepo {
	return &facoreStatsRepo{blocks: make(chan facorePersistBlock, 2)}
}

func (r *facoreStatsRepo) blockNext() facorePersistBlock {
	block := facorePersistBlock{entered: make(chan map[string]int64, 1), release: make(chan struct{})}
	r.blocks <- block
	return block
}

func (r *facoreStatsRepo) UpdateSystemStat(context.Context, string, int64) error { return nil }
func (r *facoreStatsRepo) GetSystemStats(context.Context) (map[string]int64, error) {
	return nil, errors.New("no seed stats")
}
func (r *facoreStatsRepo) AddBytesDownloadedToDailyStat(context.Context, int64) error { return nil }
func (r *facoreStatsRepo) AddProviderBytesToHourlyStat(context.Context, string, int64) error {
	return nil
}
func (r *facoreStatsRepo) RecordProviderSpeedTest(context.Context, string, float64) error {
	return nil
}
func (r *facoreStatsRepo) GetProviderHourlyStats(context.Context, int) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (r *facoreStatsRepo) ClearProviderHourlyStats(context.Context) error { return nil }
func (r *facoreStatsRepo) GetOldestStatDate(context.Context) (time.Time, error) {
	return time.Time{}, nil
}
func (r *facoreStatsRepo) GetOldestProviderStatDates(context.Context) (map[string]time.Time, error) {
	return map[string]time.Time{}, nil
}
func (r *facoreStatsRepo) BatchUpdateSystemStats(_ context.Context, stats map[string]int64) error {
	copyOfStats := maps.Clone(stats)
	select {
	case block := <-r.blocks:
		block.entered <- copyOfStats
		<-block.release
	default:
	}
	return nil
}

func TestFACORECHG003FoldOnceCurrentGaugesAndClearOrdering(t *testing.T) {
	reset := time.Unix(1_900_002_000, 321)
	old := newFACOREGeneration("old",
		nntppool.ProviderStats{Name: "old.invalid+acct", ProviderID: "acct-id", Errors: 2, BytesConsumed: 100, AvgSpeed: 1 << 20, QuotaBytes: 100, QuotaUsed: 10, QuotaResetAt: reset},
		nntppool.ProviderStats{Name: "old.invalid+removed", ProviderID: "removed-id", Errors: 1, BytesConsumed: 40, AvgSpeed: 2 << 20, QuotaBytes: 100, QuotaUsed: 9, QuotaResetAt: reset},
	)
	candidate := newFACOREGeneration("candidate",
		nntppool.ProviderStats{Name: "new.invalid+acct", ProviderID: "acct-id", Errors: 3, BytesConsumed: 30, AvgSpeed: 5 << 20, QuotaBytes: 100},
		nntppool.ProviderStats{Name: "new.invalid+new", ProviderID: "new-id", Errors: 4, BytesConsumed: 7, AvgSpeed: 7 << 20, QuotaBytes: 100, QuotaUsed: 2, QuotaResetAt: reset},
	)
	repo := newFACOREStatsRepo()
	m := facoreManager(t, repo, old, candidate)
	require.NoError(t, m.SetProviders(facoreProviders("acct-id", "removed-id")))
	tracker := m.metricsTracker
	require.NotNil(t, tracker)
	facade, _ := m.GetPool()

	replacementPersist := repo.blockNext()
	swapDone := make(chan error, 1)
	go func() { swapDone <- m.SetProviders(facoreProviders("acct-id", "new-id")) }()
	retiredStats := facoreAwait(t, replacementPersist.entered, "retired-generation persistence")
	require.Zero(t, old.closeCalls.Load(), "old generation closed before final persistence")
	require.Equal(t, int64(100), retiredStats["provider_bytes:acct-id"])
	require.Equal(t, int64(40), retiredStats["provider_bytes:removed-id"])
	require.Equal(t, int64(2), retiredStats["provider_error:acct-id"])
	require.Equal(t, int64(1), retiredStats["provider_error:removed-id"])
	require.NotContains(t, retiredStats, "provider_bytes:old.invalid+acct")
	require.NotContains(t, retiredStats, "provider_error:old.invalid+acct")
	close(replacementPersist.release)
	require.NoError(t, facoreAwait(t, swapDone, "replacement"))
	require.Same(t, tracker, m.metricsTracker, "replacement reloaded the long-lived metrics tracker")

	for range 2 { // repeated reads must not fold the retired generation again
		snapshot, err := m.GetMetrics()
		require.NoError(t, err)
		require.Equal(t, map[string]int64{"acct-id": 130, "removed-id": 40, "new-id": 7}, snapshot.ProviderBytes)
		require.Equal(t, map[string]int64{"acct-id": 5, "removed-id": 1, "new-id": 4}, snapshot.ProviderErrors)
		require.Equal(t, int64(10), snapshot.TotalErrors)
		require.Equal(t, map[string]float64{"acct-id": 5 << 20, "new-id": 7 << 20}, snapshot.ProviderSpeeds)
		require.Equal(t, map[string]ProviderQuotaSnapshot{
			"acct-id": {QuotaBytes: 100, QuotaUsed: 10, QuotaResetAt: reset},
			"new-id":  {QuotaBytes: 100, QuotaUsed: 2, QuotaResetAt: reset},
		}, snapshot.ProviderQuotas)
	}

	gate := make(chan struct{})
	candidate.speedGate = gate
	leaseDone := make(chan error, 1)
	go func() {
		_, err := facade.SpeedTest(context.Background(), nntppool.SpeedTestOptions{})
		leaseDone <- err
	}()
	facoreAwait(t, candidate.speedEnter, "candidate lease")
	clearPersist := repo.blockNext()
	clearDone := make(chan error, 1)
	go func() { clearDone <- m.ClearPool() }()
	facoreAwaitPaused(t, facade)
	facorePending(t, clearDone, "clear with active lease")
	require.Zero(t, candidate.closeCalls.Load())
	close(gate)
	require.NoError(t, facoreAwait(t, leaseDone, "candidate lease"))
	finalStats := facoreAwait(t, clearPersist.entered, "final-generation persistence")
	require.Zero(t, candidate.closeCalls.Load(), "candidate closed before final persistence")
	require.Equal(t, int64(130), finalStats["provider_bytes:acct-id"])
	require.Equal(t, int64(40), finalStats["provider_bytes:removed-id"])
	require.Equal(t, int64(7), finalStats["provider_bytes:new-id"])
	require.Equal(t, int64(5), finalStats["provider_error:acct-id"])
	require.Equal(t, int64(1), finalStats["provider_error:removed-id"])
	require.Equal(t, int64(4), finalStats["provider_error:new-id"])
	require.Equal(t, int64(10), finalStats["quota_used:acct-id"])
	require.Equal(t, int64(2), finalStats["quota_used:new-id"])
	require.Equal(t, reset.UnixNano(), finalStats["quota_reset_at:acct-id"])
	require.Equal(t, reset.UnixNano(), finalStats["quota_reset_at:new-id"])
	require.NotContains(t, finalStats, "quota_used:removed-id")
	require.NotContains(t, finalStats, "quota_reset_at:removed-id")
	for _, alias := range []string{"old.invalid+acct", "old.invalid+removed", "new.invalid+acct", "new.invalid+new"} {
		for _, prefix := range []string{"provider_bytes:", "provider_error:", "quota_used:", "quota_reset_at:"} {
			require.NotContains(t, finalStats, prefix+alias)
		}
	}
	close(clearPersist.release)
	require.NoError(t, facoreAwait(t, clearDone, "clear"))
	require.Equal(t, int32(1), candidate.closeCalls.Load())
}
