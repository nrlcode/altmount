package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/nntppool/v4"
)

// Manager provides centralized NNTP connection pool management.
type Manager interface {
	// GetPool returns the current connection pool or error if not available.
	// The returned client exposes the narrow NntpClient surface so tests can
	// substitute a fake (see internal/testsupport/fakepool). In production it
	// is backed by *nntppool.Client.
	GetPool() (NntpClient, error)

	// SetProviders creates/recreates the pool with new providers
	SetProviders(providers []nntppool.Provider) error

	// ClearPool shuts down and removes the current pool
	ClearPool() error

	// HasPool returns true if a pool is currently available
	HasPool() bool

	// GetMetrics returns the current pool metrics with calculated speeds
	GetMetrics() (MetricsSnapshot, error)

	// ResetMetrics resets specific cumulative metrics
	ResetMetrics(ctx context.Context, resetPeak bool, resetTotals bool) error

	// ResetProviderErrors zeroes all per-provider error counts without
	// affecting bytes downloaded, peak speed, or history.
	ResetProviderErrors(ctx context.Context) error

	// IncArticlesDownloaded increments the count of articles successfully downloaded
	IncArticlesDownloaded()

	// UpdateDownloadProgress updates the bytes downloaded for a specific stream
	UpdateDownloadProgress(id string, bytesDownloaded int64)

	// IncArticlesPosted increments the count of articles successfully posted
	IncArticlesPosted()

	// ResetProviderQuota resets the download quota counter for a provider,
	// clearing its consumed-bytes counter and exceeded flag in-place.
	ResetProviderQuota(ctx context.Context, providerID string) error

	// AcquireImportSlot blocks until an admission slot is available for an
	// NZB import to start, or ctx is cancelled. The returned release function
	// must be called exactly once when the import has finished (success or
	// failure). When the admission cap is unconfigured (0) it is a no-op.
	AcquireImportSlot(ctx context.Context) (release func(), err error)

	// SetAdmissionCap configures the cap on concurrently running NZB imports.
	// A cap of 0 means unlimited.
	SetAdmissionCap(cap int)

	// AcquireImportConnection blocks until the global import connection
	// budget grants a token for one segment (body) fetch, or ctx is
	// cancelled. The returned release function must be called exactly once
	// when the fetch is done. No-op while the budget capacity is unset (0).
	AcquireImportConnection(ctx context.Context) (release func(), err error)

	// SetImportConnCapacity sets the import connection budget to the pool's
	// total connection count (sum of provider max connections).
	SetImportConnCapacity(total int)

	// ImportConnCapacity returns the current budget capacity snapshot,
	// useful for sizing import worker pools.
	ImportConnCapacity() int

	// SetStreamSource wires the activity signal so the import connection
	// budget can shrink while streams are active.
	SetStreamSource(src StreamActivitySource)

	// NotifyStreamChange must be called by the stream source whenever its
	// active stream count changes, so the budget can re-evaluate.
	NotifyStreamChange()
}

// StatsRepository defines the interface for persisting pool statistics
type StatsRepository interface {
	UpdateSystemStat(ctx context.Context, key string, value int64) error
	BatchUpdateSystemStats(ctx context.Context, stats map[string]int64) error
	GetSystemStats(ctx context.Context) (map[string]int64, error)
	AddBytesDownloadedToDailyStat(ctx context.Context, bytes int64) error
	AddProviderBytesToHourlyStat(ctx context.Context, providerID string, bytes int64) error
	RecordProviderSpeedTest(ctx context.Context, providerID string, speedMbps float64) error
	GetProviderHourlyStats(ctx context.Context, hours int) (map[string]int64, error)
	ClearProviderHourlyStats(ctx context.Context) error
	GetOldestStatDate(ctx context.Context) (time.Time, error)
	GetOldestProviderStatDates(ctx context.Context) (map[string]time.Time, error)
}

// manager implements the Manager interface
type manager struct {
	mu               sync.RWMutex
	transitionMu     sync.Mutex
	facade           *leasedClient
	metricsTracker   *MetricsTracker
	repo             StatsRepository
	ctx              context.Context
	logger           *slog.Logger
	quotaWatchCancel context.CancelFunc
	admission        *ImportAdmission
	budget           *ImportBudget
	newClient        func(context.Context, []nntppool.Provider) (generationClient, error)
	handoverTimeout  time.Duration
}

const defaultHandoverTimeout = 30 * time.Second

// NewManager creates a new pool manager
func NewManager(ctx context.Context, repo StatsRepository) Manager {
	return &manager{
		ctx:             ctx,
		repo:            repo,
		logger:          slog.Default().With("component", "pool"),
		facade:          &leasedClient{},
		admission:       NewImportAdmission(),
		budget:          NewImportBudget(),
		newClient:       newGenerationClient,
		handoverTimeout: defaultHandoverTimeout,
	}
}

func newGenerationClient(ctx context.Context, providers []nntppool.Provider) (generationClient, error) {
	return nntppool.NewClient(
		ctx,
		providers,
		nntppool.WithDispatchStrategy(nntppool.DispatchFIFO),
		nntppool.WithStatProbe(false),
		nntppool.WithProviderCircuitBreaker(true),
	)
}

// injectQuotaState loads persisted quota counters from the database and sets
// QuotaUsed / QuotaResetAt on each provider so nntppool can resume quota
// tracking across restarts.
func (m *manager) injectQuotaState(ctx context.Context, providers []nntppool.Provider) {
	if m.repo == nil {
		return
	}

	stats, err := m.repo.GetSystemStats(ctx)
	if err != nil {
		m.logger.ErrorContext(ctx, "Failed to load quota state from database", "error", err)
		return
	}

	now := time.Now()
	for i := range providers {
		providerID := providers[i].ID
		if providerID == "" {
			continue
		}

		hasResetTime := false
		var resetTime time.Time
		if resetNano := stats["quota_reset_at:"+providerID]; resetNano > 0 {
			resetTime = time.Unix(0, resetNano)
			hasResetTime = true
		}

		if hasResetTime {
			if resetTime.After(now) {
				// The quota period is still active: restore the used bytes and the reset time
				if used := stats["quota_used:"+providerID]; used > 0 {
					providers[i].QuotaUsed = used
				}
				providers[i].QuotaResetAt = resetTime
			} else {
				// The quota period has already expired: discard the old usage (it resets to 0)
				providers[i].QuotaUsed = 0
				providers[i].QuotaResetAt = time.Time{}
			}
		} else {
			// No persisted reset time: restore whatever usage we have (fallback)
			if used := stats["quota_used:"+providerID]; used > 0 {
				providers[i].QuotaUsed = used
			}
		}
	}
}

// GetPool returns the current connection pool or error if not available.
// Every successful call returns the same manager-owned facade.
func (m *manager) GetPool() (NntpClient, error) {
	if !m.facade.hasGeneration() {
		return nil, fmt.Errorf("NNTP connection pool not available - no providers configured")
	}
	return m.facade, nil
}

// SetProviders retains the compatibility API by preparing and immediately
// committing the same transaction used by configuration persistence.
func (m *manager) SetProviders(providers []nntppool.Provider) error {
	prepared, err := m.PrepareProviders(m.ctx, providers)
	if err != nil {
		return err
	}
	if prepared.Commit != nil {
		prepared.Commit()
	}
	return nil
}

// PrepareProviders builds an unpublished generation and stages an atomic
// handover. The returned Commit or Abort must be invoked exactly once by the
// caller; both are idempotent to protect compatibility callers.
func (m *manager) PrepareProviders(ctx context.Context, providers []nntppool.Provider) (config.PreparedChange, error) {
	m.transitionMu.Lock()
	locked := true
	defer func() {
		if locked {
			m.transitionMu.Unlock()
		}
	}()

	var candidate generationClient
	if len(providers) > 0 {
		candidateProviders := append([]nntppool.Provider(nil), providers...)
		m.injectQuotaState(ctx, candidateProviders)
		m.logger.InfoContext(ctx, "Creating NNTP connection pool", "provider_count", len(candidateProviders))
		var err error
		candidate, err = m.newClient(m.ctx, candidateProviders)
		if err != nil {
			return config.PreparedChange{}, fmt.Errorf("failed to create NNTP connection pool: %w", err)
		}
	}

	old := m.facade.currentGeneration()
	var oldStats nntppool.ClientStats
	if old != nil {
		var drained <-chan struct{}
		old, drained = m.facade.pause()
		timeout := m.handoverTimeout
		if timeout <= 0 {
			timeout = defaultHandoverTimeout
		}
		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		select {
		case <-drained:
			cancel()
		case <-waitCtx.Done():
			waitErr := waitCtx.Err()
			cancel()
			m.facade.publish(old)
			if candidate != nil {
				_ = candidate.Close()
			}
			return config.PreparedChange{}, fmt.Errorf("waiting for NNTP handover leases: %w", waitErr)
		}

		oldStats = old.Stats()
		if candidate != nil {
			states := retainedQuotaStates(oldStats, candidate.Stats())
			if err := candidate.RestoreProviderQuotas(states); err != nil {
				m.facade.publish(old)
				_ = candidate.Close()
				return config.PreparedChange{}, fmt.Errorf("restore provider quota handover: %w", err)
			}
		}
	}

	var once sync.Once
	finish := func(commit bool) {
		once.Do(func() {
			defer m.transitionMu.Unlock()
			if !commit {
				if old != nil {
					m.facade.publish(old)
				}
				if candidate != nil {
					_ = candidate.Close()
				}
				return
			}
			m.commitGeneration(old, candidate, oldStats)
		})
	}
	locked = false
	return config.PreparedChange{
		Commit: func() { finish(true) },
		Abort:  func() { finish(false) },
	}, nil
}

func retainedQuotaStates(oldStats, candidateStats nntppool.ClientStats) map[string]nntppool.ProviderQuotaState {
	retained := make(map[string]struct{}, len(candidateStats.Providers))
	for _, provider := range candidateStats.Providers {
		providerID := providerStatsID(provider)
		if providerID != "" && provider.QuotaBytes > 0 {
			retained[providerID] = struct{}{}
		}
	}
	states := make(map[string]nntppool.ProviderQuotaState)
	for _, provider := range oldStats.Providers {
		providerID := providerStatsID(provider)
		if providerID == "" || provider.QuotaBytes == 0 {
			continue
		}
		if _, ok := retained[providerID]; !ok {
			continue
		}
		states[providerID] = nntppool.ProviderQuotaState{Used: provider.QuotaUsed, ResetAt: provider.QuotaResetAt}
	}
	return states
}

func (m *manager) commitGeneration(old, candidate generationClient, oldStats nntppool.ClientStats) {
	m.mu.RLock()
	tracker := m.metricsTracker
	m.mu.RUnlock()

	if old != nil && tracker != nil {
		tracker.FoldGeneration(context.Background(), oldStats)
	}
	if candidate == nil {
		m.mu.Lock()
		m.stopQuotaWatcher()
		m.mu.Unlock()
	}
	if tracker == nil && candidate != nil {
		tracker = NewMetricsTracker(candidate, m.repo)
		m.mu.Lock()
		m.metricsTracker = tracker
		m.mu.Unlock()
		tracker.Start(m.ctx)
	} else if tracker != nil {
		tracker.SetClient(candidate)
	}

	m.facade.publish(candidate)
	if old != nil {
		m.logger.InfoContext(m.ctx, "Shutting down existing NNTP connection pool")
		if err := old.Close(); err != nil {
			m.logger.ErrorContext(m.ctx, "Failed to close retired NNTP connection pool", "error", err)
		}
	}
	if candidate != nil {
		m.mu.Lock()
		m.startQuotaWatcher()
		m.mu.Unlock()
		m.logger.InfoContext(m.ctx, "NNTP connection pool created successfully")
	} else {
		m.logger.InfoContext(m.ctx, "No NNTP providers configured - pool cleared")
	}
}

// ClearPool shuts down and removes the current pool
func (m *manager) ClearPool() error {
	prepared, err := m.PrepareProviders(context.Background(), nil)
	if err != nil {
		return err
	}
	if prepared.Commit != nil {
		prepared.Commit()
	}
	return nil
}

// HasPool returns true if a pool is currently available
func (m *manager) HasPool() bool {
	return m.facade.hasGeneration()
}

// GetMetrics returns the current pool metrics with calculated speeds
func (m *manager) GetMetrics() (MetricsSnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.facade.hasGeneration() {
		return MetricsSnapshot{}, fmt.Errorf("NNTP connection pool not available")
	}

	if m.metricsTracker == nil {
		return MetricsSnapshot{}, fmt.Errorf("metrics tracker not available")
	}

	return m.metricsTracker.GetSnapshot(), nil
}

// ResetMetrics resets specific cumulative metrics
func (m *manager) ResetMetrics(ctx context.Context, resetPeak bool, resetTotals bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.metricsTracker != nil {
		return m.metricsTracker.Reset(ctx, resetPeak, resetTotals)
	}

	// If tracker not available, still try to reset DB directly
	if m.repo != nil {
		currentStats, err := m.repo.GetSystemStats(ctx)
		if err == nil {
			resetMap := make(map[string]int64)
			for k := range currentStats {
				if resetTotals {
					if _, providerScoped := persistedProviderKeyID(k); providerScoped {
						continue
					}
					resetMap[k] = 0
				}
			}

			if resetTotals {
				resetMap["bytes_downloaded"] = 0
				resetMap["articles_downloaded"] = 0
				resetMap["bytes_uploaded"] = 0
				resetMap["articles_posted"] = 0
			}

			if resetPeak {
				resetMap["max_download_speed"] = 0
			}

			if len(resetMap) > 0 {
				_ = m.repo.BatchUpdateSystemStats(ctx, resetMap)
			}
		}
	}

	return nil
}

// ResetProviderErrors zeroes per-provider error counts without affecting other metrics.
func (m *manager) ResetProviderErrors(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker == nil {
		return nil
	}

	return m.metricsTracker.ResetProviderErrors(ctx)
}

// IncArticlesDownloaded increments the count of articles successfully downloaded
func (m *manager) IncArticlesDownloaded() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.IncArticlesDownloaded()
	}
}

// UpdateDownloadProgress updates the bytes downloaded for a specific stream
func (m *manager) UpdateDownloadProgress(id string, bytesDownloaded int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.UpdateDownloadProgress(id, bytesDownloaded)
	}
}

// IncArticlesPosted increments the count of articles successfully posted
func (m *manager) IncArticlesPosted() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.metricsTracker != nil {
		m.metricsTracker.IncArticlesPosted()
	}
}

func (m *manager) resetProviderQuota(ctx context.Context, generation generationClient, providerID string) error {
	m.logger.InfoContext(ctx, "Resetting provider quota", "provider", providerID)
	if err := generation.ResetProviderQuota(providerID); err != nil {
		return fmt.Errorf("failed to reset provider quota: %w", err)
	}

	if m.repo != nil {
		stats := map[string]int64{
			"quota_used:" + providerID:     0,
			"quota_reset_at:" + providerID: 0,
		}
		if err := m.repo.BatchUpdateSystemStats(ctx, stats); err != nil {
			m.logger.ErrorContext(ctx, "Failed to clear persisted quota state", "err", err, "provider", providerID)
		}
	}

	return nil
}

// ResetProviderQuota resets the download quota counter for a provider,
// clearing its consumed-bytes counter and exceeded flag in-place.
func (m *manager) ResetProviderQuota(ctx context.Context, providerID string) error {
	generation, release, err := m.facade.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	found := false
	for _, provider := range generation.Stats().Providers {
		if providerID != "" && provider.ProviderID == providerID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("provider ID %q is not present in the current generation", providerID)
	}

	return m.resetProviderQuota(ctx, generation, providerID)
}

// AcquireImportSlot blocks until an import admission slot is available or ctx
// is cancelled. See ImportAdmission.Acquire.
func (m *manager) AcquireImportSlot(ctx context.Context) (func(), error) {
	return m.admission.Acquire(ctx)
}

// SetAdmissionCap configures the cap on concurrently running NZB imports.
// A cap of 0 means unlimited.
func (m *manager) SetAdmissionCap(cap int) {
	m.admission.SetCap(cap)
}

// AcquireImportConnection blocks until the import connection budget grants a
// token or ctx is cancelled. See ImportBudget.Acquire.
func (m *manager) AcquireImportConnection(ctx context.Context) (func(), error) {
	return m.budget.Acquire(ctx)
}

// SetImportConnCapacity sets the import connection budget to the pool's total
// connection count.
func (m *manager) SetImportConnCapacity(total int) {
	m.budget.SetCapacity(total)
}

// ImportConnCapacity returns the current budget capacity snapshot.
func (m *manager) ImportConnCapacity() int {
	return m.budget.Capacity()
}

// SetStreamSource wires the source used to determine whether streams are
// currently active.
func (m *manager) SetStreamSource(src StreamActivitySource) {
	m.budget.SetStreamSource(src)
}

// NotifyStreamChange forwards a stream-count change to the import connection
// budget so it can wake or hold waiters according to the new effective cap.
func (m *manager) NotifyStreamChange() {
	m.budget.NotifyStreamChange()
}

// startQuotaWatcher starts the background quota watcher if not already running.
// Must be called with m.mu held.
func (m *manager) startQuotaWatcher() {
	if m.quotaWatchCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.quotaWatchCancel = cancel
	go m.quotaWatchLoop(ctx)
}

// stopQuotaWatcher stops the background quota watcher if running.
// Must be called with m.mu held.
func (m *manager) stopQuotaWatcher() {
	if m.quotaWatchCancel != nil {
		m.quotaWatchCancel()
		m.quotaWatchCancel = nil
	}
}

// quotaWatchLoop runs a periodic check for providers whose quota period has elapsed.
func (m *manager) quotaWatchLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndResetExpiredQuotas(ctx)
		}
	}
}

// checkAndResetExpiredQuotas resets any provider whose quota period has elapsed
// but whose quota counter was never cleared (because no new request arrived to
// trigger nntppool's on-demand reset path).
func (m *manager) checkAndResetExpiredQuotas(ctx context.Context) {
	generation, release, err := m.facade.acquire(ctx)
	if err != nil {
		return
	}
	defer release()

	now := time.Now()
	for _, ps := range generation.Stats().Providers {
		// Skip providers with no quota configured or no tracked usage to reset.
		// Without the QuotaUsed guard, providers that simply have a quota period
		// configured but have never downloaded anything would trigger a spurious
		// reset + DB write on every tick once their reset time expires.
		if ps.QuotaBytes == 0 || ps.QuotaUsed == 0 {
			continue
		}
		if ps.QuotaResetAt.IsZero() || !ps.QuotaResetAt.Before(now) {
			continue
		}

		providerID := providerStatsID(ps)
		if providerID == "" {
			continue
		}
		m.logger.InfoContext(ctx, "Auto-resetting expired provider quota",
			"provider", providerID, "reset_at", ps.QuotaResetAt)
		if err := m.resetProviderQuota(ctx, generation, providerID); err != nil {
			m.logger.ErrorContext(ctx, "Failed to auto-reset provider quota",
				"provider", providerID, "error", err)
		}
	}
}
