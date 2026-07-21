package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/javi11/altmount/internal/arrs/model"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/holes"
	"github.com/javi11/altmount/internal/importer"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/javi11/altmount/internal/progress"
	"github.com/sourcegraph/conc/pool"
)

// ARRsRepairService abstracts the ARR repair operations needed by HealthWorker.
type ARRsRepairService interface {
	TriggerFileRescan(ctx context.Context, pathForRescan string, relativePath string, metadataStr *string) error
	DiscoverFileMetadata(ctx context.Context, filePath, relativePath, nzbName, libraryPath string) (*model.WebhookMetadata, error)
}

// ErrHealthCheckAdmissionConflict means a direct check lost its exact claim
// because the health row changed after it was observed.
var ErrHealthCheckAdmissionConflict = errors.New("health record changed before direct check")

// PlaybackActivitySource is the narrow admission signal used by the temporary
// PR3 health-sweep pause. Repair notifications and manual checks are not
// blocked by this source.
type PlaybackActivitySource interface {
	ActiveStreams() int
}

type healthAdmissionSource interface {
	AcquireHealthAdmission() (release func(), admitted bool)
}

// WorkerStatus represents the current status of the health worker
type WorkerStatus string

const (
	WorkerStatusStopped  WorkerStatus = "stopped"
	WorkerStatusStarting WorkerStatus = "starting"
	WorkerStatusRunning  WorkerStatus = "running"
	WorkerStatusStopping WorkerStatus = "stopping"
)

// WorkerStats represents statistics about the health worker
type WorkerStats struct {
	Status                 WorkerStatus `json:"status"`
	LastRunTime            *time.Time   `json:"last_run_time,omitempty"`
	NextRunTime            *time.Time   `json:"next_run_time,omitempty"`
	TotalRunsCompleted     int64        `json:"total_runs_completed"`
	TotalFilesChecked      int64        `json:"total_files_checked"`
	TotalFilesHealthy      int64        `json:"total_files_healthy"`
	TotalFilesCorrupted    int64        `json:"total_files_corrupted"`
	CurrentRunStartTime    *time.Time   `json:"current_run_start_time,omitempty"`
	CurrentRunFilesChecked int          `json:"current_run_files_checked"`
	LastError              *string      `json:"last_error,omitempty"`
	ErrorCount             int64        `json:"error_count"`
}

// HealthWorker manages continuous health monitoring and manual check requests
type HealthWorker struct {
	healthChecker       *HealthChecker
	healthRepo          *database.HealthRepository
	metadataService     *metadata.MetadataService
	configGetter        config.ConfigGetter
	progressBroadcaster *progress.ProgressBroadcaster // optional, may be nil
	playbackSource      PlaybackActivitySource

	// Worker state
	status       WorkerStatus
	running      bool
	cycleRunning bool // Flag to prevent overlapping cycles
	stopChan     chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex

	// Active checks tracking for cancellation
	activeChecks   map[string]context.CancelFunc // filePath -> cancel function
	activeChecksMu sync.RWMutex

	// Statistics
	stats   WorkerStats
	statsMu sync.RWMutex
}

// SetPlaybackActivitySource supplies the live stream counter used for ordinary
// health admission. It is safe to replace while the worker is running.
func (hw *HealthWorker) SetPlaybackActivitySource(source PlaybackActivitySource) {
	hw.mu.Lock()
	hw.playbackSource = source
	hw.mu.Unlock()
}

func (hw *HealthWorker) shouldPauseForPlayback() bool {
	if !hw.configGetter().GetPauseHealthDuringPlayback() {
		return false
	}
	hw.mu.RLock()
	source := hw.playbackSource
	hw.mu.RUnlock()
	return source != nil && source.ActiveStreams() > 0
}

func (hw *HealthWorker) acquireHealthAdmission() (release func(), admitted bool, serialized bool) {
	if !hw.configGetter().GetPauseHealthDuringPlayback() {
		return func() {}, true, false
	}

	hw.mu.RLock()
	source := hw.playbackSource
	hw.mu.RUnlock()
	boundary, ok := source.(healthAdmissionSource)
	if !ok {
		return func() {}, true, false
	}
	release, admitted = boundary.AcquireHealthAdmission()
	if release == nil {
		release = func() {}
	}
	return release, admitted, true
}

// NewHealthWorker creates a new health worker
func NewHealthWorker(
	healthChecker *HealthChecker,
	healthRepo *database.HealthRepository,
	metadataService *metadata.MetadataService,
	arrsService ARRsRepairService,
	importerService importer.ImportService,
	configGetter config.ConfigGetter,
	broadcaster *progress.ProgressBroadcaster,
) *HealthWorker {
	return &HealthWorker{
		healthChecker:       healthChecker,
		healthRepo:          healthRepo,
		metadataService:     metadataService,
		configGetter:        configGetter,
		progressBroadcaster: broadcaster,
		status:              WorkerStatusStopped,
		stopChan:            make(chan struct{}),
		activeChecks:        make(map[string]context.CancelFunc),
		stats: WorkerStats{
			Status: WorkerStatusStopped,
		},
	}
}

// broadcastHealthChanged notifies SSE subscribers that health state has changed.
func (hw *HealthWorker) broadcastHealthChanged() {
	if hw.progressBroadcaster != nil {
		hw.progressBroadcaster.BroadcastHealthChanged()
	}
}

// Start begins the health worker service
func (hw *HealthWorker) Start(ctx context.Context) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.running {
		return fmt.Errorf("health worker already running")
	}

	if !hw.configGetter().GetHealthEnabled() {
		slog.WarnContext(ctx, "Health worker is disabled via configuration, not starting")
		return nil
	}

	hw.running = true
	hw.status = WorkerStatusStarting
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStarting
		s.LastError = nil
	})

	// Initialize health system - reset any files stuck in 'checking' status
	if err := hw.healthRepo.ResetFileAllChecking(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to reset checking files during initialization", "error", err)
		// Don't fail startup for this - just log and continue
	}

	// Reset pending files that exhausted retries so they can be rechecked
	if err := hw.healthRepo.ResetStalePendingFiles(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to reset stale pending files during initialization", "error", err)
		// Don't fail startup for this - just log and continue
	}

	// Start the main worker goroutine
	hw.wg.Go(func() {
		hw.run(ctx)
	})

	hw.status = WorkerStatusRunning
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusRunning
	})

	slog.InfoContext(ctx, "Health worker started successfully", "check_interval", hw.getCheckInterval(), "max_concurrent_jobs", hw.getMaxConcurrentJobs())
	return nil
}

// Stop gracefully stops the health worker
func (hw *HealthWorker) Stop(ctx context.Context) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if !hw.running {
		return fmt.Errorf("health worker not running")
	}

	hw.status = WorkerStatusStopping
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStopping
	})

	slog.InfoContext(ctx, "Stopping health worker...")
	close(hw.stopChan)
	hw.running = false

	// Wait for all goroutines to finish
	hw.wg.Wait()

	hw.status = WorkerStatusStopped
	hw.updateStats(func(s *WorkerStats) {
		s.Status = WorkerStatusStopped
		s.CurrentRunStartTime = nil
		s.CurrentRunFilesChecked = 0
	})

	slog.InfoContext(ctx, "Health worker stopped")
	return nil
}

// IsRunning returns whether the health worker is currently running
func (hw *HealthWorker) IsRunning() bool {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	return hw.running
}

// GetStats returns current worker statistics
func (hw *HealthWorker) GetStats() WorkerStats {
	hw.statsMu.RLock()
	defer hw.statsMu.RUnlock()

	return hw.stats
}

// CancelHealthCheck cancels an active health check for the specified file
func (hw *HealthWorker) CancelHealthCheck(ctx context.Context, filePath string) error {
	hw.activeChecksMu.Lock()
	defer hw.activeChecksMu.Unlock()

	cancelFunc, exists := hw.activeChecks[filePath]
	if !exists {
		return fmt.Errorf("no active health check found for file: %s", filePath)
	}

	// Cancel the context
	cancelFunc()

	// Remove from active checks
	delete(hw.activeChecks, filePath)

	// Update file status to pending to allow retry
	err := hw.healthRepo.UpdateFileHealth(ctx, filePath, database.HealthStatusPending, nil, nil, nil, false)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to update file status after cancellation", "file_path", filePath, "error", err)
		return fmt.Errorf("failed to update file status after cancellation: %w", err)
	}

	hw.broadcastHealthChanged()
	slog.InfoContext(ctx, "Health check cancelled", "file_path", filePath)
	return nil
}

// IsCheckActive returns whether a health check is currently active for the specified file
func (hw *HealthWorker) IsCheckActive(filePath string) bool {
	hw.activeChecksMu.RLock()
	defer hw.activeChecksMu.RUnlock()

	_, exists := hw.activeChecks[filePath]
	return exists
}

// run is the main worker loop
func (hw *HealthWorker) run(ctx context.Context) {
	ticker := time.NewTicker(hw.getCheckInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Health worker stopped by context")
			return
		case <-hw.stopChan:
			slog.InfoContext(ctx, "Health worker stopped by stop signal")
			return
		case <-ticker.C:
			// Check if a cycle is already running
			hw.mu.RLock()
			isCycleRunning := hw.cycleRunning
			hw.mu.RUnlock()

			if isCycleRunning {
				slog.DebugContext(ctx, "Skipping health check cycle - previous cycle still running")
				continue
			}

			if err := hw.safeRunHealthCheckCycle(ctx); err != nil {
				slog.ErrorContext(ctx, "Health check cycle failed", "error", err)
				hw.updateStats(func(s *WorkerStats) {
					s.ErrorCount++
					errMsg := err.Error()
					s.LastError = &errMsg
				})
			}
		}
	}
}

// safeRunHealthCheckCycle runs a health check cycle with panic recovery
func (hw *HealthWorker) safeRunHealthCheckCycle(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in health check cycle: %v", r)
			slog.ErrorContext(ctx, "Panic in health check cycle", "panic", r)
		}
	}()
	return hw.runHealthCheckCycle(ctx)
}

// AddToHealthCheck adds a file to the health check list with pending status
func (hw *HealthWorker) AddToHealthCheck(ctx context.Context, filePath string, sourceNzb *string) error {
	// Check if file already exists in health database
	existingHealth, err := hw.healthRepo.GetFileHealth(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to check existing health record: %w", err)
	}

	// If file doesn't exist in health database, add it with a short jitter (0–5 min) so
	// newly imported files are checked soon without all firing at the exact same instant.
	if existingHealth == nil {
		scheduledAt := calculateInitialCheckForNewFile()
		err = hw.healthRepo.UpdateFileHealthScheduled(ctx,
			filePath,
			database.HealthStatusPending,
			nil,
			sourceNzb,
			nil,
			false,
			scheduledAt,
		)
		if err != nil {
			return fmt.Errorf("failed to add file to health database: %w", err)
		}

		slog.InfoContext(ctx, "Added file to health check list", "file_path", filePath, "scheduled_at", scheduledAt)
	} else {
		// File already exists, just reset to pending status if not already pending
		if existingHealth.Status != database.HealthStatusPending {
			err = hw.healthRepo.UpdateFileHealth(ctx,
				filePath,
				database.HealthStatusPending,
				nil,
				sourceNzb,
				nil,
				false,
			)
			if err != nil {
				return fmt.Errorf("failed to update file status to pending: %w", err)
			}
			slog.InfoContext(ctx, "Reset file status to pending for health check", "file_path", filePath)
		}
	}

	return nil
}

// PerformBackgroundCheck starts a health check in background and returns immediately
func (hw *HealthWorker) PerformBackgroundCheck(ctx context.Context, filePath string) error {
	if !hw.IsRunning() {
		return fmt.Errorf("health worker is not running")
	}

	fh, err := hw.healthRepo.GetFileHealth(ctx, filePath)
	if err != nil {
		return fmt.Errorf("failed to get file health state: %w", err)
	}
	if fh == nil {
		return fmt.Errorf("file health record not found: %s", filePath)
	}
	claimed, err := hw.healthRepo.ClaimFilesCheckingBulk(ctx, []*database.FileHealth{fh})
	if err != nil {
		return fmt.Errorf("failed to claim direct health check: %w", err)
	}
	if len(claimed) != 1 {
		return fmt.Errorf("%w: %s", ErrHealthCheckAdmissionConflict, filePath)
	}

	// Start health check in background
	go func(fh *database.FileHealth) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		checkErr := hw.performClaimedDirectCheck(ctx, fh)
		if checkErr != nil {
			if errors.Is(checkErr, context.DeadlineExceeded) {
				slog.ErrorContext(ctx, "Background health check timed out after 10 minutes", "file_path", filePath)
			} else {
				slog.ErrorContext(ctx, "Background health check failed", "file_path", filePath, "error", checkErr)
			}
		}
	}(claimed[0])

	return nil
}

// prepareUpdateForResult maps checked evidence to SQL only. Automatic repair,
// deletion, metadata movement/status changes, and indexer effects are disabled.
func (hw *HealthWorker) prepareUpdateForResult(ctx context.Context, fh *database.FileHealth, event HealthEvent) (*database.HealthStatusUpdate, func() error) {
	_ = ctx
	noEffect := func() error { return nil }
	update := &database.HealthStatusUpdate{
		FilePath:     fh.FilePath,
		ErrorMessage: fh.LastError,
		ErrorDetails: fh.ErrorDetails,
	}
	if event.Error != nil {
		text := event.Error.Error()
		update.ErrorMessage = &text
	}
	if event.Details != nil {
		update.ErrorDetails = event.Details
	}

	switch event.Type {
	case EventTypeFileHealthy:
		releaseDate := fh.ReleaseDate
		if releaseDate == nil {
			releaseDate = &fh.CreatedAt
		}
		update.Type = database.UpdateTypeHealthy
		update.Status = database.HealthStatusHealthy
		update.ScheduledCheckAt = CalculateNextCheck(releaseDate.UTC(), time.Now().UTC())
		return update, noEffect
	case EventTypeCheckFailed, EventTypeFileRemoved:
		exponent := min(max(fh.RetryCount, 0), 6)
		update.Type = database.UpdateTypeInconclusive
		update.Status = database.HealthStatusPending
		update.ScheduledCheckAt = time.Now().UTC().Add(time.Duration(15*(1<<exponent)) * time.Minute)
		return update, noEffect
	}

	if event.Classification != nil &&
		event.Classification.Verdict == holes.VerdictDegraded &&
		fh.Status != database.HealthStatusRepairTriggered {
		releaseDate := fh.ReleaseDate
		if releaseDate == nil {
			releaseDate = &fh.CreatedAt
		}
		update.Type = database.UpdateTypeDegraded
		update.Status = database.HealthStatusDegraded
		update.ScheduledCheckAt = CalculateNextCheck(releaseDate.UTC(), time.Now().UTC())
		return update, noEffect
	}

	maxRetries := fh.MaxRetries
	if maxRetries <= 0 {
		maxRetries = hw.configGetter().GetMaxRetries()
	}
	if maxRetries <= 0 || fh.RetryCount >= maxRetries-1 {
		update.Type = database.UpdateTypeCorrupted
		update.Status = database.HealthStatusCorrupted
		return update, noEffect
	}
	update.Type = database.UpdateTypeRetry
	update.Status = database.HealthStatusPending
	update.ScheduledCheckAt = time.Now().UTC().Add(time.Duration(15*(1<<min(max(fh.RetryCount, 0), 6))) * time.Minute)
	return update, noEffect
}

// performClaimedDirectCheck checks and publishes one already-claimed snapshot.
func (hw *HealthWorker) performClaimedDirectCheck(ctx context.Context, fh *database.FileHealth) error {
	filePath := fh.FilePath
	claimed := []*database.FileHealth{fh}

	// Create cancellable context for this check
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track active check
	hw.activeChecksMu.Lock()
	hw.activeChecks[filePath] = cancel
	hw.activeChecksMu.Unlock()

	// Ensure cleanup on exit
	defer func() {
		hw.activeChecksMu.Lock()
		delete(hw.activeChecks, filePath)
		hw.activeChecksMu.Unlock()
	}()

	// Check if already cancelled
	select {
	case <-checkCtx.Done():
		return checkCtx.Err()
	default:
	}

	// Delegate to HealthChecker
	event := hw.healthChecker.CheckFile(checkCtx, filePath, CheckOptions{})

	// Check if cancelled during check
	select {
	case <-checkCtx.Done():
		return checkCtx.Err()
	default:
	}

	update, _ := hw.prepareUpdateForResult(ctx, fh, event)
	if err := hw.healthRepo.PublishClaimedHealthStatusBulk(ctx, claimed, []database.HealthStatusUpdate{*update}); err != nil {
		return fmt.Errorf("failed to publish direct health evidence: %w", err)
	}

	hw.broadcastHealthChanged()
	hw.healthChecker.notifyRcloneVFS(filePath, event)

	// Update stats
	hw.updateStats(func(s *WorkerStats) {
		s.TotalFilesChecked++
		switch event.Type {
		case EventTypeFileHealthy:
			s.TotalFilesHealthy++
		case EventTypeFileCorrupted:
			s.TotalFilesCorrupted++
		}
	})

	return nil
}

// updateStats safely updates worker statistics
// runHealthCheckCycle runs a single cycle of health checks
func (hw *HealthWorker) runHealthCheckCycle(ctx context.Context) error {
	// Set the cycle running flag
	hw.mu.Lock()
	hw.cycleRunning = true
	hw.mu.Unlock()

	// Ensure we clear the flag when done
	defer func() {
		hw.mu.Lock()
		hw.cycleRunning = false
		hw.mu.Unlock()
	}()

	now := time.Now().UTC()
	hw.updateStats(func(s *WorkerStats) {
		s.CurrentRunStartTime = &now
		s.CurrentRunFilesChecked = 0
	})

	maxJobs := hw.getMaxConcurrentJobs()
	cfg := hw.configGetter()
	strategy := string(cfg.Import.ImportStrategy)
	libraryDir := ""
	if cfg.Health.LibraryDir != nil {
		libraryDir = *cfg.Health.LibraryDir
	}

	// Get files due for checking (ordered by scheduled_check_at)
	// New logic: Only check files with library_path (imported) unless strategy is NONE
	// The fetch limit is intentionally larger than maxJobs: segment availability
	// for the whole batch is verified in one cross-file StatMany sweep
	// (CheckFilesBatch), so NNTP throughput no longer depends on per-file job
	// concurrency. maxJobs still bounds the per-file result handling below
	// (repair side effects, ARR API calls).
	var unhealthyFiles []*database.FileHealth
	var err error
	releaseAdmission, admitted, admissionSerialized := hw.acquireHealthAdmission()
	if admissionSerialized && !admitted {
		slog.InfoContext(ctx, "Deferring ordinary health batch because playback won admission")
	} else if !admissionSerialized && hw.shouldPauseForPlayback() {
		slog.InfoContext(ctx, "Pausing admission of ordinary health checks during active playback")
	} else {
		if admissionSerialized {
			defer releaseAdmission()
		}
		// Keep a successful shared token through due-row selection so the
		// selected snapshots cannot race a new playback start before claiming.
		unhealthyFiles, err = hw.healthRepo.GetUnhealthyFiles(ctx, cfg.GetCheckBatchSize(), strategy, libraryDir, hw.configGetter().GetMaxRetries())
		if err != nil {
			return fmt.Errorf("failed to get unhealthy files: %w", err)
		}
	}

	// Legacy playback sources lack the shared boundary, so retain a final
	// snapshot before claiming for those implementations.
	if len(unhealthyFiles) > 0 && !admissionSerialized && hw.shouldPauseForPlayback() {
		slog.InfoContext(ctx, "Deferring ordinary health batch because playback became active",
			"files", len(unhealthyFiles))
		unhealthyFiles = nil
	}

	// Claim only after the shared boundary or legacy final snapshot admits the
	// batch. From here onward only current claimed snapshots may be checked.
	unhealthyFiles, err = hw.healthRepo.ClaimFilesCheckingBulk(ctx, unhealthyFiles)
	if err != nil {
		return fmt.Errorf("failed to claim health check batch: %w", err)
	}

	if len(unhealthyFiles) == 0 {
		hw.updateStats(func(s *WorkerStats) {
			s.CurrentRunStartTime = nil
			s.CurrentRunFilesChecked = 0
			s.TotalRunsCompleted++
			s.LastRunTime = &now
			nextRun := now.Add(hw.getCheckInterval())
			s.NextRunTime = &nextRun
		})
		return nil
	}

	slog.InfoContext(ctx, "Found files to process",
		"health_check_files", len(unhealthyFiles),
		"total", len(unhealthyFiles),
		"max_concurrent_jobs", maxJobs)

	// Process files in parallel with bounded concurrency
	p := pool.New().WithMaxGoroutines(maxJobs)
	results := make([]database.HealthStatusUpdate, len(unhealthyFiles))

	paths := make([]string, len(unhealthyFiles))
	for i, fh := range unhealthyFiles {
		slog.InfoContext(ctx, "Checking unhealthy file", "file_path", fh.FilePath)
		paths[i] = fh.FilePath
	}
	events := hw.healthChecker.CheckFilesBatch(ctx, paths)
	if len(events) != len(unhealthyFiles) {
		return fmt.Errorf("health checker returned %d events for %d claims", len(events), len(unhealthyFiles))
	}

	// Build pure SQL evidence in parallel. Index alignment preserves the exact
	// one-to-one relationship between claims, events, and publication updates.
	for i, fileHealth := range unhealthyFiles {
		index := i
		fh := fileHealth
		event := events[i]
		p.Go(func() {
			update, _ := hw.prepareUpdateForResult(ctx, fh, event)
			results[index] = *update
		})
	}
	p.Wait()

	if err := hw.healthRepo.PublishClaimedHealthStatusBulk(ctx, unhealthyFiles, results); err != nil {
		return fmt.Errorf("failed to publish claimed health evidence: %w", err)
	}

	// Observers are notified only after the whole checked batch commits.
	healthyCount := int64(0)
	corruptedCount := int64(0)
	for i, event := range events {
		hw.healthChecker.notifyRcloneVFS(unhealthyFiles[i].FilePath, event)
		switch event.Type {
		case EventTypeFileHealthy:
			healthyCount++
		case EventTypeFileCorrupted:
			corruptedCount++
		}
	}
	hw.updateStats(func(s *WorkerStats) {
		s.CurrentRunFilesChecked = len(unhealthyFiles)
		s.TotalFilesChecked += int64(len(unhealthyFiles))
		s.TotalFilesHealthy += healthyCount
		s.TotalFilesCorrupted += corruptedCount
	})
	hw.broadcastHealthChanged()

	// Update final stats
	hw.updateStats(func(s *WorkerStats) {
		s.CurrentRunStartTime = nil
		s.CurrentRunFilesChecked = 0
		s.TotalRunsCompleted++
		s.LastRunTime = &now
		nextRun := now.Add(hw.getCheckInterval())
		s.NextRunTime = &nextRun
	})

	slog.InfoContext(ctx, "Health check cycle completed",
		"health_check_files", len(unhealthyFiles),
		"total_files", len(unhealthyFiles),
		"duration", time.Since(now))

	return nil
}

// updateStats safely updates worker statistics
func (hw *HealthWorker) updateStats(updateFunc func(*WorkerStats)) {
	hw.statsMu.Lock()
	defer hw.statsMu.Unlock()
	updateFunc(&hw.stats)
}

// Helper methods to get dynamic health config values
func (hw *HealthWorker) getCheckInterval() time.Duration {
	return hw.configGetter().GetCheckInterval()
}

func (hw *HealthWorker) getMaxConcurrentJobs() int {
	return hw.configGetter().GetMaxConcurrentJobs()
}
