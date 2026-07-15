package database

import (
	"context"
	"database/sql"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ObservationDiscoveryUpsertPreservesExistingHealthEvidence(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	originalLastChecked := "2026-01-02 03:04:05"
	originalSchedule := "2026-01-03 03:04:05"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (
			file_path, library_path, status, last_checked, last_error,
			error_details, retry_count, max_retries, repair_retry_count,
			max_repair_retries, source_nzb_path, release_date,
			scheduled_check_at, priority, streaming_failure_count, is_masked
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "shows/example.mkv", "/old/library/example.mkv", HealthStatusDegraded,
		originalLastChecked, "synthetic provider absence", "synthetic attempt evidence",
		2, 3, 1, 4, "/metadata/old.nzb", "2025-01-01 00:00:00",
		originalSchedule, 2, 7, true)
	require.NoError(t, err)

	newLibraryPath := "/new/library/example.mkv"
	newSourcePath := "/metadata/replacement.nzb"
	newReleaseDate := time.Date(2026, time.February, 1, 0, 0, 0, 0, time.UTC)
	newSchedule := time.Date(2026, time.February, 2, 0, 0, 0, 0, time.UTC)
	require.NoError(t, repo.BatchUpsertObservationDiscoveries(ctx, []AutomaticHealthCheckRecord{{
		FilePath:         "/shows/example.mkv",
		LibraryPath:      &newLibraryPath,
		ReleaseDate:      &newReleaseDate,
		ScheduledCheckAt: &newSchedule,
		SourceNzbPath:    &newSourcePath,
		MaxRetries:       8,
		MaxRepairRetries: 9,
	}}))

	var (
		libraryPath, status, lastChecked, lastError, details       string
		retryCount, maxRetries, repairRetryCount, maxRepairRetries int
		sourcePath, releaseDate, scheduledAt                       string
		priority, streamingFailures                                int
		masked                                                     bool
	)
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT library_path, status, CAST(last_checked AS TEXT), last_error,
			error_details, retry_count, max_retries, repair_retry_count,
			max_repair_retries, source_nzb_path, CAST(release_date AS TEXT),
			CAST(scheduled_check_at AS TEXT), priority, streaming_failure_count,
			is_masked
		FROM file_health WHERE file_path = ?
	`, "shows/example.mkv").Scan(
		&libraryPath, &status, &lastChecked, &lastError, &details,
		&retryCount, &maxRetries, &repairRetryCount, &maxRepairRetries,
		&sourcePath, &releaseDate, &scheduledAt, &priority,
		&streamingFailures, &masked,
	))

	require.Equal(t, newLibraryPath, libraryPath)
	require.Equal(t, newSourcePath, sourcePath)
	require.Equal(t, "2026-02-01 00:00:00", releaseDate)
	require.Equal(t, 8, maxRetries)
	require.Equal(t, 9, maxRepairRetries)
	require.Equal(t, string(HealthStatusDegraded), status)
	require.Equal(t, originalLastChecked, lastChecked)
	require.Equal(t, "synthetic provider absence", lastError)
	require.Equal(t, "synthetic attempt evidence", details)
	require.Equal(t, 2, retryCount)
	require.Equal(t, 1, repairRetryCount)
	require.Equal(t, originalSchedule, scheduledAt)
	require.Equal(t, 2, priority)
	require.Equal(t, 7, streamingFailures)
	require.True(t, masked)
}

func TestPR5ObservationDiscoveryUpsertCreatesPendingDueWorkWithoutEvidence(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	sourcePath := "/metadata/new.nzb"
	releaseDate := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	scheduledAt := time.Date(2026, time.March, 2, 0, 0, 0, 0, time.UTC)

	require.NoError(t, repo.BatchUpsertObservationDiscoveries(ctx, []AutomaticHealthCheckRecord{{
		FilePath:         "/movies/new.mkv",
		ReleaseDate:      &releaseDate,
		ScheduledCheckAt: &scheduledAt,
		SourceNzbPath:    &sourcePath,
		MaxRetries:       3,
		MaxRepairRetries: 4,
	}}))

	var (
		status                          string
		lastChecked, lastError, details sql.NullString
		retryCount, repairRetryCount    int
		schedule                        string
	)
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT status, last_checked, last_error, error_details, retry_count,
			repair_retry_count, CAST(scheduled_check_at AS TEXT)
		FROM file_health WHERE file_path = ?
	`, "movies/new.mkv").Scan(
		&status, &lastChecked, &lastError, &details, &retryCount,
		&repairRetryCount, &schedule,
	))
	require.Equal(t, string(HealthStatusPending), status)
	require.False(t, lastChecked.Valid, "discovery is not provider evidence")
	require.False(t, lastError.Valid)
	require.False(t, details.Valid)
	require.Zero(t, retryCount)
	require.Zero(t, repairRetryCount)
	require.Equal(t, "2026-03-02 00:00:00", schedule)
}

func TestPR5ObservationDiscoveryUpsertRequiresDueSchedule(t *testing.T) {
	repo := setupTestDB(t)
	err := repo.BatchUpsertObservationDiscoveries(context.Background(), []AutomaticHealthCheckRecord{{
		FilePath: "movies/unscheduled.mkv",
	}})
	require.ErrorContains(t, err, "scheduled check")
}

func TestPR5ObservationDueSelectionIgnoresLegacyRepairEligibility(t *testing.T) {
	db, state := newPR4Repository(t)
	repo := NewHealthRepository(db.Connection(), DialectSQLite)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	rows := []struct {
		path       string
		status     HealthStatus
		retries    int
		maxRetries int
		schedule   *time.Time
		source     *string
	}{
		{"library/corrupted-due.mkv", HealthStatusCorrupted, 7, 3, &past, stringPointer("synthetic-a.nzb")},
		{"library/retry-exhausted.mkv", HealthStatusPending, 3, 3, nil, stringPointer("synthetic-b.nzb")},
		{"library/future.mkv", HealthStatusHealthy, 0, 3, &future, stringPointer("synthetic-c.nzb")},
		{"library/retained-history.mkv", HealthStatusCorrupted, 9, 3, nil, nil},
	}
	for _, row := range rows {
		_, err := db.Connection().ExecContext(ctx, `
			INSERT INTO file_health
				(file_path, status, retry_count, max_retries, scheduled_check_at,
				 source_nzb_path, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, row.path, row.status, row.retries, row.maxRetries, row.schedule,
			row.source, now.Add(-48*time.Hour), now.Add(-48*time.Hour))
		require.NoError(t, err)
	}
	active, err := state.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath:          "library/active-revision-no-schedule.mkv",
		LayoutFingerprint: "sha256:active-no-schedule", VirtualSize: 6, SegmentCount: 2,
	})
	require.NoError(t, err)
	require.True(t, active.Active)

	due, err := repo.ListDueObservationFiles(ctx, now, now.Add(-24*time.Hour), 16)
	require.NoError(t, err)
	paths := make([]string, 0, len(due))
	for _, file := range due {
		paths = append(paths, file.FilePath)
	}
	sort.Strings(paths)
	assert.Equal(t, []string{
		"library/active-revision-no-schedule.mkv",
		"library/corrupted-due.mkv",
		"library/retry-exhausted.mkv",
	}, paths)
	for _, file := range due {
		if file.FilePath == "library/retry-exhausted.mkv" ||
			file.FilePath == "library/active-revision-no-schedule.mkv" {
			assert.Nil(t, file.ScheduledCheckAt)
		}
	}
}

func TestPR5ObservationDueSelectionUsesDurableRunFreshnessWhenScheduleIsMissing(t *testing.T) {
	db, state := newPR4Repository(t)
	repo := NewHealthRepository(db.Connection(), DialectSQLite)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	revision, err := state.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath:          "library/fresh-without-schedule.mkv",
		LayoutFingerprint: "sha256:fresh-without-schedule", VirtualSize: 6, SegmentCount: 2,
	})
	require.NoError(t, err)
	_, err = state.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "freshness-provider", DisplayName: "Synthetic provider",
		Endpoint: "freshness.invalid", Port: 119, Account: "synthetic-account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	snapshot, err := state.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := state.CreateHealthRun(ctx, HealthRunSpec{
		ID: "freshness-manual-run", FileRevisionID: revision.ID,
		ProviderSnapshotID: snapshot.ID, Trigger: "manual", Mode: "observation",
		TotalSegments: revision.SegmentCount, CreatedAt: now,
	})
	require.NoError(t, err)
	lease, err := state.AcquireRunLease(ctx, run.ID, "freshness-worker", time.Minute)
	require.NoError(t, err)
	completedAt := now.Add(time.Second)
	require.NoError(t, state.CompleteHealthRun(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, completedAt,
	))

	due, err := repo.ListDueObservationFiles(
		ctx, now.Add(2*time.Second), completedAt.Add(-24*time.Hour), 8,
	)
	require.NoError(t, err)
	assert.Empty(t, due, "a fresh durable observation replaces a missing coarse schedule")

	due, err = repo.ListDueObservationFiles(
		ctx, now.Add(48*time.Hour), completedAt.Add(time.Second), 8,
	)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Equal(t, "library/fresh-without-schedule.mkv", due[0].FilePath)
}

func stringPointer(value string) *string { return &value }
