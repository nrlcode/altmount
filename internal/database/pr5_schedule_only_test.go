package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPR5DeferScheduledHealthCheckDoesNotSynthesizeHealth(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	lastChecked := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	oldSchedule := lastChecked.Add(time.Hour)
	newSchedule := lastChecked.Add(24 * time.Hour)

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (
			file_path, status, last_checked, last_error, retry_count,
			repair_retry_count, scheduled_check_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "movie.mkv", HealthStatusPending, lastChecked, "still unresolved", 2, 1, oldSchedule)
	require.NoError(t, err)

	require.NoError(t, repo.DeferScheduledHealthCheck(ctx, "/movie.mkv", newSchedule))

	var (
		status           HealthStatus
		gotLastChecked   time.Time
		lastError        sql.NullString
		retryCount       int
		repairRetryCount int
		gotSchedule      time.Time
	)
	err = repo.db.QueryRowContext(ctx, `
		SELECT status, last_checked, last_error, retry_count,
		       repair_retry_count, scheduled_check_at
		FROM file_health WHERE file_path = ?
	`, "movie.mkv").Scan(
		&status,
		&gotLastChecked,
		&lastError,
		&retryCount,
		&repairRetryCount,
		&gotSchedule,
	)
	require.NoError(t, err)
	require.Equal(t, HealthStatusPending, status)
	require.True(t, gotLastChecked.Equal(lastChecked), "%v != %v", gotLastChecked, lastChecked)
	require.Equal(t, sql.NullString{String: "still unresolved", Valid: true}, lastError)
	require.Equal(t, 2, retryCount)
	require.Equal(t, 1, repairRetryCount)
	require.True(t, gotSchedule.Equal(newSchedule), "%v != %v", gotSchedule, newSchedule)
}

func TestPR5DeferScheduledHealthCheckRequiresExistingRecord(t *testing.T) {
	repo := setupTestDB(t)

	err := repo.DeferScheduledHealthCheck(context.Background(), "missing.mkv", time.Now().UTC())
	require.ErrorContains(t, err, "no health check found")
}
