package database

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const facoreCHG014MigrationVersion int64 = 37

func ensureCHG014PostgresRepositoryFixture(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
) {
	t.Helper()
	if backend.dialect != DialectPostgres {
		return
	}
	// Migration 027's legacy schema-free guard can see another test schema and
	// skip this unrelated canonical column. Keep the isolated fixture complete.
	_, err := backend.db.ExecContext(ctx,
		`ALTER TABLE file_health ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT NULL`)
	require.NoError(t, err)
}

func requireCHG014ClaimGenerationAbsent(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
) {
	t.Helper()
	var generation int64
	err := backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT claim_generation
		FROM file_health
		WHERE file_path = ?
	`, "movies/migrated-checking.mkv").Scan(&generation)
	require.Error(t, err, "schema version 36 must not contain claim_generation")
	require.NotErrorIs(t, err, sql.ErrNoRows,
		"the populated control row must still exist while checking column absence")
}

func assertCHG014MigratedEvidence(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	wantStatus HealthStatus,
) {
	t.Helper()
	var (
		status     HealthStatus
		retryCount int
		lastError  string
	)
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT status, retry_count, last_error
		FROM file_health
		WHERE file_path = ?
	`, "movies/migrated-checking.mkv").Scan(&status, &retryCount, &lastError))
	assert.Equal(t, wantStatus, status)
	assert.Equal(t, 2, retryCount)
	assert.Equal(t, "preserved evidence", lastError)
}

func TestFACORECHG014ClaimGenerationMigrationRoundTrip(t *testing.T) {
	forEachFACORECHG009MigrationBackend(t, func(
		t *testing.T,
		ctx context.Context,
		backend facoreCHG009MigrationBackend,
	) {
		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		_, err := backend.dialectAwareSQL.ExecContext(ctx, `
			INSERT INTO file_health
				(file_path, status, retry_count, max_retries, last_error)
			VALUES (?, ?, ?, ?, ?)
		`, "movies/migrated-checking.mkv", HealthStatusChecking, 2, 3, "preserved evidence")
		require.NoError(t, err)
		requireCHG014ClaimGenerationAbsent(t, ctx, backend)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
		))
		require.Equal(t, facoreCHG014MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		ensureCHG014PostgresRepositoryFixture(t, ctx, backend)

		metadata := facoreCHG009ReadColumnMetadata(t, backend, "file_health", "claim_generation")
		if backend.dialect == DialectPostgres {
			assert.Equal(t, "bigint", metadata.dataType)
		} else {
			assert.Equal(t, "INTEGER", strings.ToUpper(metadata.dataType))
		}
		assert.True(t, metadata.notNull)
		require.True(t, metadata.defaultValue.Valid)
		assert.Contains(t, metadata.defaultValue.String, "0")

		var generation int64
		require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
			SELECT claim_generation FROM file_health WHERE file_path = ?
		`, "movies/migrated-checking.mkv").Scan(&generation))
		assert.Zero(t, generation, "migration cannot fabricate ownership for existing checking rows")
		assertCHG014MigratedEvidence(t, ctx, backend, HealthStatusChecking)
		_, err = backend.dialectAwareSQL.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status)
			VALUES (?, ?)
		`, "movies/new-after-migration.mkv", HealthStatusPending)
		require.NoError(t, err)
		require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
			SELECT claim_generation FROM file_health WHERE file_path = ?
		`, "movies/new-after-migration.mkv").Scan(&generation))
		assert.Zero(t, generation, "new rows must start outside fenced ownership")

		_, err = backend.dialectAwareSQL.ExecContext(ctx, `
			UPDATE file_health SET claim_generation = -1 WHERE file_path = ?
		`, "movies/migrated-checking.mkv")
		require.Error(t, err, "negative claim generations must be rejected")

		repo := NewHealthRepository(backend.db, backend.dialect)
		require.NoError(t, repo.ResetFileAllChecking(ctx))
		owner := claimCHG014Health(t, repo, "movies/migrated-checking.mkv")
		assert.Equal(t, int64(1), owner.ClaimGeneration,
			"the first fenced claim after migration must advance zero to one")
		assertCHG014MigratedEvidence(t, ctx, backend, HealthStatusChecking)

		_, err = backend.dialectAwareSQL.ExecContext(ctx, `
			UPDATE file_health SET status = ? WHERE file_path = ?
		`, HealthStatusPending, "movies/migrated-checking.mkv")
		require.NoError(t, err)
		require.NoError(t, goose.DownToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		requireCHG014ClaimGenerationAbsent(t, ctx, backend)
		assertCHG014MigratedEvidence(t, ctx, backend, HealthStatusPending)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
		))
		require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
			SELECT claim_generation FROM file_health WHERE file_path = ?
		`, "movies/migrated-checking.mkv").Scan(&generation))
		assert.Zero(t, generation, "reapplying migration 037 must restore the zero sentinel")
		assertCHG014MigratedEvidence(t, ctx, backend, HealthStatusPending)
	})
}

func TestFACORECHG014PostgresClaimOwnershipParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	backend := newFACORECHG009PostgresMigrationBackend(t, ctx)
	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect(backend.gooseDialect))
	require.NoError(t, goose.UpToContext(
		ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
	))
	ensureCHG014PostgresRepositoryFixture(t, ctx, backend)

	repoA := NewHealthRepository(backend.db, DialectPostgres)
	repoB := NewHealthRepository(backend.db, DialectPostgres)
	const path = "movies/postgres-claim-ownership.mkv"
	priorError := "prior PostgreSQL evidence"
	_, err := repoA.db.ExecContext(ctx, `
		INSERT INTO file_health
			(file_path, status, retry_count, max_retries, last_error, scheduled_check_at)
		VALUES (?, ?, 2, 3, ?, ?)
	`, path, HealthStatusPending, priorError, time.Now().UTC().Add(-time.Minute))
	require.NoError(t, err)

	staleSelection, err := repoA.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, staleSelection)
	ownerA, err := repoA.ClaimFilesCheckingBulk(ctx, []*FileHealth{staleSelection})
	require.NoError(t, err)
	require.Len(t, ownerA, 1)

	newerEvidence := "newer PostgreSQL owner deferred the check"
	require.NoError(t, repoA.PublishClaimedHealthStatusBulk(ctx, ownerA, []HealthStatusUpdate{{
		Type:             UpdateTypeInconclusive,
		Status:           HealthStatusPending,
		FilePath:         path,
		ErrorMessage:     &newerEvidence,
		ScheduledCheckAt: time.Now().UTC().Add(2 * time.Hour),
	}}))
	published, err := repoA.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.Equal(t, HealthStatusPending, published.Status)

	staleClaim, err := repoB.ClaimFilesCheckingBulk(ctx, []*FileHealth{staleSelection})
	require.NoError(t, err)
	require.Empty(t, staleClaim)
	current, err := repoB.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.Equal(t, published, current)

	ownerB, err := repoB.ClaimFilesCheckingBulk(ctx, []*FileHealth{published})
	require.NoError(t, err)
	require.Len(t, ownerB, 1)
	require.Greater(t, ownerB[0].ClaimGeneration, ownerA[0].ClaimGeneration)
	require.Error(t, repoA.PublishClaimedHealthStatusBulk(ctx, ownerA, []HealthStatusUpdate{{
		Type:             UpdateTypeHealthy,
		Status:           HealthStatusHealthy,
		FilePath:         path,
		ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
	}}))
	require.NoError(t, repoA.ReleaseClaimedHealthRows(ctx, ownerA))
	current, err = repoA.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.Equal(t, ownerB[0], current, "stale publication and release must preserve the newer owner")

	require.NoError(t, repoB.ReleaseClaimedHealthRows(ctx, ownerB))
	released, err := repoB.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.Equal(t, HealthStatusPending, released.Status)
	require.Equal(t, ownerB[0].ClaimGeneration, released.ClaimGeneration)
	require.Equal(t, 2, released.RetryCount)
	require.Equal(t, &newerEvidence, released.LastError)
	require.NotNil(t, released.ScheduledCheckAt)
	require.False(t, released.ScheduledCheckAt.After(time.Now().UTC().Add(time.Second)))
}
