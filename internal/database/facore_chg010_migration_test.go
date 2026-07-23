package database

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const facoreCHG010GapID = "facore-chg010-gap"

func seedFACORECHG010GapPauseState(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	createdAt := time.Unix(1_701_000_000, 0).UTC()
	confirmedAt := createdAt.Add(time.Minute)
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := backend.dialectAwareSQL.ExecContext(ctx, query, args...)
		require.NoError(t, err)
	}

	exec(`UPDATE health_runs SET status = ?, pause_requested = ? WHERE id = ?`,
		"paused", true, seed.runID)
	exec(`
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, status,
			 created_at, confirmed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, facoreCHG010GapID, seed.revisionID, "confirmed_unusable", int64(4), int64(2),
		"active", createdAt, confirmedAt)
	exec(`
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, facoreCHG010GapID, seed.providerID, int64(1), "corrupt", int64(2), confirmedAt)
}

func assertFACORECHG010GapPauseState(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	var (
		runStatus, gapKind, gapStatus, cause, providerID string
		pauseRequested                                   bool
		startSegment, segmentCount                       int64
		providerGeneration, confirmationCount            int64
		createdAt, gapConfirmedAt, causeConfirmedAt      time.Time
		clearedAt                                        *time.Time
	)
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT hr.status, hr.pause_requested,
		       g.kind, g.start_segment, g.segment_count, g.status,
		       g.created_at, g.confirmed_at, g.cleared_at,
		       c.provider_id, c.provider_generation, c.cause,
		       c.confirmation_count, c.confirmed_at
		FROM health_runs hr
		JOIN health_gap_ranges g ON g.file_revision_id = hr.file_revision_id
		JOIN health_gap_provider_causes c ON c.gap_id = g.id
		WHERE hr.id = ? AND g.id = ?
	`, seed.runID, facoreCHG010GapID).Scan(
		&runStatus, &pauseRequested,
		&gapKind, &startSegment, &segmentCount, &gapStatus,
		&createdAt, &gapConfirmedAt, &clearedAt,
		&providerID, &providerGeneration, &cause,
		&confirmationCount, &causeConfirmedAt,
	))

	wantCreatedAt := time.Unix(1_701_000_000, 0).UTC()
	wantConfirmedAt := wantCreatedAt.Add(time.Minute)
	assert.Equal(t, "paused", runStatus)
	assert.True(t, pauseRequested)
	assert.Equal(t, "confirmed_unusable", gapKind)
	assert.Equal(t, int64(4), startSegment)
	assert.Equal(t, int64(2), segmentCount)
	assert.Equal(t, "active", gapStatus)
	assert.True(t, createdAt.Equal(wantCreatedAt))
	assert.True(t, gapConfirmedAt.Equal(wantConfirmedAt))
	assert.Nil(t, clearedAt)
	assert.Equal(t, seed.providerID, providerID)
	assert.Equal(t, int64(1), providerGeneration)
	assert.Equal(t, "corrupt", cause)
	assert.Equal(t, int64(2), confirmationCount)
	assert.True(t, causeConfirmedAt.Equal(wantConfirmedAt))
}

func assertFACORECHG010GapKeysRemainSufficient(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	createdAt := time.Unix(1_701_000_000, 0).UTC()
	_, err := backend.dialectAwareSQL.ExecContext(ctx, `
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "facore-chg010-duplicate-gap", seed.revisionID, "confirmed_unusable",
		int64(4), int64(2), "active", createdAt)
	require.Error(t, err,
		"the existing natural gap key must reject a second identity for the same range")

	_, err = backend.dialectAwareSQL.ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, cause, confirmation_count)
		VALUES (?, ?, ?, ?, ?)
	`, facoreCHG010GapID, seed.providerID, int64(1), "absent", int64(3))
	require.Error(t, err,
		"the existing cause primary key must reject a duplicate provider generation")
	assertFACORECHG010GapPauseState(t, ctx, backend, seed)
}

func TestFACORECHG010Migrations035And036RemainImmutable(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantSHA256 string
	}{
		{"sqlite_035", "migrations/sqlite/035_add_durable_health_state.sql", "f25566918481a10226ece359087b6da08c238d7ccf969ebb81a3a4c7be70d23b"},
		{"sqlite_036", "migrations/sqlite/036_add_health_run_progress_identity.sql", "bbcb02366efdf8b93c1685fb8990faf95ac8569e096c023eb8dbe1e0c8e89a87"},
		{"postgres_035", "migrations/postgres/035_add_durable_health_state.sql", "575b419695ff5dceb28b3cf459066741e43eabebbf6083005205162cbaf48282"},
		{"postgres_036", "migrations/postgres/036_add_health_run_progress_identity.sql", "ca8cf5a284ed9ad4c0fb72d37cf06b745d9eb231c5a06970d524ecaba73d4511"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents, err := embedMigrations.ReadFile(test.path)
			require.NoError(t, err)
			assert.Equal(t, test.wantSHA256, fmt.Sprintf("%x", sha256.Sum256(contents)),
				"migrations 035 and 036 are immutable CHG-010 controls")
		})
	}
}

func TestFACORECHG010PopulatedGapPauseStateSurvives036RoundTrip(t *testing.T) {
	forEachFACORECHG009MigrationBackend(t, func(
		t *testing.T,
		ctx context.Context,
		backend facoreCHG009MigrationBackend,
	) {
		require.NoError(t, goose.UpToContext(ctx, backend.db, backend.migrationsDir, 35))
		require.Equal(t, int64(35), facoreCHG009DatabaseVersion(t, ctx, backend))
		seed := seedFACORECHG009Populated035(t, ctx, backend)
		seedFACORECHG010GapPauseState(t, ctx, backend, seed)
		assertFACORECHG010GapPauseState(t, ctx, backend, seed)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend), "CHG-010 round trip must stop at 036")
		assertFACORECHG010GapPauseState(t, ctx, backend, seed)

		require.NoError(t, goose.DownToContext(ctx, backend.db, backend.migrationsDir, 35))
		require.Equal(t, int64(35), facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG010GapPauseState(t, ctx, backend, seed)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG010GapPauseState(t, ctx, backend, seed)

		setAndAssertFACORECHG009IntegrityState(t, ctx, backend, seed)
		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		), "reapplying migration 036 must be a no-op")
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend), "CHG-010 round trip must remain at 036")
		assertFACORECHG010GapPauseState(t, ctx, backend, seed)
		assertFACORECHG009IntegrityState(t, ctx, backend, seed)
		assertFACORECHG010GapKeysRemainSufficient(t, ctx, backend, seed)
	})
}
