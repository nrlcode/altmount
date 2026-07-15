package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5MigrationAuditFixture struct {
	filePath     string
	revisionID   string
	providerID   string
	oldGapID     string
	activeGapID  string
	syntheticID  string
	createdAt    time.Time
	fileHealthID int64
}

func newPR5MigrationAuditFixture() pr5MigrationAuditFixture {
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	return pr5MigrationAuditFixture{
		filePath:    "library/pr5-migration-audit-" + token + ".mkv",
		revisionID:  "pr5-migration-revision-" + token,
		providerID:  "pr5-migration-provider-" + token,
		oldGapID:    "pr5-migration-gap-old-" + token,
		activeGapID: "pr5-migration-gap-active-" + token,
		syntheticID: "pr5-migration-synthetic-" + token,
		createdAt:   time.Unix(1_713_000_000, 0).UTC(),
	}
}

func pr5MigrationDirectory(dialect Dialect) (string, string) {
	if dialect == DialectPostgres {
		return "postgres", "migrations/postgres"
	}
	return "sqlite3", "migrations/sqlite"
}

func setPR5GooseDialect(t *testing.T, dialect Dialect) string {
	t.Helper()
	goose.SetBaseFS(embedMigrations)
	gooseDialect, directory := pr5MigrationDirectory(dialect)
	require.NoError(t, goose.SetDialect(gooseDialect))
	return directory
}

func insertPopulatedPR4MigrationFixture(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fixture *pr5MigrationAuditFixture,
) {
	t.Helper()
	q := newDialectAwareDB(db, dialect)
	_, err := q.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, created_at, updated_at)
		VALUES (?, 'pending', ?, ?)
	`, fixture.filePath, fixture.createdAt, fixture.createdAt)
	require.NoError(t, err)
	require.NoError(t, q.QueryRowContext(ctx,
		`SELECT id FROM file_health WHERE file_path = ?`, fixture.filePath,
	).Scan(&fixture.fileHealthID))

	_, err = q.ExecContext(ctx, `
		INSERT INTO health_file_revisions
			(id, file_health_id, layout_fingerprint, virtual_size, segment_count,
			 active, created_at, activated_at)
		VALUES (?, ?, ?, 1000, 10, TRUE, ?, ?)
	`, fixture.revisionID, fixture.fileHealthID, "sha256:"+fixture.revisionID,
		fixture.createdAt, fixture.createdAt)
	require.NoError(t, err)

	_, err = q.ExecContext(ctx, `
		INSERT INTO health_providers
			(id, display_name, role, configured_order, active, current_generation,
			 tombstoned_at, created_at, updated_at)
		VALUES (?, 'Synthetic audit provider', 'primary', 0, FALSE, 1, ?, ?, ?)
	`, fixture.providerID, fixture.createdAt, fixture.createdAt, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_provider_generations
			(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
		VALUES (?, 1, 'migration-audit.example.invalid', 119, 'synthetic-account', ?, ?)
	`, fixture.providerID, "sha256:"+fixture.providerID, fixture.createdAt)
	require.NoError(t, err)

	_, err = q.ExecContext(ctx, `
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, status,
			 created_at, confirmed_at, cleared_at)
		VALUES (?, ?, 'confirmed_absent', 2, 1, 'cleared', ?, ?, ?)
	`, fixture.oldGapID, fixture.revisionID, fixture.createdAt,
		fixture.createdAt, fixture.createdAt.Add(time.Minute))
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, cause, confirmation_count, confirmed_at)
		VALUES (?, ?, 1, 'absent', 2, ?)
	`, fixture.oldGapID, fixture.providerID, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_synthetic_ranges
			(id, gap_id, file_revision_id, byte_start, byte_end, emitted_at,
			 recovered_at, verified_at)
		VALUES (?, ?, ?, 200, 299, ?, NULL, NULL)
	`, fixture.syntheticID, fixture.oldGapID, fixture.revisionID, fixture.createdAt)
	require.NoError(t, err)
}

func insertActiveRecurringGap(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fixture pr5MigrationAuditFixture,
) {
	t.Helper()
	q := newDialectAwareDB(db, dialect)
	_, err := q.ExecContext(ctx, `
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, episode,
			 status, created_at, confirmed_at, cleared_at)
		VALUES (?, ?, 'confirmed_absent', 2, 1, 2, 'active', ?, ?, NULL)
	`, fixture.activeGapID, fixture.revisionID, fixture.createdAt.Add(2*time.Minute),
		fixture.createdAt.Add(2*time.Minute))
	require.NoError(t, err)
}

func cleanupPR5MigrationAuditFixture(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fixture pr5MigrationAuditFixture,
) {
	t.Helper()
	q := newDialectAwareDB(db, dialect)
	_, err := q.ExecContext(ctx, `DELETE FROM file_health WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		DELETE FROM health_provider_generations WHERE provider_id = ?
	`, fixture.providerID)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `DELETE FROM health_providers WHERE id = ?`, fixture.providerID)
	require.NoError(t, err)
}

func runPR5Migration036SyntheticHistoryRoundTrip(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
) {
	t.Helper()
	ctx := context.Background()
	directory := setPR5GooseDialect(t, dialect)
	fixture := newPR5MigrationAuditFixture()

	require.NoError(t, goose.DownToContext(ctx, db, directory, 35))
	insertPopulatedPR4MigrationFixture(t, ctx, db, dialect, &fixture)
	require.NoError(t, goose.UpContext(ctx, db, directory))

	q := newDialectAwareDB(db, dialect)
	var episode int64
	require.NoError(t, q.QueryRowContext(ctx,
		`SELECT episode FROM health_gap_ranges WHERE id = ?`, fixture.oldGapID,
	).Scan(&episode))
	assert.Equal(t, int64(1), episode, "populated PR4 gaps must backfill as episode one")
	var oldSyntheticGap string
	require.NoError(t, q.QueryRowContext(ctx,
		`SELECT gap_id FROM health_synthetic_ranges WHERE id = ?`, fixture.syntheticID,
	).Scan(&oldSyntheticGap))
	assert.Equal(t, fixture.oldGapID, oldSyntheticGap)

	insertActiveRecurringGap(t, ctx, db, dialect, fixture)
	require.NoError(t, goose.DownToContext(ctx, db, directory, 35))

	// Capture the downgraded result before restoring version 36. Restoration is
	// unconditional so an optional shared PostgreSQL test database is never left
	// on the old schema merely because an assertion found data loss.
	var retainedGapID string
	gapErr := q.QueryRowContext(ctx, `
		SELECT id FROM health_gap_ranges
		WHERE file_revision_id = ? AND kind = 'confirmed_absent'
		  AND start_segment = 2 AND segment_count = 1
	`, fixture.revisionID).Scan(&retainedGapID)
	var retainedSyntheticGap string
	syntheticErr := q.QueryRowContext(ctx,
		`SELECT gap_id FROM health_synthetic_ranges WHERE id = ?`, fixture.syntheticID,
	).Scan(&retainedSyntheticGap)

	require.NoError(t, goose.UpContext(ctx, db, directory))
	require.NoError(t, gapErr)
	assert.Equal(t, fixture.activeGapID, retainedGapID,
		"PR4 rollback should retain the currently active exact-range episode")
	require.NoError(t, syntheticErr,
		"rollback must not erase immutable unrecovered synthetic-output history")
	assert.Equal(t, fixture.activeGapID, retainedSyntheticGap,
		"synthetic history from a discarded episode must be rebound to the exact retained range")

	var restoredSyntheticGap string
	require.NoError(t, q.QueryRowContext(ctx,
		`SELECT gap_id FROM health_synthetic_ranges WHERE id = ?`, fixture.syntheticID,
	).Scan(&restoredSyntheticGap))
	assert.Equal(t, fixture.activeGapID, restoredSyntheticGap,
		"a subsequent 35 to 36 upgrade must preserve the rebound synthetic history")
	cleanupPR5MigrationAuditFixture(t, ctx, db, dialect, fixture)
}

func runPR5Migration036GapCauseActivationBackfill(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
) {
	t.Helper()
	ctx := context.Background()
	directory := setPR5GooseDialect(t, dialect)
	fixture := newPR5MigrationAuditFixture()

	require.NoError(t, goose.DownToContext(ctx, db, directory, 35))
	insertPopulatedPR4MigrationFixture(t, ctx, db, dialect, &fixture)
	require.NoError(t, goose.UpContext(ctx, db, directory))

	q := newDialectAwareDB(db, dialect)
	var activationEpoch int64
	err := q.QueryRowContext(ctx, `
		SELECT provider_activation_epoch
		FROM health_gap_provider_causes
		WHERE gap_id = ? AND provider_id = ? AND provider_generation = 1
	`, fixture.oldGapID, fixture.providerID).Scan(&activationEpoch)
	require.NoError(t, err,
		"gap causes need an activation boundary so reactivation cannot reuse old confirmation counts")
	assert.Equal(t, int64(1), activationEpoch)
	cleanupPR5MigrationAuditFixture(t, ctx, db, dialect, fixture)
}

func insertPR5EpochOneObservationHistory(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	dialect Dialect,
	fixture pr5MigrationAuditFixture,
) {
	t.Helper()
	q := newDialectAwareDB(db, dialect)
	snapshotID := fixture.providerID + "-snapshot"
	runID := fixture.providerID + "-run"
	chunkID := fixture.providerID + "-chunk"

	_, err := q.ExecContext(ctx, `
		UPDATE health_providers
		SET active = TRUE, activation_epoch = 1, activated_at = ?
		WHERE id = ?
	`, fixture.createdAt, fixture.providerID)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_provider_snapshots (id, created_at) VALUES (?, ?)
	`, snapshotID, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_provider_snapshot_entries
			(snapshot_id, provider_id, provider_generation,
			 provider_activation_epoch, role, configured_order)
		VALUES (?, ?, 1, 1, 'primary', 0)
	`, snapshotID, fixture.providerID)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_runs
			(id, file_revision_id, provider_snapshot_id, trigger, mode, status,
			 total_segments, created_at, updated_at)
		VALUES (?, ?, ?, 'migration_audit', 'observation', 'completed', 10, ?, ?)
	`, runID, fixture.revisionID, snapshotID, fixture.createdAt, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_run_chunks
			(id, run_id, provider_id, provider_generation, provider_activation_epoch,
			 stage, observation_kind, segment_start, segment_count,
			 tested_bitmap, present_bitmap, absent_bitmap, corrupt_bitmap,
			 temporary_bitmap, inconclusive_bitmap, resolved_bitmap,
			 commit_digest, fencing_token, committed_at)
		VALUES (?, ?, ?, 1, 1, 'provider_scan', 'stat', 2, 1,
			 ?, ?, ?, ?, ?, ?, ?, 'migration-audit-digest', 1, ?)
	`, chunkID, runID, fixture.providerID,
		[]byte{1}, []byte{0}, []byte{1}, []byte{0}, []byte{0}, []byte{0}, []byte{1},
		fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_provider_coverage
			(id, file_revision_id, provider_id, provider_generation,
			 provider_activation_epoch, observation_kind, segment_start,
			 segment_count, tested_bitmap, present_bitmap, resolved_bitmap,
			 source_chunk_id, observed_at)
		VALUES (?, ?, ?, 1, 1, 'stat', 2, 1, ?, ?, ?, ?, ?)
	`, fixture.providerID+"-coverage", fixture.revisionID, fixture.providerID,
		[]byte{1}, []byte{0}, []byte{1}, chunkID, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_segment_exceptions
			(file_revision_id, provider_id, provider_generation,
			 provider_activation_epoch, segment_index, outcome,
			 source_chunk_id, observed_at)
		VALUES (?, ?, 1, 1, 2, 'hard_absence', ?, ?)
	`, fixture.revisionID, fixture.providerID, chunkID, fixture.createdAt)
	require.NoError(t, err)
	_, err = q.ExecContext(ctx, `
		INSERT INTO health_confirmation_events
			(idempotency_key, source_chunk_id, file_revision_id, provider_id,
			 provider_generation, provider_activation_epoch, segment_index,
			 cause, observed_at)
		VALUES (?, ?, ?, ?, 1, 1, 2, 'absent', ?)
	`, fixture.providerID+"-confirmation", chunkID, fixture.revisionID,
		fixture.providerID, fixture.createdAt)
	require.NoError(t, err)

	_, err = q.ExecContext(ctx, `
		UPDATE health_providers
		SET activation_epoch = 2, activated_at = ?
		WHERE id = ?
	`, fixture.createdAt.Add(time.Hour), fixture.providerID)
	require.NoError(t, err)
}

func runPR5Migration036ActivationEpochRoundTrip(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
) {
	t.Helper()
	ctx := context.Background()
	directory := setPR5GooseDialect(t, dialect)
	fixture := newPR5MigrationAuditFixture()

	require.NoError(t, goose.DownToContext(ctx, db, directory, 35))
	insertPopulatedPR4MigrationFixture(t, ctx, db, dialect, &fixture)
	require.NoError(t, goose.UpContext(ctx, db, directory))
	insertPR5EpochOneObservationHistory(t, ctx, db, dialect, fixture)

	// The PR4 schema cannot represent activation epochs. A downgrade must leave
	// retained epoch-one history structurally older than the provider identity
	// that becomes current after the subsequent PR5 upgrade.
	require.NoError(t, goose.DownToContext(ctx, db, directory, 35))
	require.NoError(t, goose.UpContext(ctx, db, directory))

	q := newDialectAwareDB(db, dialect)
	var currentGeneration, currentActivationEpoch int64
	require.NoError(t, q.QueryRowContext(ctx, `
		SELECT current_generation, activation_epoch
		FROM health_providers WHERE id = ?
	`, fixture.providerID).Scan(&currentGeneration, &currentActivationEpoch))
	assert.Equal(t, int64(2), currentGeneration,
		"downgrading an epoch-two provider must conservatively advance its representable identity")
	assert.Equal(t, int64(1), currentActivationEpoch,
		"a re-upgrade begins the replacement generation at its first activation epoch")

	rows, err := q.QueryContext(ctx, `
		SELECT evidence.kind, COUNT(*),
		       SUM(CASE
		           WHEN evidence.provider_generation = provider.current_generation
		            AND evidence.provider_activation_epoch = provider.activation_epoch
		           THEN 1 ELSE 0 END)
		FROM (
			SELECT 'coverage' AS kind, provider_id, provider_generation,
			       provider_activation_epoch
			FROM health_provider_coverage
			UNION ALL
			SELECT 'absence', provider_id, provider_generation,
			       provider_activation_epoch
			FROM health_segment_exceptions
			UNION ALL
			SELECT 'confirmation', provider_id, provider_generation,
			       provider_activation_epoch
			FROM health_confirmation_events
			UNION ALL
			SELECT 'gap_cause', provider_id, provider_generation,
			       provider_activation_epoch
			FROM health_gap_provider_causes
		) evidence
		JOIN health_providers provider ON provider.id = evidence.provider_id
		WHERE evidence.provider_id = ?
		GROUP BY evidence.kind
	`, fixture.providerID)
	require.NoError(t, err)
	defer rows.Close()
	seenKinds := make(map[string]struct{})
	for rows.Next() {
		var kind string
		var retained, current int
		require.NoError(t, rows.Scan(&kind, &retained, &current))
		seenKinds[kind] = struct{}{}
		assert.Equal(t, 1, retained, kind+" epoch-one history should remain auditable")
		assert.Zero(t, current, kind+" epoch-one history must not become current again")
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, map[string]struct{}{
		"coverage": {}, "absence": {}, "confirmation": {}, "gap_cause": {},
	}, seenKinds)

	_, err = q.ExecContext(ctx, `
		DELETE FROM health_provider_snapshot_entries WHERE provider_id = ?
	`, fixture.providerID)
	require.NoError(t, err)
	cleanupPR5MigrationAuditFixture(t, ctx, db, dialect, fixture)
	_, err = q.ExecContext(ctx, `
		DELETE FROM health_provider_snapshots WHERE id = ?
	`, fixture.providerID+"-snapshot")
	require.NoError(t, err)
}

func TestPR5SQLiteMigration036PreservesUnrecoveredSyntheticHistoryAcrossEpisodes(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "migration-036-history.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036SyntheticHistoryRoundTrip(t, db.Connection(), DialectSQLite)
}

func TestPR5PostgresMigration036PreservesUnrecoveredSyntheticHistoryAcrossEpisodes(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036SyntheticHistoryRoundTrip(t, db.Connection(), DialectPostgres)
}

func TestPR5SQLiteMigration036BackfillsGapCauseActivationIdentity(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "migration-036-activation.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036GapCauseActivationBackfill(t, db.Connection(), DialectSQLite)
}

func TestPR5PostgresMigration036BackfillsGapCauseActivationIdentity(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036GapCauseActivationBackfill(t, db.Connection(), DialectPostgres)
}

func TestPR5SQLiteMigration036RoundTripDoesNotReactivateOlderProviderEvidence(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "migration-036-epoch-roundtrip.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036ActivationEpochRoundTrip(t, db.Connection(), DialectSQLite)
}

func TestPR5PostgresMigration036RoundTripDoesNotReactivateOlderProviderEvidence(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runPR5Migration036ActivationEpochRoundTrip(t, db.Connection(), DialectPostgres)
}

func normalizedSchemaSQL(value string) string {
	return strings.NewReplacer("\"", "", "`", "").Replace(
		strings.Join(strings.Fields(strings.ToLower(value)), ""),
	)
}

func pr5TableContractSQL(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
	table string,
) string {
	t.Helper()
	if dialect == DialectSQLite {
		var definition string
		require.NoError(t, db.QueryRow(`
			SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?
		`, table).Scan(&definition))
		return normalizedSchemaSQL(definition)
	}
	var definition string
	require.NoError(t, db.QueryRow(`
		SELECT COALESCE(string_agg(pg_get_constraintdef(c.oid), ' ' ORDER BY c.conname), '')
		FROM pg_constraint c
		WHERE c.conrelid = $1::regclass
	`, table).Scan(&definition))
	return normalizedSchemaSQL(definition)
}

func pr5PrimaryKeyColumns(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
	table string,
) []string {
	t.Helper()
	if dialect == DialectSQLite {
		rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
		require.NoError(t, err)
		defer rows.Close()
		type primaryColumn struct {
			position int
			name     string
		}
		var primary []primaryColumn
		for rows.Next() {
			var cid, notNull, primaryPosition int
			var name, columnType string
			var defaultValue any
			require.NoError(t, rows.Scan(
				&cid, &name, &columnType, &notNull, &defaultValue, &primaryPosition,
			))
			if primaryPosition > 0 {
				primary = append(primary, primaryColumn{primaryPosition, name})
			}
		}
		require.NoError(t, rows.Err())
		sort.Slice(primary, func(i, j int) bool { return primary[i].position < primary[j].position })
		result := make([]string, len(primary))
		for i := range primary {
			result[i] = primary[i].name
		}
		return result
	}
	rows, err := db.Query(`
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON kcu.constraint_catalog = tc.constraint_catalog
		 AND kcu.constraint_schema = tc.constraint_schema
		 AND kcu.constraint_name = tc.constraint_name
		WHERE tc.table_schema = current_schema()
		  AND tc.table_name = $1
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`, table)
	require.NoError(t, err)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		result = append(result, name)
	}
	require.NoError(t, rows.Err())
	return result
}

func pr5ColumnDefault(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
	table, column string,
) sql.NullString {
	t.Helper()
	if dialect == DialectPostgres {
		var value sql.NullString
		require.NoError(t, db.QueryRow(`
			SELECT column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
		`, table, column).Scan(&value))
		return value
	}
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var value sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &value, &primaryKey))
		if name == column {
			return value
		}
	}
	require.NoError(t, rows.Err())
	t.Fatalf("column %s.%s does not exist", table, column)
	return sql.NullString{}
}

func pr5IndexDefinition(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
	name string,
) string {
	t.Helper()
	var definition string
	if dialect == DialectPostgres {
		require.NoError(t, db.QueryRow(`
			SELECT indexdef FROM pg_indexes
			WHERE schemaname = current_schema() AND indexname = $1
		`, name).Scan(&definition))
	} else {
		require.NoError(t, db.QueryRow(`
			SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?
		`, name).Scan(&definition))
	}
	// PostgreSQL renders partial predicates with one cosmetic parenthesis pair
	// after WHERE, unlike SQLite. Normalize only that representation so table
	// and foreign-key parentheses remain available to the schema assertions.
	return strings.Replace(normalizedSchemaSQL(definition), "where(", "where", 1)
}

func requirePR5Migration036CriticalSchemaParity(
	t *testing.T,
	db *sql.DB,
	dialect Dialect,
) {
	t.Helper()
	assert.Contains(t, pr4Columns(t, db, dialect, "health_gap_provider_causes"),
		"provider_activation_epoch")
	assert.Equal(t, []string{
		"gap_id", "provider_id", "provider_generation", "provider_activation_epoch",
	}, pr5PrimaryKeyColumns(t, db, dialect, "health_gap_provider_causes"))

	gapSQL := pr5TableContractSQL(t, db, dialect, "health_gap_ranges")
	assert.Contains(t, gapSQL, "episode")
	assert.Contains(t, gapSQL, "episode>=1")

	scheduleSQL := pr5TableContractSQL(t, db, dialect, "health_run_schedule")
	assert.Contains(t, scheduleSQL, "referenceshealth_runs(id)ondeletecascade")
	assert.Contains(t, scheduleSQL,
		"foreignkey(target_provider_id,target_provider_generation)referenceshealth_provider_generations(provider_id,generation)")
	assert.Contains(t, scheduleSQL, "target_provider_activation_epoch")
	assert.Contains(t, scheduleSQL, "priority>=0")
	assert.Contains(t, scheduleSQL, "priority<=2")

	importSQL := pr5TableContractSQL(t, db, dialect, "health_import_validations")
	assert.Contains(t, importSQL, "referencesimport_queue(id)ondeletecascade")
	assert.Contains(t, importSQL, "referenceshealth_file_revisions(id)")
	assert.Contains(t, importSQL, "referenceshealth_runs(id)")
	assert.Contains(t, importSQL, "strict")
	assert.Contains(t, importSQL, "tolerant")
	assert.Contains(t, importSQL, "confirmation_wait")

	activeGapIndex := pr5IndexDefinition(t, db, dialect, "idx_health_gap_ranges_one_active_exact")
	assert.Contains(t, activeGapIndex, "createuniqueindex")
	assert.Contains(t, activeGapIndex, "wherestatus")
	activeScheduleIndex := pr5IndexDefinition(t, db, dialect, "idx_health_run_schedule_active_dedupe")
	assert.Contains(t, activeScheduleIndex, "createuniqueindex")
	assert.Contains(t, activeScheduleIndex, "whereactive")

	episodeDefault := pr5ColumnDefault(t, db, dialect, "health_gap_ranges", "episode")
	assert.True(t, episodeDefault.Valid && strings.Contains(episodeDefault.String, "1"),
		"both backends should expose the same episode-one insertion default")

	_, modelHasActivationEpoch := reflect.TypeOf(GapProviderCause{}).
		FieldByName("ProviderActivationEpoch")
	assert.True(t, modelHasActivationEpoch,
		"the repository model must carry the same activation identity as the schema")
}

func TestPR5SQLiteMigration036CriticalSchemaConstraintsMatchContract(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "migration-036-contract.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR5Migration036CriticalSchemaParity(t, db.Connection(), DialectSQLite)

	rows, err := db.Connection().Query(`PRAGMA foreign_key_check`)
	require.NoError(t, err)
	defer rows.Close()
	assert.False(t, rows.Next(), "applied SQLite migration has a foreign-key violation")
}

func TestPR5PostgresMigration036CriticalSchemaConstraintsMatchContract(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR5Migration036CriticalSchemaParity(t, db.Connection(), DialectPostgres)
}
