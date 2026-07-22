package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const facoreCHG009MigrationVersion int64 = 36

type facoreCHG009MigrationBackend struct {
	dialect         Dialect
	gooseDialect    string
	migrationsDir   string
	db              *sql.DB
	dialectAwareSQL *dialectAwareDB
}

type facoreCHG009ColumnMetadata struct {
	dataType     string
	notNull      bool
	defaultValue sql.NullString
}

type facoreCHG009Seed struct {
	filePath        string
	revisionID      string
	providerID      string
	snapshotID      string
	runID           string
	chunkID         string
	runStage        string
	chunkStage      string
	commitDigest    string
	cursorSegment   int64
	totalSegments   int64
	segmentStart    int64
	segmentCount    int64
	testedBitmap    []byte
	presentBitmap   []byte
	absentBitmap    []byte
	corruptBitmap   []byte
	temporaryBitmap []byte
	inconclusive    []byte
}

func forEachFACORECHG009MigrationBackend(
	t *testing.T,
	test func(*testing.T, context.Context, facoreCHG009MigrationBackend),
) {
	t.Helper()
	for _, name := range []string{"sqlite", "postgres"} {
		name := name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			var backend facoreCHG009MigrationBackend
			if name == "postgres" {
				backend = newFACORECHG009PostgresMigrationBackend(t, ctx)
			} else {
				backend = newFACORECHG009SQLiteMigrationBackend(t, ctx)
			}
			goose.SetBaseFS(embedMigrations)
			require.NoError(t, goose.SetDialect(backend.gooseDialect))
			test(t, ctx, backend)
		})
	}
}

func newFACORECHG009SQLiteMigrationBackend(
	t *testing.T,
	ctx context.Context,
) facoreCHG009MigrationBackend {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "facore-chg009-migrations.db")
	db, err := sql.Open("sqlite3", databasePath+"?_foreign_keys=on&_busy_timeout=30000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return facoreCHG009MigrationBackend{
		dialect:         DialectSQLite,
		gooseDialect:    "sqlite3",
		migrationsDir:   "migrations/sqlite",
		db:              db,
		dialectAwareSQL: newDialectAwareDB(db, DialectSQLite),
	}
}

func newFACORECHG009PostgresMigrationBackend(
	t *testing.T,
	ctx context.Context,
) facoreCHG009MigrationBackend {
	t.Helper()
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}

	adminConfig, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	admin := stdlib.OpenDB(*adminConfig)
	var db *sql.DB
	schemaCreated := false
	schemaName := "facore_chg009_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	t.Cleanup(func() {
		if db != nil {
			assert.NoError(t, db.Close())
		}
		if schemaCreated {
			_, dropErr := admin.Exec("DROP SCHEMA IF EXISTS " + quotedSchema + " CASCADE")
			assert.NoError(t, dropErr)
		}
		assert.NoError(t, admin.Close())
	})
	require.NoError(t, admin.PingContext(ctx))

	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema)
	require.NoError(t, err)
	schemaCreated = true

	isolatedConfig := adminConfig.Copy()
	if isolatedConfig.RuntimeParams == nil {
		isolatedConfig.RuntimeParams = make(map[string]string)
	}
	isolatedConfig.RuntimeParams["search_path"] = schemaName
	db = stdlib.OpenDB(*isolatedConfig)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	require.NoError(t, db.PingContext(ctx))

	return facoreCHG009MigrationBackend{
		dialect:         DialectPostgres,
		gooseDialect:    "postgres",
		migrationsDir:   "migrations/postgres",
		db:              db,
		dialectAwareSQL: newDialectAwareDB(db, DialectPostgres),
	}
}

func facoreCHG009DatabaseVersion(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
) int64 {
	t.Helper()
	version, err := goose.GetDBVersionContext(ctx, backend.db)
	require.NoError(t, err)
	return version
}

func facoreCHG009ReadColumnMetadata(
	t *testing.T,
	backend facoreCHG009MigrationBackend,
	table string,
	column string,
) facoreCHG009ColumnMetadata {
	t.Helper()
	if backend.dialect == DialectPostgres {
		var metadata facoreCHG009ColumnMetadata
		var nullable string
		err := backend.db.QueryRow(`
			SELECT data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
			  AND column_name = $2
		`, table, column).Scan(&metadata.dataType, &nullable, &metadata.defaultValue)
		require.NoError(t, err)
		metadata.notNull = nullable == "NO"
		return metadata
	}

	rows, err := backend.db.Query(`PRAGMA table_info(` + table + `)`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(
			&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey,
		))
		if name == column {
			return facoreCHG009ColumnMetadata{
				dataType: dataType, notNull: notNull == 1, defaultValue: defaultValue,
			}
		}
	}
	require.NoError(t, rows.Err())
	t.Fatalf("column %s.%s does not exist", table, column)
	return facoreCHG009ColumnMetadata{}
}

func assertFACORECHG009Schema(
	t *testing.T,
	backend facoreCHG009MigrationBackend,
) bool {
	t.Helper()
	runColumns := pr4Columns(t, backend.db, backend.dialect, "health_runs")
	chunkColumns := pr4Columns(t, backend.db, backend.dialect, "health_run_chunks")
	cursorPresent := assert.Contains(t, runColumns, "cursor_sequence",
		"migration 036 must add health_runs.cursor_sequence")
	resolvedPresent := assert.Contains(t, chunkColumns, "resolved_bitmap",
		"migration 036 must add health_run_chunks.resolved_bitmap")
	if !cursorPresent || !resolvedPresent {
		return false
	}

	cursor := facoreCHG009ReadColumnMetadata(t, backend, "health_runs", "cursor_sequence")
	segment := facoreCHG009ReadColumnMetadata(t, backend, "health_runs", "cursor_segment")
	assert.Equal(t, strings.ToLower(segment.dataType), strings.ToLower(cursor.dataType),
		"cursor_sequence must have the same integer width as cursor_segment")
	assert.True(t, cursor.notNull, "health_runs.cursor_sequence must reject NULL")
	if assert.True(t, cursor.defaultValue.Valid,
		"health_runs.cursor_sequence must declare a default") {
		assert.Equal(t, "0", strings.TrimSpace(cursor.defaultValue.String),
			"health_runs.cursor_sequence must default to zero")
	}

	resolved := facoreCHG009ReadColumnMetadata(t, backend, "health_run_chunks", "resolved_bitmap")
	wantResolvedType := "blob"
	if backend.dialect == DialectPostgres {
		wantResolvedType = "bytea"
	}
	assert.Equal(t, wantResolvedType, strings.ToLower(resolved.dataType))
	assert.False(t, resolved.notNull, "health_run_chunks.resolved_bitmap must remain nullable")
	return true
}

func assertFACORECHG009ColumnsAbsentAt035(
	t *testing.T,
	backend facoreCHG009MigrationBackend,
) {
	t.Helper()
	runColumns := pr4Columns(t, backend.db, backend.dialect, "health_runs")
	chunkColumns := pr4Columns(t, backend.db, backend.dialect, "health_run_chunks")
	assert.Contains(t, runColumns, "cursor_segment", "migration 035 control is incomplete")
	assert.Contains(t, chunkColumns, "tested_bitmap", "migration 035 control is incomplete")
	assert.NotContains(t, runColumns, "cursor_sequence",
		"the immutable migration 035 must not be rewritten with cursor_sequence")
	assert.NotContains(t, chunkColumns, "resolved_bitmap",
		"the immutable migration 035 must not be rewritten with resolved_bitmap")
}

func seedFACORECHG009Populated035(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
) facoreCHG009Seed {
	t.Helper()
	seed := facoreCHG009Seed{
		filePath:        "facore/chg009/populated-035.mkv",
		revisionID:      "facore-chg009-revision",
		providerID:      "facore-chg009-provider",
		snapshotID:      "facore-chg009-snapshot",
		runID:           "facore-chg009-run",
		chunkID:         "facore-chg009-chunk",
		runStage:        "provider_scan",
		chunkStage:      "provider_scan",
		commitDigest:    "sha256:facore-chg009-existing-commit",
		cursorSegment:   3,
		totalSegments:   8,
		segmentStart:    2,
		segmentCount:    4,
		testedBitmap:    []byte{0x0f},
		presentBitmap:   []byte{0x03},
		absentBitmap:    []byte{0x04},
		corruptBitmap:   []byte{0x00},
		temporaryBitmap: []byte{0x08},
		inconclusive:    []byte{0x00},
	}
	now := time.Unix(1_700_900_000, 0).UTC()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := backend.dialectAwareSQL.ExecContext(ctx, query, args...)
		require.NoError(t, err)
	}

	exec(`INSERT INTO file_health (id, file_path, status) VALUES (?, ?, ?)`,
		int64(9009), seed.filePath, "pending")
	exec(`
		INSERT INTO health_file_revisions
			(id, file_health_id, layout_fingerprint, virtual_size, segment_count, created_at, activated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, seed.revisionID, int64(9009), "sha256:facore-chg009-layout", int64(8192), seed.totalSegments, now, now)
	exec(`
		INSERT INTO health_providers
			(id, display_name, role, configured_order, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, seed.providerID, "FACORE CHG009 provider", "primary", 0, now, now)
	exec(`
		INSERT INTO health_provider_generations
			(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, seed.providerID, int64(1), "facore-chg009.invalid", 119, "migration-test",
		"sha256:facore-chg009-provider", now)
	exec(`INSERT INTO health_provider_snapshots (id, created_at) VALUES (?, ?)`, seed.snapshotID, now)
	exec(`
		INSERT INTO health_provider_snapshot_entries
			(snapshot_id, provider_id, provider_generation, role, configured_order)
		VALUES (?, ?, ?, ?, ?)
	`, seed.snapshotID, seed.providerID, int64(1), "primary", 0)
	exec(`
		INSERT INTO health_runs
			(id, file_revision_id, provider_snapshot_id, trigger, mode, total_segments,
			 stage, cursor_segment, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, seed.runID, seed.revisionID, seed.snapshotID, "scheduled", "full", seed.totalSegments,
		seed.runStage, seed.cursorSegment, now, now)
	exec(`
		INSERT INTO health_run_chunks
			(id, run_id, provider_id, provider_generation, stage, observation_kind,
			 segment_start, segment_count, tested_bitmap, present_bitmap, absent_bitmap,
			 corrupt_bitmap, temporary_bitmap, inconclusive_bitmap, commit_digest,
			 fencing_token, committed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, seed.chunkID, seed.runID, seed.providerID, int64(1), seed.chunkStage, "validated_body",
		seed.segmentStart, seed.segmentCount, seed.testedBitmap, seed.presentBitmap, seed.absentBitmap,
		seed.corruptBitmap, seed.temporaryBitmap, seed.inconclusive, seed.commitDigest, int64(7), now)
	return seed
}

func assertFACORECHG009LegacyRowsPreserved(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	var filePath, revisionID, runStage string
	var cursorSegment, totalSegments int64
	err := backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT fh.file_path, hr.file_revision_id, hr.stage, hr.cursor_segment, hr.total_segments
		FROM health_runs hr
		JOIN health_file_revisions hfr ON hfr.id = hr.file_revision_id
		JOIN file_health fh ON fh.id = hfr.file_health_id
		WHERE hr.id = ?
	`, seed.runID).Scan(&filePath, &revisionID, &runStage, &cursorSegment, &totalSegments)
	require.NoError(t, err)
	assert.Equal(t, seed.filePath, filePath)
	assert.Equal(t, seed.revisionID, revisionID)
	assert.Equal(t, seed.runStage, runStage)
	assert.Equal(t, seed.cursorSegment, cursorSegment)
	assert.Equal(t, seed.totalSegments, totalSegments)

	var providerID, chunkStage, digest string
	var segmentStart, segmentCount int64
	var tested, present, absent, corrupt, temporary, inconclusive []byte
	err = backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT provider_id, stage, commit_digest, segment_start, segment_count,
		       tested_bitmap, present_bitmap, absent_bitmap, corrupt_bitmap,
		       temporary_bitmap, inconclusive_bitmap
		FROM health_run_chunks
		WHERE id = ? AND run_id = ?
	`, seed.chunkID, seed.runID).Scan(
		&providerID, &chunkStage, &digest, &segmentStart, &segmentCount,
		&tested, &present, &absent, &corrupt, &temporary, &inconclusive,
	)
	require.NoError(t, err)
	assert.Equal(t, seed.providerID, providerID)
	assert.Equal(t, seed.chunkStage, chunkStage)
	assert.Equal(t, seed.commitDigest, digest)
	assert.Equal(t, seed.segmentStart, segmentStart)
	assert.Equal(t, seed.segmentCount, segmentCount)
	assert.Equal(t, seed.testedBitmap, tested)
	assert.Equal(t, seed.presentBitmap, present)
	assert.Equal(t, seed.absentBitmap, absent)
	assert.Equal(t, seed.corruptBitmap, corrupt)
	assert.Equal(t, seed.temporaryBitmap, temporary)
	assert.Equal(t, seed.inconclusive, inconclusive)
}

func assertFACORECHG009UpgradeDefaultsAndConstraint(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	var cursorSequence int64
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx,
		`SELECT cursor_sequence FROM health_runs WHERE id = ?`, seed.runID,
	).Scan(&cursorSequence))
	assert.Zero(t, cursorSequence, "existing health runs must receive cursor_sequence zero")

	var resolvedBitmap []byte
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx,
		`SELECT resolved_bitmap FROM health_run_chunks WHERE id = ?`, seed.chunkID,
	).Scan(&resolvedBitmap))
	assert.Nil(t, resolvedBitmap, "existing health chunks must receive a NULL resolved_bitmap")

	_, err := backend.dialectAwareSQL.ExecContext(ctx,
		`UPDATE health_runs SET cursor_sequence = ? WHERE id = ?`, int64(-1), seed.runID)
	assert.Error(t, err, "health_runs.cursor_sequence must reject negative values")
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx,
		`SELECT cursor_sequence FROM health_runs WHERE id = ?`, seed.runID,
	).Scan(&cursorSequence))
	assert.Zero(t, cursorSequence, "a rejected negative cursor sequence must not alter the row")
}

func setAndAssertFACORECHG009IntegrityState(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	wantSequence := int64(11)
	wantBitmap := []byte{0x0d}
	_, err := backend.dialectAwareSQL.ExecContext(ctx,
		`UPDATE health_runs SET cursor_sequence = ? WHERE id = ?`, wantSequence, seed.runID)
	require.NoError(t, err)
	_, err = backend.dialectAwareSQL.ExecContext(ctx,
		`UPDATE health_run_chunks SET resolved_bitmap = ? WHERE id = ?`, wantBitmap, seed.chunkID)
	require.NoError(t, err)

	assertFACORECHG009IntegrityState(t, ctx, backend, seed)
}

func assertFACORECHG009IntegrityState(
	t *testing.T,
	ctx context.Context,
	backend facoreCHG009MigrationBackend,
	seed facoreCHG009Seed,
) {
	t.Helper()
	var gotSequence int64
	var gotBitmap []byte
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT hr.cursor_sequence, hrc.resolved_bitmap
		FROM health_runs hr
		JOIN health_run_chunks hrc ON hrc.run_id = hr.id
		WHERE hr.id = ? AND hrc.id = ?
	`, seed.runID, seed.chunkID).Scan(&gotSequence, &gotBitmap))
	assert.Equal(t, int64(11), gotSequence)
	assert.Equal(t, []byte{0x0d}, gotBitmap)
}

func TestFACORECHG009Migration035IsImmutable(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantSHA256 string
	}{
		{
			name:       "sqlite",
			path:       "migrations/sqlite/035_add_durable_health_state.sql",
			wantSHA256: "f25566918481a10226ece359087b6da08c238d7ccf969ebb81a3a4c7be70d23b",
		},
		{
			name:       "postgres",
			path:       "migrations/postgres/035_add_durable_health_state.sql",
			wantSHA256: "575b419695ff5dceb28b3cf459066741e43eabebbf6083005205162cbaf48282",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			contents, err := embedMigrations.ReadFile(test.path)
			require.NoError(t, err)
			got := fmt.Sprintf("%x", sha256.Sum256(contents))
			assert.Equal(t, test.wantSHA256, got,
				"migration 035 is immutable; add run/chunk integrity columns in migration 036")
		})
	}
}

func TestFACORECHG009Migration035SchemaControl(t *testing.T) {
	forEachFACORECHG009MigrationBackend(t, func(
		t *testing.T,
		ctx context.Context,
		backend facoreCHG009MigrationBackend,
	) {
		require.NoError(t, goose.UpToContext(ctx, backend.db, backend.migrationsDir, 35))
		require.Equal(t, int64(35), facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG009ColumnsAbsentAt035(t, backend)
	})
}

func TestFACORECHG009Fresh036Schema(t *testing.T) {
	forEachFACORECHG009MigrationBackend(t, func(
		t *testing.T,
		ctx context.Context,
		backend facoreCHG009MigrationBackend,
	) {
		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		assert.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend),
			"latest schema must include immutable forward migration 036")
		if !assertFACORECHG009Schema(t, backend) {
			return
		}

		seed := seedFACORECHG009Populated035(t, ctx, backend)
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)
		assertFACORECHG009UpgradeDefaultsAndConstraint(t, ctx, backend, seed)
		setAndAssertFACORECHG009IntegrityState(t, ctx, backend, seed)
	})
}

func TestFACORECHG009Populated035To036RoundTrip(t *testing.T) {
	forEachFACORECHG009MigrationBackend(t, func(
		t *testing.T,
		ctx context.Context,
		backend facoreCHG009MigrationBackend,
	) {
		require.NoError(t, goose.UpToContext(ctx, backend.db, backend.migrationsDir, 35))
		require.Equal(t, int64(35), facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG009ColumnsAbsentAt035(t, backend)
		seed := seedFACORECHG009Populated035(t, ctx, backend)
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		assert.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend),
			"the populated version-35 database must advance through migration 036")
		if !assertFACORECHG009Schema(t, backend) {
			return
		}
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)
		assertFACORECHG009UpgradeDefaultsAndConstraint(t, ctx, backend, seed)

		require.NoError(t, goose.DownToContext(ctx, backend.db, backend.migrationsDir, 35))
		require.Equal(t, int64(35), facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG009ColumnsAbsentAt035(t, backend)
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)

		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		))
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		require.True(t, assertFACORECHG009Schema(t, backend))
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)
		assertFACORECHG009UpgradeDefaultsAndConstraint(t, ctx, backend, seed)

		setAndAssertFACORECHG009IntegrityState(t, ctx, backend, seed)
		require.NoError(t, goose.UpToContext(
			ctx, backend.db, backend.migrationsDir, facoreCHG009MigrationVersion,
		), "reapplying migration 036 must be a no-op")
		require.Equal(t, facoreCHG009MigrationVersion,
			facoreCHG009DatabaseVersion(t, ctx, backend))
		assertFACORECHG009LegacyRowsPreserved(t, ctx, backend, seed)
		assertFACORECHG009IntegrityState(t, ctx, backend, seed)
	})
}
