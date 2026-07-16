package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertTokenOnlyHealthClaimSchema(t *testing.T, db *sql.DB, dialect Dialect, wantToken bool) {
	t.Helper()
	columns := pr4Columns(t, db, dialect, "file_health")
	assert.Equal(t, wantToken, contains(columns, "health_claim_token"), "health_claim_token presence must match migration state")
	assert.False(t, contains(columns, "health_claim_version"), "health ownership uses globally fresh tokens, never a persistent version column")
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func exerciseHealthClaimRevocation(t *testing.T, db *sql.DB, dialect Dialect, path string) {
	t.Helper()
	placeholder := "?"
	if dialect == DialectPostgres {
		placeholder = "$1"
	}

	cases := []struct {
		name        string
		businessSQL string
		renamed     bool
	}{
		{name: "status", businessSQL: `UPDATE file_health SET status = 'degraded' WHERE file_path = ` + placeholder},
		{name: "file_path", businessSQL: `UPDATE file_health SET file_path = file_path || '.renamed' WHERE file_path = ` + placeholder, renamed: true},
		{name: "library_path", businessSQL: `UPDATE file_health SET library_path = '/replacement/library.mkv' WHERE file_path = ` + placeholder},
		{name: "metadata", businessSQL: `UPDATE file_health SET metadata = '{"business":"replacement"}' WHERE file_path = ` + placeholder},
		{name: "source_nzb_path", businessSQL: `UPDATE file_health SET source_nzb_path = '/replacement/source.nzb' WHERE file_path = ` + placeholder},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rowPath := path + "/" + tc.name
			_, _ = db.Exec(`DELETE FROM file_health WHERE file_path = `+placeholder, rowPath)
			_, _ = db.Exec(`DELETE FROM file_health WHERE file_path = `+placeholder, rowPath+".renamed")
			_, err := db.Exec(`
				INSERT INTO file_health (file_path, status, updated_at)
				VALUES (`+placeholder+`, 'pending', '2001-02-03 04:05:06')
			`, rowPath)
			require.NoError(t, err)

			var beforeUpdated string
			require.NoError(t, db.QueryRow(`SELECT CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, rowPath).Scan(&beforeUpdated))

			_, err = db.Exec(`UPDATE file_health SET status = 'checking', health_claim_token = 'claim-token' WHERE file_path = `+placeholder, rowPath)
			require.NoError(t, err)
			var token sql.NullString
			var afterClaimUpdated string
			require.NoError(t, db.QueryRow(`SELECT health_claim_token, CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, rowPath).Scan(&token, &afterClaimUpdated))
			require.True(t, token.Valid)
			assert.Equal(t, "claim-token", token.String, "installing a checking token must survive timestamp bookkeeping")
			assert.Equal(t, beforeUpdated, afterClaimUpdated, "claim-only bookkeeping must not alter business updated_at")

			result, err := db.Exec(`UPDATE file_health SET health_claim_token = 'rotated-token' WHERE file_path = `+placeholder+` AND health_claim_token = 'claim-token'`, rowPath)
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), affected)
			require.NoError(t, db.QueryRow(`SELECT health_claim_token, CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, rowPath).Scan(&token, &afterClaimUpdated))
			assert.Equal(t, "rotated-token", token.String)
			assert.Equal(t, beforeUpdated, afterClaimUpdated, "token rotation must not alter business updated_at")

			_, err = db.Exec(tc.businessSQL, rowPath)
			require.NoError(t, err)
			currentPath := rowPath
			if tc.renamed {
				currentPath += ".renamed"
			}
			require.NoError(t, db.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = `+placeholder, currentPath).Scan(&token))
			assert.False(t, token.Valid, "%s writes must revoke an existing claim", tc.name)

			// Owner B publishes a complete current state under a fresh token.
			result, err = db.Exec(`
				UPDATE file_health
				SET status = 'healthy', metadata = '{"owner":"b"}',
				    health_claim_token = 'owner-b',
				    scheduled_check_at = '2042-03-04 05:06:07',
				    last_error = 'owner-b-error',
				    error_details = '{"owner":"b","evidence":"complete"}'
				WHERE file_path = `+placeholder+` AND health_claim_token IS NULL
			`, currentPath)
			require.NoError(t, err)
			affected, err = result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), affected)

			result, err = db.Exec(`
				UPDATE file_health
				SET status = 'corrupted', metadata = '{"owner":"a"}', health_claim_token = NULL
				WHERE file_path = `+placeholder+` AND health_claim_token = 'rotated-token'
			`, currentPath)
			require.NoError(t, err)
			affected, err = result.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, int64(0), affected, "owner A must not finalize after owner B reclaims the row")

			var status, metadata, scheduledAt, lastError, errorDetails string
			require.NoError(t, db.QueryRow(`
				SELECT status, metadata, health_claim_token, CAST(scheduled_check_at AS TEXT), last_error, error_details
				FROM file_health WHERE file_path = `+placeholder,
				currentPath,
			).Scan(&status, &metadata, &token, &scheduledAt, &lastError, &errorDetails))
			assert.Equal(t, "healthy", status)
			assert.JSONEq(t, `{"owner":"b"}`, metadata)
			require.True(t, token.Valid)
			assert.Equal(t, "owner-b", token.String)
			assert.Contains(t, scheduledAt, "2042-03-04 05:06:07")
			assert.Equal(t, "owner-b-error", lastError)
			assert.JSONEq(t, `{"owner":"b","evidence":"complete"}`, errorDetails)
		})
	}
}

func TestHealthClaimSQLiteMigrationSurvivesAndRevokesDeterministically(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "claim.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	assertTokenOnlyHealthClaimSchema(t, conn, DialectSQLite, true)
	exerciseHealthClaimRevocation(t, conn, DialectSQLite, "claim/sqlite.mkv")
}

func TestHealthClaimSQLiteMigrationDownUpPreservesPopulatedRows(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "claim-roundtrip.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	const path = "claim/roundtrip.mkv"
	_, err = conn.Exec(`INSERT INTO file_health (file_path, status, health_claim_token) VALUES (?, 'checking', 'old-claim')`, path)
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("sqlite3"))
	require.NoError(t, goose.DownToContext(context.Background(), conn, "migrations/sqlite", 35))
	assertTokenOnlyHealthClaimSchema(t, conn, DialectSQLite, false)
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = ?`, path).Scan(&status))
	assert.Equal(t, "checking", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/sqlite"))
	assertTokenOnlyHealthClaimSchema(t, conn, DialectSQLite, true)
	var token sql.NullString
	require.NoError(t, conn.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = ?`, path).Scan(&token))
	assert.False(t, token.Valid)
}

func TestHealthClaimPostgresMigrationSurvivesAndRevokesDeterministically(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	assertTokenOnlyHealthClaimSchema(t, conn, DialectPostgres, true)
	exerciseHealthClaimRevocation(t, conn, DialectPostgres, "claim/postgres.mkv")
}

func TestHealthClaimPostgresMigrationDownUpPreservesPopulatedRows(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	const path = "claim/postgres-roundtrip.mkv"
	_, err = conn.Exec(`
		INSERT INTO file_health (file_path, status, health_claim_token)
		VALUES ($1, 'checking', 'old-claim')
		ON CONFLICT(file_path) DO UPDATE
		SET status = EXCLUDED.status, health_claim_token = EXCLUDED.health_claim_token
	`, path)
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("postgres"))
	require.NoError(t, goose.DownToContext(context.Background(), conn, "migrations/postgres", 35))
	assertTokenOnlyHealthClaimSchema(t, conn, DialectPostgres, false)
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = $1`, path).Scan(&status))
	assert.Equal(t, "checking", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/postgres"))
	assertTokenOnlyHealthClaimSchema(t, conn, DialectPostgres, true)
	var token sql.NullString
	require.NoError(t, conn.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = $1`, path).Scan(&token))
	assert.False(t, token.Valid)
}
