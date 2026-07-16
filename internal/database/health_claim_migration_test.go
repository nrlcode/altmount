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

func healthClaimColumnExists(t *testing.T, db *sql.DB, dialect Dialect) bool {
	t.Helper()
	columns := pr4Columns(t, db, dialect, "file_health")
	return contains(columns, "health_claim_token")
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
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, status, updated_at)
		VALUES (`+placeholder+`, 'pending', '2001-02-03 04:05:06')
	`, path)
	require.NoError(t, err)

	var beforeUpdated string
	require.NoError(t, db.QueryRow(`SELECT CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, path).Scan(&beforeUpdated))

	claimSQL := `UPDATE file_health SET status = 'checking', health_claim_token = 'claim-token' WHERE file_path = ` + placeholder
	_, err = db.Exec(claimSQL, path)
	require.NoError(t, err)
	var token sql.NullString
	var afterClaimUpdated string
	require.NoError(t, db.QueryRow(`SELECT health_claim_token, CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, path).Scan(&token, &afterClaimUpdated))
	require.True(t, token.Valid)
	assert.Equal(t, "claim-token", token.String, "installing a new checking token must survive timestamp bookkeeping")
	assert.Equal(t, beforeUpdated, afterClaimUpdated, "claim-only bookkeeping must not alter business updated_at")

	rotateSQL := `UPDATE file_health SET health_claim_token = 'rotated-token' WHERE file_path = ` + placeholder + ` AND health_claim_token = 'claim-token'`
	result, err := db.Exec(rotateSQL, path)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.NoError(t, db.QueryRow(`SELECT health_claim_token, CAST(updated_at AS TEXT) FROM file_health WHERE file_path = `+placeholder, path).Scan(&token, &afterClaimUpdated))
	assert.Equal(t, "rotated-token", token.String)
	assert.Equal(t, beforeUpdated, afterClaimUpdated, "token rotation must not alter business updated_at")

	metadataSQL := `UPDATE file_health SET metadata = '{}' WHERE file_path = ` + placeholder
	_, err = db.Exec(metadataSQL, path)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = `+placeholder, path).Scan(&token))
	assert.False(t, token.Valid, "ownership-relevant writes must revoke an existing claim")

	// Owner B publishes a complete current state under a fresh token. Owner A's
	// stale token cannot match after revocation/reclaim, even if broad phases
	// cycle through values A previously observed.
	claimBSQL := `UPDATE file_health SET status = 'healthy', metadata = '{"owner":"b"}', health_claim_token = 'owner-b' WHERE file_path = ` + placeholder + ` AND health_claim_token IS NULL`
	_, err = db.Exec(claimBSQL, path)
	require.NoError(t, err)
	staleFinalizeSQL := `UPDATE file_health SET status = 'corrupted', metadata = '{"owner":"a"}', health_claim_token = NULL WHERE file_path = ` + placeholder + ` AND health_claim_token = 'rotated-token'`
	result, err = db.Exec(staleFinalizeSQL, path)
	require.NoError(t, err)
	affected, err = result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(0), affected, "owner A must not finalize after owner B reclaims the row")
	var status, metadata string
	require.NoError(t, db.QueryRow(`SELECT status, metadata, health_claim_token FROM file_health WHERE file_path = `+placeholder, path).Scan(&status, &metadata, &token))
	assert.Equal(t, "healthy", status)
	assert.JSONEq(t, `{"owner":"b"}`, metadata)
	require.True(t, token.Valid)
	assert.Equal(t, "owner-b", token.String)
}

func TestHealthClaimSQLiteMigrationSurvivesAndRevokesDeterministically(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "claim.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	require.True(t, healthClaimColumnExists(t, conn, DialectSQLite), "migration 036 must add the claim token")
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
	assert.False(t, healthClaimColumnExists(t, conn, DialectSQLite))
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = ?`, path).Scan(&status))
	assert.Equal(t, "checking", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/sqlite"))
	assert.True(t, healthClaimColumnExists(t, conn, DialectSQLite))
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
	require.True(t, healthClaimColumnExists(t, conn, DialectPostgres), "migration 036 must add the claim token")
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
	assert.False(t, healthClaimColumnExists(t, conn, DialectPostgres))
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = $1`, path).Scan(&status))
	assert.Equal(t, "checking", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/postgres"))
	assert.True(t, healthClaimColumnExists(t, conn, DialectPostgres))
	var token sql.NullString
	require.NoError(t, conn.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = $1`, path).Scan(&token))
	assert.False(t, token.Valid)
}
