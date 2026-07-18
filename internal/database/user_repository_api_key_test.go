package database

import (
	"context"
	"database/sql"
	"encoding/base64"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestGenerateAPIKeyIsPureAndExactly32Characters(t *testing.T) {
	var repo *UserRepository
	apiKey, err := repo.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(apiKey) != 32 {
		t.Fatalf("API key length = %d, want 32", len(apiKey))
	}
	decoded, err := base64.RawURLEncoding.DecodeString(apiKey)
	if err != nil {
		t.Fatalf("API key is not raw URL-safe base64: %v", err)
	}
	if len(decoded) != 24 {
		t.Fatalf("decoded entropy length = %d, want 24", len(decoded))
	}
}

func TestUpdateAPIKeyPersistsExactKeyForExistingUser(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE users (
			user_id TEXT PRIMARY KEY,
			api_key TEXT,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		INSERT INTO users (user_id, api_key) VALUES ('admin', 'old');
	`); err != nil {
		t.Fatal(err)
	}

	repo := NewUserRepository(db, DialectSQLite)
	want := "0123456789abcdefghijklmnopqrstuv"
	if err := repo.UpdateAPIKey(context.Background(), "admin", want); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.QueryRow(`SELECT api_key FROM users WHERE user_id = 'admin'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("stored API key = %q, want exact key %q", got, want)
	}
	if err := repo.UpdateAPIKey(context.Background(), "missing", want); err == nil {
		t.Fatal("UpdateAPIKey accepted a missing user")
	}
}
