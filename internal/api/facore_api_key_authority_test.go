package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/auth"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
)

type apiKeyAuthorityConfigManager struct {
	ConfigManager
	cfg           *config.Config
	revision      uint64
	snapshotErr   error
	casErr        error
	snapshotCalls int
	casCalls      int
	casCandidate  *config.Config
	casHook       func()
}

func (m *apiKeyAuthorityConfigManager) GetConfig() *config.Config { return m.cfg }

func (m *apiKeyAuthorityConfigManager) Snapshot() (config.ConfigSnapshot, error) {
	m.snapshotCalls++
	if m.snapshotErr != nil {
		return config.ConfigSnapshot{}, m.snapshotErr
	}
	return config.ConfigSnapshot{Config: m.cfg.DeepCopy(), Revision: m.revision}, nil
}

func (m *apiKeyAuthorityConfigManager) CompareAndSwap(_ context.Context, revision uint64, candidate *config.Config) (config.ConfigSnapshot, error) {
	m.casCalls++
	if m.casHook != nil {
		m.casHook()
	}
	if m.casErr != nil {
		return config.ConfigSnapshot{}, m.casErr
	}
	m.revision = revision + 1
	m.casCandidate = candidate.DeepCopy()
	m.cfg = candidate.DeepCopy()
	return config.ConfigSnapshot{Config: m.cfg.DeepCopy(), Revision: m.revision}, nil
}

func newAPIKeyTestRepository(t *testing.T) (*database.DB, *database.UserRepository) {
	t.Helper()
	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "api-key.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, database.NewUserRepository(db.Connection(), database.DialectSQLite)
}

func decodeAPIKeyResponse(t *testing.T, app *fiber.App) (int, string) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("POST", "/regenerate", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var envelope struct {
		Data struct {
			APIKey string `json:"api_key"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, envelope.Data.APIKey
}

func TestRegenerateAPIKeyBootstrapsThenRotatesAdmin(t *testing.T) {
	_, repo := newAPIKeyTestRepository(t)
	cfg := config.DefaultConfig()
	loginRequired := false
	cfg.Auth.LoginRequired = &loginRequired
	cm := &apiKeyAuthorityConfigManager{cfg: cfg, revision: 1}
	s := &Server{configManager: cm, userRepo: repo}
	app := fiber.New()
	app.Post("/regenerate", s.handleRegenerateAPIKey)

	status, firstKey := decodeAPIKeyResponse(t, app)
	if status != fiber.StatusOK || len(firstKey) != 32 {
		t.Fatalf("first regeneration = status %d key length %d, want 200/32", status, len(firstKey))
	}
	admin, err := repo.GetUserByID(context.Background(), "admin")
	if err != nil || admin == nil || admin.APIKey == nil || *admin.APIKey != firstKey {
		t.Fatalf("bootstrapped admin key = %#v, err = %v, want returned key", admin, err)
	}

	status, secondKey := decodeAPIKeyResponse(t, app)
	if status != fiber.StatusOK || len(secondKey) != 32 || secondKey == firstKey {
		t.Fatalf("second regeneration = status %d key %q, want a rotated 32-character key", status, secondKey)
	}
	admin, err = repo.GetUserByID(context.Background(), "admin")
	if err != nil || admin == nil || admin.APIKey == nil || *admin.APIKey != secondKey {
		t.Fatalf("rotated admin key = %#v, err = %v, want second returned key", admin, err)
	}
}

func TestRegenerateAPIKeyCommitsOverrideBeforeDatabaseMirrorAndReturnsActiveKeyOnMirrorFailure(t *testing.T) {
	db, repo := newAPIKeyTestRepository(t)
	oldDBKey := strings.Repeat("d", 32)
	user := &database.User{UserID: "alice", Provider: "direct", APIKey: &oldDBKey, IsAdmin: true}
	if err := repo.CreateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Connection().Exec(`
		CREATE TRIGGER fail_api_key_mirror
		BEFORE UPDATE OF api_key ON users
		BEGIN SELECT RAISE(FAIL, 'mirror failure'); END;
	`); err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.API.KeyOverride = strings.Repeat("o", 32)
	cm := &apiKeyAuthorityConfigManager{cfg: cfg, revision: 9}
	casSawOldDBKey := false
	cm.casHook = func() {
		stored, err := repo.GetUserByID(context.Background(), user.UserID)
		if err != nil {
			t.Errorf("read database during CAS: %v", err)
			return
		}
		casSawOldDBKey = stored != nil && stored.APIKey != nil && *stored.APIKey == oldDBKey
	}
	s := &Server{configManager: cm, userRepo: repo}
	app := fiber.New()
	app.Post("/regenerate", func(c *fiber.Ctx) error {
		c.Locals(string(auth.UserContextKey), user)
		return s.handleRegenerateAPIKey(c)
	})

	status, activeKey := decodeAPIKeyResponse(t, app)
	if status != fiber.StatusOK || len(activeKey) != 32 {
		t.Fatalf("regeneration = status %d key length %d, want 200/32", status, len(activeKey))
	}
	if !casSawOldDBKey {
		t.Fatal("database changed before the override CompareAndSwap")
	}
	if cm.casCandidate == nil || cm.casCandidate.API.KeyOverride != activeKey {
		t.Fatalf("committed override = %#v, want returned active key", cm.casCandidate)
	}
	stored, err := repo.GetUserByID(context.Background(), user.UserID)
	if err != nil || stored == nil || stored.APIKey == nil || *stored.APIKey != oldDBKey {
		t.Fatalf("database mirror after injected failure = %#v, err = %v, want old key retained", stored, err)
	}
}

func TestRegenerateAPIKeyCASFailureHasNoDatabaseSideEffect(t *testing.T) {
	_, repo := newAPIKeyTestRepository(t)
	oldDBKey := strings.Repeat("d", 32)
	user := &database.User{UserID: "alice", Provider: "direct", APIKey: &oldDBKey, IsAdmin: true}
	if err := repo.CreateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.API.KeyOverride = strings.Repeat("o", 32)
	cm := &apiKeyAuthorityConfigManager{cfg: cfg, revision: 3, casErr: config.ErrConfigConflict}
	s := &Server{configManager: cm, userRepo: repo}
	app := fiber.New()
	app.Post("/regenerate", func(c *fiber.Ctx) error {
		c.Locals(string(auth.UserContextKey), user)
		return s.handleRegenerateAPIKey(c)
	})

	status, _ := decodeAPIKeyResponse(t, app)
	if status != fiber.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	stored, err := repo.GetUserByID(context.Background(), user.UserID)
	if err != nil || stored == nil || stored.APIKey == nil || *stored.APIKey != oldDBKey {
		t.Fatalf("database after CAS failure = %#v, err = %v, want no side effect", stored, err)
	}
}

func TestRegenerateAPIKeySnapshotFailureHasNoBootstrapSideEffect(t *testing.T) {
	_, repo := newAPIKeyTestRepository(t)
	cfg := config.DefaultConfig()
	loginRequired := false
	cfg.Auth.LoginRequired = &loginRequired
	cm := &apiKeyAuthorityConfigManager{cfg: cfg, snapshotErr: errors.New("snapshot unavailable")}
	s := &Server{configManager: cm, userRepo: repo}
	app := fiber.New()
	app.Post("/regenerate", s.handleRegenerateAPIKey)

	status, _ := decodeAPIKeyResponse(t, app)
	if status != fiber.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
	admin, err := repo.GetUserByID(context.Background(), "admin")
	if err != nil || admin != nil {
		t.Fatalf("admin after snapshot failure = %#v, err = %v, want no insert", admin, err)
	}
}

func TestValidateAPIKeyFallsBackToAnyDatabaseUserWhenOverrideDiffers(t *testing.T) {
	_, repo := newAPIKeyTestRepository(t)
	dbKey := strings.Repeat("d", 32)
	if err := repo.CreateUser(context.Background(), &database.User{UserID: "second-user", Provider: "direct", APIKey: &dbKey}); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.API.KeyOverride = strings.Repeat("o", 32)
	s := &Server{configManager: &apiKeyAuthorityConfigManager{cfg: cfg}, userRepo: repo}
	app := fiber.New()
	app.Get("/validate", func(c *fiber.Ctx) error {
		return c.SendStatus(map[bool]int{true: fiber.StatusNoContent, false: fiber.StatusUnauthorized}[s.validateAPIKey(c, dbKey)])
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/validate", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusNoContent {
		t.Fatalf("database fallback status = %d, want 204", resp.StatusCode)
	}
}
