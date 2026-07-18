package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/nntppool/v4"
)

type authorityConfigManager struct {
	ConfigManager
	cfg           *config.Config
	revision      uint64
	casCandidate  *config.Config
	casRevision   uint64
	snapshotCalls int
	casCalls      int
	legacyWrites  int
	writeErr      error
}

func (m *authorityConfigManager) GetConfig() *config.Config { return m.cfg }
func (m *authorityConfigManager) UpdateConfig(*config.Config) error {
	m.legacyWrites++
	return m.writeErr
}
func (m *authorityConfigManager) SaveConfig() error {
	m.legacyWrites++
	return m.writeErr
}
func (m *authorityConfigManager) Snapshot() (config.ConfigSnapshot, error) {
	m.snapshotCalls++
	return config.ConfigSnapshot{Config: m.cfg.DeepCopy(), Revision: m.revision}, nil
}
func (m *authorityConfigManager) CompareAndSwap(_ context.Context, revision uint64, candidate *config.Config) (config.ConfigSnapshot, error) {
	m.casCalls++
	m.casRevision = revision
	m.casCandidate = candidate
	return config.ConfigSnapshot{}, m.writeErr
}

type authorityNntpClient struct {
	pool.NntpClient
	stats       nntppool.ClientStats
	speedResult *nntppool.SpeedTestResult
	speedCalls  int
	speedTarget string
}

func (c *authorityNntpClient) Stats() nntppool.ClientStats { return c.stats }
func (c *authorityNntpClient) SpeedTest(_ context.Context, opts nntppool.SpeedTestOptions) (*nntppool.SpeedTestResult, error) {
	c.speedCalls++
	c.speedTarget = opts.ProviderName
	return c.speedResult, nil
}

type authorityPoolManager struct {
	pool.Manager
	client  pool.NntpClient
	metrics pool.MetricsSnapshot
	resetID string
}

func (m *authorityPoolManager) HasPool() bool                             { return true }
func (m *authorityPoolManager) GetPool() (pool.NntpClient, error)         { return m.client, nil }
func (m *authorityPoolManager) GetMetrics() (pool.MetricsSnapshot, error) { return m.metrics, nil }
func (m *authorityPoolManager) ResetProviderQuota(_ context.Context, id string) error {
	m.resetID = id
	return nil
}

func TestHandleGetPoolMetricsUsesCanonicalProviderID(t *testing.T) {
	started := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
	providers := []config.ProviderConfig{
		{ID: "account-a", Name: "Account A", Host: "news.shared.invalid", Port: 563, Username: "alpha"},
		{ID: "account-b", Name: "Account B", Host: "news.shared.invalid", Port: 563, Username: "beta"},
	}
	aliases := []string{"news.old.invalid:563+alpha", "news.old.invalid:563+beta"}
	client := &authorityNntpClient{stats: nntppool.ClientStats{Providers: []nntppool.ProviderStats{
		{Name: aliases[0], ProviderID: providers[0].ID, AvgSpeed: 10, Missing: 3, MaxConnections: 1},
		{Name: aliases[1], ProviderID: providers[1].ID, AvgSpeed: 20, Missing: 4, MaxConnections: 2},
		{Name: "legacy-alias-without-id", AvgSpeed: 1000, MaxConnections: 99},
	}}}
	metrics := pool.MetricsSnapshot{
		ProviderErrors:         map[string]int64{"account-a": 11, "account-b": 22, aliases[0]: 1001, aliases[1]: 1002},
		ProviderBytes:          map[string]int64{"account-a": 111, "account-b": 222, aliases[0]: 2001, aliases[1]: 2002},
		ProviderBytes24h:       map[string]int64{"account-a": 33, "account-b": 66, aliases[0]: 3001, aliases[1]: 3002},
		ProviderStartedAt:      map[string]time.Time{"account-a": started.Add(time.Hour), "account-b": started.Add(2 * time.Hour), aliases[0]: started.Add(-time.Hour), aliases[1]: started.Add(-2 * time.Hour)},
		ProviderMissingRates:   map[string]float64{"account-a": 1.25, "account-b": 2.5, aliases[0]: 101, aliases[1]: 102},
		ProviderMissingWarning: map[string]bool{"account-a": true, "account-b": false, aliases[0]: false, aliases[1]: true},
		ProviderQuotas: map[string]pool.ProviderQuotaSnapshot{
			"account-a": {QuotaBytes: 100, QuotaUsed: 10},
			"account-b": {QuotaBytes: 200, QuotaUsed: 20},
			aliases[0]:  {QuotaBytes: 900, QuotaUsed: 901, QuotaExceeded: true},
			aliases[1]:  {QuotaBytes: 902, QuotaUsed: 903, QuotaExceeded: true},
		},
	}
	cfg := config.DefaultConfig()
	cfg.Providers = providers
	s := &Server{configManager: &authorityConfigManager{cfg: cfg}, poolManager: &authorityPoolManager{client: client, metrics: metrics}}
	app := fiber.New()
	app.Get("/metrics", s.handleGetPoolMetrics)

	resp, err := app.Test(httptest.NewRequest("GET", "/metrics", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var envelope struct {
		Data PoolMetricsResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data.Providers) != 2 {
		t.Fatalf("providers = %d, want 2", len(envelope.Data.Providers))
	}
	gotByID := make(map[string]ProviderStatusResponse, 2)
	for _, got := range envelope.Data.Providers {
		gotByID[got.ID] = got
	}
	for i, configured := range providers {
		got, ok := gotByID[configured.ID]
		if !ok {
			t.Fatalf("same-host account %q missing; response IDs = %#v", configured.ID, gotByID)
		}
		factor := int64(i + 1)
		if got.Name != configured.Name || got.Host != configured.Host || got.Username != configured.Username {
			t.Errorf("provider %s config join = name %q host %q user %q", got.ID, got.Name, got.Host, got.Username)
		}
		if got.ErrorCount != 11*factor || got.ByteCount != 111*factor {
			t.Errorf("provider %s counters = (%d,%d), want canonical-only (%d,%d)", got.ID, got.ErrorCount, got.ByteCount, 11*factor, 111*factor)
		}
		if got.ByteCount24h != 33*factor || got.StartedAt != started.Add(time.Duration(factor)*time.Hour) {
			t.Errorf("provider %s 24h/start = (%d,%s), want canonical-only", got.ID, got.ByteCount24h, got.StartedAt)
		}
		wantWarning := i == 0
		if got.MissingRatePerMinute != 1.25*float64(factor) || got.MissingWarning != wantWarning {
			t.Errorf("provider %s missing telemetry = (%.2f,%t), want canonical-only", got.ID, got.MissingRatePerMinute, got.MissingWarning)
		}
		if got.QuotaBytes != 100*factor || got.QuotaUsed != 10*factor || got.QuotaExceeded {
			t.Errorf("provider %s quota = (%d,%d,%t), want canonical-only", got.ID, got.QuotaBytes, got.QuotaUsed, got.QuotaExceeded)
		}
	}
}

func TestHandleResetProviderQuotaPassesCanonicalID(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{ID: "quota-account", Host: "news.invalid", Port: 563, Username: "alias", QuotaBytes: 100}}
	pm := &authorityPoolManager{}
	s := &Server{configManager: &authorityConfigManager{cfg: cfg}, poolManager: pm}
	app := fiber.New()
	app.Post("/providers/:id/reset-quota", s.handleResetProviderQuota)

	resp, err := app.Test(httptest.NewRequest("POST", "/providers/quota-account/reset-quota", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if pm.resetID != "quota-account" {
		t.Fatalf("ResetProviderQuota target = %q, want canonical ID %q", pm.resetID, "quota-account")
	}
}

func TestProviderSpeedTestUsesFacadeAndCanonicalID(t *testing.T) {
	p := config.ProviderConfig{ID: "speed-account", Host: "news.invalid", Port: 563, Username: "alias", MaxConnections: 0}
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{p}
	client := &authorityNntpClient{speedResult: &nntppool.SpeedTestResult{
		Elapsed:      time.Second,
		WireSpeedBps: 1 * 1024 * 1024,
		Providers: []nntppool.ProviderStats{
			{ProviderID: "other-account", Name: p.NNTPPoolName(), AvgSpeed: 2 * 1024 * 1024},
			{ProviderID: p.ID, Name: "stale-operational-alias", AvgSpeed: 7 * 1024 * 1024},
		},
	}}
	cm := &authorityConfigManager{cfg: cfg, revision: 41, writeErr: errors.New("stop after observing CAS")}
	s := &Server{configManager: cm, poolManager: &authorityPoolManager{client: client}}
	app := fiber.New()
	app.Post("/providers/:id/speedtest", s.handleTestProviderSpeed)

	resp, err := app.Test(httptest.NewRequest("POST", "/providers/speed-account/speedtest", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if client.speedCalls != 1 {
		t.Fatalf("NntpClient.SpeedTest calls = %d, want 1 (handler must not require *nntppool.Client)", client.speedCalls)
	}
	if client.speedTarget != p.ID {
		t.Errorf("speed-test target = %q, want canonical ID %q", client.speedTarget, p.ID)
	}
	if cm.snapshotCalls != 1 || cm.casCalls != 1 || cm.casRevision != 41 {
		t.Fatalf("Snapshot/CAS calls/revision = %d/%d/%d, want 1/1/41", cm.snapshotCalls, cm.casCalls, cm.casRevision)
	}
	if cm.legacyWrites != 0 {
		t.Fatalf("speed-test used %d legacy UpdateConfig/SaveConfig writes", cm.legacyWrites)
	}
	if cm.casCandidate == nil {
		t.Fatal("speed-test result was not submitted through CompareAndSwap")
	}
	if got := cm.casCandidate.Providers[0].LastSpeedTestMbps; got != 7 {
		t.Errorf("saved speed = %.1f MB/s, want canonical provider's 7.0 MB/s", got)
	}
}

func TestProviderSpeedTestPersistsExactZeroForCanonicalProvider(t *testing.T) {
	p := config.ProviderConfig{ID: "zero-speed-account", Host: "news.invalid", Port: 563, Username: "alias"}
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{p}
	client := &authorityNntpClient{speedResult: &nntppool.SpeedTestResult{
		Elapsed:      time.Second,
		WireSpeedBps: 9 * 1024 * 1024,
		Providers:    []nntppool.ProviderStats{{ProviderID: p.ID, AvgSpeed: 0}},
	}}
	cm := &authorityConfigManager{cfg: cfg, revision: 7, writeErr: errors.New("observe CAS")}
	s := &Server{configManager: cm, poolManager: &authorityPoolManager{client: client}}
	app := fiber.New()
	app.Post("/providers/:id/speedtest", s.handleTestProviderSpeed)

	resp, err := app.Test(httptest.NewRequest("POST", "/providers/zero-speed-account/speedtest", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if cm.casCandidate == nil {
		t.Fatal("zero canonical speed was not submitted through CompareAndSwap")
	}
	if got := cm.casCandidate.Providers[0].LastSpeedTestMbps; got != 0 {
		t.Fatalf("saved speed = %.1f MB/s, want exact canonical zero (not aggregate fallback)", got)
	}
}

func TestProviderSpeedTestRejectsResultWithoutCanonicalProvider(t *testing.T) {
	p := config.ProviderConfig{ID: "missing-speed-account", Host: "news.invalid", Port: 563, Username: "alias"}
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{p}
	client := &authorityNntpClient{speedResult: &nntppool.SpeedTestResult{
		Elapsed:      time.Second,
		WireSpeedBps: 9 * 1024 * 1024,
		Providers:    []nntppool.ProviderStats{{ProviderID: "other-account", AvgSpeed: 3 * 1024 * 1024}},
	}}
	cm := &authorityConfigManager{cfg: cfg, revision: 8}
	s := &Server{configManager: cm, poolManager: &authorityPoolManager{client: client}}
	app := fiber.New()
	app.Post("/providers/:id/speedtest", s.handleTestProviderSpeed)

	resp, err := app.Test(httptest.NewRequest("POST", "/providers/missing-speed-account/speedtest", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if cm.casCalls != 0 {
		t.Fatalf("CompareAndSwap calls = %d, want 0 for incomplete speed-test result", cm.casCalls)
	}
}

func TestHandleGetPoolMetricsPreservesRemovedProviderCounters(t *testing.T) {
	client := &authorityNntpClient{stats: nntppool.ClientStats{Providers: []nntppool.ProviderStats{
		{ProviderID: "current-account", MaxConnections: 1},
	}}}
	metrics := pool.MetricsSnapshot{
		TotalErrors:    12,
		ProviderErrors: map[string]int64{"current-account": 5, "removed-account": 7},
		ProviderBytes:  map[string]int64{"current-account": 50, "removed-account": 70},
	}
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{ID: "current-account", Name: "Current"}}
	s := &Server{configManager: &authorityConfigManager{cfg: cfg}, poolManager: &authorityPoolManager{client: client, metrics: metrics}}
	app := fiber.New()
	app.Get("/metrics", s.handleGetPoolMetrics)

	resp, err := app.Test(httptest.NewRequest("GET", "/metrics", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var envelope struct {
		Data PoolMetricsResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if got := envelope.Data.ProviderErrors["removed-account"]; got != 7 {
		t.Fatalf("removed provider errors = %d, want folded cumulative 7", got)
	}
	if got := envelope.Data.ProviderBytes["removed-account"]; got != 70 {
		t.Fatalf("removed provider bytes = %d, want folded cumulative 70", got)
	}
	var errorSum int64
	for _, count := range envelope.Data.ProviderErrors {
		errorSum += count
	}
	if errorSum != envelope.Data.TotalErrors {
		t.Fatalf("provider error breakdown sum = %d, total = %d", errorSum, envelope.Data.TotalErrors)
	}
}
