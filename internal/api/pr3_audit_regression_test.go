package api

import (
	"bytes"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/require"
)

// TestPR3ConcurrentDuplicateRemovePreservesOtherActiveStream forces two
// removers past the stream lookup before either can finish. Stream removal
// must claim the entry exactly once so an unrelated active playback remains
// visible to the temporary PR3 health-admission gate.
func TestPR3ConcurrentDuplicateRemovePreservesOtherActiveStream(t *testing.T) {
	tracker := NewStreamTracker(nil)
	t.Cleanup(tracker.Stop)

	victim := tracker.Add("victim.mkv", "test", "", "", "", 1)
	tracker.Add("still-playing.mkv", "test", "", "", "", 1)
	require.Equal(t, 2, tracker.ActiveStreams())

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	tracker.SetCancelFunc(victim, func() {
		entered <- struct{}{}
		<-release
	})

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.Remove(victim)
		}()
	}

	<-entered
	<-entered
	close(release)
	wg.Wait()

	require.Equal(t, 1, tracker.ActiveStreams(),
		"duplicate removal must not hide a different active playback stream")
	require.Len(t, tracker.GetHistory(), 1,
		"one stream must create exactly one completion record")
}

func TestPR3ProviderCreateDoesNotReuseExistingStableID(t *testing.T) {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{
		ID:             "provider_2",
		Host:           "existing.invalid",
		Port:           119,
		Username:       "existing",
		MaxConnections: 1,
		Enabled:        &enabled,
	}}
	manager := &mockConfigManager{cfg: cfg}
	server := &Server{configManager: manager}
	app := fiber.New()
	app.Post("/providers", server.handleCreateProvider)

	req := httptest.NewRequest("POST", "/providers", bytes.NewBufferString(`{
		"host":"new.invalid",
		"port":119,
		"username":"new",
		"max_connections":1,
		"enabled":true
	}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, 200, resp.StatusCode)
	require.Len(t, manager.cfg.Providers, 2)
	require.NotEqual(t, manager.cfg.Providers[0].ID, manager.cfg.Providers[1].ID,
		"provider creation after a deletion must not reuse an active transport identity")
}
