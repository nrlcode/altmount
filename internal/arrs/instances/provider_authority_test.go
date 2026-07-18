package instances

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/javi11/altmount/internal/config"
)

type authorityConfigManager struct {
	mu            sync.Mutex
	current       *config.Config
	revision      uint64
	snapshotCalls int
	barrier       chan struct{}
	advanceOnce   bool
}

func (m *authorityConfigManager) getConfig() *config.Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.DeepCopy()
}

func (m *authorityConfigManager) Snapshot() (config.ConfigSnapshot, error) {
	m.mu.Lock()
	m.snapshotCalls++
	snapshot := config.ConfigSnapshot{Config: m.current.DeepCopy(), Revision: m.revision}
	if m.advanceOnce {
		m.revision++
		m.advanceOnce = false
	}
	barrier := m.barrier
	if barrier != nil && m.snapshotCalls == 2 {
		close(barrier)
	}
	m.mu.Unlock()
	if barrier != nil {
		<-barrier
	}
	return snapshot, nil
}

func (m *authorityConfigManager) CompareAndSwap(_ context.Context, revision uint64, candidate *config.Config) (config.ConfigSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if revision != m.revision {
		return config.ConfigSnapshot{}, config.ErrConfigConflict
	}
	m.current = candidate.DeepCopy()
	m.revision++
	return config.ConfigSnapshot{Config: m.current.DeepCopy(), Revision: m.revision}, nil
}

func radarrStatusServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"appName": "Radarr"}); err != nil {
			t.Errorf("encode Radarr status: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestRegisterInstanceRejectsStaleRevisionWithoutPublishingCandidate(t *testing.T) {
	server := radarrStatusServer(t)
	manager := &authorityConfigManager{current: config.DefaultConfig(), revision: 2, advanceOnce: true}
	instances := NewManager(manager.getConfig, manager)

	registered, err := instances.RegisterInstance(context.Background(), server.URL, "key")
	if registered || !errors.Is(err, config.ErrConfigConflict) {
		t.Fatalf("RegisterInstance = (%t, %v), want false/config conflict", registered, err)
	}
	if got := len(manager.getConfig().Arrs.RadarrInstances); got != 0 {
		t.Fatalf("published Radarr instances = %d, want 0 after stale CAS", got)
	}
}

func TestConcurrentRegisterInstancePublishesOneSameURL(t *testing.T) {
	server := radarrStatusServer(t)
	manager := &authorityConfigManager{
		current:  config.DefaultConfig(),
		revision: 1,
		barrier:  make(chan struct{}),
	}
	instances := NewManager(manager.getConfig, manager)

	type result struct {
		registered bool
		err        error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			registered, err := instances.RegisterInstance(context.Background(), server.URL, "key")
			results <- result{registered: registered, err: err}
		}()
	}

	successes, conflicts := 0, 0
	for range 2 {
		result := <-results
		if result.registered && result.err == nil {
			successes++
		} else if !result.registered && errors.Is(result.err, config.ErrConfigConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent result: registered=%t err=%v", result.registered, result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes/conflicts = %d/%d, want 1/1", successes, conflicts)
	}
	if got := len(manager.getConfig().Arrs.RadarrInstances); got != 1 {
		t.Fatalf("published Radarr instances = %d, want exactly 1", got)
	}
}
