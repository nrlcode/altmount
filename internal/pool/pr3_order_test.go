package pool

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/nntppool/v4"
)

type orderRecordingManager struct {
	Manager
	setProviders [][]nntppool.Provider
	added        []nntppool.Provider
}

func (m *orderRecordingManager) SetProviders(providers []nntppool.Provider) error {
	m.setProviders = append(m.setProviders, append([]nntppool.Provider(nil), providers...))
	return nil
}

func (m *orderRecordingManager) AddProvider(provider nntppool.Provider) error {
	m.added = append(m.added, provider)
	return nil
}

func TestPR3ProviderChangesRebuildConfiguredOrder(t *testing.T) {
	enabled := true
	oldConfig := &config.Config{Providers: []config.ProviderConfig{
		{ID: "primary-a", Host: "a.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled},
	}}
	newConfig := &config.Config{Providers: []config.ProviderConfig{
		{ID: "primary-b", Host: "b.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled},
		{ID: "primary-a", Host: "a.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled},
	}}
	mgr := &orderRecordingManager{}

	handleProviderChanges(context.Background(), oldConfig, newConfig, mgr)

	if len(mgr.setProviders) != 1 {
		t.Fatalf("SetProviders calls = %d, want 1 ordered rebuild", len(mgr.setProviders))
	}
	if len(mgr.added) != 0 {
		t.Fatalf("incremental AddProvider calls = %d, want none", len(mgr.added))
	}
	got := mgr.setProviders[0]
	if len(got) != 2 || got[0].Host != "b.invalid:119" || got[1].Host != "a.invalid:119" {
		t.Fatalf("rebuilt provider order = %#v, want b then a", got)
	}
}
