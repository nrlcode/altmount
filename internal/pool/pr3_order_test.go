package pool

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/nntppool/v4"
)

type orderRecordingManager struct {
	Manager
	preparedProviders [][]nntppool.Provider
	hasPool           bool
}

func (m *orderRecordingManager) HasPool() bool { return m.hasPool }

func (m *orderRecordingManager) PrepareProviders(_ context.Context, providers []nntppool.Provider) (config.PreparedChange, error) {
	m.preparedProviders = append(m.preparedProviders, append([]nntppool.Provider(nil), providers...))
	return config.PreparedChange{}, nil
}

func orderedTestProvider(id, host string, enabled bool) config.ProviderConfig {
	return config.ProviderConfig{
		ID:             id,
		Host:           host,
		Port:           119,
		MaxConnections: 1,
		Enabled:        &enabled,
	}
}

func TestPR3ProviderChangesPrepareConfiguredOrder(t *testing.T) {
	oldConfig := &config.Config{Providers: []config.ProviderConfig{
		orderedTestProvider("primary-a", "a.invalid", true),
	}}
	newConfig := &config.Config{Providers: []config.ProviderConfig{
		orderedTestProvider("primary-b", "b.invalid", true),
		orderedTestProvider("primary-a", "a.invalid", true),
	}}
	mgr := &orderRecordingManager{hasPool: true}

	if _, err := prepareProviderChanges(context.Background(), oldConfig, newConfig, mgr); err != nil {
		t.Fatalf("prepare provider changes: %v", err)
	}

	if len(mgr.preparedProviders) != 1 {
		t.Fatalf("PrepareProviders calls = %d, want 1 ordered rebuild", len(mgr.preparedProviders))
	}
	got := mgr.preparedProviders[0]
	if len(got) != 2 || got[0].Host != "b.invalid:119" || got[1].Host != "a.invalid:119" {
		t.Fatalf("rebuilt provider order = %#v, want b then a", got)
	}
}

func TestPR3ProviderChangesUseEffectiveTransportProjection(t *testing.T) {
	tests := []struct {
		name        string
		change      func(oldConfig, newConfig *config.Config)
		poolMissing bool
		wantCalls   int
	}{
		{
			name:        "startup without a generation",
			change:      func(_, _ *config.Config) {},
			poolMissing: true,
			wantCalls:   1,
		},
		{
			name: "display and telemetry only",
			change: func(_, newConfig *config.Config) {
				provider := &newConfig.Providers[0]
				provider.Name = "renamed"
				provider.LastRTTMs = 12
				provider.LastSpeedTestMbps = 34
				provider.AccountExpirationDate = "2099-01-01"
			},
		},
		{
			name: "disabled edits and reorder",
			change: func(oldConfig, newConfig *config.Config) {
				oldConfig.Providers = append(oldConfig.Providers,
					orderedTestProvider("disabled-a", "disabled-a.invalid", false),
					orderedTestProvider("disabled-b", "disabled-b.invalid", false),
				)
				enabled := newConfig.Providers[0]
				newConfig.Providers = []config.ProviderConfig{
					orderedTestProvider("disabled-b-edited", "edited.invalid", false),
					enabled,
					orderedTestProvider("disabled-a-edited", "also-edited.invalid", false),
				}
			},
		},
		{
			name: "nil and false backup",
			change: func(_, newConfig *config.Config) {
				isBackup := false
				newConfig.Providers[0].IsBackupProvider = &isBackup
			},
		},
		{
			name: "insecure TLS while TLS disabled",
			change: func(_, newConfig *config.Config) {
				newConfig.Providers[0].InsecureTLS = true
			},
		},
		{
			name: "effective inflight defaults",
			change: func(_, newConfig *config.Config) {
				newConfig.Providers[0].InflightRequests = 10
				newConfig.Providers[0].StatInflightRequests = 100
			},
		},
		{
			name: "enabled order",
			change: func(oldConfig, newConfig *config.Config) {
				oldConfig.Providers = append(oldConfig.Providers, orderedTestProvider("primary-b", "b.invalid", true))
				newConfig.Providers = []config.ProviderConfig{
					orderedTestProvider("primary-b", "b.invalid", true),
					newConfig.Providers[0],
				}
			},
			wantCalls: 1,
		},
		{
			name: "enabled transport",
			change: func(_, newConfig *config.Config) {
				newConfig.Providers[0].Host = "changed.invalid"
			},
			wantCalls: 1,
		},
		{
			name: "enabled provider ID",
			change: func(_, newConfig *config.Config) {
				newConfig.Providers[0].ID = "replacement-id"
			},
			wantCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldConfig := &config.Config{Providers: []config.ProviderConfig{
				orderedTestProvider("primary-a", "a.invalid", true),
			}}
			newConfig := oldConfig.DeepCopy()
			test.change(oldConfig, newConfig)
			mgr := &orderRecordingManager{hasPool: !test.poolMissing}

			if _, err := prepareProviderChanges(context.Background(), oldConfig, newConfig, mgr); err != nil {
				t.Fatalf("prepare provider changes: %v", err)
			}
			if got := len(mgr.preparedProviders); got != test.wantCalls {
				t.Fatalf("PrepareProviders calls = %d, want %d", got, test.wantCalls)
			}
		})
	}
}
