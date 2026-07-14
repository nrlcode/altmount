package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR3ProviderValidationMatchesTransportConstruction(t *testing.T) {
	enabled := true
	disabled := false
	primary := false
	backup := true

	tests := []struct {
		name      string
		providers []ProviderConfig
		wantErr   bool
	}{
		{
			name: "duplicate stable ids",
			providers: []ProviderConfig{
				{ID: "provider-1", Host: "a.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
				{ID: "provider-1", Host: "b.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
			},
			wantErr: true,
		},
		{
			name: "duplicate stable ids include disabled providers",
			providers: []ProviderConfig{
				{ID: "provider-1", Host: "a.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
				{ID: "provider-1", Host: "b.invalid", Port: 119, MaxConnections: 1, Enabled: &disabled, IsBackupProvider: &primary},
			},
			wantErr: true,
		},
		{
			name: "duplicate enabled pool identities",
			providers: []ProviderConfig{
				{ID: "provider-1", Host: "same.invalid", Port: 119, Username: "account", MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
				{ID: "provider-2", Host: "same.invalid", Port: 119, Username: "account", MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
			},
			wantErr: true,
		},
		{
			name: "duplicate disabled pool identity is inert",
			providers: []ProviderConfig{
				{ID: "provider-1", Host: "same.invalid", Port: 119, Username: "account", MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
				{ID: "provider-2", Host: "same.invalid", Port: 119, Username: "account", MaxConnections: 1, Enabled: &disabled, IsBackupProvider: &primary},
			},
		},
		{
			name: "enabled backup-only set",
			providers: []ProviderConfig{{
				ID: "backup-only", Host: "backup.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &backup,
			}},
			wantErr: true,
		},
		{
			name: "disabled backup-only set clears transport",
			providers: []ProviderConfig{{
				ID: "backup-only", Host: "backup.invalid", Port: 119, MaxConnections: 1, Enabled: &disabled, IsBackupProvider: &backup,
			}},
		},
		{
			name: "legacy empty ids remain compatible when fallback identities differ",
			providers: []ProviderConfig{
				{Host: "a.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
				{Host: "b.invalid", Port: 119, MaxConnections: 1, Enabled: &enabled, IsBackupProvider: &primary},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Providers = tt.providers
			err := cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
