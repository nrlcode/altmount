package config

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticProviderIdentitySource struct {
	snapshot ProviderIdentityRegistrySnapshot
	err      error
	reads    int
}

func (s *staticProviderIdentitySource) ReadProviderIdentityRegistrySnapshot(context.Context) (ProviderIdentityRegistrySnapshot, error) {
	s.reads++
	return s.snapshot, s.err
}

func resolutionBool(v bool) *bool { return &v }

func resolutionProvider(id, host, account string, enabled bool) ProviderConfig {
	return ProviderConfig{
		ID: id, Host: host, Port: 119, Username: account, MaxConnections: 1,
		Enabled: resolutionBool(enabled), IsBackupProvider: resolutionBool(false),
	}
}

func newResolutionManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, SaveToFile(cfg, path))
	return NewManager(cfg, path), path
}

func identityGeneration(t *testing.T, id string, generation int64, endpoint, account string) ProviderIdentityGeneration {
	t.Helper()
	fingerprint, err := ProviderIdentityFingerprint(endpoint, 119, account)
	require.NoError(t, err)
	return ProviderIdentityGeneration{
		ProviderID: id, Generation: generation, Endpoint: endpoint, Port: 119,
		Account: account, IdentityFingerprint: fingerprint,
	}
}

func TestConfigValidateReservesProviderIDsAndOperationalAliasesGlobally(t *testing.T) {
	aliasA := "a.invalid:119+AccountA"
	aliasB := "b.invalid:119+AccountB"
	tests := []struct {
		name      string
		providers []ProviderConfig
		wantError bool
	}{
		{
			name: "earlier id collides with later alias",
			providers: []ProviderConfig{
				resolutionProvider(aliasB, "a.invalid", "AccountA", true),
				resolutionProvider("provider-b", "b.invalid", "AccountB", true),
			}, wantError: true,
		},
		{
			name: "later id collides with earlier alias",
			providers: []ProviderConfig{
				resolutionProvider("provider-a", "a.invalid", "AccountA", true),
				resolutionProvider(aliasA, "b.invalid", "AccountB", true),
			}, wantError: true,
		},
		{
			name: "disabled id reserves against enabled alias",
			providers: []ProviderConfig{
				resolutionProvider(aliasB, "disabled.invalid", "Disabled", false),
				resolutionProvider("provider-b", "b.invalid", "AccountB", true),
			}, wantError: true,
		},
		{
			name: "disabled alias reserves against enabled id",
			providers: []ProviderConfig{
				resolutionProvider("disabled-provider", "a.invalid", "AccountA", false),
				resolutionProvider(aliasA, "enabled.invalid", "Enabled", true),
			}, wantError: true,
		},
		{name: "disabled duplicate ids remain invalid", providers: []ProviderConfig{
			resolutionProvider("duplicate-id", "one.invalid", "One", false),
			resolutionProvider("duplicate-id", "two.invalid", "Two", false),
		}, wantError: true},
		{name: "disabled duplicate aliases remain invalid", providers: []ProviderConfig{
			resolutionProvider("provider-one", "same.invalid", "Account", false),
			resolutionProvider("provider-two", "same.invalid", "Account", false),
		}, wantError: true},
		{name: "own id may equal own alias", providers: []ProviderConfig{
			resolutionProvider(aliasA, "a.invalid", "AccountA", true),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig(t.TempDir())
			cfg.Providers = test.providers
			err := cfg.Validate()
			if test.wantError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "provider")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestManagerResolvesEmptyProviderIDsFromOneRegistrySnapshot(t *testing.T) {
	match := identityGeneration(t, "retained-one", 1, "match.invalid", "Account")
	mismatched := match
	mismatched.IdentityFingerprint = "sha256:inconsistent"
	tombstonedAt := time.Unix(99, 0).UTC()
	tests := []struct {
		name          string
		snapshot      ProviderIdentityRegistrySnapshot
		sourceError   error
		withoutSource bool
		wantID        string
		wantError     bool
	}{
		{name: "zero retained matches", wantID: "generated-id"},
		{name: "identity source missing", withoutSource: true, wantError: true},
		{
			name: "inconsistent retained fingerprint",
			snapshot: ProviderIdentityRegistrySnapshot{
				Providers:   []ProviderIdentityRecord{{ID: "retained-one"}},
				Generations: []ProviderIdentityGeneration{mismatched},
			}, wantError: true,
		},
		{
			name:      "empty retained provider id",
			snapshot:  ProviderIdentityRegistrySnapshot{Providers: []ProviderIdentityRecord{{}}},
			wantError: true,
		},
		{
			name: "one tombstoned retained match",
			snapshot: ProviderIdentityRegistrySnapshot{
				Providers: []ProviderIdentityRecord{{
					ID: "retained-one", Active: false, TombstonedAt: &tombstonedAt, CurrentGeneration: 1,
				}},
				Generations: []ProviderIdentityGeneration{match},
			}, wantID: "retained-one",
		},
		{
			name: "multiple retained matches",
			snapshot: ProviderIdentityRegistrySnapshot{
				Providers: []ProviderIdentityRecord{
					{ID: "retained-one", Active: true, CurrentGeneration: 1},
					{ID: "retained-two", CurrentGeneration: 1},
				},
				Generations: []ProviderIdentityGeneration{
					match, identityGeneration(t, "retained-two", 1, "match.invalid", "Account"),
				},
			}, wantError: true,
		},
		{name: "identity source failure", sourceError: assert.AnError, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, path := newResolutionManager(t)
			source := &staticProviderIdentitySource{snapshot: test.snapshot, err: test.sourceError}
			if !test.withoutSource {
				m.SetProviderIdentitySource(source)
			}
			generatorCalls := 0
			m.newProviderID = func() string {
				generatorCalls++
				return "generated-id"
			}
			precommits, callbacks := 0, 0
			m.SetPrecommit(func(context.Context, *Config, *Config) (PreparedChange, error) {
				precommits++
				return PreparedChange{}, nil
			})
			m.OnConfigChange(func(_, _ *Config) { callbacks++ })
			base, err := m.Snapshot()
			require.NoError(t, err)
			beforeDisk, err := os.ReadFile(path)
			require.NoError(t, err)
			candidate := base.Config.DeepCopy()
			candidate.MountPath = "/library/next"
			candidate.Providers = []ProviderConfig{
				resolutionProvider("", " MATCH.INVALID. ", " Account ", false),
			}

			next, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
			if test.wantError {
				require.Error(t, err)
				if test.sourceError != nil {
					assert.ErrorIs(t, err, test.sourceError)
				}
				after, snapshotErr := m.Snapshot()
				require.NoError(t, snapshotErr)
				assert.Equal(t, base, after)
				assert.Zero(t, precommits)
				assert.Zero(t, callbacks)
				assert.Zero(t, generatorCalls)
				assert.False(t, m.NeedsLibrarySync())
				afterDisk, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				assert.True(t, bytes.Equal(beforeDisk, afterDisk))
			} else {
				require.NoError(t, err)
				require.Len(t, next.Config.Providers, 1)
				assert.Equal(t, test.wantID, next.Config.Providers[0].ID)
				assert.Equal(t, 1, precommits)
				assert.Equal(t, 1, callbacks)
				if test.wantID == "generated-id" {
					assert.Equal(t, 1, generatorCalls)
				} else {
					assert.Zero(t, generatorCalls)
				}
			}
			assert.Empty(t, candidate.Providers[0].ID)
			if test.withoutSource {
				assert.Zero(t, source.reads)
			} else {
				assert.Equal(t, 1, source.reads)
			}
		})
	}
}

func TestManagerDoesNotReadIdentitySourceWhenAllIDsAreStable(t *testing.T) {
	m, _ := newResolutionManager(t)
	source := &staticProviderIdentitySource{err: assert.AnError}
	m.SetProviderIdentitySource(source)
	m.newProviderID = func() string { panic("stable IDs must bypass generation") }
	base, err := m.Snapshot()
	require.NoError(t, err)
	candidate := base.Config.DeepCopy()
	candidate.Providers = []ProviderConfig{resolutionProvider("chosen-id", "stable.invalid", "Account", false)}

	next, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
	require.NoError(t, err)
	assert.Equal(t, "chosen-id", next.Config.Providers[0].ID)
	assert.Zero(t, source.reads)
}

func TestManagerRejectsClaimedOrCompetingRetainedIdentity(t *testing.T) {
	snapshot := ProviderIdentityRegistrySnapshot{
		Providers: []ProviderIdentityRecord{{ID: "retained", Active: true, CurrentGeneration: 2}},
		Generations: []ProviderIdentityGeneration{
			identityGeneration(t, "retained", 1, "old.invalid", "Account"),
			identityGeneration(t, "retained", 2, "new.invalid", "Account"),
		},
	}
	for _, test := range []struct {
		name      string
		providers []ProviderConfig
	}{
		{name: "explicitly claimed retained id", providers: []ProviderConfig{
			resolutionProvider("retained", "new.invalid", "Account", false),
			resolutionProvider("", "old.invalid", "Account", false),
		}},
		{name: "empty ids compete for one retained provider", providers: []ProviderConfig{
			resolutionProvider("", "old.invalid", "Account", false),
			resolutionProvider("", "new.invalid", "Account", false),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, path := newResolutionManager(t)
			source := &staticProviderIdentitySource{snapshot: snapshot}
			m.SetProviderIdentitySource(source)
			generatorCalls := 0
			m.newProviderID = func() string { generatorCalls++; return "must-not-be-used" }
			precommits, callbacks := 0, 0
			m.SetPrecommit(func(context.Context, *Config, *Config) (PreparedChange, error) {
				precommits++
				return PreparedChange{}, nil
			})
			m.OnConfigChange(func(_, _ *Config) { callbacks++ })
			base, err := m.Snapshot()
			require.NoError(t, err)
			beforeDisk, err := os.ReadFile(path)
			require.NoError(t, err)
			candidate := base.Config.DeepCopy()
			candidate.MountPath = "/library/rejected"
			candidate.Providers = test.providers

			_, err = m.CompareAndSwap(context.Background(), base.Revision, candidate)
			require.Error(t, err)
			after, snapshotErr := m.Snapshot()
			require.NoError(t, snapshotErr)
			assert.Equal(t, base, after)
			assert.Zero(t, precommits)
			assert.Zero(t, callbacks)
			assert.Zero(t, generatorCalls)
			assert.False(t, m.NeedsLibrarySync())
			afterDisk, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			assert.True(t, bytes.Equal(beforeDisk, afterDisk))
			assert.Equal(t, 1, source.reads)
		})
	}
}

func TestManagerGeneratedProviderIDAvoidsReservedNamespaces(t *testing.T) {
	tombstonedAt := time.Unix(100, 0).UTC()
	source := &staticProviderIdentitySource{snapshot: ProviderIdentityRegistrySnapshot{
		Providers: []ProviderIdentityRecord{
			{ID: "active-id", Active: true, CurrentGeneration: 1},
			{ID: "tombstoned-id", TombstonedAt: &tombstonedAt, CurrentGeneration: 1},
			{ID: "parent-without-generation", TombstonedAt: &tombstonedAt, CurrentGeneration: 1},
		},
	}}
	m, _ := newResolutionManager(t)
	m.SetProviderIdentitySource(source)
	collisions := []string{
		"configured-id", "alias-owner", "cfg.invalid:119+Config", "alias.invalid:119+Alias",
		"target-a.invalid:119+TargetA", "target-b.invalid:119+TargetB",
		"active-id", "tombstoned-id", "parent-without-generation",
	}
	offers := append(append([]string(nil), collisions...), "fresh-one", "fresh-one", "fresh-two")
	attempt := 0
	m.newProviderID = func() string {
		if attempt >= len(offers) {
			panic("provider ID generator exhausted")
		}
		id := offers[attempt]
		attempt++
		return id
	}
	base, err := m.Snapshot()
	require.NoError(t, err)
	candidate := base.Config.DeepCopy()
	candidate.Providers = []ProviderConfig{
		resolutionProvider("configured-id", "cfg.invalid", "Config", false),
		resolutionProvider("alias-owner", "alias.invalid", "Alias", false),
		resolutionProvider("", "target-a.invalid", "TargetA", false),
		resolutionProvider("", "target-b.invalid", "TargetB", false),
	}

	next, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
	require.NoError(t, err)
	require.Len(t, next.Config.Providers, 4)
	assert.Equal(t, "fresh-one", next.Config.Providers[2].ID)
	assert.Equal(t, "fresh-two", next.Config.Providers[3].ID)
	assert.Equal(t, len(offers), attempt)
	assert.Equal(t, 1, source.reads)
}
