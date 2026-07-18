package config

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func authorityBool(v bool) *bool { return &v }

func newAuthorityManager(t *testing.T) (*Manager, *Config, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.API.AllowedOrigins = []string{"https://original.invalid"}
	cfg.RClone.RCOptions = map[string]string{"mode": "original"}
	cfg.Providers = []ProviderConfig{{
		ID: "stable-provider", Host: "news.invalid", Port: 119, Username: "Account",
		MaxConnections: 1, Enabled: authorityBool(false), IsBackupProvider: authorityBool(false),
	}}
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, SaveToFile(cfg, path))
	return NewManager(cfg, path), cfg, path
}

func TestManagerSnapshotCASAndCallbacksAreDeeplyIsolated(t *testing.T) {
	m, input, path := newAuthorityManager(t)
	input.API.AllowedOrigins[0] = "caller-poison"
	input.RClone.RCOptions["mode"] = "caller-poison"
	*input.Providers[0].Enabled = true

	first, err := m.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, "https://original.invalid", first.Config.API.AllowedOrigins[0])
	assert.Equal(t, "original", first.Config.RClone.RCOptions["mode"])
	assert.False(t, *first.Config.Providers[0].Enabled)

	first.Config.API.AllowedOrigins[0] = "snapshot-poison"
	first.Config.RClone.RCOptions["mode"] = "snapshot-poison"
	*first.Config.Providers[0].Enabled = true
	base, err := m.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, "https://original.invalid", base.Config.API.AllowedOrigins[0])
	assert.Equal(t, "original", base.Config.RClone.RCOptions["mode"])
	assert.False(t, *base.Config.Providers[0].Enabled)

	var callbackOrder []string
	m.OnConfigChange(func(oldConfig, newConfig *Config) {
		callbackOrder = append(callbackOrder, "first")
		oldConfig.API.AllowedOrigins[0] = "callback-old-poison"
		newConfig.API.AllowedOrigins[0] = "callback-new-poison"
		newConfig.RClone.RCOptions["mode"] = "callback-poison"
		*newConfig.Providers[0].Enabled = true
	})
	m.OnConfigChange(func(oldConfig, newConfig *Config) {
		callbackOrder = append(callbackOrder, "second")
		assert.Equal(t, "https://original.invalid", oldConfig.API.AllowedOrigins[0])
		assert.Equal(t, "https://next.invalid", newConfig.API.AllowedOrigins[0])
		assert.Equal(t, "next", newConfig.RClone.RCOptions["mode"])
		assert.False(t, *newConfig.Providers[0].Enabled)
	})

	candidate := base.Config.DeepCopy()
	candidate.MountPath = "/library/next"
	candidate.Streaming.MaxPrefetch = 0 // validation normalizes only the manager-owned copy
	candidate.API.AllowedOrigins[0] = "https://next.invalid"
	candidate.RClone.RCOptions["mode"] = "next"
	next, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
	require.NoError(t, err)
	assert.Equal(t, base.Revision+1, next.Revision)
	assert.Equal(t, 0, candidate.Streaming.MaxPrefetch, "CAS must not mutate its input during validation")
	assert.Equal(t, []string{"first", "second"}, callbackOrder)

	next.Config.API.AllowedOrigins[0] = "return-poison"
	next.Config.RClone.RCOptions["mode"] = "return-poison"
	*next.Config.Providers[0].Enabled = true
	candidate.API.AllowedOrigins[0] = "post-CAS-poison"
	candidate.RClone.RCOptions["mode"] = "post-CAS-poison"
	stored, err := m.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, "https://next.invalid", stored.Config.API.AllowedOrigins[0])
	assert.Equal(t, "next", stored.Config.RClone.RCOptions["mode"])
	assert.Equal(t, 60, stored.Config.Streaming.MaxPrefetch)
	assert.True(t, m.NeedsLibrarySync())
	assert.Equal(t, "", m.GetPreviousMountPath())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var persisted Config
	require.NoError(t, yaml.Unmarshal(data, &persisted))
	assert.Equal(t, stored.Config.MountPath, persisted.MountPath)
	assert.Equal(t, stored.Config.API.AllowedOrigins, persisted.API.AllowedOrigins)
}

func TestManagerCASAllowsOnlyOneSameRevisionContender(t *testing.T) {
	m, _, _ := newAuthorityManager(t)
	base, err := m.Snapshot()
	require.NoError(t, err)
	var persistCalls atomic.Int32
	m.persist = func(*Config, string) error { persistCalls.Add(1); return nil }

	var callbacks atomic.Int32
	m.OnConfigChange(func(_, _ *Config) { callbacks.Add(1) })
	candidates := []*Config{base.Config.DeepCopy(), base.Config.DeepCopy()}
	candidates[0].Network.HTTPProxy = "http://winner-a.invalid"
	candidates[1].Network.HTTPProxy = "http://winner-b.invalid"

	type result struct {
		snapshot ConfigSnapshot
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, candidate := range candidates {
		wg.Add(1)
		go func(candidate *Config) {
			defer wg.Done()
			<-start
			snapshot, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
			results <- result{snapshot: snapshot, err: err}
		}(candidate)
	}
	close(start)
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("same-revision contenders did not complete")
	}
	close(results)

	var winner ConfigSnapshot
	successes, conflicts := 0, 0
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.snapshot
			continue
		}
		if errors.Is(result.err, ErrConfigConflict) {
			conflicts++
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
	assert.Equal(t, int32(1), callbacks.Load())
	assert.Equal(t, int32(1), persistCalls.Load())
	assert.Equal(t, base.Revision+1, winner.Revision)
	final, err := m.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, winner, final)
}

func TestManagerCASFailuresHaveNoEffects(t *testing.T) {
	for _, failure := range []string{"stale", "validation", "precommit", "persist"} {
		t.Run(failure, func(t *testing.T) {
			m, _, path := newAuthorityManager(t)
			before, err := m.Snapshot()
			require.NoError(t, err)
			beforeDisk, err := os.ReadFile(path)
			require.NoError(t, err)

			candidate := before.Config.DeepCopy()
			candidate.MountPath = "/library/rejected"
			if failure == "validation" {
				candidate.WebDAV.Port = 0
			}
			candidateBefore := candidate.DeepCopy()
			var events []string
			staged := false
			m.SetPrecommit(func(context.Context, *Config, *Config) (PreparedChange, error) {
				events = append(events, "precommit")
				if failure == "precommit" {
					return PreparedChange{}, assert.AnError
				}
				staged = true
				return PreparedChange{
					Commit: func() { events = append(events, "commit") },
					Abort: func() {
						events = append(events, "abort")
						staged = false
					},
				}, nil
			})
			persistCalls := 0
			m.persist = func(*Config, string) error {
				persistCalls++
				events = append(events, "persist")
				if failure == "persist" {
					return assert.AnError
				}
				return nil
			}
			callbackCalls := 0
			m.OnConfigChange(func(_, _ *Config) { callbackCalls++ })

			expected := before.Revision
			if failure == "stale" {
				expected++
			}
			_, err = m.CompareAndSwap(context.Background(), expected, candidate)
			require.Error(t, err)
			if failure == "stale" {
				assert.ErrorIs(t, err, ErrConfigConflict)
			}

			after, snapshotErr := m.Snapshot()
			require.NoError(t, snapshotErr)
			assert.Equal(t, before, after)
			assert.Equal(t, candidateBefore, candidate, "a rejected CAS must not mutate its input")
			assert.False(t, staged)
			assert.Zero(t, callbackCalls)
			assert.False(t, m.NeedsLibrarySync())
			assert.Empty(t, m.GetPreviousMountPath())
			afterDisk, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			assert.True(t, bytes.Equal(beforeDisk, afterDisk))

			switch failure {
			case "stale", "validation":
				assert.Empty(t, events)
				assert.Zero(t, persistCalls)
			case "precommit":
				assert.Equal(t, []string{"precommit"}, events)
				assert.Zero(t, persistCalls)
			case "persist":
				assert.Equal(t, []string{"precommit", "persist", "abort"}, events)
				assert.Equal(t, 1, persistCalls)
			}
		})
	}
}

func TestManagerPreparedChangeOrdering(t *testing.T) {
	m, _, _ := newAuthorityManager(t)
	base, err := m.Snapshot()
	require.NoError(t, err)
	candidate := base.Config.DeepCopy()
	candidate.Network.NoProxy = "next.invalid"

	var events []string
	prepared, persisted := false, false
	m.SetPrecommit(func(context.Context, *Config, *Config) (PreparedChange, error) {
		events = append(events, "precommit")
		assert.Empty(t, m.current.Network.NoProxy)
		prepared = true
		return PreparedChange{
			Commit: func() {
				events = append(events, "commit")
				assert.True(t, persisted)
				assert.Empty(t, m.current.Network.NoProxy, "runtime commit must precede manager publication")
				prepared = false
			},
			Abort: func() { events = append(events, "abort") },
		}, nil
	})
	m.persist = func(*Config, string) error {
		events = append(events, "persist")
		assert.True(t, prepared)
		assert.Empty(t, m.current.Network.NoProxy, "persistence must precede publication")
		persisted = true
		return nil
	}
	m.OnConfigChange(func(_, _ *Config) {
		events = append(events, "callback-1")
		assert.False(t, prepared)
		assert.Equal(t, "next.invalid", m.current.Network.NoProxy)
	})
	m.OnConfigChange(func(_, _ *Config) { events = append(events, "callback-2") })

	_, err = m.CompareAndSwap(context.Background(), base.Revision, candidate)
	require.NoError(t, err)
	assert.Equal(t, []string{"precommit", "persist", "commit", "callback-1", "callback-2"}, events)
}

func TestManagerCASCommitsAfterPostRenameDirectorySyncFailure(t *testing.T) {
	m, _, path := newAuthorityManager(t)
	base, err := m.Snapshot()
	require.NoError(t, err)
	candidate := base.Config.DeepCopy()
	candidate.Network.NoProxy = "committed.invalid"

	var events []string
	staged := false
	syncCalls := 0
	syncSawCandidate := false
	m.SetPrecommit(func(context.Context, *Config, *Config) (PreparedChange, error) {
		events = append(events, "precommit")
		staged = true
		return PreparedChange{
			Commit: func() {
				events = append(events, "commit")
				staged = false
			},
			Abort: func() {
				events = append(events, "abort")
				staged = false
			},
		}, nil
	})
	m.persist = func(config *Config, filename string) error {
		events = append(events, "persist")
		return saveToFileWithDirectorySync(config, filename, func(*os.File) error {
			events = append(events, "dir-sync")
			syncCalls++
			data, readErr := os.ReadFile(filename)
			require.NoError(t, readErr)
			var visible Config
			require.NoError(t, yaml.Unmarshal(data, &visible))
			syncSawCandidate = visible.Network.NoProxy == "committed.invalid"
			return assert.AnError
		})
	}
	m.OnConfigChange(func(_, _ *Config) { events = append(events, "callback") })

	committed, err := m.CompareAndSwap(context.Background(), base.Revision, candidate)
	require.NoError(t, err)
	assert.Equal(t, base.Revision+1, committed.Revision)
	assert.Equal(t, "committed.invalid", committed.Config.Network.NoProxy)
	assert.False(t, staged)
	assert.Equal(t, 1, syncCalls)
	assert.True(t, syncSawCandidate, "directory sync ran before the candidate rename was visible")
	assert.Equal(t, []string{"precommit", "persist", "dir-sync", "commit", "callback"}, events)

	current, err := m.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, committed, current)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var persisted Config
	require.NoError(t, yaml.Unmarshal(data, &persisted))
	assert.Equal(t, "committed.invalid", persisted.Network.NoProxy)
}
