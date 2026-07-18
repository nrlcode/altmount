package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/nntppool/v4"
)

type speedtestTransportMutation struct {
	name   string
	mutate func(*config.ProviderConfig)
}

func speedtestTransportMutations() []speedtestTransportMutation {
	return []speedtestTransportMutation{
		{"host", func(p *config.ProviderConfig) { p.Host = "changed-host.invalid" }},
		{"port", func(p *config.ProviderConfig) { p.Port++ }},
		{"username", func(p *config.ProviderConfig) { p.Username = "changed-user" }},
		{"password", func(p *config.ProviderConfig) { p.Password = "changed-password-secret" }},
		{"TLS", func(p *config.ProviderConfig) { p.TLS = false }},
		{"insecure TLS", func(p *config.ProviderConfig) { p.InsecureTLS = true }},
		{"effective connections", func(p *config.ProviderConfig) { p.MaxConnections++ }},
		{"effective inflight", func(p *config.ProviderConfig) { p.InflightRequests++ }},
	}
}

func baseSpeedtestProvider() config.ProviderConfig {
	return config.ProviderConfig{
		ID: "stable-provider-id", Name: "Display Name", Host: "opaque-host.invalid", Port: 563,
		Username: "opaque-user", Password: "raw-password-secret", TLS: true,
		MaxConnections: 1, InflightRequests: 10,
	}
}

func coordinatorWithoutJanitor() *speedtestCoordinator {
	return &speedtestCoordinator{clients: make(map[string]*cachedSpeedtestClient), stopCh: make(chan struct{})}
}

func awaitSpeedtest[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func TestSpeedtestCacheKeyTransportIdentity(t *testing.T) {
	base := baseSpeedtestProvider()
	baseKey := speedtestCacheKey(&base)
	for _, tc := range speedtestTransportMutations() {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.mutate(&changed)
			if got := speedtestCacheKey(&changed); got == baseKey {
				t.Fatalf("cache key unchanged after %s changed", tc.name)
			}
		})
	}
}

func TestSpeedtestCacheKeyIgnoresObservationsAndHidesSecrets(t *testing.T) {
	base := baseSpeedtestProvider()
	observed := base
	observed.Name = "Renamed Display"
	observed.LastRTTMs = 999
	observed.LastSpeedTestMbps = 123.5
	now := time.Unix(1_900_000_000, 0)
	observed.LastSpeedTestTime = &now
	if speedtestCacheKey(&observed) != speedtestCacheKey(&base) {
		t.Fatal("display name or observational telemetry invalidated speed-test cache key")
	}

	defaulted := base
	defaulted.InflightRequests = 0
	if speedtestCacheKey(&defaulted) != speedtestCacheKey(&base) {
		t.Fatal("default-equivalent effective inflight invalidated speed-test cache key")
	}
	otherID := base
	otherID.ID = "different-provider-id"
	if speedtestCacheKey(&otherID) == speedtestCacheKey(&base) {
		t.Fatal("changing only canonical provider ID did not change speed-test cache key")
	}
	key := speedtestCacheKey(&base)
	for _, raw := range []string{base.Host, base.Username, base.Password} {
		if strings.Contains(key, raw) {
			t.Errorf("key exposes unhashed transport/secret material %q: %q", raw, key)
		}
	}
}

func TestSpeedtestCacheAndSingleflightUseCompoundKey(t *testing.T) {
	sc := coordinatorWithoutJanitor()
	t.Cleanup(sc.shutdown)
	buildCalls := 0
	sc.buildClient = func(context.Context, *config.ProviderConfig, int, int) (*nntppool.Client, error) {
		buildCalls++
		return nil, nil
	}
	base := baseSpeedtestProvider()
	changed := base
	changed.Password = "rotated-password-secret"
	for _, p := range []*config.ProviderConfig{&base, &base, &changed} {
		_, target, err := sc.getOrBuildClient(context.Background(), p)
		if err != nil {
			t.Fatal(err)
		}
		if target != p.ID {
			t.Fatalf("coordinator target = %q, want canonical ID %q", target, p.ID)
		}
	}
	if buildCalls != 2 {
		t.Fatalf("builder calls = %d, want 2 (reuse identical transport, rebuild changed transport)", buildCalls)
	}
	if len(sc.clients) != 2 {
		t.Fatalf("cache entries = %d, want 2 compound identities", len(sc.clients))
	}

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	done := make(chan error, 2)
	run := func(p *config.ProviderConfig) {
		go func() {
			_, err := sc.run(context.Background(), p, func(*nntppool.Client, string) (any, error) {
				entered <- struct{}{}
				<-release
				return nil, nil
			})
			done <- err
		}()
	}
	run(&base)
	awaitSpeedtest(t, entered, "first speed test")
	run(&changed)
	select {
	case <-entered:
		close(release)
		released = true
	case <-time.After(500 * time.Millisecond):
		close(release)
		released = true
		for range 2 {
			awaitSpeedtest(t, done, "coalesced speed-test return")
		}
		t.Fatal("singleflight coalesced equal provider IDs with different transport fingerprints")
	}
	for range 2 {
		if err := awaitSpeedtest(t, done, "independent speed-test return"); err != nil {
			t.Error(err)
		}
	}
}
