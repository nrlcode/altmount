package database

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chg007RegistryState struct {
	providers         []HealthProvider
	providerRows      int
	generationRows    int
	snapshotRows      int
	snapshotEntryRows int
}

type chg007PreexistingRegistryState struct {
	registry chg007RegistryState
	snapshot *ProviderSnapshot
}

type chg007IdentityBackend struct {
	db   *DB
	repo *HealthStateRepository
}

func forEachFACORECHG007IdentityBackend(
	t *testing.T,
	test func(*testing.T, chg007IdentityBackend),
) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		db, repo := newPR4Repository(t)
		test(t, chg007IdentityBackend{db: db, repo: repo})
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
		}
		db, err := NewDB(Config{Type: "postgres", DSN: dsn})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		_, err = db.Connection().Exec(`TRUNCATE TABLE health_provider_snapshots, health_providers CASCADE`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, cleanupErr := db.Connection().Exec(
				`TRUNCATE TABLE health_provider_snapshots, health_providers CASCADE`,
			)
			assert.NoError(t, cleanupErr)
		})
		test(t, chg007IdentityBackend{
			db: db, repo: NewHealthStateRepository(db.Connection(), DialectPostgres),
		})
	})
}

func chg007ProviderRegistryState(
	t *testing.T,
	ctx context.Context,
	db *DB,
	repo *HealthStateRepository,
) chg007RegistryState {
	t.Helper()
	providers, err := repo.ListProviders(ctx, true)
	require.NoError(t, err)
	state := chg007RegistryState{providers: providers}
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM health_providers),
			(SELECT COUNT(*) FROM health_provider_generations),
			(SELECT COUNT(*) FROM health_provider_snapshots),
			(SELECT COUNT(*) FROM health_provider_snapshot_entries)
	`).Scan(
		&state.providerRows, &state.generationRows,
		&state.snapshotRows, &state.snapshotEntryRows,
	))
	return state
}

func chg007CapturePreexistingRegistryState(
	t *testing.T,
	ctx context.Context,
	db *DB,
	repo *HealthStateRepository,
) chg007PreexistingRegistryState {
	t.Helper()
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, time.Unix(1_700_700_000, 0).UTC())
	require.NoError(t, err)
	stored, err := repo.GetProviderSnapshot(ctx, snapshot.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.NotEmpty(t, stored.Entries, "preexisting state must include an active provider")
	return chg007PreexistingRegistryState{
		registry: chg007ProviderRegistryState(t, ctx, db, repo),
		snapshot: stored,
	}
}

func assertCHG007PreexistingRegistryStatePreserved(
	t *testing.T,
	ctx context.Context,
	db *DB,
	repo *HealthStateRepository,
	want chg007PreexistingRegistryState,
) {
	t.Helper()
	assert.Equal(t, want.registry, chg007ProviderRegistryState(t, ctx, db, repo),
		"rejected reconciliation must preserve provider, generation, and snapshot row counts")
	retained, err := repo.GetProviderSnapshot(ctx, want.snapshot.ID)
	require.NoError(t, err)
	assert.Equal(t, want.snapshot, retained, "rejected reconciliation must preserve the captured snapshot")
}

func TestFACORECHG007AmbiguousRetainedProviderIdentityFailsWithoutChurn(t *testing.T) {
	forEachFACORECHG007IdentityBackend(t, func(t *testing.T, backend chg007IdentityBackend) {
		ctx := context.Background()
		suffix := uuid.NewString()
		identity := ProviderSpec{
			DisplayName: "Identity", Endpoint: "shared-history-" + suffix + ".invalid", Port: 119,
			Account: "shared", Role: ProviderRolePrimary, Order: 0,
		}

		first := identity
		first.StableID = "retained-a-" + suffix
		_, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{first})
		require.NoError(t, err)
		second := identity
		second.StableID = "retained-b-" + suffix
		_, err = backend.repo.ReconcileProviders(ctx, []ProviderSpec{second})
		require.NoError(t, err)
		before := chg007CapturePreexistingRegistryState(t, ctx, backend.db, backend.repo)

		for attempt := 0; attempt < 2; attempt++ {
			_, err = backend.repo.ReconcileProviders(ctx, []ProviderSpec{identity})
			assert.ErrorContains(t, err, "multiple retained provider IDs")
			assertCHG007PreexistingRegistryStatePreserved(
				t, ctx, backend.db, backend.repo, before,
			)
		}
	})
}

func TestFACORECHG007ProviderIdentityReuseBoundaries(t *testing.T) {
	forEachFACORECHG007IdentityBackend(t, func(t *testing.T, backend chg007IdentityBackend) {
		t.Run("zero retained matches mints once", func(t *testing.T) {
			ctx := context.Background()
			suffix := uuid.NewString()
			before := chg007ProviderRegistryState(t, ctx, backend.db, backend.repo)
			spec := ProviderSpec{
				DisplayName: "New", Endpoint: "new-" + suffix + ".example.invalid", Port: 563,
				Account: "account", Role: ProviderRolePrimary, Order: 0,
			}

			initial, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{spec})
			require.NoError(t, err)
			require.Len(t, initial, 1)
			require.NoError(t, uuid.Validate(initial[0].ID))
			assert.Equal(t, int64(1), initial[0].CurrentGeneration)

			spec.DisplayName = "Renamed"
			reconciled, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{spec})
			require.NoError(t, err)
			require.Len(t, reconciled, 1)
			assert.Equal(t, initial[0].ID, reconciled[0].ID)
			assert.Equal(t, int64(1), reconciled[0].CurrentGeneration)
			state := chg007ProviderRegistryState(t, ctx, backend.db, backend.repo)
			assert.Equal(t, before.providerRows+1, state.providerRows)
			assert.Equal(t, before.generationRows+1, state.generationRows)
		})

		t.Run("one unclaimed tombstoned match relinks exact ID", func(t *testing.T) {
			ctx := context.Background()
			suffix := uuid.NewString()
			providerID := "retained-provider-" + suffix
			spec := ProviderSpec{
				StableID: providerID, DisplayName: "Retained",
				Endpoint: "retained-" + suffix + ".example.invalid", Port: 563, Account: "account",
				Role: ProviderRolePrimary, Order: 0,
			}
			_, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{spec})
			require.NoError(t, err)
			seeded := chg007ProviderRegistryState(t, ctx, backend.db, backend.repo)
			_, err = backend.repo.ReconcileProviders(ctx, nil)
			require.NoError(t, err)

			spec.StableID = ""
			spec.DisplayName = "Relinked"
			relinked, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{spec})
			require.NoError(t, err)
			require.Len(t, relinked, 1)
			assert.Equal(t, providerID, relinked[0].ID)
			assert.Equal(t, int64(1), relinked[0].CurrentGeneration)
			state := chg007ProviderRegistryState(t, ctx, backend.db, backend.repo)
			assert.Equal(t, seeded.providerRows, state.providerRows)
			assert.Equal(t, seeded.generationRows, state.generationRows)
			assert.Equal(t, seeded.snapshotRows, state.snapshotRows)
			assert.Equal(t, seeded.snapshotEntryRows, state.snapshotEntryRows)
		})
	})
}

func TestFACORECHG007CompetingRetainedProviderIdentitiesFailWithoutChurn(t *testing.T) {
	forEachFACORECHG007IdentityBackend(t, func(t *testing.T, backend chg007IdentityBackend) {
		ctx := context.Background()
		suffix := uuid.NewString()
		providerID := "multi-generation-" + suffix
		first := ProviderSpec{
			StableID: providerID, DisplayName: "First",
			Endpoint: "first-" + suffix + ".example.invalid", Port: 563, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		}
		_, err := backend.repo.ReconcileProviders(ctx, []ProviderSpec{first})
		require.NoError(t, err)
		second := first
		second.DisplayName = "Second"
		second.Endpoint = "second-" + suffix + ".example.invalid"
		_, err = backend.repo.ReconcileProviders(ctx, []ProviderSpec{second})
		require.NoError(t, err)
		generations, err := backend.repo.ListProviderGenerations(ctx, providerID)
		require.NoError(t, err)
		require.Len(t, generations, 2)
		require.NotEqual(t, generations[0].IdentityFingerprint, generations[1].IdentityFingerprint)
		before := chg007CapturePreexistingRegistryState(t, ctx, backend.db, backend.repo)

		first.StableID = ""
		second.StableID = ""
		second.Order = 1
		_, err = backend.repo.ReconcileProviders(ctx, []ProviderSpec{first, second})
		assert.ErrorContains(t, err, "retained provider identity")
		assertCHG007PreexistingRegistryStatePreserved(t, ctx, backend.db, backend.repo, before)
	})
}
