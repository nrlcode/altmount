package database

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chg007RegistryState struct {
	providers      []HealthProvider
	providerRows   int
	generationRows int
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
			(SELECT COUNT(*) FROM health_provider_generations)
	`).Scan(&state.providerRows, &state.generationRows))
	return state
}

func TestFACORECHG007AmbiguousRetainedProviderIdentityFailsWithoutChurn(t *testing.T) {
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	identity := ProviderSpec{
		DisplayName: "Identity", Endpoint: "shared-history.example.invalid", Port: 119,
		Account: "shared", Role: ProviderRolePrimary, Order: 0,
	}

	first := identity
	first.StableID = "retained-a"
	_, err := repo.ReconcileProviders(ctx, []ProviderSpec{first})
	require.NoError(t, err)
	second := identity
	second.StableID = "retained-b"
	_, err = repo.ReconcileProviders(ctx, []ProviderSpec{second})
	require.NoError(t, err)
	before := chg007ProviderRegistryState(t, ctx, db, repo)

	for attempt := 0; attempt < 2; attempt++ {
		_, err = repo.ReconcileProviders(ctx, []ProviderSpec{identity})
		assert.ErrorContains(t, err, "multiple retained provider IDs")
		assert.Equal(t, before, chg007ProviderRegistryState(t, ctx, db, repo),
			"ambiguous empty-ID reconciliation must not mint UUIDs or mutate durable history")
	}
}

func TestFACORECHG007ProviderIdentityReuseBoundaries(t *testing.T) {
	t.Run("zero retained matches mints once", func(t *testing.T) {
		db, repo := newPR4Repository(t)
		ctx := context.Background()
		spec := ProviderSpec{
			DisplayName: "New", Endpoint: "new.example.invalid", Port: 563,
			Account: "account", Role: ProviderRolePrimary, Order: 0,
		}

		initial, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
		require.NoError(t, err)
		require.Len(t, initial, 1)
		require.NoError(t, uuid.Validate(initial[0].ID))
		assert.Equal(t, int64(1), initial[0].CurrentGeneration)

		spec.DisplayName = "Renamed"
		reconciled, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
		require.NoError(t, err)
		require.Len(t, reconciled, 1)
		assert.Equal(t, initial[0].ID, reconciled[0].ID)
		assert.Equal(t, int64(1), reconciled[0].CurrentGeneration)
		state := chg007ProviderRegistryState(t, ctx, db, repo)
		assert.Equal(t, 1, state.providerRows)
		assert.Equal(t, 1, state.generationRows)
	})

	t.Run("one unclaimed tombstoned match relinks exact ID", func(t *testing.T) {
		db, repo := newPR4Repository(t)
		ctx := context.Background()
		spec := ProviderSpec{
			StableID: "retained-provider", DisplayName: "Retained",
			Endpoint: "retained.example.invalid", Port: 563, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		}
		_, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
		require.NoError(t, err)
		_, err = repo.ReconcileProviders(ctx, nil)
		require.NoError(t, err)

		spec.StableID = ""
		spec.DisplayName = "Relinked"
		relinked, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
		require.NoError(t, err)
		require.Len(t, relinked, 1)
		assert.Equal(t, "retained-provider", relinked[0].ID)
		assert.Equal(t, int64(1), relinked[0].CurrentGeneration)
		state := chg007ProviderRegistryState(t, ctx, db, repo)
		assert.Equal(t, 1, state.providerRows)
		assert.Equal(t, 1, state.generationRows)
	})
}
