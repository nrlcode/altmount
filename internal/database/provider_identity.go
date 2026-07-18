package database

import (
	"context"
	"database/sql"
	"fmt"

	appconfig "github.com/javi11/altmount/internal/config"
)

// These aliases keep the durable repository API source-compatible while the
// dependency-neutral identity contract is owned by config.
type ProviderIdentityRecord = appconfig.ProviderIdentityRecord
type ProviderIdentityGeneration = appconfig.ProviderIdentityGeneration
type ProviderIdentityRegistrySnapshot = appconfig.ProviderIdentityRegistrySnapshot

// ProviderIdentityFingerprint is the database-facing compatibility wrapper
// around the canonical config-owned identity algorithm.
func ProviderIdentityFingerprint(endpoint string, port int, account string) (string, error) {
	return appconfig.ProviderIdentityFingerprint(endpoint, port, account)
}

// ReadProviderIdentityRegistrySnapshot reads every retained provider parent
// and generation from one read-only statement. The LEFT JOIN deliberately
// retains parents that have no generation rows.
func (r *HealthStateRepository) ReadProviderIdentityRegistrySnapshot(ctx context.Context) (ProviderIdentityRegistrySnapshot, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT p.id, p.active, p.tombstoned_at, p.current_generation,
		       g.provider_id, g.generation, g.endpoint, g.port, g.account,
		       g.identity_fingerprint
		FROM health_providers p
		LEFT JOIN health_provider_generations g ON g.provider_id = p.id
		ORDER BY p.id, g.generation
	`)
	if err != nil {
		return ProviderIdentityRegistrySnapshot{}, fmt.Errorf("read provider identity registry snapshot: %w", err)
	}
	defer rows.Close()

	var snapshot ProviderIdentityRegistrySnapshot
	seenProviders := make(map[string]struct{})
	for rows.Next() {
		var provider ProviderIdentityRecord
		var generationProviderID, endpoint, account, fingerprint sql.NullString
		var generation, port sql.NullInt64
		if err := rows.Scan(
			&provider.ID,
			&provider.Active,
			&provider.TombstonedAt,
			&provider.CurrentGeneration,
			&generationProviderID,
			&generation,
			&endpoint,
			&port,
			&account,
			&fingerprint,
		); err != nil {
			return ProviderIdentityRegistrySnapshot{}, fmt.Errorf("scan provider identity registry snapshot: %w", err)
		}

		if _, seen := seenProviders[provider.ID]; !seen {
			seenProviders[provider.ID] = struct{}{}
			snapshot.Providers = append(snapshot.Providers, provider)
		}
		if generationProviderID.Valid {
			snapshot.Generations = append(snapshot.Generations, ProviderIdentityGeneration{
				ProviderID:          generationProviderID.String,
				Generation:          generation.Int64,
				Endpoint:            endpoint.String,
				Port:                int(port.Int64),
				Account:             account.String,
				IdentityFingerprint: fingerprint.String,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return ProviderIdentityRegistrySnapshot{}, fmt.Errorf("iterate provider identity registry snapshot: %w", err)
	}
	return snapshot, nil
}
