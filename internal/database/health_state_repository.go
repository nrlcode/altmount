package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	appconfig "github.com/javi11/altmount/internal/config"
)

var (
	ErrStaleHealthLease         = errors.New("stale or expired health run lease")
	ErrHealthChunkConflict      = errors.New("health chunk identity conflicts with committed content")
	ErrProviderSnapshotMismatch = errors.New("provider generation is not in the run dispatch snapshot")
)

// HealthStateRepository owns the additive PR4 durable provider, revision, run,
// observation, gap, and recovery state. The PR3 health engine is intentionally
// not wired to it until PR5 observation mode.
type HealthStateRepository struct {
	db      *dialectAwareDB
	dialect dialectHelper
	now     func() time.Time
}

func NewHealthStateRepository(db *sql.DB, d Dialect) *HealthStateRepository {
	return &HealthStateRepository{
		db: newDialectAwareDB(db, d), dialect: dialectHelper{d: d},
		now: func() time.Time { return time.Now().UTC() },
	}
}

func (r *HealthStateRepository) withTransaction(ctx context.Context, fn func(*dialectAwareTx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin health state transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit health state transaction: %w", err)
	}
	return nil
}

func (r *HealthStateRepository) EnsureFileRevision(ctx context.Context, spec FileRevisionSpec) (*HealthFileRevision, error) {
	spec.FilePath = normalizeHealthPath(spec.FilePath)
	if spec.FilePath == "" || spec.LayoutFingerprint == "" {
		return nil, fmt.Errorf("file path and layout fingerprint are required")
	}
	if spec.VirtualSize < 0 || spec.SegmentCount < 0 {
		return nil, fmt.Errorf("file revision sizes must be non-negative")
	}
	now := time.Now().UTC()
	var revision HealthFileRevision
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status, created_at, updated_at)
			VALUES (?, 'pending', ?, ?)
			ON CONFLICT(file_path) DO NOTHING
		`, spec.FilePath, now, now)
		if err != nil {
			return fmt.Errorf("ensure file health identity: %w", err)
		}
		var fileHealthID int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM file_health WHERE file_path = ?`, spec.FilePath).Scan(&fileHealthID); err != nil {
			return fmt.Errorf("read file health identity: %w", err)
		}

		// Deactivate first so the partial unique index never observes two active
		// revisions, including when a retained historical layout is reactivated.
		if _, err := tx.ExecContext(ctx, `UPDATE health_file_revisions SET active = FALSE WHERE file_health_id = ? AND active = TRUE`, fileHealthID); err != nil {
			return fmt.Errorf("deactivate prior file revision: %w", err)
		}

		err = scanHealthFileRevision(tx.QueryRowContext(ctx, `
			SELECT id, file_health_id, layout_fingerprint, virtual_size, segment_count,
			       active, created_at, activated_at
			FROM health_file_revisions
			WHERE file_health_id = ? AND layout_fingerprint = ?
		`, fileHealthID, spec.LayoutFingerprint), &revision)
		switch {
		case err == nil:
			if revision.VirtualSize != spec.VirtualSize || revision.SegmentCount != spec.SegmentCount {
				return fmt.Errorf("retained layout fingerprint has different file dimensions")
			}
			_, err = tx.ExecContext(ctx, `
				UPDATE health_file_revisions
				SET active = TRUE, activated_at = ?, virtual_size = ?, segment_count = ?
				WHERE id = ?
			`, now, spec.VirtualSize, spec.SegmentCount, revision.ID)
			if err != nil {
				return fmt.Errorf("reactivate file revision: %w", err)
			}
			revision.Active = true
			revision.ActivatedAt = now
			revision.VirtualSize = spec.VirtualSize
			revision.SegmentCount = spec.SegmentCount
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("find file revision: %w", err)
		}

		revision = HealthFileRevision{
			ID: uuid.NewString(), FileHealthID: fileHealthID,
			LayoutFingerprint: spec.LayoutFingerprint, VirtualSize: spec.VirtualSize,
			SegmentCount: spec.SegmentCount, Active: true, CreatedAt: now, ActivatedAt: now,
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_file_revisions
				(id, file_health_id, layout_fingerprint, virtual_size, segment_count, active, created_at, activated_at)
			VALUES (?, ?, ?, ?, ?, TRUE, ?, ?)
		`, revision.ID, revision.FileHealthID, revision.LayoutFingerprint, revision.VirtualSize,
			revision.SegmentCount, revision.CreatedAt, revision.ActivatedAt)
		if err != nil {
			return fmt.Errorf("insert file revision: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

func scanHealthFileRevision(row rowScanner, revision *HealthFileRevision) error {
	return row.Scan(&revision.ID, &revision.FileHealthID, &revision.LayoutFingerprint,
		&revision.VirtualSize, &revision.SegmentCount, &revision.Active,
		&revision.CreatedAt, &revision.ActivatedAt)
}

func (r *HealthStateRepository) ListFileRevisions(ctx context.Context, filePath string) ([]HealthFileRevision, error) {
	filePath = normalizeHealthPath(filePath)
	rows, err := r.db.QueryContext(ctx, `
		SELECT r.id, r.file_health_id, r.layout_fingerprint, r.virtual_size,
		       r.segment_count, r.active, r.created_at, r.activated_at
		FROM health_file_revisions r
		JOIN file_health f ON f.id = r.file_health_id
		WHERE f.file_path = ?
		ORDER BY r.created_at, r.id
	`, filePath)
	if err != nil {
		return nil, fmt.Errorf("list file revisions: %w", err)
	}
	defer rows.Close()
	var revisions []HealthFileRevision
	for rows.Next() {
		var revision HealthFileRevision
		if err := scanHealthFileRevision(rows, &revision); err != nil {
			return nil, fmt.Errorf("scan file revision: %w", err)
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

func normalizeProviderSpec(spec ProviderSpec) (ProviderSpec, string, error) {
	spec.StableID = strings.TrimSpace(spec.StableID)
	spec.DisplayName = strings.TrimSpace(spec.DisplayName)
	spec.Endpoint, spec.Account = appconfig.NormalizeProviderIdentity(spec.Endpoint, spec.Account)
	if spec.DisplayName == "" || spec.Endpoint == "" {
		return ProviderSpec{}, "", fmt.Errorf("provider display name and endpoint are required")
	}
	if spec.Port <= 0 || spec.Port > 65535 {
		return ProviderSpec{}, "", fmt.Errorf("provider port is outside 1..65535")
	}
	if spec.Role != ProviderRolePrimary && spec.Role != ProviderRoleBackup {
		return ProviderSpec{}, "", fmt.Errorf("invalid provider role %q", spec.Role)
	}
	if spec.Order < 0 {
		return ProviderSpec{}, "", fmt.Errorf("provider order must be non-negative")
	}
	identity, err := appconfig.ProviderIdentityFingerprint(spec.Endpoint, spec.Port, spec.Account)
	if err != nil {
		return ProviderSpec{}, "", err
	}
	return spec, identity, nil
}

func (r *HealthStateRepository) ReconcileProviders(ctx context.Context, specs []ProviderSpec) ([]HealthProvider, error) {
	type normalizedProvider struct {
		spec     ProviderSpec
		identity string
	}
	normalized := make([]normalizedProvider, len(specs))
	orders := make(map[int]struct{}, len(specs))
	stableIDs := make(map[string]struct{}, len(specs))
	for i, spec := range specs {
		n, identity, err := normalizeProviderSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("provider %d: %w", i, err)
		}
		if _, exists := orders[n.Order]; exists {
			return nil, fmt.Errorf("duplicate configured provider order %d", n.Order)
		}
		orders[n.Order] = struct{}{}
		if n.StableID != "" {
			if _, exists := stableIDs[n.StableID]; exists {
				return nil, fmt.Errorf("duplicate stable provider ID")
			}
			stableIDs[n.StableID] = struct{}{}
		}
		normalized[i] = normalizedProvider{spec: n, identity: identity}
	}
	sort.SliceStable(normalized, func(i, j int) bool { return normalized[i].spec.Order < normalized[j].spec.Order })

	now := time.Now().UTC()
	seenIDs := make(map[string]struct{}, len(normalized))
	reservedIDs := make(map[string]struct{}, len(stableIDs))
	for id := range stableIDs {
		reservedIDs[id] = struct{}{}
	}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_providers
			SET active = FALSE, tombstoned_at = ?, updated_at = ?
			WHERE active = TRUE
		`, now, now); err != nil {
			return fmt.Errorf("tombstone prior providers: %w", err)
		}

		for _, desired := range normalized {
			providerID := desired.spec.StableID
			if providerID == "" {
				matches, err := providerIDsForIdentity(ctx, tx, desired.identity)
				if err != nil {
					return err
				}
				if len(matches) == 1 {
					_, alreadyClaimed := seenIDs[matches[0]]
					_, explicitlyReserved := reservedIDs[matches[0]]
					if !alreadyClaimed && !explicitlyReserved {
						providerID = matches[0]
					}
				}
				if providerID == "" {
					providerID = uuid.NewString()
				}
			}
			if _, duplicate := seenIDs[providerID]; duplicate {
				return fmt.Errorf("provider configuration resolves to duplicate stable identity")
			}
			seenIDs[providerID] = struct{}{}

			var generation int64
			var currentIdentity string
			err := tx.QueryRowContext(ctx, `
				SELECT p.current_generation, g.identity_fingerprint
				FROM health_providers p
				JOIN health_provider_generations g
				  ON g.provider_id = p.id AND g.generation = p.current_generation
				WHERE p.id = ?
			`, providerID).Scan(&generation, &currentIdentity)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				generation = 1
				_, err = tx.ExecContext(ctx, `
					INSERT INTO health_providers
						(id, display_name, role, configured_order, active, current_generation,
						 tombstoned_at, created_at, updated_at)
					VALUES (?, ?, ?, ?, TRUE, 1, NULL, ?, ?)
				`, providerID, desired.spec.DisplayName, desired.spec.Role, desired.spec.Order, now, now)
				if err != nil {
					return fmt.Errorf("insert provider registry row: %w", err)
				}
				if err := insertProviderGeneration(ctx, tx, providerID, generation, desired, now); err != nil {
					return err
				}
			case err != nil:
				return fmt.Errorf("read provider registry row: %w", err)
			default:
				if currentIdentity != desired.identity {
					generation++
					if err := insertProviderGeneration(ctx, tx, providerID, generation, desired, now); err != nil {
						return err
					}
				}
				_, err = tx.ExecContext(ctx, `
					UPDATE health_providers
					SET display_name = ?, role = ?, configured_order = ?, active = TRUE,
					    current_generation = ?, tombstoned_at = NULL, updated_at = ?
					WHERE id = ?
				`, desired.spec.DisplayName, desired.spec.Role, desired.spec.Order, generation, now, providerID)
				if err != nil {
					return fmt.Errorf("update provider registry row: %w", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.ListProviders(ctx, false)
}

func providerIDsForIdentity(ctx context.Context, tx *dialectAwareTx, identity string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT provider_id
		FROM health_provider_generations
		WHERE identity_fingerprint = ?
		ORDER BY provider_id
	`, identity)
	if err != nil {
		return nil, fmt.Errorf("find retained provider identity: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan retained provider identity: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func insertProviderGeneration(ctx context.Context, tx *dialectAwareTx, providerID string, generation int64, desired struct {
	spec     ProviderSpec
	identity string
}, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO health_provider_generations
			(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, providerID, generation, desired.spec.Endpoint, desired.spec.Port, desired.spec.Account, desired.identity, now)
	if err != nil {
		return fmt.Errorf("insert provider generation: %w", err)
	}
	return nil
}

func scanHealthProvider(row rowScanner, provider *HealthProvider) error {
	return row.Scan(&provider.ID, &provider.DisplayName, &provider.Role, &provider.Order,
		&provider.Active, &provider.CurrentGeneration, &provider.TombstonedAt,
		&provider.CreatedAt, &provider.UpdatedAt)
}

func (r *HealthStateRepository) ListProviders(ctx context.Context, includeTombstoned bool) ([]HealthProvider, error) {
	query := `
		SELECT id, display_name, role, configured_order, active, current_generation,
		       tombstoned_at, created_at, updated_at
		FROM health_providers
	`
	if !includeTombstoned {
		query += ` WHERE active = TRUE`
	}
	query += ` ORDER BY active DESC, configured_order, id`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer rows.Close()
	var providers []HealthProvider
	for rows.Next() {
		var provider HealthProvider
		if err := scanHealthProvider(rows, &provider); err != nil {
			return nil, fmt.Errorf("scan provider: %w", err)
		}
		providers = append(providers, provider)
	}
	return providers, rows.Err()
}

func (r *HealthStateRepository) ListProviderGenerations(ctx context.Context, providerID string) ([]HealthProviderGeneration, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT provider_id, generation, endpoint, port, account, identity_fingerprint, created_at
		FROM health_provider_generations
		WHERE provider_id = ?
		ORDER BY generation
	`, providerID)
	if err != nil {
		return nil, fmt.Errorf("list provider generations: %w", err)
	}
	defer rows.Close()
	var generations []HealthProviderGeneration
	for rows.Next() {
		var generation HealthProviderGeneration
		if err := rows.Scan(&generation.ProviderID, &generation.Generation, &generation.Endpoint,
			&generation.Port, &generation.Account, &generation.IdentityFingerprint,
			&generation.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan provider generation: %w", err)
		}
		generations = append(generations, generation)
	}
	return generations, rows.Err()
}

func (r *HealthStateRepository) CaptureActiveProviderSnapshot(ctx context.Context, at time.Time) (*ProviderSnapshot, error) {
	if at.IsZero() {
		return nil, fmt.Errorf("snapshot time is required")
	}
	snapshot := &ProviderSnapshot{ID: uuid.NewString(), CreatedAt: at.UTC()}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO health_provider_snapshots (id, created_at) VALUES (?, ?)`, snapshot.ID, snapshot.CreatedAt); err != nil {
			return fmt.Errorf("insert provider snapshot: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT id, current_generation, role, configured_order
			FROM health_providers
			WHERE active = TRUE
			ORDER BY configured_order, id
		`)
		if err != nil {
			return fmt.Errorf("read active providers for snapshot: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var entry ProviderSnapshotEntry
			if err := rows.Scan(&entry.ProviderID, &entry.ProviderGeneration, &entry.Role, &entry.Order); err != nil {
				return fmt.Errorf("scan provider snapshot entry: %w", err)
			}
			snapshot.Entries = append(snapshot.Entries, entry)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, entry := range snapshot.Entries {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO health_provider_snapshot_entries
					(snapshot_id, provider_id, provider_generation, role, configured_order)
				VALUES (?, ?, ?, ?, ?)
			`, snapshot.ID, entry.ProviderID, entry.ProviderGeneration, entry.Role, entry.Order); err != nil {
				return fmt.Errorf("insert provider snapshot entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (r *HealthStateRepository) GetProviderSnapshot(ctx context.Context, id string) (*ProviderSnapshot, error) {
	var snapshot ProviderSnapshot
	if err := r.db.QueryRowContext(ctx, `SELECT id, created_at FROM health_provider_snapshots WHERE id = ?`, id).Scan(&snapshot.ID, &snapshot.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get provider snapshot: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT provider_id, provider_generation, role, configured_order
		FROM health_provider_snapshot_entries
		WHERE snapshot_id = ?
		ORDER BY configured_order, provider_id
	`, id)
	if err != nil {
		return nil, fmt.Errorf("get provider snapshot entries: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry ProviderSnapshotEntry
		if err := rows.Scan(&entry.ProviderID, &entry.ProviderGeneration, &entry.Role, &entry.Order); err != nil {
			return nil, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	return &snapshot, rows.Err()
}
