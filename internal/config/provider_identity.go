package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ProviderIdentityRecord is the retained parent identity needed while assigning
// stable IDs. Inactive records remain reserved permanently.
type ProviderIdentityRecord struct {
	ID                string
	Active            bool
	TombstonedAt      *time.Time
	CurrentGeneration int64
}

// ProviderIdentityGeneration is one retained endpoint/account identity for a
// stable provider ID.
type ProviderIdentityGeneration struct {
	ProviderID          string
	Generation          int64
	Endpoint            string
	Port                int
	Account             string
	IdentityFingerprint string
}

// ProviderIdentityRegistrySnapshot is one coherent read of the durable
// provider registry and all of its retained generations.
type ProviderIdentityRegistrySnapshot struct {
	Providers   []ProviderIdentityRecord
	Generations []ProviderIdentityGeneration
}

// ProviderIdentitySource supplies retained identities without making config
// depend on the database package.
type ProviderIdentitySource interface {
	ReadProviderIdentityRegistrySnapshot(context.Context) (ProviderIdentityRegistrySnapshot, error)
}

// NormalizeProviderIdentity applies the canonical endpoint/account rules used
// by both configuration assignment and durable provider generations.
func NormalizeProviderIdentity(endpoint, account string) (string, string) {
	endpoint = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(endpoint)), ".")
	return endpoint, strings.TrimSpace(account)
}

// ProviderIdentityFingerprint returns the stable endpoint/port/account
// fingerprint. Endpoint comparison is case-insensitive and removes exactly one
// trailing dot; account comparison remains case-sensitive.
func ProviderIdentityFingerprint(endpoint string, port int, account string) (string, error) {
	endpoint, account = NormalizeProviderIdentity(endpoint, account)
	if endpoint == "" {
		return "", fmt.Errorf("provider endpoint is required")
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("provider port is outside 1..65535")
	}
	identity := sha256.New()
	fmt.Fprintf(identity, "%d:%s|%d|%d:%s", len(endpoint), endpoint, port, len(account), account)
	return "sha256:" + hex.EncodeToString(identity.Sum(nil)), nil
}

func resolveProviderIDs(
	ctx context.Context,
	candidate *Config,
	source ProviderIdentitySource,
	newProviderID func() string,
) error {
	needsResolution := false
	reserved := make(map[string]struct{}, len(candidate.Providers)*2)
	claimed := make(map[string]struct{}, len(candidate.Providers))
	for i := range candidate.Providers {
		provider := &candidate.Providers[i]
		reserved[provider.NNTPPoolName()] = struct{}{}
		if provider.ID == "" {
			needsResolution = true
			continue
		}
		reserved[provider.ID] = struct{}{}
		claimed[provider.ID] = struct{}{}
	}
	if !needsResolution {
		return nil
	}

	if source == nil {
		return fmt.Errorf("provider identity source is required to resolve empty provider ids")
	}
	registry, err := source.ReadProviderIdentityRegistrySnapshot(ctx)
	if err != nil {
		return fmt.Errorf("read provider identity registry: %w", err)
	}

	for _, provider := range registry.Providers {
		if provider.ID == "" {
			return fmt.Errorf("provider identity registry contains an empty provider id")
		}
		reserved[provider.ID] = struct{}{}
	}
	matches := make(map[string]map[string]struct{}, len(registry.Generations))
	for _, generation := range registry.Generations {
		if generation.ProviderID == "" {
			return fmt.Errorf("provider identity generation contains an empty provider id")
		}
		fingerprint, err := ProviderIdentityFingerprint(generation.Endpoint, generation.Port, generation.Account)
		if err != nil {
			return fmt.Errorf("retained provider %q generation %d: %w", generation.ProviderID, generation.Generation, err)
		}
		if generation.IdentityFingerprint != fingerprint {
			return fmt.Errorf("retained provider %q generation %d has an inconsistent identity fingerprint", generation.ProviderID, generation.Generation)
		}
		reserved[generation.ProviderID] = struct{}{}
		byProvider := matches[fingerprint]
		if byProvider == nil {
			byProvider = make(map[string]struct{})
			matches[fingerprint] = byProvider
		}
		byProvider[generation.ProviderID] = struct{}{}
	}

	for i := range candidate.Providers {
		provider := &candidate.Providers[i]
		if provider.ID != "" {
			continue
		}
		fingerprint, err := ProviderIdentityFingerprint(provider.Host, provider.Port, provider.Username)
		if err != nil {
			return fmt.Errorf("provider %d: %w", i, err)
		}
		retained := matches[fingerprint]
		switch len(retained) {
		case 0:
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				id := newProviderID()
				if id == "" {
					continue
				}
				if _, collision := reserved[id]; collision {
					continue
				}
				provider.ID = id
				reserved[id] = struct{}{}
				claimed[id] = struct{}{}
				break
			}
		case 1:
			var id string
			for retainedID := range retained {
				id = retainedID
			}
			if _, collision := claimed[id]; collision {
				return fmt.Errorf("provider %d: retained provider id %q is already claimed", i, id)
			}
			provider.ID = id
			claimed[id] = struct{}{}
		case 2:
			fallthrough
		default:
			return fmt.Errorf("provider %d: identity matches multiple retained provider ids", i)
		}
	}
	return nil
}
