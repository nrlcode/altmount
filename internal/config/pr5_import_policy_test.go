package config

import (
	"strings"
	"testing"
)

func TestPR5ImportDamagePolicyDefaultsToStrict(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())

	if got := cfg.Import.DamagePolicy; got != string(ImportDamagePolicyStrict) {
		t.Fatalf("default import damage_policy = %q, want strict", got)
	}
	if cfg.GetImportDamagePolicyTolerant() {
		t.Fatal("default import damage policy is tolerant, want strict")
	}
}

func TestPR5ImportDamagePolicyCompatibilityAndValidation(t *testing.T) {
	tests := []struct {
		name         string
		policy       string
		wantPolicy   string
		wantTolerant bool
		wantErr      bool
	}{
		{
			name:       "omitted normalizes to strict",
			policy:     "",
			wantPolicy: string(ImportDamagePolicyStrict),
		},
		{
			name:       "strict remains strict",
			policy:     string(ImportDamagePolicyStrict),
			wantPolicy: string(ImportDamagePolicyStrict),
		},
		{
			name:         "tolerant remains opt in",
			policy:       string(ImportDamagePolicyTolerant),
			wantPolicy:   string(ImportDamagePolicyTolerant),
			wantTolerant: true,
		},
		{
			name:    "unknown value is rejected",
			policy:  "permissive",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig(t.TempDir())
			cfg.Import.DamagePolicy = tt.policy

			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Validate() error = nil, want invalid damage_policy error")
				}
				if !strings.Contains(err.Error(), "damage_policy") {
					t.Fatalf("Validate() error = %q, want damage_policy context", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if got := cfg.Import.DamagePolicy; got != tt.wantPolicy {
				t.Fatalf("normalized damage_policy = %q, want %q", got, tt.wantPolicy)
			}
			if got := cfg.GetImportDamagePolicyTolerant(); got != tt.wantTolerant {
				t.Fatalf("GetImportDamagePolicyTolerant() = %v, want %v", got, tt.wantTolerant)
			}
		})
	}
}
