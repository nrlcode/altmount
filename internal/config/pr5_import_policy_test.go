package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

func TestPR5ImportDamagePolicyTolerantYAMLRoundTrip(t *testing.T) {
	input := DefaultConfig(t.TempDir())
	input.Import.DamagePolicy = string(ImportDamagePolicyTolerant)

	encoded, err := yaml.Marshal(input)
	if err != nil {
		t.Fatalf("marshal tolerant config: %v", err)
	}
	var restored Config
	if err := yaml.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("unmarshal tolerant config: %v", err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("validate restored tolerant config: %v", err)
	}
	if restored.Import.DamagePolicy != string(ImportDamagePolicyTolerant) ||
		!restored.GetImportDamagePolicyTolerant() {
		t.Fatalf("restored damage_policy = %q, tolerant = %v",
			restored.Import.DamagePolicy, restored.GetImportDamagePolicyTolerant())
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
