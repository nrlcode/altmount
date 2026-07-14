package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/javi11/altmount/internal/config"
)

func TestPR5ImportDamagePolicyIsExposedByConfigAPI(t *testing.T) {
	response := ToImportAPIResponse(config.ImportConfig{
		DamagePolicy: string(config.ImportDamagePolicyTolerant),
	})

	if response.DamagePolicy != config.ImportDamagePolicyTolerant {
		t.Fatalf("damage_policy = %q, want tolerant", response.DamagePolicy)
	}

	payload, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal import config response: %v", err)
	}
	if got, want := string(payload), `"damage_policy":"tolerant"`; !strings.Contains(got, want) {
		t.Fatalf("import config response = %s, want field %s", got, want)
	}
}
