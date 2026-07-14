package config

import (
	"reflect"
	"testing"
)

func TestPR3ProviderUsesStableConfiguredTransportID(t *testing.T) {
	provider := ProviderConfig{ID: "provider-stable-id", Host: "example.invalid", Port: 119, MaxConnections: 1}
	converted := reflect.ValueOf(provider.ToNNTPProvider())
	id := converted.FieldByName("ID")
	if !id.IsValid() {
		t.Fatal("nntppool dependency has no stable Provider.ID; corrected fork revision is required")
	}
	if got := id.String(); got != provider.ID {
		t.Fatalf("transport Provider.ID = %q, want configured ID %q", got, provider.ID)
	}
}
