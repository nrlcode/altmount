package scanner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/arrs/clients"
	"github.com/javi11/altmount/internal/arrs/data"
	"github.com/javi11/altmount/internal/arrs/instances"
	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTriggerFileRescanPropagatesCallerCancellation(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var startedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(requestStarted) })
		select {
		case <-r.Context().Done():
			return
		case <-releaseRequest:
			http.Error(w, "released test request", http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Arrs.RadarrInstances = []config.ArrsInstanceConfig{{
		Name:    "cancelled-radarr",
		URL:     server.URL,
		APIKey:  "test-key",
		Enabled: &enabled,
	}}
	getter := func() *config.Config { return cfg }
	mgr := NewManager(
		getter,
		instances.NewManager(getter, nil),
		clients.NewManager(server.Client()),
		data.NewManager(),
		nil,
		nil,
	)
	metadata := `{"instanceName":"cancelled-radarr","movie":{"id":42}}`
	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		callDone <- mgr.TriggerFileRescan(ctx, "/library/cancelled.mkv", "cancelled.mkv", &metadata)
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		close(releaseRequest)
		t.Fatal("ARR request did not start")
	}
	cancel()

	var callErr error
	returnedAfterCancel := false
	select {
	case callErr = <-callDone:
		returnedAfterCancel = true
	case <-time.After(250 * time.Millisecond):
	}
	if !returnedAfterCancel {
		close(releaseRequest)
		select {
		case callErr = <-callDone:
		case <-time.After(2 * time.Second):
			t.Fatal("detached ARR request did not finish after test release")
		}
	}

	assert.True(t, returnedAfterCancel, "caller cancellation must stop the in-flight ARR operation")
	require.Error(t, callErr)
	assert.True(t, errors.Is(callErr, context.Canceled), "returned error must retain caller cancellation: %v", callErr)
}
