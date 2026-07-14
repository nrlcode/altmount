package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5HealthRunRepository struct {
	mu sync.Mutex

	runs       map[string]database.HealthRun
	listed     []database.HealthRun
	listLimit  int
	listCalls  int
	getCalls   []string
	pauseCalls []pr5PauseCall
	cancelIDs  []string
	listErr    error
	getErr     error
	pauseErr   error
	cancelErr  error
}

type pr5PauseCall struct {
	id        string
	requested bool
	at        time.Time
}

func (r *pr5HealthRunRepository) ListHealthRuns(_ context.Context, limit int) ([]database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	r.listLimit = limit
	return append([]database.HealthRun(nil), r.listed...), r.listErr
}

func (r *pr5HealthRunRepository) GetHealthRun(_ context.Context, id string) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getCalls = append(r.getCalls, id)
	if r.getErr != nil {
		return nil, r.getErr
	}
	run, ok := r.runs[id]
	if !ok {
		return nil, nil
	}
	return &run, nil
}

func (r *pr5HealthRunRepository) RequestRunPause(
	_ context.Context,
	id string,
	requested bool,
	at time.Time,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pauseCalls = append(r.pauseCalls, pr5PauseCall{id: id, requested: requested, at: at})
	if r.pauseErr != nil {
		return r.pauseErr
	}
	run, ok := r.runs[id]
	if !ok {
		return sql.ErrNoRows
	}
	run.PauseRequested = requested
	run.UpdatedAt = at
	r.runs[id] = run
	return nil
}

func (r *pr5HealthRunRepository) RequestRunCancel(_ context.Context, id string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelIDs = append(r.cancelIDs, id)
	if r.cancelErr != nil {
		return r.cancelErr
	}
	run, ok := r.runs[id]
	if !ok {
		return sql.ErrNoRows
	}
	run.Status = database.HealthRunCanceled
	run.CancelRequested = true
	run.UpdatedAt = at
	run.CompletedAt = &at
	r.runs[id] = run
	return nil
}

func newPR5HealthRunAPI(t *testing.T, repo HealthRunProgressRepository) (*fiber.App, *progress.ProgressBroadcaster) {
	t.Helper()
	loginRequired := false
	broadcaster := progress.NewProgressBroadcaster()
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	server := &Server{
		config: DefaultConfig(),
		configManager: &mockConfigManager{cfg: &config.Config{
			Auth: config.AuthConfig{LoginRequired: &loginRequired},
			API:  config.APIConfig{AllowedOrigins: []string{"http://test.invalid"}},
		}},
		progressBroadcaster: broadcaster,
	}
	server.SetHealthRunRepository(repo)
	app := fiber.New()
	server.SetupRoutes(app)
	return app, broadcaster
}

func pr5RunFixture(id string) database.HealthRun {
	now := time.Date(2026, time.July, 14, 12, 30, 0, 0, time.UTC)
	providerID := "provider-stable-1"
	providerGeneration := int64(9)
	leaseOwner := "internal-worker-with-sensitive-context"
	return database.HealthRun{
		ID: id, FileRevisionID: "internal-file-revision", ProviderSnapshotID: "internal-snapshot",
		Trigger: "scheduled_revalidation", Mode: "observation", Status: database.HealthRunRunning,
		LeaseOwner: &leaseOwner, FencingToken: 71, TotalSegments: 100, ResolvedSegments: 40,
		ProviderChecks: 45, MissingCandidates: 3, InconclusiveCount: 2,
		Stage: "stat", CurrentProviderID: &providerID, CurrentProviderGeneration: &providerGeneration,
		CursorSegment: 40, CreatedAt: now.Add(-time.Minute), StartedAt: &now,
		UpdatedAt: now, LastError: "upstream <raw-message-id@example.invalid>",
	}
}

func decodePR5APIResponse(t *testing.T, response *http.Response) map[string]any {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded), "response body: %s", body)
	return decoded
}

func TestPR5HealthRunsStaticListRouteUsesDurableRepository(t *testing.T) {
	repo := &pr5HealthRunRepository{
		runs: map[string]database.HealthRun{},
		listed: []database.HealthRun{
			pr5RunFixture("run-list-1"),
		},
	}
	app, _ := newPR5HealthRunAPI(t, repo)

	response, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/health/runs?limit=25", nil), -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode,
		"the static runs route must be registered before legacy /health/:id")
	decoded := decodePR5APIResponse(t, response)
	assert.Equal(t, true, decoded["success"])
	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Equal(t, 1, repo.listCalls)
	assert.Equal(t, 25, repo.listLimit)
}

func TestPR5HealthRunProgressResponseOnlyExposesSafeCommittedFields(t *testing.T) {
	run := pr5RunFixture("run-safe-response")
	repo := &pr5HealthRunRepository{runs: map[string]database.HealthRun{run.ID: run}}
	app, _ := newPR5HealthRunAPI(t, repo)

	response, err := app.Test(httptest.NewRequest(
		http.MethodGet, "/api/health/runs/"+run.ID, nil,
	), -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	body := decodePR5APIResponse(t, response)
	data, ok := body["data"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, run.ID, data["id"])
	assert.Equal(t, "provider-stable-1", data["current_provider_id"])
	assert.Equal(t, "health run failed", data["last_error"])
	assert.NotContains(t, data, "lease_owner")
	assert.NotContains(t, data, "lease_expires_at")
	assert.NotContains(t, data, "fencing_token")
	assert.NotContains(t, data, "file_revision_id")
	assert.NotContains(t, data, "provider_snapshot_id")
	assert.NotContains(t, data, "current_provider_generation")
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "raw-message-id")
	assert.NotContains(t, string(encoded), "internal-worker")
}

func TestPR5HealthRunControlsPersistIntentAndBroadcastInvalidation(t *testing.T) {
	tests := []struct {
		action         string
		wantPauseCalls int
		wantPause      bool
		wantCancel     bool
		wantStatus     string
	}{
		{action: "pause", wantPauseCalls: 1, wantPause: true, wantStatus: "running"},
		{action: "resume", wantPauseCalls: 1, wantPause: false, wantStatus: "running"},
		{action: "cancel", wantCancel: true, wantStatus: "canceled"},
	}
	for _, test := range tests {
		t.Run(test.action, func(t *testing.T) {
			run := pr5RunFixture("run-control-" + test.action)
			repo := &pr5HealthRunRepository{runs: map[string]database.HealthRun{run.ID: run}}
			app, broadcaster := newPR5HealthRunAPI(t, repo)
			subscriberID, updates := broadcaster.Subscribe()
			defer broadcaster.Unsubscribe(subscriberID)

			response, err := app.Test(httptest.NewRequest(
				http.MethodPost, "/api/health/runs/"+run.ID+"/"+test.action, nil,
			), -1)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			decoded := decodePR5APIResponse(t, response)
			data, ok := decoded["data"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, test.wantStatus, data["status"])

			repo.mu.Lock()
			assert.Len(t, repo.pauseCalls, test.wantPauseCalls)
			if test.wantPauseCalls == 1 {
				assert.Equal(t, test.wantPause, repo.pauseCalls[0].requested)
				assert.Equal(t, time.UTC, repo.pauseCalls[0].at.Location())
			}
			assert.Equal(t, test.wantCancel, len(repo.cancelIDs) == 1)
			repo.mu.Unlock()

			select {
			case update := <-updates:
				assert.Equal(t, "health_changed", update.Status)
			case <-time.After(time.Second):
				t.Fatal("successful control did not invalidate health SSE subscribers")
			}
		})
	}
}

func TestPR5HealthRunEndpointsRejectInvalidLimitIDAndAction(t *testing.T) {
	run := pr5RunFixture("run-validation")
	repo := &pr5HealthRunRepository{runs: map[string]database.HealthRun{run.ID: run}}
	app, _ := newPR5HealthRunAPI(t, repo)

	requests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/health/runs?limit=0"},
		{method: http.MethodGet, path: "/api/health/runs?limit=not-a-number"},
		{method: http.MethodGet, path: "/api/health/runs/run$invalid"},
		{method: http.MethodPost, path: "/api/health/runs/" + run.ID + "/restart"},
	}
	for _, request := range requests {
		response, err := app.Test(httptest.NewRequest(request.method, request.path, nil), -1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, response.StatusCode, request.path)
		response.Body.Close()
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Zero(t, repo.listCalls)
	assert.Empty(t, repo.getCalls)
	assert.Empty(t, repo.pauseCalls)
	assert.Empty(t, repo.cancelIDs)
}

func TestPR5HealthRunControlsReturnNotFoundForUnknownRun(t *testing.T) {
	repo := &pr5HealthRunRepository{
		runs:     map[string]database.HealthRun{},
		pauseErr: sql.ErrNoRows,
	}
	app, _ := newPR5HealthRunAPI(t, repo)

	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/api/health/runs/run-does-not-exist/pause", nil,
	), -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, response.StatusCode)
	response.Body.Close()
}
