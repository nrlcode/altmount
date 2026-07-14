package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5HealthObservationController struct {
	status       health.ObservationServiceStatus
	scheduledIDs []int64
	intents      []health.ObservationScheduleIntent
	canceledIDs  []int64
	cancelActive bool
	result       health.ObservationScheduleResult
	err          error
}

func (c *pr5HealthObservationController) Status() health.ObservationServiceStatus {
	return c.status
}

func (c *pr5HealthObservationController) ScheduleFile(
	_ context.Context,
	fileHealthID int64,
	intent health.ObservationScheduleIntent,
) (health.ObservationScheduleResult, error) {
	c.scheduledIDs = append(c.scheduledIDs, fileHealthID)
	c.intents = append(c.intents, intent)
	return c.result, c.err
}

func (c *pr5HealthObservationController) CancelFile(
	_ context.Context,
	fileHealthID int64,
) error {
	c.canceledIDs = append(c.canceledIDs, fileHealthID)
	if c.err != nil {
		return c.err
	}
	if !c.cancelActive {
		return health.ErrObservationRunNotActive
	}
	return nil
}

func newPR5HealthObservationCompatibilityAPI(
	t *testing.T,
	controller *pr5HealthObservationController,
) (*fiber.App, *database.HealthRepository, int64) {
	t.Helper()
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "health-observation.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
	due := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	require.NoError(t, repo.BatchUpsertObservationDiscoveries(
		context.Background(), []database.AutomaticHealthCheckRecord{{
			FilePath: "library/synthetic.mkv", ScheduledCheckAt: &due,
		}},
	))
	item, err := repo.GetFileHealth(context.Background(), "library/synthetic.mkv")
	require.NoError(t, err)
	require.NotNil(t, item)

	loginRequired := false
	server := &Server{
		config:     DefaultConfig(),
		healthRepo: repo,
		configManager: &mockConfigManager{cfg: &config.Config{
			Auth: config.AuthConfig{LoginRequired: &loginRequired},
			API:  config.APIConfig{AllowedOrigins: []string{"http://test.invalid"}},
		}},
	}
	server.SetHealthObservationController(controller)
	app := fiber.New()
	server.SetupRoutes(app)
	return app, repo, item.ID
}

func TestPR5LegacyHealthStatusUsesObservationRuntime(t *testing.T) {
	controller := &pr5HealthObservationController{status: health.ObservationServiceRunning}
	app, _, _ := newPR5HealthObservationCompatibilityAPI(t, controller)

	response, err := app.Test(
		httptest.NewRequest(http.MethodGet, "/api/health/worker/status", nil), -1,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	decoded := decodePR5APIResponse(t, response)
	data := decoded["data"].(map[string]any)
	assert.Equal(t, "running", data["status"])
}

func TestPR5LegacyCheckNowSchedulesManualObservationWithoutChangingHealth(t *testing.T) {
	controller := &pr5HealthObservationController{
		status: health.ObservationServiceRunning,
		result: health.ObservationScheduleResult{Created: 1},
	}
	app, repo, id := newPR5HealthObservationCompatibilityAPI(t, controller)

	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/api/health/"+formatInt64(id)+"/check-now", nil,
	), -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, []int64{id}, controller.scheduledIDs)
	assert.Equal(t, []health.ObservationScheduleIntent{
		health.ObservationScheduleIntentManual,
	}, controller.intents)
	item, err := repo.GetFileHealthByID(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, database.HealthStatusPending, item.Status,
		"observation scheduling must not fabricate legacy checking state")
}

func TestPR5LegacyCancelTargetsActiveDurableObservation(t *testing.T) {
	controller := &pr5HealthObservationController{
		status: health.ObservationServiceRunning, cancelActive: true,
	}
	app, _, id := newPR5HealthObservationCompatibilityAPI(t, controller)

	response, err := app.Test(httptest.NewRequest(
		http.MethodPost, "/api/health/"+formatInt64(id)+"/cancel", nil,
	), -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, []int64{id}, controller.canceledIDs)

	controller.cancelActive = false
	response, err = app.Test(httptest.NewRequest(
		http.MethodPost, "/api/health/"+formatInt64(id)+"/cancel", nil,
	), -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode)
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
