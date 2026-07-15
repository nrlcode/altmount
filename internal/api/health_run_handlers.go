package api

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/database"
)

const (
	defaultHealthRunListLimit = 50
	maxHealthRunListLimit     = 200
)

// HealthRunProgressRepository is the narrow durable state surface required by
// the PR5 progress and control endpoints. Keeping it behind a setter preserves
// source compatibility for existing NewServer callers.
type HealthRunProgressRepository interface {
	ListHealthRuns(context.Context, int) ([]database.HealthRun, error)
	GetHealthRun(context.Context, string) (*database.HealthRun, error)
	RequestRunPause(context.Context, string, bool, time.Time) error
	RequestRunCancel(context.Context, string, time.Time) error
}

// HealthRunProgressResponse contains only progress already committed with the
// durable run. Lease/fencing details and content/provider connection data are
// intentionally excluded.
type HealthRunProgressResponse struct {
	ID                string                   `json:"id"`
	FileRevisionID    string                   `json:"file_revision_id"`
	Trigger           string                   `json:"trigger"`
	Mode              string                   `json:"mode"`
	Status            database.HealthRunStatus `json:"status"`
	TotalSegments     int64                    `json:"total_segments"`
	ResolvedSegments  int64                    `json:"resolved_segments"`
	ProviderChecks    int64                    `json:"provider_checks"`
	MissingCandidates int64                    `json:"missing_candidates"`
	InconclusiveCount int64                    `json:"inconclusive_count"`
	Stage             string                   `json:"stage"`
	CurrentProviderID *string                  `json:"current_provider_id,omitempty"`
	CursorSegment     int64                    `json:"cursor_segment"`
	PauseRequested    bool                     `json:"pause_requested"`
	CancelRequested   bool                     `json:"cancel_requested"`
	CreatedAt         time.Time                `json:"created_at"`
	StartedAt         *time.Time               `json:"started_at,omitempty"`
	UpdatedAt         time.Time                `json:"updated_at"`
	CompletedAt       *time.Time               `json:"completed_at,omitempty"`
	LastError         string                   `json:"last_error,omitempty"`
	ChecksPerSecond   float64                  `json:"checks_per_second"`
	EstimatedSeconds  int64                    `json:"estimated_completion_seconds"`
}

func toHealthRunProgressResponse(run database.HealthRun) HealthRunProgressResponse {
	var providerID *string
	if run.CurrentProviderID != nil && validHealthRunToken(*run.CurrentProviderID) {
		value := *run.CurrentProviderID
		providerID = &value
	}
	response := HealthRunProgressResponse{
		ID: run.ID, Trigger: run.Trigger, Mode: run.Mode, Status: run.Status,
		TotalSegments: run.TotalSegments, ResolvedSegments: run.ResolvedSegments,
		ProviderChecks: run.ProviderChecks, MissingCandidates: run.MissingCandidates,
		InconclusiveCount: run.InconclusiveCount, Stage: run.Stage,
		CurrentProviderID: providerID, CursorSegment: run.CursorSegment,
		PauseRequested: run.PauseRequested, CancelRequested: run.CancelRequested,
		CreatedAt: run.CreatedAt, StartedAt: run.StartedAt, UpdatedAt: run.UpdatedAt,
		CompletedAt: run.CompletedAt, LastError: sanitizeHealthRunLastError(run.LastError),
	}
	if validHealthRunToken(run.FileRevisionID) {
		response.FileRevisionID = run.FileRevisionID
	}
	start := run.CreatedAt
	if run.StartedAt != nil {
		start = run.StartedAt.UTC()
	}
	if elapsed := run.UpdatedAt.UTC().Sub(start); elapsed > 0 && run.ProviderChecks > 0 {
		response.ChecksPerSecond = float64(run.ProviderChecks) / elapsed.Seconds()
		remaining := max(run.TotalSegments-run.ResolvedSegments, 0)
		if remaining > 0 && run.Status == database.HealthRunRunning {
			response.EstimatedSeconds = int64(math.Ceil(float64(remaining) / response.ChecksPerSecond))
		}
	}
	return response
}

func validHealthRunToken(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed != value || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '-' || character == '_' ||
			character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

// sanitizeHealthRunLastError is a defense-in-depth boundary. The repository
// stores typed, sanitized reasons; this prevents a malformed/legacy row from
// exposing article identifiers or transport details through the API.
func sanitizeHealthRunLastError(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) > 128 {
		return "health run failed"
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == ' ' || character == '_' ||
			character == '-' || character == '.' {
			continue
		}
		return "health run failed"
	}
	return value
}

func (s *Server) handleListHealthRuns(c *fiber.Ctx) error {
	if s.healthRunRepository == nil {
		return RespondServiceUnavailable(c, "Durable health progress is unavailable", "")
	}
	limit := defaultHealthRunListLimit
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > maxHealthRunListLimit {
			return RespondValidationError(c, "Invalid health run limit", "limit must be between 1 and 200")
		}
		limit = parsed
	}
	runs, err := s.healthRunRepository.ListHealthRuns(c.Context(), limit)
	if err != nil {
		return RespondInternalError(c, "Failed to list durable health runs", "")
	}
	responses := make([]HealthRunProgressResponse, 0, len(runs))
	for _, run := range runs {
		if !validHealthRunToken(run.ID) {
			continue
		}
		responses = append(responses, toHealthRunProgressResponse(run))
	}
	return RespondSuccess(c, responses)
}

func (s *Server) handleGetHealthRun(c *fiber.Ctx) error {
	if s.healthRunRepository == nil {
		return RespondServiceUnavailable(c, "Durable health progress is unavailable", "")
	}
	id := strings.TrimSpace(c.Params("id"))
	if !validHealthRunToken(id) {
		return RespondValidationError(c, "Invalid health run identifier", "")
	}
	run, err := s.healthRunRepository.GetHealthRun(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve durable health run", "")
	}
	if run == nil {
		return RespondNotFound(c, "Health run", "")
	}
	return RespondSuccess(c, toHealthRunProgressResponse(*run))
}

func (s *Server) handleControlHealthRun(c *fiber.Ctx) error {
	if s.healthRunRepository == nil {
		return RespondServiceUnavailable(c, "Durable health progress is unavailable", "")
	}
	id := strings.TrimSpace(c.Params("id"))
	if !validHealthRunToken(id) {
		return RespondValidationError(c, "Invalid health run identifier", "")
	}
	action := strings.TrimSpace(c.Params("action"))
	if action != "pause" && action != "resume" && action != "cancel" {
		return RespondValidationError(c, "Invalid health run action", "supported actions are pause, resume, and cancel")
	}
	run, err := s.healthRunRepository.GetHealthRun(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve durable health run", "")
	}
	if run == nil {
		return RespondNotFound(c, "Health run", "")
	}
	if run.Trigger == "import" {
		return RespondConflict(c, "Import validation runs cannot be controlled here",
			"Import admission owns pause, resume, and cancellation so validation remains resumable")
	}
	now := time.Now().UTC()
	switch action {
	case "pause":
		err = s.healthRunRepository.RequestRunPause(c.Context(), id, true, now)
	case "resume":
		err = s.healthRunRepository.RequestRunPause(c.Context(), id, false, now)
	case "cancel":
		err = s.healthRunRepository.RequestRunCancel(c.Context(), id, now)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return RespondNotFound(c, "Health run", "")
	}
	if errors.Is(err, database.ErrImportRunControl) {
		return RespondConflict(c, "Import validation runs cannot be controlled here",
			"Import admission owns pause, resume, and cancellation so validation remains resumable")
	}
	if err != nil {
		return RespondInternalError(c, "Failed to control durable health run", "")
	}
	if s.progressBroadcaster != nil {
		s.progressBroadcaster.BroadcastHealthChanged()
	}
	run, err = s.healthRunRepository.GetHealthRun(c.Context(), id)
	if err != nil {
		return RespondInternalError(c, "Failed to retrieve durable health run", "")
	}
	if run == nil {
		return RespondNotFound(c, "Health run", "")
	}
	return RespondSuccess(c, toHealthRunProgressResponse(*run))
}
