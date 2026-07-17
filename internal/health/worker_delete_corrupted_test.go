package health

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareUpdateForResultIncompleteNeverDeletesOrRepairs(t *testing.T) {
	tempDir := t.TempDir()
	env := newRepairTestEnv(t, tempDir, nil, func(c *config.Config) {
		c.Health.CorruptionAction = "delete"
	})

	fh := database.FileHealth{
		FilePath:   "/movies/incomplete.mp4",
		Status:     database.HealthStatusPending,
		RetryCount: 99,
	}
	event := HealthEvent{
		Type:     EventTypeCheckFailed,
		FilePath: fh.FilePath,
		Status:   database.HealthStatusCorrupted,
		Error:    context.DeadlineExceeded,
	}

	update, sideEffect := env.hw.prepareUpdateForResult(context.Background(), &fh, event)
	assert.False(t, update.Skip, "incomplete checks must not enter delete-on-corruption")
	assert.Equal(t, database.UpdateTypeRetry, update.Type)
	assert.Equal(t, database.HealthStatusPending, update.Status)
	require.NotNil(t, sideEffect)
	require.NoError(t, sideEffect())
	assert.Empty(t, env.mockARRs.calls, "incomplete checks must not trigger repair")
}
