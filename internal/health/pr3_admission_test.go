package health

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

type fixedPlaybackActivity struct {
	active int
}

func (s fixedPlaybackActivity) ActiveStreams() int { return s.active }

func TestPR3OrdinaryHealthCyclePausesDuringPlayback(t *testing.T) {
	enabled := true
	env := newRepairTestEnv(t, t.TempDir(), nil, func(cfg *config.Config) {
		cfg.Health.PauseDuringPlayback = &enabled
	})
	filePath := "movies/pause-during-playback.mkv"
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, validSegmentMeta(env.metadataService, 1024)))
	insertFileHealth(t, env.db, filePath, "", 0, 3)
	env.hw.SetPlaybackActivitySource(fixedPlaybackActivity{active: 1})

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	require.Equal(t, database.HealthStatusPending, fh.Status)
	require.Zero(t, fh.RetryCount, "paused work must not consume a health retry")
}

func TestPR3HealthPlaybackPauseIsFeatureGated(t *testing.T) {
	disabled := false
	env := newRepairTestEnv(t, t.TempDir(), nil, func(cfg *config.Config) {
		cfg.Health.PauseDuringPlayback = &disabled
	})
	env.hw.SetPlaybackActivitySource(fixedPlaybackActivity{active: 1})

	if env.hw.shouldPauseForPlayback() {
		t.Fatal("disabled pause feature gate still blocked ordinary health work")
	}
}
