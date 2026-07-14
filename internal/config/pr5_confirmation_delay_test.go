package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPR5GapConfirmationMinimumDelayConfiguration(t *testing.T) {
	t.Run("default is ten minutes", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())

		require.Equal(t, 10, cfg.Health.GapConfirmationDelayMinutes)
		require.Equal(t, 10*time.Minute, cfg.GetGapConfirmationMinimumDelay())
	})

	t.Run("configured value is exposed as a duration", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.Health.GapConfirmationDelayMinutes = 23

		require.Equal(t, 23*time.Minute, cfg.GetGapConfirmationMinimumDelay())
	})

	t.Run("non-positive accessor values fail safe", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.Health.GapConfirmationDelayMinutes = 0
		require.Equal(t, 10*time.Minute, cfg.GetGapConfirmationMinimumDelay())

		cfg.Health.GapConfirmationDelayMinutes = -1
		require.Equal(t, 10*time.Minute, cfg.GetGapConfirmationMinimumDelay())
	})

	t.Run("negative configured value is rejected", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.Health.GapConfirmationDelayMinutes = -1

		err := cfg.Validate()
		require.ErrorContains(t, err, "health gap_confirmation_delay_minutes must be non-negative")
	})
}
