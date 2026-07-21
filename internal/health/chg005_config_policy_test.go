package health

import (
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCHG005PauseHealthDuringPlaybackDefaultsOn(t *testing.T) {
	var cfg config.Config
	assert.True(t, cfg.GetPauseHealthDuringPlayback(),
		"an omitted pause policy must preserve the admission safeguard")

	defaults := config.DefaultConfig(t.TempDir())
	assert.True(t, defaults.GetPauseHealthDuringPlayback())
	assert.False(t, defaults.GetHealthEnabled(),
		"the default health-enabled state remains disabled for compatibility")
}

func TestCHG005PauseHealthDuringPlaybackPreservesExplicitValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{name: "enabled", want: true},
		{name: "disabled", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := tc.want
			cfg := config.Config{}
			cfg.Health.PauseDuringPlayback = &value
			require.Equal(t, tc.want, cfg.GetPauseHealthDuringPlayback())
		})
	}
}
