package postprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5CompletedImportCoverageSuppressesImmediateLegacyHealthCheck(t *testing.T) {
	coordinator, metadataService, healthRepo, _ := setupSchedulerTest(t)
	path := "/movies/Covered.2026.mkv"
	writeTestMetadata(t, metadataService, path)
	coordinator.SetReuseDurableImportCoverage(true)

	require.NoError(t, coordinator.ScheduleHealthCheck(context.Background(), nil, path, []string{path}))
	health, err := healthRepo.GetFileHealth(context.Background(), "movies/Covered.2026.mkv")
	require.NoError(t, err)
	assert.Nil(t, health,
		"the full fingerprint-bound import STAT run already is ordinary health coverage")
}
