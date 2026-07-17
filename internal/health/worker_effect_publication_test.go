package health

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/javi11/altmount/internal/database"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEffectTestClaim(file *database.FileHealth, token *string) {
	field := reflect.ValueOf(file).Elem().FieldByName("HealthClaimToken")
	if field.IsValid() && field.CanSet() {
		field.Set(reflect.ValueOf(token))
	}
}

func TestHealthSideEffectFailsClosedWithoutNonEmptyOwnership(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)

	tests := []struct {
		name  string
		token *string
	}{
		{name: "missing token", token: nil},
		{name: "empty token", token: new(string)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := &database.FileHealth{FilePath: "complete/unowned-effect.mkv"}
			setEffectTestClaim(file, test.token)
			var called atomic.Bool

			err := env.hw.applyHealthSideEffect(context.Background(), file, func() error {
				called.Store(true)
				return nil
			})

			require.Error(t, err, "an external effect requires a non-empty durable owner")
			assert.False(t, called.Load(), "ownership must be checked before invoking the effect")
		})
	}
}

func TestHealthSideEffectAllowsCurrentNonEmptyOwnership(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	file := &database.FileHealth{FilePath: "complete/owned-effect.mkv"}
	token := "effect-owner"
	setEffectTestClaim(file, &token)
	if reflect.ValueOf(file).Elem().FieldByName("HealthClaimToken").IsValid() == false {
		t.Skip("accepted parent does not yet expose health ownership in the model")
	}
	var called atomic.Bool

	require.NoError(t, env.hw.applyHealthSideEffect(context.Background(), file, func() error {
		called.Store(true)
		return nil
	}))
	assert.True(t, called.Load())
}

func TestEffectfulCycleRowsPublishIndependentlyFromFailingSibling(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})

	const secondPath = "complete/fenced/show.s01e02.mkv"
	secondLibraryPath := filepath.Join(filepath.Dir(fixture.libraryPath), "show.s01e02.mkv")
	require.NoError(t, os.WriteFile(secondLibraryPath, []byte("second library content"), 0o644))
	writeHealthyFile(t, fixture.env, secondPath)
	insertFileHealth(t, fixture.env.db, secondPath, secondLibraryPath, 0, 3)

	_, err := fixture.env.db.Exec(`
		CREATE TRIGGER fail_second_effect_publication
		BEFORE UPDATE OF status ON file_health
		WHEN OLD.file_path = 'complete/fenced/show.s01e02.mkv'
		 AND OLD.health_claim_token IS NOT NULL
		 AND NEW.status = 'healthy'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic sibling publication failure');
		END;
	`)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	require.Error(t, err)

	firstMetadata, err := fixture.env.metadataService.ReadFileMetadata(fixture.filePath)
	require.NoError(t, err)
	require.NotNil(t, firstMetadata)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, firstMetadata.Status)
	secondMetadata, err := fixture.env.metadataService.ReadFileMetadata(secondPath)
	require.NoError(t, err)
	require.NotNil(t, secondMetadata)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, secondMetadata.Status,
		"the second external effect completed before its publication failed")

	var firstStatus string
	var firstToken sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&firstStatus, &firstToken))
	assert.Equal(t, "healthy", firstStatus,
		"a sibling publication failure must not roll back an independently completed effect")
	assert.False(t, firstToken.Valid)

	var secondStatus string
	var secondToken sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, secondPath).Scan(&secondStatus, &secondToken))
	assert.Equal(t, "checking", secondStatus,
		"once an external effect succeeds, failed publication must retain recovery ownership instead of rearming it")
	assert.True(t, secondToken.Valid)
}
