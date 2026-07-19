package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCHG004BodyValidationEvidencePreservesDistinctMethods(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "body-validation-worker", time.Minute)
	require.NoError(t, err)

	commit := pr4Commit(f, "body-validation-chunk", lease.FencingToken, "body-validation-worker", 0)
	commit.Stage = "body_delivery"
	commit.ObservationKind = HealthObservationValidatedBody
	commit.PresentBitmap = []byte{0b00001111}
	commit.AbsentBitmap = []byte{0}
	commit.TemporaryBitmap = []byte{0}
	commit.ResolvedDelta = 4
	commit.MissingCandidatesDelta = 0
	commit.InconclusiveDelta = 0
	commit.Confirmations = nil
	commit.Retry = nil
	commit.Attempts = []HealthAttemptEvidence{
		{
			IdempotencyKey: "body-validation:uu", SegmentIndex: 0,
			Operation: "BODY", Outcome: "present", BodyValidation: "uu_structural",
			ObservedAt: f.now.Add(time.Minute),
		},
		{
			IdempotencyKey: "body-validation:yenc", SegmentIndex: 1,
			Operation: "BODY", Outcome: "present", BodyValidation: "yenc_crc",
			ObservedAt: f.now.Add(time.Minute),
		},
	}

	_, err = f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)

	rows, err := f.db.Connection().QueryContext(ctx, `
		SELECT body_validation
		FROM health_attempt_evidence
		ORDER BY segment_index
	`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	var got []string
	for rows.Next() {
		var method string
		require.NoError(t, rows.Scan(&method))
		got = append(got, method)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"uu_structural", "yenc_crc"}, got)
}
