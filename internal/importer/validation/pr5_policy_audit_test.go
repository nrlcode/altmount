package validation

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pr5AuditFinalFingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pr5AuditOtherFingerprint = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pr5AuditPrimaryProvider  = "provider-primary"
	pr5AuditBackupProvider   = "provider-backup"
)

func pr5AuditProviders(activationEpoch int64) []ImportAvailabilityProvider {
	return []ImportAvailabilityProvider{
		{ID: pr5AuditPrimaryProvider, Generation: 1, ActivationEpoch: activationEpoch},
		{ID: pr5AuditBackupProvider, Generation: 1, ActivationEpoch: activationEpoch},
	}
}

func pr5AuditCheck(
	providerID string,
	activationEpoch int64,
	outcome nntppool.OutcomeKind,
	disposition ImportCheckDisposition,
) ImportProviderCheck {
	return ImportProviderCheck{
		ProviderID:              providerID,
		ProviderGeneration:      1,
		ProviderActivationEpoch: activationEpoch,
		Operation:               nntppool.OperationStat,
		Outcome:                 outcome,
		BodyValidation:          nntppool.BodyValidationNotApplicable,
		CompletionDisposition:   disposition,
	}
}

func pr5AuditSuccess(providerID string, activationEpoch int64) ImportProviderCheck {
	return pr5AuditCheck(
		providerID,
		activationEpoch,
		nntppool.OutcomeSuccess,
		ImportCheckDispositionAttempted,
	)
}

func pr5AuditUnavailableChecks(activationEpoch int64) []ImportProviderCheck {
	return []ImportProviderCheck{
		pr5AuditCheck(
			pr5AuditPrimaryProvider,
			activationEpoch,
			nntppool.OutcomeHardArticleAbsence,
			ImportCheckDispositionAttempted,
		),
		pr5AuditCheck(
			pr5AuditBackupProvider,
			activationEpoch,
			nntppool.OutcomeTemporaryFailure,
			ImportCheckDispositionAttempted,
		),
	}
}

func pr5AuditPosition(
	index int,
	initial []ImportProviderCheck,
	confirmation []ImportProviderCheck,
) ImportAvailabilityPosition {
	return ImportAvailabilityPosition{
		Index:              index,
		InitialChecks:      initial,
		ConfirmationChecks: confirmation,
	}
}

func pr5AuditInput(positions []ImportAvailabilityPosition, spans []int64) ImportAdmissionInput {
	return ImportAdmissionInput{
		DamagePolicy:              "strict",
		Filename:                  "fixture-video.mkv",
		FileSize:                  10_000,
		FinalLayoutFingerprint:    pr5AuditFinalFingerprint,
		EvidenceLayoutFingerprint: pr5AuditFinalFingerprint,
		CanonicalUsableBytes:      append([]int64(nil), spans...),
		UncomplicatedStandalone:   true,
		InitialPassComplete:       true,
		ActiveProviders:           pr5AuditProviders(2),
		Positions:                 positions,
	}
}

func TestPR5AuditImportAdmissionRequiresCompleteFingerprintBoundLayout(t *testing.T) {
	t.Parallel()

	t.Run("omitted canonical position cannot be hidden by completion flag", func(t *testing.T) {
		input := pr5AuditInput([]ImportAvailabilityPosition{
			pr5AuditPosition(0, []ImportProviderCheck{pr5AuditSuccess(pr5AuditPrimaryProvider, 2)}, nil),
			pr5AuditPosition(2, []ImportProviderCheck{pr5AuditSuccess(pr5AuditPrimaryProvider, 2)}, nil),
		}, []int64{3_000, 3_000, 4_000})

		decision, err := DecideImportAdmission(context.Background(), input)
		require.Error(t, err)
		assert.NotEqual(t, ImportAdmissionAccept, decision.Status)
	})

	t.Run("positive size with empty canonical layout cannot be accepted", func(t *testing.T) {
		input := pr5AuditInput(nil, nil)

		decision, err := DecideImportAdmission(context.Background(), input)
		require.Error(t, err)
		assert.NotEqual(t, ImportAdmissionAccept, decision.Status)
	})

	t.Run("provisional evidence fingerprint cannot promote to final layout", func(t *testing.T) {
		input := pr5AuditInput([]ImportAvailabilityPosition{
			pr5AuditPosition(0, []ImportProviderCheck{pr5AuditSuccess(pr5AuditPrimaryProvider, 2)}, nil),
		}, []int64{10_000})
		input.EvidenceLayoutFingerprint = pr5AuditOtherFingerprint

		decision, err := DecideImportAdmission(context.Background(), input)
		require.Error(t, err)
		assert.NotEqual(t, ImportAdmissionAccept, decision.Status)
	})
}

func TestPR5AuditImportConfirmationUsesTerminalCheckNotNestedAttempt(t *testing.T) {
	t.Parallel()

	primary := pr5AuditCheck(
		pr5AuditPrimaryProvider,
		2,
		nntppool.OutcomeCancellation,
		ImportCheckDispositionIncomplete,
	)
	primary.RawAttempts = []nntppool.AttemptEvidence{
		{
			ProviderID:   pr5AuditPrimaryProvider,
			Operation:    nntppool.OperationStat,
			Outcome:      nntppool.OutcomeTemporaryFailure,
			ResponseCode: 451,
		},
		{
			ProviderID: pr5AuditPrimaryProvider,
			Operation:  nntppool.OperationStat,
			Outcome:    nntppool.OutcomeCancellation,
		},
	}
	backup := pr5AuditCheck(
		pr5AuditBackupProvider,
		2,
		nntppool.OutcomeHardArticleAbsence,
		ImportCheckDispositionAttempted,
	)

	input := pr5AuditInput([]ImportAvailabilityPosition{
		pr5AuditPosition(0, pr5AuditUnavailableChecks(2), []ImportProviderCheck{primary, backup}),
	}, []int64{100})
	input.SecondPassComplete = true

	decision, err := DecideImportAdmission(context.Background(), input)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAwaitConfirmation, decision.Status)
	assert.Equal(t, []ImportConfirmationTarget{
		{
			Position:                0,
			ProviderID:              pr5AuditPrimaryProvider,
			ProviderGeneration:      1,
			ProviderActivationEpoch: 2,
		},
	}, decision.ConfirmationTargets)
}

func TestPR5AuditImportEvidenceHonorsProviderActivationEpoch(t *testing.T) {
	t.Parallel()

	stale := pr5AuditCheck(
		pr5AuditPrimaryProvider,
		1,
		nntppool.OutcomeHardArticleAbsence,
		ImportCheckDispositionAttempted,
	)
	currentBackup := pr5AuditCheck(
		pr5AuditBackupProvider,
		2,
		nntppool.OutcomeHardArticleAbsence,
		ImportCheckDispositionAttempted,
	)
	input := pr5AuditInput([]ImportAvailabilityPosition{
		pr5AuditPosition(0, pr5AuditUnavailableChecks(2), []ImportProviderCheck{stale, currentBackup}),
	}, []int64{100})
	input.SecondPassComplete = true

	decision, err := DecideImportAdmission(context.Background(), input)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAwaitConfirmation, decision.Status)
	assert.Equal(t, []ImportConfirmationTarget{
		{
			Position:                0,
			ProviderID:              pr5AuditPrimaryProvider,
			ProviderGeneration:      1,
			ProviderActivationEpoch: 2,
		},
	}, decision.ConfirmationTargets)
}

func TestPR5AuditTolerantImportRequiresUncomplicatedStandaloneProvenance(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		uncomplicated bool
		want          ImportAdmissionStatus
	}{
		{name: "direct standalone may remain pending", uncomplicated: true, want: ImportAdmissionHealthPending},
		{name: "unknown or complicated provenance rejects", uncomplicated: false, want: ImportAdmissionReject},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AuditInput([]ImportAvailabilityPosition{
				pr5AuditPosition(0, []ImportProviderCheck{pr5AuditSuccess(pr5AuditPrimaryProvider, 2)}, nil),
				pr5AuditPosition(1, pr5AuditUnavailableChecks(2), pr5AuditUnavailableChecks(2)),
			}, []int64{9_900, 100})
			input.DamagePolicy = "tolerant"
			input.SecondPassComplete = true
			input.UncomplicatedStandalone = tt.uncomplicated

			decision, err := DecideImportAdmission(context.Background(), input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, decision.Status)
			assert.Equal(t, 2, decision.Impact.TotalSegments)
		})
	}
}

func TestPR5AuditImportProviderChecksRejectMalformedTerminalEvidence(t *testing.T) {
	t.Parallel()

	validBackup := pr5AuditCheck(
		pr5AuditBackupProvider,
		2,
		nntppool.OutcomeHardArticleAbsence,
		ImportCheckDispositionAttempted,
	)
	tests := []struct {
		name   string
		checks []ImportProviderCheck
	}{
		{
			name: "unknown outcome",
			checks: []ImportProviderCheck{
				pr5AuditCheck(pr5AuditPrimaryProvider, 2, nntppool.OutcomeKind("fixture-unknown"), ImportCheckDispositionAttempted),
				validBackup,
			},
		},
		{
			name: "provider outside frozen snapshot",
			checks: []ImportProviderCheck{
				pr5AuditCheck("provider-not-in-snapshot", 2, nntppool.OutcomeHardArticleAbsence, ImportCheckDispositionAttempted),
				validBackup,
			},
		},
		{
			name: "duplicate terminal provider check",
			checks: []ImportProviderCheck{
				pr5AuditCheck(pr5AuditPrimaryProvider, 2, nntppool.OutcomeTemporaryFailure, ImportCheckDispositionAttempted),
				pr5AuditCheck(pr5AuditPrimaryProvider, 2, nntppool.OutcomeHardArticleAbsence, ImportCheckDispositionAttempted),
				validBackup,
			},
		},
		{
			name: "provisional body success",
			checks: func() []ImportProviderCheck {
				check := pr5AuditSuccess(pr5AuditPrimaryProvider, 2)
				check.Operation = nntppool.OperationBody
				check.BodyValidation = nntppool.BodyValidationIncomplete
				return []ImportProviderCheck{check, validBackup}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AuditInput([]ImportAvailabilityPosition{
				pr5AuditPosition(0, pr5AuditUnavailableChecks(2), tt.checks),
			}, []int64{100})
			input.SecondPassComplete = true

			decision, err := DecideImportAdmission(context.Background(), input)
			require.Error(t, err)
			assert.NotEqual(t, ImportAdmissionReject, decision.Status)
			assert.NotEqual(t, ImportAdmissionAccept, decision.Status)
		})
	}
}
