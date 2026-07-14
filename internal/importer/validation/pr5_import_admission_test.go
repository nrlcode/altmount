package validation

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
)

const (
	pr5ProviderPrimary   = "provider-primary"
	pr5ProviderBackup    = "provider-backup"
	pr5ProviderEpoch     = int64(1)
	pr5LayoutFingerprint = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func pr5Code(code int) *int { return &code }

func pr5Check(provider string, outcome nntppool.OutcomeKind, code int) ImportProviderCheck {
	check := ImportProviderCheck{
		ProviderID:              provider,
		ProviderGeneration:      1,
		ProviderActivationEpoch: pr5ProviderEpoch,
		Operation:               nntppool.OperationStat,
		Outcome:                 outcome,
		BodyValidation:          nntppool.BodyValidationNotApplicable,
		CompletionDisposition:   ImportCheckDispositionAttempted,
		RawAttempts: []nntppool.AttemptEvidence{
			{
				ProviderID:     provider,
				Operation:      nntppool.OperationStat,
				Outcome:        outcome,
				ResponseCode:   code,
				BodyValidation: nntppool.BodyValidationNotApplicable,
			},
		},
	}
	if code != 0 {
		check.ResponseCode = pr5Code(code)
	}
	return check
}

func pr5IncompleteCheck(provider string, outcome nntppool.OutcomeKind, includeRawAttempt bool) ImportProviderCheck {
	check := ImportProviderCheck{
		ProviderID:              provider,
		ProviderGeneration:      1,
		ProviderActivationEpoch: pr5ProviderEpoch,
		Operation:               nntppool.OperationStat,
		Outcome:                 outcome,
		BodyValidation:          nntppool.BodyValidationNotApplicable,
		CompletionDisposition:   ImportCheckDispositionIncomplete,
	}
	if includeRawAttempt {
		check.RawAttempts = []nntppool.AttemptEvidence{
			{
				ProviderID:     provider,
				Operation:      nntppool.OperationStat,
				Outcome:        outcome,
				BodyValidation: nntppool.BodyValidationNotApplicable,
			},
		}
	}
	return check
}

func pr5Position(index int, start, end int64, initial, confirmation []ImportProviderCheck) ImportAvailabilityPosition {
	return ImportAvailabilityPosition{
		Index:              index,
		StartOffset:        start,
		EndOffset:          end,
		InitialChecks:      initial,
		ConfirmationChecks: confirmation,
	}
}

func pr5AdmissionInput(policy string, fileSize int64, fixtures []ImportAvailabilityPosition) ImportAdmissionInput {
	fixtureByIndex := make(map[int]ImportAvailabilityPosition, len(fixtures))
	maxIndex := -1
	var explicitBytes int64
	for _, fixture := range fixtures {
		fixtureByIndex[fixture.Index] = fixture
		if fixture.Index > maxIndex {
			maxIndex = fixture.Index
		}
		explicitBytes += fixture.EndOffset - fixture.StartOffset + 1
	}

	// Fill any omitted canonical positions with healthy synthetic coverage. If
	// every listed position is explicit, append one healthy position so the
	// fixture still describes the complete virtual file rather than a sample.
	positionCount := maxIndex + 1
	fillerCount := positionCount - len(fixtures)
	if fillerCount == 0 {
		positionCount++
		fillerCount++
	}
	fillerBytes := fileSize - explicitBytes
	positions := make([]ImportAvailabilityPosition, positionCount)
	spans := make([]int64, positionCount)
	firstFiller := true
	for index := range positions {
		if fixture, ok := fixtureByIndex[index]; ok {
			spans[index] = fixture.EndOffset - fixture.StartOffset + 1
			fixture.StartOffset = 0
			fixture.EndOffset = 0
			positions[index] = fixture
			continue
		}

		span := int64(1)
		if firstFiller {
			span += fillerBytes - int64(fillerCount)
			firstFiller = false
		}
		spans[index] = span
		positions[index] = ImportAvailabilityPosition{
			Index: index,
			InitialChecks: []ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
			},
		}
	}

	return ImportAdmissionInput{
		DamagePolicy:              policy,
		Filename:                  "synthetic-video.mkv",
		FileSize:                  fileSize,
		FinalLayoutFingerprint:    pr5LayoutFingerprint,
		EvidenceLayoutFingerprint: pr5LayoutFingerprint,
		CanonicalUsableBytes:      spans,
		UncomplicatedStandalone:   true,
		InitialPassComplete:       true,
		ActiveProviders: []ImportAvailabilityProvider{
			{ID: pr5ProviderPrimary, Generation: 1, ActivationEpoch: pr5ProviderEpoch},
			{ID: pr5ProviderBackup, Generation: 1, ActivationEpoch: pr5ProviderEpoch},
		},
		Positions: positions,
	}
}

func TestPR5ImportAdmissionCleanCompleteCoverageIsAccepted(t *testing.T) {
	input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, []ImportProviderCheck{
			pr5Check(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
		}, nil),
		pr5Position(1, 100, 199, []ImportProviderCheck{
			pr5Check(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
		}, nil),
	})

	got, err := DecideImportAdmission(context.Background(), input)
	if err != nil {
		t.Fatalf("DecideImportAdmission() error = %v", err)
	}
	if got.Status != ImportAdmissionAccept {
		t.Fatalf("status = %q, want %q", got.Status, ImportAdmissionAccept)
	}
	if len(got.Unresolved) != 0 || len(got.ConfirmationTargets) != 0 {
		t.Fatalf("clean coverage returned unresolved work: %+v", got)
	}
}

func TestPR5ImportAdmissionReturnsDurableThirtySecondTargetPlan(t *testing.T) {
	input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, []ImportProviderCheck{
			pr5Check(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
		}, nil),
		pr5Position(1, 100, 199, []ImportProviderCheck{
			pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
			pr5Check(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
		}, nil),
		pr5Position(2, 200, 299, []ImportProviderCheck{
			pr5Check(pr5ProviderPrimary, nntppool.OutcomeTemporaryFailure, 451),
			pr5Check(pr5ProviderBackup, nntppool.OutcomeProviderUnavailable, 0),
		}, nil),
	})

	got, err := DecideImportAdmission(context.Background(), input)
	if err != nil {
		t.Fatalf("DecideImportAdmission() error = %v", err)
	}
	if got.Status != ImportAdmissionAwaitConfirmation {
		t.Fatalf("status = %q, want %q", got.Status, ImportAdmissionAwaitConfirmation)
	}
	if got.RetryAfter != 30*time.Second {
		t.Fatalf("RetryAfter = %v, want 30s", got.RetryAfter)
	}

	want := []ImportConfirmationTarget{
		{Position: 1, ProviderID: pr5ProviderPrimary, ProviderGeneration: 1, ProviderActivationEpoch: pr5ProviderEpoch},
		{Position: 1, ProviderID: pr5ProviderBackup, ProviderGeneration: 1, ProviderActivationEpoch: pr5ProviderEpoch},
		{Position: 2, ProviderID: pr5ProviderPrimary, ProviderGeneration: 1, ProviderActivationEpoch: pr5ProviderEpoch},
		{Position: 2, ProviderID: pr5ProviderBackup, ProviderGeneration: 1, ProviderActivationEpoch: pr5ProviderEpoch},
	}
	if !reflect.DeepEqual(got.ConfirmationTargets, want) {
		t.Fatalf("confirmation targets = %#v, want only unresolved positions across every provider %#v", got.ConfirmationTargets, want)
	}
}

func TestPR5ImportAdmissionAnyProviderSuccessWins(t *testing.T) {
	input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
		pr5Position(0, 0, 99,
			[]ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Check(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
			},
			[]ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Check(pr5ProviderBackup, nntppool.OutcomeSuccess, 223),
			}),
	})
	input.SecondPassComplete = true

	got, err := DecideImportAdmission(context.Background(), input)
	if err != nil {
		t.Fatalf("DecideImportAdmission() error = %v", err)
	}
	if got.Status != ImportAdmissionAccept {
		t.Fatalf("status = %q, want accept because one active provider succeeded", got.Status)
	}
	if len(got.Unresolved) != 0 {
		t.Fatalf("successful fallback remained unresolved: %+v", got.Unresolved)
	}
}

func TestPR5StrictImportRejectsCompletedUnresolvedAndPreservesCauses(t *testing.T) {
	input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
		pr5Position(0, 0, 99,
			[]ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Check(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
			},
			[]ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Check(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
			}),
	})
	input.SecondPassComplete = true

	got, err := DecideImportAdmission(context.Background(), input)
	if err != nil {
		t.Fatalf("DecideImportAdmission() error = %v", err)
	}
	if got.Status != ImportAdmissionReject {
		t.Fatalf("status = %q, want strict rejection", got.Status)
	}
	if len(got.Unresolved) != 1 {
		t.Fatalf("unresolved positions = %d, want 1", len(got.Unresolved))
	}

	evidence := got.Unresolved[0].ConfirmationChecks
	if len(evidence) != 2 {
		t.Fatalf("preserved confirmation checks = %d, want 2", len(evidence))
	}
	if evidence[0].Outcome != nntppool.OutcomeHardArticleAbsence || evidence[0].ResponseCode == nil || *evidence[0].ResponseCode != 430 || len(evidence[0].RawAttempts) != 1 || evidence[0].RawAttempts[0].ResponseCode != 430 {
		t.Fatalf("hard-absence evidence changed: %+v", evidence[0])
	}
	if evidence[1].Outcome != nntppool.OutcomeTemporaryFailure || evidence[1].ResponseCode == nil || *evidence[1].ResponseCode != 451 || len(evidence[1].RawAttempts) != 1 || evidence[1].RawAttempts[0].ResponseCode != 451 {
		t.Fatalf("451 evidence was rewritten instead of retained as temporary: %+v", evidence[1])
	}
}

func TestPR5TolerantImportUsesExactPositionAndByteEnvelope(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		fileSize  int64
		positions []ImportAvailabilityPosition
		want      ImportAdmissionStatus
	}{
		{
			name:     "eligible exact one percent span remains health pending",
			filename: "synthetic-video.mkv",
			fileSize: 10_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingChecks(), []ImportProviderCheck{
					pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
					pr5Check(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
				}),
			},
			want: ImportAdmissionHealthPending,
		},
		{
			name:     "exact three percent span rejects despite one missing position",
			filename: "synthetic-video.mkv",
			fileSize: 10_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_299, pr5CompletedMissingChecks(), []ImportProviderCheck{
					pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
					pr5Check(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
				}),
			},
			want: ImportAdmissionReject,
		},
		{
			name:     "five-position run rejects",
			filename: "synthetic-video.mkv",
			fileSize: 100_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
				pr5Position(3, 2_100, 2_199, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
				pr5Position(4, 2_200, 2_299, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
				pr5Position(5, 2_300, 2_399, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
				pr5Position(6, 2_400, 2_499, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
			},
			want: ImportAdmissionReject,
		},
		{
			name:     "archive is never eligible",
			filename: "synthetic-volume.rar",
			fileSize: 100_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingChecks(), pr5CompletedMissingChecks()),
			},
			want: ImportAdmissionReject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AdmissionInput("tolerant", tt.fileSize, tt.positions)
			input.Filename = tt.filename
			input.SecondPassComplete = true

			got, err := DecideImportAdmission(context.Background(), input)
			if err != nil {
				t.Fatalf("DecideImportAdmission() error = %v", err)
			}
			if got.Status != tt.want {
				t.Fatalf("status = %q, want %q (result=%+v)", got.Status, tt.want, got)
			}
		})
	}
}

func pr5CompletedMissingChecks() []ImportProviderCheck {
	return []ImportProviderCheck{
		pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
		pr5Check(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
	}
}

func TestPR5ImportCannotRejectIncompleteConfirmationWork(t *testing.T) {
	tests := []struct {
		name   string
		checks []ImportProviderCheck
	}{
		{
			name: "provider was never attempted",
			checks: []ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
			},
		},
		{
			name: "canceled provider attempt",
			checks: []ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5IncompleteCheck(pr5ProviderBackup, nntppool.OutcomeCancellation, true),
			},
		},
		{
			name: "omitted result remains incomplete",
			checks: []ImportProviderCheck{
				pr5Check(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5IncompleteCheck(pr5ProviderBackup, nntppool.OutcomeInconclusive, false),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
				pr5Position(0, 0, 99,
					pr5CompletedMissingChecks(),
					tt.checks),
			})
			input.SecondPassComplete = true

			got, err := DecideImportAdmission(context.Background(), input)
			if err != nil {
				t.Fatalf("DecideImportAdmission() error = %v", err)
			}
			if got.Status != ImportAdmissionAwaitConfirmation {
				t.Fatalf("status = %q, want await_confirmation; incomplete work must not reject", got.Status)
			}
		})
	}
}

func TestPR5ImportCancellationNeverBecomesAdmissionRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	input := pr5AdmissionInput("strict", 10_000, []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, pr5CompletedMissingChecks(), nil),
	})

	got, err := DecideImportAdmission(ctx, input)
	if err == nil {
		t.Fatal("DecideImportAdmission() error = nil, want context cancellation")
	}
	if got.Status == ImportAdmissionReject {
		t.Fatal("caller cancellation became an import rejection")
	}
}
