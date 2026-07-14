package validation

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
)

const (
	pr5ProviderPrimary = "provider-primary"
	pr5ProviderBackup  = "provider-backup"
)

func pr5Code(code int) *int { return &code }

func pr5Attempt(provider string, outcome nntppool.OutcomeKind, code int) ImportAvailabilityAttempt {
	attempt := ImportAvailabilityAttempt{
		ProviderID:         provider,
		ProviderGeneration: 1,
		Outcome:            outcome,
		CauseClass:         "synthetic-" + string(outcome),
		Completed:          true,
	}
	if code != 0 {
		attempt.ResponseCode = pr5Code(code)
	}
	return attempt
}

func pr5Position(index int, start, end int64, initial, confirmation []ImportAvailabilityAttempt) ImportAvailabilityPosition {
	return ImportAvailabilityPosition{
		Index:                index,
		StartOffset:          start,
		EndOffset:            end,
		InitialAttempts:      initial,
		ConfirmationAttempts: confirmation,
	}
}

func pr5AdmissionInput(policy string, positions []ImportAvailabilityPosition) ImportAdmissionInput {
	return ImportAdmissionInput{
		DamagePolicy:        policy,
		Filename:            "synthetic-video.mkv",
		FileSize:            10_000,
		InitialPassComplete: true,
		ActiveProviders: []ImportAvailabilityProvider{
			{ID: pr5ProviderPrimary, Generation: 1},
			{ID: pr5ProviderBackup, Generation: 1},
		},
		Positions: positions,
	}
}

func TestPR5ImportAdmissionCleanCompleteCoverageIsAccepted(t *testing.T) {
	input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, []ImportAvailabilityAttempt{
			pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
		}, nil),
		pr5Position(1, 100, 199, []ImportAvailabilityAttempt{
			pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
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
	input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, []ImportAvailabilityAttempt{
			pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeSuccess, 223),
		}, nil),
		pr5Position(1, 100, 199, []ImportAvailabilityAttempt{
			pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
			pr5Attempt(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
		}, nil),
		pr5Position(2, 200, 299, []ImportAvailabilityAttempt{
			pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeTemporaryFailure, 451),
			pr5Attempt(pr5ProviderBackup, nntppool.OutcomeProviderUnavailable, 0),
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
		{Position: 1, ProviderID: pr5ProviderPrimary, ProviderGeneration: 1},
		{Position: 1, ProviderID: pr5ProviderBackup, ProviderGeneration: 1},
		{Position: 2, ProviderID: pr5ProviderPrimary, ProviderGeneration: 1},
		{Position: 2, ProviderID: pr5ProviderBackup, ProviderGeneration: 1},
	}
	if !reflect.DeepEqual(got.ConfirmationTargets, want) {
		t.Fatalf("confirmation targets = %#v, want only unresolved positions across every provider %#v", got.ConfirmationTargets, want)
	}
}

func TestPR5ImportAdmissionAnyProviderSuccessWins(t *testing.T) {
	input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
		pr5Position(0, 0, 99,
			[]ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Attempt(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
			},
			[]ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Attempt(pr5ProviderBackup, nntppool.OutcomeSuccess, 223),
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
	input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
		pr5Position(0, 0, 99,
			[]ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Attempt(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
			},
			[]ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				pr5Attempt(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
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

	evidence := got.Unresolved[0].ConfirmationAttempts
	if len(evidence) != 2 {
		t.Fatalf("preserved confirmation attempts = %d, want 2", len(evidence))
	}
	if evidence[0].Outcome != nntppool.OutcomeHardArticleAbsence || evidence[0].ResponseCode == nil || *evidence[0].ResponseCode != 430 {
		t.Fatalf("hard-absence evidence changed: %+v", evidence[0])
	}
	if evidence[1].Outcome != nntppool.OutcomeTemporaryFailure || evidence[1].ResponseCode == nil || *evidence[1].ResponseCode != 451 {
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
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingAttempts(), []ImportAvailabilityAttempt{
					pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
					pr5Attempt(pr5ProviderBackup, nntppool.OutcomeTemporaryFailure, 451),
				}),
			},
			want: ImportAdmissionHealthPending,
		},
		{
			name:     "exact three percent span rejects despite one missing position",
			filename: "synthetic-video.mkv",
			fileSize: 10_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_299, pr5CompletedMissingAttempts(), []ImportAvailabilityAttempt{
					pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
					pr5Attempt(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
				}),
			},
			want: ImportAdmissionReject,
		},
		{
			name:     "five-position run rejects",
			filename: "synthetic-video.mkv",
			fileSize: 100_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
				pr5Position(3, 2_100, 2_199, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
				pr5Position(4, 2_200, 2_299, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
				pr5Position(5, 2_300, 2_399, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
				pr5Position(6, 2_400, 2_499, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
			},
			want: ImportAdmissionReject,
		},
		{
			name:     "archive is never eligible",
			filename: "synthetic-volume.rar",
			fileSize: 100_000,
			positions: []ImportAvailabilityPosition{
				pr5Position(2, 2_000, 2_099, pr5CompletedMissingAttempts(), pr5CompletedMissingAttempts()),
			},
			want: ImportAdmissionReject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AdmissionInput("tolerant", tt.positions)
			input.Filename = tt.filename
			input.FileSize = tt.fileSize
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

func pr5CompletedMissingAttempts() []ImportAvailabilityAttempt {
	return []ImportAvailabilityAttempt{
		pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
		pr5Attempt(pr5ProviderBackup, nntppool.OutcomeHardArticleAbsence, 430),
	}
}

func TestPR5ImportCannotRejectIncompleteConfirmationWork(t *testing.T) {
	tests := []struct {
		name     string
		attempts []ImportAvailabilityAttempt
	}{
		{
			name: "provider was never attempted",
			attempts: []ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
			},
		},
		{
			name: "canceled provider attempt",
			attempts: []ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				{
					ProviderID:         pr5ProviderBackup,
					ProviderGeneration: 1,
					Outcome:            nntppool.OutcomeCancellation,
					CauseClass:         "synthetic-canceled",
					Completed:          false,
				},
			},
		},
		{
			name: "omitted result remains incomplete",
			attempts: []ImportAvailabilityAttempt{
				pr5Attempt(pr5ProviderPrimary, nntppool.OutcomeHardArticleAbsence, 430),
				{
					ProviderID:         pr5ProviderBackup,
					ProviderGeneration: 1,
					Outcome:            nntppool.OutcomeInconclusive,
					CauseClass:         "synthetic-omitted",
					Completed:          false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
				pr5Position(0, 0, 99,
					pr5CompletedMissingAttempts(),
					tt.attempts),
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
	input := pr5AdmissionInput("strict", []ImportAvailabilityPosition{
		pr5Position(0, 0, 99, pr5CompletedMissingAttempts(), nil),
	})

	got, err := DecideImportAdmission(ctx, input)
	if err == nil {
		t.Fatal("DecideImportAdmission() error = nil, want context cancellation")
	}
	if got.Status == ImportAdmissionReject {
		t.Fatal("caller cancellation became an import rejection")
	}
}
