package health

import (
	"testing"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
)

func TestPR5CompleteCanonicalImportCoverageSuppressesImmediateDuplicateSTAT(t *testing.T) {
	coverage := committedObservationCoverage{
		LayoutFingerprint:  "sha256:synthetic-final-layout",
		ProviderSnapshotID: "snapshot-a",
		ObservationKind:    database.HealthObservationSTAT,
		TotalSegments:      100,
		CoveredSegments:    100,
		CanonicalLayout:    true,
		Completed:          true,
	}
	requirement := observationCoverageRequirement{
		LayoutFingerprint:  "sha256:synthetic-final-layout",
		ProviderSnapshotID: "snapshot-a",
		ObservationKind:    database.HealthObservationSTAT,
		TotalSegments:      100,
	}

	assert.True(t, canReuseCommittedCoverage(coverage, requirement),
		"a completed import pass is already a full positional availability health check")

	incomplete := coverage
	incomplete.CoveredSegments = 99
	assert.False(t, canReuseCommittedCoverage(incomplete, requirement))

	provisional := coverage
	provisional.CanonicalLayout = false
	assert.False(t, canReuseCommittedCoverage(provisional, requirement),
		"raw-NZB coverage cannot be promoted without an exact final canonical layout")

	wrongFingerprint := coverage
	wrongFingerprint.LayoutFingerprint = "sha256:synthetic-replacement-layout"
	assert.False(t, canReuseCommittedCoverage(wrongFingerprint, requirement))

	wrongSnapshot := coverage
	wrongSnapshot.ProviderSnapshotID = "snapshot-b"
	assert.False(t, canReuseCommittedCoverage(wrongSnapshot, requirement))
}

func TestPR5CompleteImportSTATCoverageDoesNotClaimBODYIntegrity(t *testing.T) {
	coverage := committedObservationCoverage{
		LayoutFingerprint:  "sha256:synthetic-final-layout",
		ProviderSnapshotID: "snapshot-a",
		ObservationKind:    database.HealthObservationSTAT,
		TotalSegments:      8,
		CoveredSegments:    8,
		CanonicalLayout:    true,
		Completed:          true,
	}
	bodyRequirement := observationCoverageRequirement{
		LayoutFingerprint:  "sha256:synthetic-final-layout",
		ProviderSnapshotID: "snapshot-a",
		ObservationKind:    database.HealthObservationValidatedBody,
		TotalSegments:      8,
	}

	assert.False(t, canReuseCommittedCoverage(coverage, bodyRequirement),
		"STAT proves article availability, not yEnc/decode/CRC correctness")
}

func TestPR5RevalidationUsesSTATForAbsenceAndFreshBODYForCorruption(t *testing.T) {
	absent := revalidationDispatchForCause(database.GapCauseAbsent)
	assert.Equal(t, database.HealthObservationSTAT, absent.ObservationKind)
	assert.False(t, absent.FreshTransport)

	corrupt := revalidationDispatchForCause(database.GapCauseCorrupt)
	assert.Equal(t, database.HealthObservationValidatedBody, corrupt.ObservationKind)
	assert.True(t, corrupt.FreshTransport,
		"corrupt content must be retried through targeted validated BODY on a fresh transport")
}

func TestPR5ObservationModeHasNoPaddingOrDestructiveSideEffects(t *testing.T) {
	for _, gapKind := range []database.GapKind{
		database.GapKindProvisional,
		database.GapKindConfirmedAbsent,
		database.GapKindConfirmedUnusable,
	} {
		effects := observationSideEffects(gapKind, true)
		assert.True(t, effects.PersistEvidence, "observation mode must still retain evidence for %s", gapKind)
		assert.False(t, effects.PersistentPadding, "observation mode padded %s", gapKind)
		assert.False(t, effects.DestructiveRepair, "observation mode repaired/deleted %s", gapKind)
		assert.False(t, effects.DeleteFile, "observation mode deleted %s", gapKind)
	}
}
