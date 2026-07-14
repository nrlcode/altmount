package importer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/javi11/altmount/internal/importer/validation"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5StoreRefOperationLedger struct {
	mu         sync.Mutex
	applied    map[string]struct{}
	operations map[string]int64
	err        error
}

func (l *pr5StoreRefOperationLedger) ApplyStoreRefDeltaOnce(
	_ context.Context,
	operationKey string,
	storePath string,
	delta int64,
) (int64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, false, l.err
	}
	if l.applied == nil {
		l.applied = make(map[string]struct{})
		l.operations = make(map[string]int64)
	}
	if _, duplicate := l.applied[operationKey]; duplicate {
		return l.operations[storePath], false, nil
	}
	l.applied[operationKey] = struct{}{}
	l.operations[storePath] += delta
	return l.operations[storePath], true, nil
}

func (l *pr5StoreRefOperationLedger) GetStoreRefCount(
	_ context.Context,
	storePath string,
) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	return l.operations[storePath], nil
}

func pr5RollbackMetadata(id string, status metapb.FileStatus) *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize: 100, Status: status,
		SegmentData: []*metapb.SegmentData{{
			Id: id, SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}
}

func TestPR5RollbackJournalRetainsFirstSnapshotAcrossRestartOutsideMetadataTree(t *testing.T) {
	metadataRoot := filepath.Join(t.TempDir(), "metadata")
	metadataService := metadata.NewMetadataService(metadataRoot)
	path := "library/replacement.mkv"
	prior := pr5RollbackMetadata("fixture-prior", metapb.FileStatus_FILE_STATUS_CORRUPTED)
	require.NoError(t, metadataService.WriteFileMetadata(path, prior))
	priorBytes, err := os.ReadFile(metadataService.GetMetadataFilePath(path))
	require.NoError(t, err)
	priorLayout, err := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, err)

	journal := newDurableImportRollbackJournal(metadataService, nil)
	require.NotNil(t, journal)
	assert.NotEqual(t, metadataRoot, journal.root)
	assert.False(t, strings.HasPrefix(journal.root, metadataRoot+string(filepath.Separator)))
	require.NoError(t, journal.Record(
		context.Background(), 71, path, priorBytes, true, priorLayout.Fingerprint, "",
	))

	candidate := pr5RollbackMetadata("fixture-candidate", metapb.FileStatus_FILE_STATUS_HEALTHY)
	require.NoError(t, metadataService.WriteFileMetadata(path, candidate))
	restarted := newDurableImportRollbackJournal(metadataService, nil)
	snapshot, err := restarted.Load(71, path)
	require.NoError(t, err)
	assert.Equal(t, priorLayout.Fingerprint, snapshot.priorFingerprint)
	assert.Equal(t, priorBytes, snapshot.priorBytes)

	other := pr5RollbackMetadata("fixture-other", metapb.FileStatus_FILE_STATUS_CORRUPTED)
	otherBytes := []byte("different-private-snapshot")
	otherLayout, err := metadata.ResolveCanonicalSegmentLayout(other)
	require.NoError(t, err)
	err = restarted.Record(
		context.Background(), 71, path, otherBytes, true, otherLayout.Fingerprint, "",
	)
	require.ErrorIs(t, err, errDurableImportRollbackState,
		"the first pre-queue visibility snapshot must never be replaced")

	require.NoError(t, restarted.restore(path, snapshot.priorBytes, snapshot.priorExists))
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, priorLayout.Fingerprint, visible.LayoutFingerprint)

	require.NoError(t, filepath.WalkDir(journal.root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		assert.NotContains(t, entry.Name(), "replacement")
		if !entry.IsDir() {
			info, infoErr := entry.Info()
			require.NoError(t, infoErr)
			assert.Zero(t, info.Mode().Perm()&0o077)
		}
		return nil
	}))
}

func TestPR5RollbackJournalIntegrityFailureBlocksDestructiveCleanup(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	require.NoError(t, journal.Record(
		context.Background(), 72, "library/new.mkv", nil, false, "", "",
	))
	recordPath := filepath.Join(
		journal.queueDirectory(72), durableRollbackRecordName("library/new.mkv"),
	)
	require.NoError(t, os.WriteFile(recordPath, []byte("corrupt"), 0o600))
	require.ErrorIs(t, journal.ValidateQueue(72), errDurableImportRollbackState)
	require.ErrorIs(t, journal.DiscardQueue(72), errDurableImportRollbackState)
	_, err := os.Stat(journal.queueDirectory(72))
	require.NoError(t, err, "uncertain journal state must remain untouched for recovery")
}

func TestPR5RollbackJournalStartupRejectsWidenedPrivateRoot(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	require.NoError(t, journal.Record(
		context.Background(), 77, "library/new.mkv", nil, false, "", "",
	))
	require.NoError(t, os.Chmod(journal.root, 0o755))
	_, err := journal.PendingQueueIDs()
	require.ErrorIs(t, err, errDurableImportRollbackState,
		"startup must stop before reading raw recovery records from a non-private root")
}

func TestPR5RollbackJournalFailurePreventsMetadataReplacementAndActivation(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	path := "library/prior.mkv"
	prior := pr5RollbackMetadata("fixture-prior", metapb.FileStatus_FILE_STATUS_CORRUPTED)
	require.NoError(t, metadataService.WriteFileMetadata(path, prior))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	journal.writeAtomic = func(string, []byte) error { return errors.New("injected private write failure") }
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
	}}
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)

	candidate := pr5RollbackMetadata("fixture-candidate", metapb.FileStatus_FILE_STATUS_HEALTHY)
	err := metadataService.WriteFileMetadataAuto(
		withDurableImportIntent(
			context.Background(), 73, validation.FinalLayoutProvenanceStandalone,
		),
		path, candidate, nil, "",
	)
	require.Error(t, err)
	assert.Empty(t, stub.activations)
	visible, inspectErr := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, inspectErr)
	priorLayout, layoutErr := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, layoutErr)
	assert.Equal(t, priorLayout.Fingerprint, visible.LayoutFingerprint)
	assert.NotContains(t, err.Error(), "fixture-prior")
	assert.NotContains(t, err.Error(), "fixture-candidate")
}

func TestPR5CandidateJournalFailureRestoresPriorBeforeRevisionActivation(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	path := "library/prior.mkv"
	prior := pr5RollbackMetadata("fixture-prior", metapb.FileStatus_FILE_STATUS_CORRUPTED)
	require.NoError(t, metadataService.WriteFileMetadata(path, prior))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	writeAtomic := journal.writeAtomic
	journal.writeAtomic = func(finalPath string, data []byte) error {
		if strings.HasSuffix(finalPath, durableRollbackCandidateSuffix) {
			return errors.New("injected private candidate write failure")
		}
		return writeAtomic(finalPath, data)
	}
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
	}}
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)

	candidate := pr5RollbackMetadata("fixture-candidate", metapb.FileStatus_FILE_STATUS_HEALTHY)
	err := metadataService.WriteFileMetadataAuto(
		withDurableImportIntent(
			context.Background(), 76, validation.FinalLayoutProvenanceStandalone,
		),
		path, candidate, nil, "",
	)
	require.Error(t, err)
	assert.Empty(t, stub.activations,
		"the DB revision must not activate until its exact candidate bytes are durable")
	visible, inspectErr := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, inspectErr)
	priorLayout, layoutErr := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, layoutErr)
	assert.Equal(t, priorLayout.Fingerprint, visible.LayoutFingerprint)
	require.NoError(t, journal.Validate(76, path),
		"the first pre-queue snapshot remains available for a safe retry")
}

func TestPR5RollbackJournalStoreRefTransitionsAreIdempotent(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	ledger := &pr5StoreRefOperationLedger{}
	journal := newDurableImportRollbackJournal(metadataService, ledger)
	require.NoError(t, journal.Record(
		context.Background(), 74, "library/v3.mkv", []byte("prior-v3"), true,
		"sha256:prior", "private-prior-store",
	))
	require.NoError(t, journal.CommitQueue(context.Background(), 74))
	require.NoError(t, journal.CommitQueue(context.Background(), 74))
	assert.Equal(t, int64(-1), ledger.operations["private-prior-store"])

	require.NoError(t, journal.ApplyCandidateRollbackStoreRef(
		context.Background(), 75, "candidate-revision", "private-candidate-store",
	))
	require.NoError(t, journal.ApplyCandidateRollbackStoreRef(
		context.Background(), 75, "candidate-revision", "private-candidate-store",
	))
	assert.Equal(t, int64(0), ledger.operations["private-candidate-store"],
		"rollback converges an ambiguous candidate acquire before releasing it")
}

func TestPR5RollbackJournalRemovesOnlyZeroCountQueueStoresAndCandidateRefs(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	zeroStore := filepath.Join(t.TempDir(), "zero.nzbz")
	sharedStore := filepath.Join(t.TempDir(), "shared.nzbz")
	releasedCandidate := filepath.Join(t.TempDir(), "candidate.nzbz")
	for _, path := range []string{zeroStore, sharedStore, releasedCandidate} {
		require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
	}
	ledger := &pr5StoreRefOperationLedger{
		applied: make(map[string]struct{}),
		operations: map[string]int64{
			zeroStore: 0, sharedStore: 1, releasedCandidate: 0,
		},
	}
	journal := newDurableImportRollbackJournal(metadataService, ledger)
	require.NoError(t, journal.RecordStoreIntent(context.Background(), 81, zeroStore))
	require.NoError(t, journal.RecordStoreIntent(context.Background(), 81, sharedStore))
	require.NoError(t, journal.CleanupUnreferencedStores(context.Background(), 81))
	_, err := os.Stat(zeroStore)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(sharedStore)
	require.NoError(t, err, "a store referenced by another visible metadata file must remain")

	require.NoError(t, journal.ApplyCandidateStoreRefIncrement(
		context.Background(), 82, "candidate-revision", releasedCandidate,
	))
	require.NoError(t, journal.ReleaseCandidateStoreRef(
		context.Background(), 82, "candidate-revision", releasedCandidate,
	))
	require.NoError(t, journal.ReleaseCandidateStoreRef(
		context.Background(), 82, "candidate-revision", releasedCandidate,
	))
	_, err = os.Stat(releasedCandidate)
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Equal(t, int64(0), ledger.operations[releasedCandidate])
}

func TestPR5RollbackJournalRestartRemovesPrivateOrphanTemp(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	require.NoError(t, journal.Record(
		context.Background(), 83, "library/orphan.mkv", nil, false, "", "",
	))
	orphan := filepath.Join(journal.queueDirectory(83), ".journal-crash.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("private-partial"), 0o600))
	restarted := newDurableImportRollbackJournal(metadataService, nil)
	queueIDs, err := restarted.PendingQueueIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{83}, queueIDs)
	_, err = os.Stat(orphan)
	require.ErrorIs(t, err, os.ErrNotExist)
}
