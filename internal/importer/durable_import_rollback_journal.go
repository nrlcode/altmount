package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/javi11/altmount/internal/metadata"
)

var (
	errDurableImportRollbackJournal = errors.New("durable import rollback journal is unavailable")
	errDurableImportRollbackState   = errors.New("durable import rollback journal state is invalid")
)

const (
	durableRollbackRecordVersion   = uint16(1)
	durableRollbackRecordSuffix    = ".rollback"
	durableRollbackCandidateSuffix = ".candidate"
	durableRollbackIntentSuffix    = ".intent"
	durableRollbackStoreSuffix     = ".store"
	durableRollbackManifestName    = "queue.state"
	durableRollbackMaxRecordSize   = int64(4 << 30)
)

var (
	durableRollbackRecordMagic   = [8]byte{'A', 'M', 'R', 'B', 'R', '0', '0', '1'}
	durableRollbackManifestMagic = [8]byte{'A', 'M', 'R', 'B', 'Q', '0', '0', '1'}
)

type durableStoreRefOperationLedger interface {
	ApplyStoreRefDeltaOnce(
		context.Context,
		string,
		string,
		int64,
	) (int64, bool, error)
}

type durableStoreRefCounterReader interface {
	GetStoreRefCount(context.Context, string) (int64, error)
}

type durableImportRollbackSnapshot struct {
	priorExists      bool
	priorFingerprint string
	priorStoreRef    string
	priorBytes       []byte
	recordName       string
}

type durableImportRollbackJournal struct {
	root            string
	metadataService *metadata.MetadataService
	storeRefs       durableStoreRefOperationLedger

	// Narrow hooks keep failure/crash windows deterministic without exposing
	// raw journal data to logs or external test artifacts.
	writeAtomic func(string, []byte) error
	restore     func(string, []byte, bool) error
	inspect     func(string) (metadata.MetadataVisibilityState, error)
	capture     func(string) ([]byte, metadata.MetadataVisibilityState, error)
	removeAll   func(string) error
	removeStore func(string) error
}

func newDurableImportRollbackJournal(
	metadataService *metadata.MetadataService,
	storeRefs durableStoreRefOperationLedger,
) *durableImportRollbackJournal {
	if metadataService == nil {
		return nil
	}
	journal := &durableImportRollbackJournal{
		root:            metadataService.PrivateStateDirectory("import-rollback"),
		metadataService: metadataService,
		storeRefs:       storeRefs,
	}
	journal.writeAtomic = journal.writeAtomicFile
	journal.restore = metadataService.RestoreMetadataVisibilitySnapshot
	journal.inspect = metadataService.InspectMetadataVisibility
	journal.capture = metadataService.CaptureMetadataVisibilitySnapshot
	journal.removeAll = os.RemoveAll
	journal.removeStore = metadataService.Store().RemoveStoreDurable
	return journal
}

func (j *durableImportRollbackJournal) Record(
	ctx context.Context,
	queueItemID int64,
	virtualPath string,
	priorBytes []byte,
	priorExists bool,
	priorFingerprint string,
	priorStoreRef string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil || j == nil || queueItemID <= 0 || j.writeAtomic == nil {
		return errDurableImportRollbackJournal
	}
	if priorExists && (len(priorBytes) == 0 || strings.TrimSpace(priorFingerprint) == "") {
		return errDurableImportRollbackState
	}
	if !priorExists && (len(priorBytes) != 0 || priorFingerprint != "" || priorStoreRef != "") {
		return errDurableImportRollbackState
	}
	queueDir := j.queueDirectory(queueItemID)
	if err := j.ensureQueueDirectory(queueItemID); err != nil {
		return errDurableImportRollbackJournal
	}
	recordName := durableRollbackRecordName(path)
	recordPath := filepath.Join(queueDir, recordName)
	want := durableImportRollbackSnapshot{
		priorExists: priorExists, priorFingerprint: priorFingerprint,
		priorStoreRef: priorStoreRef, priorBytes: append([]byte(nil), priorBytes...),
		recordName: recordName,
	}
	if existing, loadErr := j.loadRecord(recordPath); loadErr == nil {
		if !sameDurableRollbackSnapshot(existing, want) {
			return errDurableImportRollbackState
		}
		return nil
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return errDurableImportRollbackState
	}
	encoded, err := encodeDurableRollbackSnapshot(want)
	if err != nil {
		return errDurableImportRollbackState
	}
	if err := j.writeAtomic(recordPath, encoded); err != nil {
		if existing, loadErr := j.loadRecord(recordPath); loadErr == nil &&
			sameDurableRollbackSnapshot(existing, want) {
			return nil
		}
		return errDurableImportRollbackJournal
	}
	return nil
}

func (j *durableImportRollbackJournal) Validate(queueItemID int64, virtualPath string) error {
	if j == nil || queueItemID <= 0 {
		return errDurableImportRollbackJournal
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil {
		return errDurableImportRollbackState
	}
	if err := j.validateManifest(queueItemID); err != nil {
		return errDurableImportRollbackState
	}
	_, err = j.loadRecord(filepath.Join(j.queueDirectory(queueItemID), durableRollbackRecordName(path)))
	if err != nil {
		return errDurableImportRollbackState
	}
	return nil
}

func (j *durableImportRollbackJournal) Load(
	queueItemID int64,
	virtualPath string,
) (durableImportRollbackSnapshot, error) {
	if err := j.Validate(queueItemID, virtualPath); err != nil {
		return durableImportRollbackSnapshot{}, err
	}
	path, _ := normalizeDurableRollbackPath(virtualPath)
	snapshot, err := j.loadRecord(filepath.Join(
		j.queueDirectory(queueItemID), durableRollbackRecordName(path),
	))
	if err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	return snapshot, nil
}

func (j *durableImportRollbackJournal) RecordCandidate(
	queueItemID int64,
	virtualPath string,
) (durableImportRollbackSnapshot, error) {
	if j == nil || j.capture == nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackJournal
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	data, state, err := j.capture(path)
	if err != nil || !state.Exists || state.LayoutFingerprint == "" || len(data) == 0 {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	recordName := durableRollbackCandidateRecordName(path)
	want := durableImportRollbackSnapshot{
		priorExists: true, priorFingerprint: state.LayoutFingerprint,
		priorStoreRef: state.StoreRef, priorBytes: append([]byte(nil), data...),
		recordName: recordName,
	}
	recordPath := filepath.Join(j.queueDirectory(queueItemID), recordName)
	if existing, loadErr := j.loadRecord(recordPath); loadErr == nil {
		// Regenerated metadata can contain new timestamps on a resumable retry.
		// The retained candidate remains a valid exact compensation image when
		// its canonical layout and backing store are unchanged.
		if !existing.priorExists || existing.priorFingerprint != want.priorFingerprint ||
			existing.priorStoreRef != want.priorStoreRef {
			return durableImportRollbackSnapshot{}, errDurableImportRollbackState
		}
		return existing, nil
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	encoded, err := encodeDurableRollbackSnapshot(want)
	if err != nil || j.writeAtomic == nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	if err := j.writeAtomic(recordPath, encoded); err != nil {
		if existing, loadErr := j.loadRecord(recordPath); loadErr == nil &&
			sameDurableRollbackSnapshot(existing, want) {
			return existing, nil
		}
		return durableImportRollbackSnapshot{}, errDurableImportRollbackJournal
	}
	return want, nil
}

func (j *durableImportRollbackJournal) LoadCandidate(
	queueItemID int64,
	virtualPath string,
) (durableImportRollbackSnapshot, error) {
	if j == nil || queueItemID <= 0 {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackJournal
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	snapshot, err := j.loadRecord(filepath.Join(
		j.queueDirectory(queueItemID), durableRollbackCandidateRecordName(path),
	))
	if err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	return snapshot, nil
}

func (j *durableImportRollbackJournal) RecordCandidateIntent(
	ctx context.Context,
	queueItemID int64,
	virtualPath string,
	revisionID string,
	layoutFingerprint string,
	storeRef string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil || j == nil || queueItemID <= 0 || strings.TrimSpace(revisionID) == "" ||
		strings.TrimSpace(layoutFingerprint) == "" || j.writeAtomic == nil {
		return errDurableImportRollbackState
	}
	if err := j.ensureQueueDirectory(queueItemID); err != nil {
		return errDurableImportRollbackJournal
	}
	recordName := durableRollbackIntentRecordName(path)
	want := durableImportRollbackSnapshot{
		priorExists: true, priorFingerprint: layoutFingerprint,
		priorStoreRef: storeRef, priorBytes: []byte(revisionID), recordName: recordName,
	}
	recordPath := filepath.Join(j.queueDirectory(queueItemID), recordName)
	if existing, loadErr := j.loadRecord(recordPath); loadErr == nil {
		if !sameDurableRollbackSnapshot(existing, want) {
			return errDurableImportRollbackState
		}
		return nil
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return errDurableImportRollbackState
	}
	encoded, err := encodeDurableRollbackSnapshot(want)
	if err != nil {
		return errDurableImportRollbackState
	}
	if err := j.writeAtomic(recordPath, encoded); err != nil {
		if existing, loadErr := j.loadRecord(recordPath); loadErr == nil &&
			sameDurableRollbackSnapshot(existing, want) {
			return nil
		}
		return errDurableImportRollbackJournal
	}
	return nil
}

func (j *durableImportRollbackJournal) LoadCandidateIntent(
	queueItemID int64,
	virtualPath string,
) (durableImportRollbackSnapshot, error) {
	if j == nil || queueItemID <= 0 {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackJournal
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	snapshot, err := j.loadRecord(filepath.Join(
		j.queueDirectory(queueItemID), durableRollbackIntentRecordName(path),
	))
	if err != nil || len(snapshot.priorBytes) == 0 || snapshot.priorFingerprint == "" {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	return snapshot, nil
}

func (j *durableImportRollbackJournal) CandidateIntentExists(
	queueItemID int64,
	virtualPath string,
) (bool, error) {
	if j == nil || queueItemID <= 0 {
		return false, errDurableImportRollbackJournal
	}
	path, err := normalizeDurableRollbackPath(virtualPath)
	if err != nil {
		return false, errDurableImportRollbackState
	}
	info, err := os.Lstat(filepath.Join(
		j.queueDirectory(queueItemID), durableRollbackIntentRecordName(path),
	))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return false, errDurableImportRollbackState
	}
	return true, nil
}

// RecordStoreIntent makes a queue-created store discoverable even if the
// process crashes or admission rejects before the first metadata write.
func (j *durableImportRollbackJournal) RecordStoreIntent(
	ctx context.Context,
	queueItemID int64,
	storeRef string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	storeRef = strings.TrimSpace(storeRef)
	if j == nil || queueItemID <= 0 || storeRef == "" || j.writeAtomic == nil {
		return errDurableImportRollbackState
	}
	if err := j.ensureQueueDirectory(queueItemID); err != nil {
		return errDurableImportRollbackJournal
	}
	recordName := durableRollbackStoreRecordName(storeRef)
	want := durableImportRollbackSnapshot{
		priorExists: true, priorFingerprint: "queue-store-v1",
		priorStoreRef: storeRef, priorBytes: []byte("owned"), recordName: recordName,
	}
	recordPath := filepath.Join(j.queueDirectory(queueItemID), recordName)
	if existing, loadErr := j.loadRecord(recordPath); loadErr == nil {
		if !sameDurableRollbackSnapshot(existing, want) {
			return errDurableImportRollbackState
		}
		return nil
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return errDurableImportRollbackState
	}
	encoded, err := encodeDurableRollbackSnapshot(want)
	if err != nil {
		return errDurableImportRollbackState
	}
	if err := j.writeAtomic(recordPath, encoded); err != nil {
		if existing, loadErr := j.loadRecord(recordPath); loadErr == nil &&
			sameDurableRollbackSnapshot(existing, want) {
			return nil
		}
		return errDurableImportRollbackJournal
	}
	return nil
}

func (j *durableImportRollbackJournal) ValidateQueue(queueItemID int64) error {
	if j == nil || queueItemID <= 0 {
		return errDurableImportRollbackJournal
	}
	info, err := os.Lstat(j.queueDirectory(queueItemID))
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errDurableImportRollbackState
	}
	if err := j.validateManifest(queueItemID); err != nil {
		return errDurableImportRollbackState
	}
	if err := j.cleanupOrphanTemps(j.queueDirectory(queueItemID)); err != nil {
		return err
	}
	entries, err := os.ReadDir(j.queueDirectory(queueItemID))
	if err != nil {
		return errDurableImportRollbackState
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == durableRollbackManifestName {
			if entry.IsDir() {
				return errDurableImportRollbackState
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), durableRollbackRecordSuffix) &&
			!strings.HasSuffix(entry.Name(), durableRollbackCandidateSuffix) &&
			!strings.HasSuffix(entry.Name(), durableRollbackIntentSuffix) &&
			!strings.HasSuffix(entry.Name(), durableRollbackStoreSuffix) {
			return errDurableImportRollbackState
		}
		if _, err := j.loadRecord(filepath.Join(j.queueDirectory(queueItemID), entry.Name())); err != nil {
			return errDurableImportRollbackState
		}
	}
	return nil
}

func (j *durableImportRollbackJournal) PendingQueueIDs() ([]int64, error) {
	if j == nil {
		return nil, errDurableImportRollbackJournal
	}
	rootInfo, err := os.Lstat(j.root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errDurableImportRollbackJournal
	}
	if !rootInfo.IsDir() || rootInfo.Mode().Perm()&0o077 != 0 {
		return nil, errDurableImportRollbackState
	}
	entries, err := os.ReadDir(j.root)
	if err != nil {
		return nil, errDurableImportRollbackJournal
	}
	queueIDs := make([]int64, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "q-") {
			return nil, errDurableImportRollbackState
		}
		manifest, err := os.ReadFile(filepath.Join(j.root, entry.Name(), durableRollbackManifestName))
		if err != nil {
			return nil, errDurableImportRollbackState
		}
		queueItemID, err := decodeDurableRollbackManifest(manifest)
		if err != nil || filepath.Base(j.queueDirectory(queueItemID)) != entry.Name() {
			return nil, errDurableImportRollbackState
		}
		if err := j.ValidateQueue(queueItemID); err != nil {
			return nil, err
		}
		queueIDs = append(queueIDs, queueItemID)
	}
	return queueIDs, nil
}

func (j *durableImportRollbackJournal) CommitQueue(
	ctx context.Context,
	queueItemID int64,
) error {
	if j == nil || queueItemID <= 0 {
		return errDurableImportRollbackJournal
	}
	if _, err := os.Stat(j.queueDirectory(queueItemID)); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errDurableImportRollbackJournal
	}
	if err := j.ValidateQueue(queueItemID); err != nil {
		return err
	}
	entries, err := os.ReadDir(j.queueDirectory(queueItemID))
	if err != nil {
		return errDurableImportRollbackJournal
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), durableRollbackRecordSuffix) {
			continue
		}
		snapshot, err := j.loadRecord(filepath.Join(j.queueDirectory(queueItemID), entry.Name()))
		if err != nil {
			return errDurableImportRollbackState
		}
		if snapshot.priorStoreRef == "" {
			continue
		}
		if err := j.applyStoreRefDelta(
			ctx, queueItemID, entry.Name(), "commit-prior", snapshot.priorStoreRef, -1,
		); err != nil {
			return err
		}
	}
	if err := j.CleanupUnreferencedStores(ctx, queueItemID); err != nil {
		return err
	}
	return j.DiscardQueue(queueItemID)
}

func (j *durableImportRollbackJournal) ApplyCandidateStoreRefIncrement(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionID string,
	storeRef string,
) error {
	if storeRef == "" {
		return nil
	}
	return j.applyStoreRefDelta(
		ctx, queueItemID, durableRollbackHash(candidateRevisionID),
		"acquire-candidate", storeRef, 1,
	)
}

func (j *durableImportRollbackJournal) ReleaseCandidateStoreRef(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionID string,
	storeRef string,
) error {
	if storeRef == "" {
		return nil
	}
	// Converge an ambiguous/crashed acquire first. If +1 committed but its
	// caller did not observe success, replay is a no-op; otherwise this applies
	// the missing ownership before the exact -1 releases it.
	if err := j.ApplyCandidateStoreRefIncrement(
		ctx, queueItemID, candidateRevisionID, storeRef,
	); err != nil {
		return err
	}
	return j.applyStoreRefDelta(
		ctx, queueItemID, durableRollbackHash(candidateRevisionID),
		"rollback-candidate", storeRef, -1,
	)
}

// ApplyCandidateRollbackStoreRef is retained for the existing internal seam;
// durable callers now converge the acquire before releasing it.
func (j *durableImportRollbackJournal) ApplyCandidateRollbackStoreRef(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionID string,
	storeRef string,
) error {
	return j.ReleaseCandidateStoreRef(ctx, queueItemID, candidateRevisionID, storeRef)
}

func (j *durableImportRollbackJournal) applyStoreRefDelta(
	ctx context.Context,
	queueItemID int64,
	subject string,
	transition string,
	storeRef string,
	delta int64,
) error {
	if storeRef == "" || delta == 0 {
		return nil
	}
	if j == nil || j.storeRefs == nil {
		return errDurableImportRollbackJournal
	}
	operationKey := "sha256:" + durableRollbackHash(strings.Join([]string{
		"store-ref", strconv.FormatInt(queueItemID, 10), subject, transition,
	}, "\x00"))
	count, _, err := j.storeRefs.ApplyStoreRefDeltaOnce(
		ctx, operationKey, storeRef, delta,
	)
	if err != nil {
		return errDurableImportRollbackJournal
	}
	if delta < 0 && count == 0 {
		if j.removeStore == nil || j.removeStore(storeRef) != nil {
			return errDurableImportRollbackJournal
		}
	}
	return nil
}

func (j *durableImportRollbackJournal) CleanupUnreferencedStores(
	ctx context.Context,
	queueItemID int64,
) error {
	if j == nil || queueItemID <= 0 || j.removeStore == nil {
		return errDurableImportRollbackJournal
	}
	entries, err := os.ReadDir(j.queueDirectory(queueItemID))
	if err != nil {
		return errDurableImportRollbackJournal
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), durableRollbackStoreSuffix) {
			continue
		}
		counter, ok := j.storeRefs.(durableStoreRefCounterReader)
		if !ok {
			return errDurableImportRollbackJournal
		}
		snapshot, err := j.loadRecord(filepath.Join(j.queueDirectory(queueItemID), entry.Name()))
		if err != nil || snapshot.priorStoreRef == "" || snapshot.priorFingerprint != "queue-store-v1" {
			return errDurableImportRollbackState
		}
		count, err := counter.GetStoreRefCount(ctx, snapshot.priorStoreRef)
		if err != nil {
			return errDurableImportRollbackJournal
		}
		if count == 0 {
			if err := j.removeStore(snapshot.priorStoreRef); err != nil {
				return errDurableImportRollbackJournal
			}
		}
	}
	return nil
}

func (j *durableImportRollbackJournal) DiscardQueue(queueItemID int64) error {
	if j == nil || queueItemID <= 0 || j.removeAll == nil {
		return errDurableImportRollbackJournal
	}
	if _, err := os.Stat(j.queueDirectory(queueItemID)); os.IsNotExist(err) {
		return nil
	}
	if err := j.ValidateQueue(queueItemID); err != nil {
		return err
	}
	if err := j.removeAll(j.queueDirectory(queueItemID)); err != nil {
		return errDurableImportRollbackJournal
	}
	if err := syncDurableRollbackDirectory(j.root); err != nil {
		return errDurableImportRollbackJournal
	}
	return nil
}

func (j *durableImportRollbackJournal) ensureQueueDirectory(queueItemID int64) error {
	rootExisted := true
	if _, err := os.Lstat(j.root); os.IsNotExist(err) {
		rootExisted = false
	} else if err != nil {
		return err
	}
	if err := os.MkdirAll(j.root, 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(j.root); err != nil || !info.IsDir() {
		return errDurableImportRollbackState
	}
	if err := os.Chmod(j.root, 0o700); err != nil {
		return err
	}
	if !rootExisted {
		if err := syncDurableRollbackDirectory(filepath.Dir(j.root)); err != nil {
			return err
		}
	}
	queueDir := j.queueDirectory(queueItemID)
	queueExisted := true
	if _, err := os.Lstat(queueDir); os.IsNotExist(err) {
		queueExisted = false
	} else if err != nil {
		return err
	}
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(queueDir); err != nil || !info.IsDir() {
		return errDurableImportRollbackState
	}
	if err := os.Chmod(queueDir, 0o700); err != nil {
		return err
	}
	if !queueExisted {
		if err := syncDurableRollbackDirectory(j.root); err != nil {
			return err
		}
	}
	manifestPath := filepath.Join(queueDir, durableRollbackManifestName)
	want := encodeDurableRollbackManifest(queueItemID)
	if existing, err := os.ReadFile(manifestPath); err == nil {
		if !bytes.Equal(existing, want) {
			return errDurableImportRollbackState
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := j.writeAtomic(manifestPath, want); err != nil {
		if existing, readErr := os.ReadFile(manifestPath); readErr == nil && bytes.Equal(existing, want) {
			return nil
		}
		return err
	}
	return nil
}

func (j *durableImportRollbackJournal) validateManifest(queueItemID int64) error {
	manifestPath := filepath.Join(j.queueDirectory(queueItemID), durableRollbackManifestName)
	info, err := os.Lstat(manifestPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errDurableImportRollbackState
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, encodeDurableRollbackManifest(queueItemID)) {
		return errDurableImportRollbackState
	}
	return nil
}

func (j *durableImportRollbackJournal) queueDirectory(queueItemID int64) string {
	return filepath.Join(j.root, "q-"+durableRollbackHash(strconv.FormatInt(queueItemID, 10)))
}

func (j *durableImportRollbackJournal) writeAtomicFile(finalPath string, data []byte) error {
	dir := filepath.Dir(finalPath)
	tmp, err := os.CreateTemp(dir, ".journal-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanupTemp := func() error {
		if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncDurableRollbackDirectory(dir)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = cleanupTemp()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = cleanupTemp()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = cleanupTemp()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = cleanupTemp()
		return err
	}
	if err := os.Link(tmpPath, finalPath); err != nil {
		_ = cleanupTemp()
		return err
	}
	if err := syncDurableRollbackDirectory(dir); err != nil {
		_ = cleanupTemp()
		return err
	}
	return cleanupTemp()
}

func (j *durableImportRollbackJournal) cleanupOrphanTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errDurableImportRollbackJournal
	}
	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".journal-") ||
			!strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return errDurableImportRollbackState
		}
		if err := os.Remove(path); err != nil {
			return errDurableImportRollbackJournal
		}
		removed = true
	}
	if removed {
		if err := syncDurableRollbackDirectory(dir); err != nil {
			return errDurableImportRollbackJournal
		}
	}
	return nil
}

func syncDurableRollbackDirectory(path string) error {
	dirHandle, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dirHandle.Sync()
	closeErr := dirHandle.Close()
	return errors.Join(syncErr, closeErr)
}

func (j *durableImportRollbackJournal) loadRecord(path string) (durableImportRollbackSnapshot, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return durableImportRollbackSnapshot{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 ||
		info.Size() <= 0 || info.Size() > durableRollbackMaxRecordSize {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	file, err := os.Open(path)
	if err != nil {
		return durableImportRollbackSnapshot{}, err
	}
	defer file.Close()
	return decodeDurableRollbackSnapshot(file, info.Size(), filepath.Base(path))
}

func encodeDurableRollbackManifest(queueItemID int64) []byte {
	data := make([]byte, 8+8+32)
	copy(data[:8], durableRollbackManifestMagic[:])
	binary.BigEndian.PutUint64(data[8:16], uint64(queueItemID))
	checksum := sha256.Sum256(data[:16])
	copy(data[16:], checksum[:])
	return data
}

func decodeDurableRollbackManifest(data []byte) (int64, error) {
	if len(data) != 48 || !bytes.Equal(data[:8], durableRollbackManifestMagic[:]) {
		return 0, errDurableImportRollbackState
	}
	checksum := sha256.Sum256(data[:16])
	if !bytes.Equal(checksum[:], data[16:]) {
		return 0, errDurableImportRollbackState
	}
	queueItemID := int64(binary.BigEndian.Uint64(data[8:16]))
	if queueItemID <= 0 {
		return 0, errDurableImportRollbackState
	}
	return queueItemID, nil
}

func encodeDurableRollbackSnapshot(snapshot durableImportRollbackSnapshot) ([]byte, error) {
	if len(snapshot.priorFingerprint) > int(^uint32(0)) ||
		len(snapshot.priorStoreRef) > int(^uint32(0)) {
		return nil, errDurableImportRollbackState
	}
	flags := byte(0)
	if snapshot.priorExists {
		flags = 1
	}
	payload := make([]byte, 0, len(snapshot.priorFingerprint)+len(snapshot.priorStoreRef)+len(snapshot.priorBytes))
	payload = append(payload, snapshot.priorFingerprint...)
	payload = append(payload, snapshot.priorStoreRef...)
	payload = append(payload, snapshot.priorBytes...)
	header := make([]byte, 8+2+1+4+4+8+32)
	copy(header[:8], durableRollbackRecordMagic[:])
	binary.BigEndian.PutUint16(header[8:10], durableRollbackRecordVersion)
	header[10] = flags
	binary.BigEndian.PutUint32(header[11:15], uint32(len(snapshot.priorFingerprint)))
	binary.BigEndian.PutUint32(header[15:19], uint32(len(snapshot.priorStoreRef)))
	binary.BigEndian.PutUint64(header[19:27], uint64(len(snapshot.priorBytes)))
	checksumInput := append(append([]byte(nil), header[:27]...), payload...)
	checksum := sha256.Sum256(checksumInput)
	copy(header[27:59], checksum[:])
	return append(header, payload...), nil
}

func decodeDurableRollbackSnapshot(
	reader io.Reader,
	totalSize int64,
	recordName string,
) (durableImportRollbackSnapshot, error) {
	const headerSize = 59
	if totalSize < headerSize {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	if !bytes.Equal(header[:8], durableRollbackRecordMagic[:]) ||
		binary.BigEndian.Uint16(header[8:10]) != durableRollbackRecordVersion ||
		header[10]&^byte(1) != 0 {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	fingerprintLength := int64(binary.BigEndian.Uint32(header[11:15]))
	storeRefLength := int64(binary.BigEndian.Uint32(header[15:19]))
	metadataLength := int64(binary.BigEndian.Uint64(header[19:27]))
	payloadLength := fingerprintLength + storeRefLength + metadataLength
	if payloadLength < 0 || payloadLength != totalSize-headerSize {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	checksumInput := append(append([]byte(nil), header[:27]...), payload...)
	checksum := sha256.Sum256(checksumInput)
	if !bytes.Equal(checksum[:], header[27:59]) {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	fingerprintEnd := fingerprintLength
	storeRefEnd := fingerprintEnd + storeRefLength
	snapshot := durableImportRollbackSnapshot{
		priorExists:      header[10]&1 != 0,
		priorFingerprint: string(payload[:fingerprintEnd]),
		priorStoreRef:    string(payload[fingerprintEnd:storeRefEnd]),
		priorBytes:       append([]byte(nil), payload[storeRefEnd:]...),
		recordName:       recordName,
	}
	if snapshot.priorExists != (len(snapshot.priorBytes) > 0) ||
		(snapshot.priorExists && snapshot.priorFingerprint == "") ||
		(!snapshot.priorExists && (snapshot.priorFingerprint != "" || snapshot.priorStoreRef != "")) {
		return durableImportRollbackSnapshot{}, errDurableImportRollbackState
	}
	return snapshot, nil
}

func sameDurableRollbackSnapshot(left, right durableImportRollbackSnapshot) bool {
	return left.priorExists == right.priorExists &&
		left.priorFingerprint == right.priorFingerprint &&
		left.priorStoreRef == right.priorStoreRef &&
		bytes.Equal(left.priorBytes, right.priorBytes)
}

func normalizeDurableRollbackPath(value string) (string, error) {
	value = filepath.ToSlash(strings.TrimSpace(value))
	value = strings.TrimPrefix(value, "/")
	value = strings.TrimSuffix(value, "/")
	if value == "" || value == "." || strings.ContainsRune(value, '\x00') {
		return "", errDurableImportRollbackState
	}
	cleaned := filepath.ToSlash(filepath.Clean(value))
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || filepath.IsAbs(cleaned) {
		return "", errDurableImportRollbackState
	}
	return cleaned, nil
}

func durableRollbackRecordName(path string) string {
	return "p-" + durableRollbackHash(path) + durableRollbackRecordSuffix
}

func durableRollbackCandidateRecordName(path string) string {
	return "p-" + durableRollbackHash(path) + durableRollbackCandidateSuffix
}

func durableRollbackIntentRecordName(path string) string {
	return "p-" + durableRollbackHash(path) + durableRollbackIntentSuffix
}

func durableRollbackStoreRecordName(storeRef string) string {
	return "s-" + durableRollbackHash(storeRef) + durableRollbackStoreSuffix
}

func durableRollbackHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest[:])
}
