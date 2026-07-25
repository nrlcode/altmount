package metadata

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type storeReleaseBoundary interface {
	RemoveStoreIfUnreferenced(context.Context, string) error
}

type storeReleaseLockCounter struct {
	getHeldCleanupLock bool
}

func (*storeReleaseLockCounter) IncStoreRef(context.Context, string) error {
	return nil
}

func (*storeReleaseLockCounter) DecStoreRef(context.Context, string) (int64, error) {
	return 0, nil
}

func (c *storeReleaseLockCounter) GetStoreRefCount(context.Context, string) (int64, error) {
	if cleanupOperationMu.TryLock() {
		cleanupOperationMu.Unlock()
		return 0, nil
	}
	c.getHeldCleanupLock = true
	return 0, nil
}

func TestStoreReleaseSerializesZeroCountDecisionWithAcquisition(t *testing.T) {
	metadataRoot := t.TempDir()
	storeRoot := filepath.Join(t.TempDir(), ".nzbs")
	storePath := filepath.Join(storeRoot, "release.nzbz")
	service := NewMetadataService(metadataRoot)
	counter := &storeReleaseLockCounter{}
	service.SetStoreRefCounter(counter)
	require.NoError(t, service.ConfigureCleanupRoots(storeRoot))
	require.NoError(t, service.Store().WriteStore(storePath, &metapb.NzbStore{}))
	_, err := service.Store().ReadStore(storePath)
	require.NoError(t, err)

	release, ok := any(service).(storeReleaseBoundary)
	require.True(t, ok, "MetadataService must expose the bounded rooted zero-owner release boundary")
	require.NoError(t, release.RemoveStoreIfUnreferenced(context.Background(), storePath))

	assert.True(t, counter.getHeldCleanupLock, "the zero-count decision must share the CHG-018 cleanup mutex")
	assert.NoFileExists(t, storePath)
	_, err = service.Store().ReadStore(storePath)
	assert.ErrorIs(t, err, os.ErrNotExist)
}
