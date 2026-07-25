package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFACORECHG021Finalization = errors.New("facore chg021 success finalization failed")

func TestFACORECHG021AutomaticFinalizationErrorEntersRecoverableHold(t *testing.T) {
	finalizationCtx, cancelFinalization := context.WithCancel(context.Background())
	t.Cleanup(cancelFinalization)
	var failureCalls atomic.Int32
	processor := &facoreCHG020Processor{
		process: func(context.Context, *database.ImportQueueItem) (string, error) {
			return "automatic-result", nil
		},
		success: func(context.Context, *database.ImportQueueItem, string) error {
			cancelFinalization()
			return errFACORECHG021Finalization
		},
		failure: func(context.Context, *database.ImportQueueItem, error) {
			failureCalls.Add(1)
		},
	}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "automatic-finalization-error.nzb")

	done := make(chan struct{})
	go func() {
		manager.processNextItem(finalizationCtx, 1)
		close(done)
	}()
	facoreCHG020Wait(t, done, "automatic finalization did not return")

	assertFACORECHG021RecoverableHold(t, manager, repo, item, failureCalls.Load())
}

func TestFACORECHG021ManualFinalizationErrorEntersRecoverableHold(t *testing.T) {
	finalizerCalled := make(chan struct{})
	var failureCalls atomic.Int32
	processor := &facoreCHG020Processor{
		process: func(context.Context, *database.ImportQueueItem) (string, error) {
			return "manual-result", nil
		},
		success: func(context.Context, *database.ImportQueueItem, string) error {
			close(finalizerCalled)
			return errFACORECHG021Finalization
		},
		failure: func(context.Context, *database.ImportQueueItem, error) {
			failureCalls.Add(1)
		},
	}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "manual-finalization-error.nzb")

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID),
		"manual execution returns admission, while finalization remains asynchronous")
	facoreCHG020Wait(t, finalizerCalled, "manual success finalizer was not called")
	require.Eventually(t, func() bool {
		manager.cancelMu.RLock()
		defer manager.cancelMu.RUnlock()
		_, present := manager.cancelFuncs[item.ID]
		return !present
	}, facoreCHG020BarrierTimeout, time.Millisecond,
		"manual finalization owner was not released")

	assertFACORECHG021RecoverableHold(t, manager, repo, item, failureCalls.Load())
}

func assertFACORECHG021RecoverableHold(
	t *testing.T,
	manager *Manager,
	repo *database.QueueRepository,
	item *database.ImportQueueItem,
	failureCalls int32,
) {
	t.Helper()

	stored, err := repo.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, database.QueueStatusFailed, stored.Status,
		"a success-finalization error must not leave an ownerless processing row")
	require.NotNil(t, stored.ErrorMessage)
	assert.ErrorContains(t, errors.New(*stored.ErrorMessage), errFACORECHG021Finalization.Error())
	assert.Equal(t, item.RetryCount, stored.RetryCount,
		"holding partial success for manual recovery must not consume an automatic retry")
	assert.Zero(t, failureCalls,
		"success-finalization errors must not enter processing-failure cleanup or SAB fallback")
	assert.ErrorIs(t, manager.CancelProcessing(item.ID), ErrQueueItemNotProcessing,
		"the runtime owner must be released after the durable recovery attempt")
}
