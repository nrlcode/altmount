package importer

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	importqueue "github.com/javi11/altmount/internal/importer/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chg020BackgroundExitBarrier struct {
	done chan struct{}
	once sync.Once
}

func (h *chg020BackgroundExitBarrier) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *chg020BackgroundExitBarrier) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "Queue item not found for background processing" {
		h.once.Do(func() { close(h.done) })
	}
	return nil
}

func (h *chg020BackgroundExitBarrier) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *chg020BackgroundExitBarrier) WithGroup(string) slog.Handler {
	return h
}

func TestFACORECHG020ServiceExecuteItemPropagatesManualAdmissionFailure(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	barrier := &chg020BackgroundExitBarrier{done: make(chan struct{})}
	env.service.log = slog.New(barrier)

	err := env.service.ExecuteItem(context.Background(), 1<<62)
	if err == nil {
		// The unchanged implementation launches detached work. Synchronize with its
		// terminal missing-row path so the red proof does not leave a goroutine behind.
		select {
		case <-barrier.done:
		case <-time.After(5 * time.Second):
			t.Fatal("detached manual execution did not reach its terminal missing-row path")
		}
	}

	require.Error(t, err, "manual admission failure must be returned to the caller")
}

func TestFACORECHG020ServiceCancelProcessingPropagatesMissingRuntimeOwner(t *testing.T) {
	env := newFbaseAdmissionEnv(t)

	require.Error(t, env.service.CancelProcessing(1<<62),
		"a missing manager-owned cancellation handle must not be reported as success")
}

type chg020ServiceQueueProcessor struct {
	process func(context.Context, *database.ImportQueueItem) (string, error)
}

func (p *chg020ServiceQueueProcessor) ProcessItem(
	ctx context.Context,
	item *database.ImportQueueItem,
) (string, error) {
	return p.process(ctx, item)
}

func (*chg020ServiceQueueProcessor) HandleSuccess(
	context.Context,
	*database.ImportQueueItem,
	string,
) error {
	return nil
}

func (*chg020ServiceQueueProcessor) HandleFailure(
	context.Context,
	*database.ImportQueueItem,
	error,
) {
}

func TestFACORECHG020ServiceUsesManagerForActiveManualExecution(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	finished := make(chan struct{})
	processor := &chg020ServiceQueueProcessor{process: func(
		ctx context.Context,
		_ *database.ImportQueueItem,
	) (string, error) {
		close(started)
		defer close(finished)
		<-ctx.Done()
		close(cancelled)
		return "", ctx.Err()
	}}
	env.service.queueManager = importqueue.NewManager(importqueue.ManagerConfig{
		Workers:      1,
		ConfigGetter: env.service.configGetter,
	}, env.database.Repository, processor, nil)

	item := &database.ImportQueueItem{
		NzbPath:    "facore-chg020-service-owner.nzb",
		Priority:   database.QueuePriorityNormal,
		Status:     database.QueueStatusPending,
		MaxRetries: 3,
	}
	require.NoError(t, env.database.Repository.AddToQueue(context.Background(), item))

	executionCtx, stopLegacyExecution := context.WithCancel(context.Background())
	defer stopLegacyExecution()
	require.NoError(t, env.service.ExecuteItem(executionCtx, item.ID))

	managerStarted := false
	select {
	case <-started:
		managerStarted = true
	case <-time.After(2 * time.Second):
	}
	if managerStarted {
		require.NoError(t, env.service.CancelProcessing(item.ID))
		select {
		case <-cancelled:
		case <-time.After(2 * time.Second):
			t.Error("Service cancellation did not reach the manager-owned execution context")
		}
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("manager-owned execution did not finish after cancellation")
		}
	} else {
		// The defective implementation launched its separate Service-owned path.
		// Cancel its caller-derived context and wait for durable exit before failing.
		stopLegacyExecution()
		require.Eventually(t, func() bool {
			stored, err := env.database.Repository.GetQueueItem(context.Background(), item.ID)
			return err == nil && stored != nil && stored.Status != database.QueueStatusProcessing
		}, 2*time.Second, 10*time.Millisecond)
	}

	assert.True(t, managerStarted,
		"Service.ExecuteItem must delegate active work to the manager that public cancellation reads")
}
