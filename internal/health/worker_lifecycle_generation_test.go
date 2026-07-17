package health

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldStatClient deliberately withholds transport completion even after its
// caller is cancelled. It models a dependency that takes time to unwind and
// lets the tests observe whether worker lifecycle ownership is joined rather
// than merely signalled.
type heldStatClient struct {
	*fakepool.Client

	mu          sync.Mutex
	started     []chan struct{}
	cancelled   []chan struct{}
	release     []chan struct{}
	releaseOnce []sync.Once
	calls       int
}

func newHeldStatClient(callCount int) *heldStatClient {
	client := &heldStatClient{
		Client:      fakepool.New(),
		started:     make([]chan struct{}, callCount),
		cancelled:   make([]chan struct{}, callCount),
		release:     make([]chan struct{}, callCount),
		releaseOnce: make([]sync.Once, callCount),
	}
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})
	for i := 0; i < callCount; i++ {
		client.started[i] = make(chan struct{})
		client.cancelled[i] = make(chan struct{})
		client.release[i] = make(chan struct{})
	}
	return client
}

func (c *heldStatClient) releaseCall(call int) {
	c.releaseOnce[call].Do(func() { close(c.release[call]) })
}

func (c *heldStatClient) releaseAll() {
	for call := range c.release {
		c.releaseCall(call)
	}
}

func (c *heldStatClient) StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	c.mu.Lock()
	call := c.calls
	c.calls++
	if call >= len(c.started) {
		c.mu.Unlock()
		out := make(chan nntppool.StatManyResult, 1)
		out <- nntppool.StatManyResult{Err: assert.AnError}
		close(out)
		return out
	}
	started := c.started[call]
	cancelled := c.cancelled[call]
	release := c.release[call]
	close(started)
	c.mu.Unlock()

	out := make(chan nntppool.StatManyResult, len(messageIDs))
	go func() {
		defer close(out)
		select {
		case <-ctx.Done():
			close(cancelled)
			<-release
		case <-release:
		}
		for result := range c.Client.StatMany(ctx, messageIDs, opts) {
			out <- result
		}
	}()
	return out
}

func TestHealthWorkerStartFailsClosedWhenOwnershipResetFails(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	insertFileHealth(t, env.db, "complete/start-reset.mkv", "/library/start-reset.mkv", 0, 3)
	_, err := env.db.Exec(`
		UPDATE file_health SET status = 'checking' WHERE file_path = 'complete/start-reset.mkv';
		CREATE TRIGGER fail_health_start_reset
		BEFORE UPDATE ON file_health
		WHEN OLD.file_path = 'complete/start-reset.mkv'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic startup ownership reset failure');
		END;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		if env.hw.IsRunning() {
			_ = env.hw.Stop(context.Background())
		}
	})

	err = env.hw.Start(context.Background())
	require.Error(t, err, "the worker must not admit work after durable ownership reset fails")
	assert.False(t, env.hw.IsRunning())
	assert.Equal(t, WorkerStatusStopped, env.hw.GetStats().Status)

	_, err = env.db.Exec(`DROP TRIGGER fail_health_start_reset`)
	require.NoError(t, err)
	require.NoError(t, env.hw.Start(context.Background()),
		"a failed startup must leave the same worker instance retryable once persistence recovers")
	assert.True(t, env.hw.IsRunning())
	require.NoError(t, env.hw.Stop(context.Background()))
}

func TestBackgroundCheckRegistersCancellationBeforeReturning(t *testing.T) {
	client := newHeldStatClient(1)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	t.Cleanup(func() {
		client.releaseAll()
		if fixture.env.hw.IsRunning() {
			_ = fixture.env.hw.Stop(context.Background())
		}
	})

	// This is intentionally an outward postcondition rather than a lock on the
	// worker's private registry: once successful admission returns, callers must
	// be able to observe and cancel that exact execution immediately. A legacy
	// return-before-registration implementation can race to satisfy this check,
	// so the held-transport Cancel test below provides the deterministic join
	// proof without prescribing the registry's representation.
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	require.True(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"a successful admission must publish its cancellation handle before returning")

	client.releaseCall(0)
	require.Eventually(t, func() bool {
		return !fixture.env.hw.IsCheckActive(fixture.filePath)
	}, 2*time.Second, time.Millisecond)
}

func TestCancelHealthCheckJoinsExactOwnerAndPreservesReplacement(t *testing.T) {
	client := newHeldStatClient(1)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	t.Cleanup(func() {
		client.releaseAll()
		if fixture.env.hw.IsRunning() {
			_ = fixture.env.hw.Stop(context.Background())
		}
	})
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	select {
	case <-client.started[0]:
	case <-time.After(2 * time.Second):
		t.Fatal("background health check did not reach held transport")
	}
	require.True(t, fixture.env.hw.IsCheckActive(fixture.filePath))

	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'checking', metadata = '{"revision":"replacement"}',
		    health_claim_token = 'replacement-owner'
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)

	cancelDone := make(chan error, 1)
	go func() {
		cancelDone <- fixture.env.hw.CancelHealthCheck(context.Background(), fixture.filePath)
	}()
	select {
	case <-client.cancelled[0]:
	case <-time.After(2 * time.Second):
		t.Fatal("CancelHealthCheck did not cancel the held transport")
	}
	select {
	case err := <-cancelDone:
		t.Fatalf("CancelHealthCheck returned before transport ownership joined: %v", err)
	default:
	}
	assert.True(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"the exact execution must remain registered until its transport and deferred cleanup join")

	client.releaseCall(0)
	select {
	case err := <-cancelDone:
		require.NoError(t, err,
			"cancelling the exact admitted owner should succeed even when that owner discovers a replacement")
	case <-time.After(2 * time.Second):
		t.Fatal("CancelHealthCheck did not return after held transport was released")
	}
	assert.False(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"CancelHealthCheck must synchronously join and unregister the exact execution")

	var status, metadata string
	var token sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, metadata, health_claim_token
		FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&status, &metadata, &token))
	assert.Equal(t, "checking", status, "cancelling an old check must not re-arm a same-path replacement")
	assert.JSONEq(t, `{"revision":"replacement"}`, metadata)
	require.True(t, token.Valid)
	assert.Equal(t, "replacement-owner", token.String)
}

func TestOldCheckCompletionCannotEraseNewActiveRegistration(t *testing.T) {
	client := newHeldStatClient(2)
	t.Cleanup(client.releaseAll)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))

	firstDone := make(chan error, 1)
	go func() { firstDone <- fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath) }()
	select {
	case <-client.started[0]:
	case <-time.After(2 * time.Second):
		t.Fatal("first health check did not reach transport")
	}

	// Install a new same-path generation while the old transport is still
	// unwinding, then let a second check acquire that generation.
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'pending', metadata = '{"revision":"new-generation"}',
		    health_claim_token = NULL
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)

	secondDone := make(chan error, 1)
	go func() { secondDone <- fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath) }()
	select {
	case <-client.started[1]:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement health check did not reach transport")
	}
	require.True(t, fixture.env.hw.IsCheckActive(fixture.filePath))

	client.releaseCall(0)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("old health check did not finish")
	}

	assert.True(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"an old generation's deferred cleanup must not erase the replacement registration")
	select {
	case err := <-secondDone:
		t.Fatalf("replacement check completed before it was released: %v", err)
	default:
	}
	client.releaseCall(1)
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement health check did not finish")
	}
}

func TestStopHonorsDeadlineAndOnlyRestartsAfterGenerationJoins(t *testing.T) {
	client := newHeldStatClient(1)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	t.Cleanup(func() {
		client.releaseAll()
		if fixture.env.hw.IsRunning() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = fixture.env.hw.Stop(stopCtx)
		}
	})
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	select {
	case <-client.started[0]:
	case <-time.After(2 * time.Second):
		t.Fatal("background health check did not reach transport")
	}

	stopCtx, cancelStop := context.WithCancel(context.Background())
	stopDone := make(chan error, 1)
	go func() { stopDone <- fixture.env.hw.Stop(stopCtx) }()
	select {
	case <-client.cancelled[0]:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel the held generation transport")
	}
	cancelStop()
	select {
	case stopErr := <-stopDone:
		assert.ErrorIs(t, stopErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not honor its caller cancellation while transport remained held")
	}
	assert.Equal(t, WorkerStatusStopping, fixture.env.hw.GetStats().Status)
	assert.Error(t, fixture.env.hw.Start(context.Background()),
		"restart must remain closed until the cancelled generation has actually joined")

	client.releaseCall(0)
	require.Eventually(t, func() bool {
		return fixture.env.hw.GetStats().Status == WorkerStatusStopped
	}, 2*time.Second, time.Millisecond)

	require.NoError(t, fixture.env.hw.Start(context.Background()),
		"a fully joined generation must permit a clean restart")
	require.NoError(t, fixture.env.hw.Stop(context.Background()))
}
