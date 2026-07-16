package health

import (
	"context"
	"database/sql"
	"runtime"
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

	mu      sync.Mutex
	started []chan struct{}
	release []chan struct{}
	calls   int
}

func newHeldStatClient(callCount int) *heldStatClient {
	client := &heldStatClient{
		Client:  fakepool.New(),
		started: make([]chan struct{}, callCount),
		release: make([]chan struct{}, callCount),
	}
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})
	for i := 0; i < callCount; i++ {
		client.started[i] = make(chan struct{})
		client.release[i] = make(chan struct{})
	}
	return client
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
	release := c.release[call]
	close(started)
	c.mu.Unlock()

	out := make(chan nntppool.StatManyResult, len(messageIDs))
	go func() {
		defer close(out)
		<-release
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
}

func TestBackgroundCheckRegistersCancellationBeforeReturning(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	client := newHeldStatClient(1)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	t.Cleanup(func() {
		select {
		case <-client.release[0]:
		default:
			close(client.release[0])
		}
		if fixture.env.hw.IsRunning() {
			_ = fixture.env.hw.Stop(context.Background())
		}
	})

	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	assert.True(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"a successful admission must publish its cancellation handle before returning")
}

func TestCancelHealthCheckJoinsExactOwnerAndPreservesReplacement(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: time.Hour})
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	t.Cleanup(func() {
		if fixture.env.hw.IsRunning() {
			_ = fixture.env.hw.Stop(context.Background())
		}
	})
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	require.Eventually(t, func() bool {
		return client.InFlight() == 1 && fixture.env.hw.IsCheckActive(fixture.filePath)
	}, time.Second, time.Millisecond)

	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'checking', metadata = '{"revision":"replacement"}',
		    health_claim_token = 'replacement-owner'
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)

	cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = fixture.env.hw.CancelHealthCheck(cancelCtx, fixture.filePath)
	require.Eventually(t, func() bool { return !fixture.env.hw.IsCheckActive(fixture.filePath) }, time.Second, time.Millisecond)

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
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))

	firstDone := make(chan error, 1)
	go func() { firstDone <- fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath) }()
	select {
	case <-client.started[0]:
	case <-time.After(time.Second):
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
	case <-time.After(time.Second):
		close(client.release[0])
		t.Fatal("replacement health check did not reach transport")
	}
	require.True(t, fixture.env.hw.IsCheckActive(fixture.filePath))

	close(client.release[0])
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		close(client.release[1])
		t.Fatal("old health check did not finish")
	}

	assert.True(t, fixture.env.hw.IsCheckActive(fixture.filePath),
		"an old generation's deferred cleanup must not erase the replacement registration")
	select {
	case err := <-secondDone:
		t.Fatalf("replacement check completed before it was released: %v", err)
	default:
	}
	close(client.release[1])
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("replacement health check did not finish")
	}
}

func TestStopHonorsDeadlineAndOnlyRestartsAfterGenerationJoins(t *testing.T) {
	client := newHeldStatClient(1)
	fixture := newDestructiveClaimFixture(t, pool.NntpClient(client))
	require.NoError(t, fixture.env.hw.Start(context.Background()))
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	select {
	case <-client.started[0]:
	case <-time.After(time.Second):
		t.Fatal("background health check did not reach transport")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelStop()
	stopDone := make(chan error, 1)
	go func() { stopDone <- fixture.env.hw.Stop(stopCtx) }()

	var stopErr error
	returnedByDeadline := false
	select {
	case stopErr = <-stopDone:
		returnedByDeadline = true
	case <-time.After(250 * time.Millisecond):
	}
	assert.True(t, returnedByDeadline, "Stop must be bounded by its caller context")
	if returnedByDeadline {
		assert.ErrorIs(t, stopErr, context.DeadlineExceeded)
	}
	assert.Equal(t, WorkerStatusStopping, fixture.env.hw.GetStats().Status)
	assert.Error(t, fixture.env.hw.Start(context.Background()),
		"restart must remain closed until the cancelled generation has actually joined")

	close(client.release[0])
	if !returnedByDeadline {
		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatal("legacy unbounded Stop did not finish after transport release")
		}
	}
	require.Eventually(t, func() bool {
		return fixture.env.hw.GetStats().Status == WorkerStatusStopped
	}, time.Second, time.Millisecond)

	require.NoError(t, fixture.env.hw.Start(context.Background()),
		"a fully joined generation must permit a clean restart")
	require.NoError(t, fixture.env.hw.Stop(context.Background()))
}
