package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/arrs/clients"
	"github.com/javi11/altmount/internal/arrs/data"
	"github.com/javi11/altmount/internal/arrs/instances"
	"github.com/javi11/altmount/internal/arrs/model"
	"github.com/javi11/altmount/internal/config"
)

func TestFACOREF009TriggerFileRescanWaiterCancellationIsIndependent(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	fixture := newRescanCancellationFixture(t, release)
	leaderDone := make(chan error, 1)
	go func() {
		leaderDone <- fixture.manager.TriggerFileRescan(
			context.Background(), fixture.path, fixture.relativePath, &fixture.metadata,
		)
	}()
	waitForBlockedARRRequest(t, fixture.transport)

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterInvoked := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterInvoked)
		waiterDone <- fixture.manager.TriggerFileRescan(
			waiterCtx, fixture.path, fixture.relativePath, &fixture.metadata,
		)
	}()
	<-waiterInvoked
	cancelWaiter()

	waiterErr, waiterReturned := awaitRescanCall(waiterDone, time.Second)
	if !waiterReturned {
		t.Error("canceled waiter did not return while the shared leader request remained blocked")
	} else if !errors.Is(waiterErr, context.Canceled) {
		t.Errorf("canceled waiter error = %v, want context.Canceled", waiterErr)
	}

	leaderErr, leaderReturned := pollRescanCall(leaderDone)
	if leaderReturned {
		t.Errorf("shared leader returned before its ARR request was released: %v", leaderErr)
	}
	if got := fixture.transport.calls.Load(); got != 1 {
		t.Errorf("ARR calls while same-key leader was blocked = %d, want 1", got)
	}

	unblock()
	if !leaderReturned {
		if _, ok := awaitRescanCall(leaderDone, 2*time.Second); !ok {
			t.Error("shared leader did not settle after its ARR request was released")
		}
	}
	if !waiterReturned {
		if _, ok := awaitRescanCall(waiterDone, 2*time.Second); !ok {
			t.Error("waiter did not settle after cleanup release")
		}
	}
}

func TestFACOREF009TriggerFileRescanLeaderCancellationDoesNotCancelWaiter(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	fixture := newRescanCancellationFixture(t, release)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		leaderDone <- fixture.manager.TriggerFileRescan(
			leaderCtx, fixture.path, fixture.relativePath, &fixture.metadata,
		)
	}()
	waitForBlockedARRRequest(t, fixture.transport)

	waiterInvoked := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterInvoked)
		waiterDone <- fixture.manager.TriggerFileRescan(
			context.Background(), fixture.path, fixture.relativePath, &fixture.metadata,
		)
	}()
	<-waiterInvoked
	cancelLeader()

	leaderErr, leaderReturned := awaitRescanCall(leaderDone, time.Second)
	if !leaderReturned {
		t.Error("canceled leader did not return while its shared ARR request remained available to the waiter")
	} else if !errors.Is(leaderErr, context.Canceled) {
		t.Errorf("canceled leader error = %v, want context.Canceled", leaderErr)
	}

	waiterErr, waiterReturned := pollRescanCall(waiterDone)
	if waiterReturned {
		t.Errorf("waiter returned before the independent shared request was released: %v", waiterErr)
	}
	if got := fixture.transport.calls.Load(); got != 1 {
		t.Errorf("ARR calls while same-key waiter was blocked = %d, want 1", got)
	}

	unblock()
	if !waiterReturned {
		var ok bool
		waiterErr, ok = awaitRescanCall(waiterDone, 2*time.Second)
		if !ok {
			t.Error("waiter did not settle after the shared ARR request was released")
		} else if errors.Is(waiterErr, context.Canceled) {
			t.Errorf("waiter inherited leader cancellation: %v", waiterErr)
		}
	}
	if !leaderReturned {
		if _, ok := awaitRescanCall(leaderDone, 2*time.Second); !ok {
			t.Error("leader did not settle after cleanup release")
		}
	}
}

type rescanCancellationFixture struct {
	manager      *Manager
	transport    *blockingARRTransport
	path         string
	relativePath string
	metadata     string
}

func newRescanCancellationFixture(t *testing.T, release <-chan struct{}) rescanCancellationFixture {
	t.Helper()

	enabled := true
	cfg := &config.Config{
		Arrs: config.ArrsConfig{
			RadarrInstances: []config.ArrsInstanceConfig{
				{
					Name:    "radarr-cancellation-test",
					URL:     "http://radarr.invalid",
					APIKey:  "test-api-key",
					Enabled: &enabled,
				},
			},
		},
	}
	configGetter := func() *config.Config { return cfg }
	transport := &blockingARRTransport{
		started: make(chan struct{}),
		release: release,
	}
	clientManager := clients.NewManager(&http.Client{Transport: transport})
	instanceManager := instances.NewManager(configGetter, nil)
	manager := NewManager(
		configGetter,
		instanceManager,
		clientManager,
		data.NewManager(),
		nil,
		nil,
	)

	metadataBytes, err := json.Marshal(model.WebhookMetadata{
		InstanceName: "radarr-cancellation-test",
	})
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}

	return rescanCancellationFixture{
		manager:      manager,
		transport:    transport,
		path:         "/library/Test Movie/Test Movie.mkv",
		relativePath: "Test Movie/Test Movie.mkv",
		metadata:     string(metadataBytes),
	}
}

type blockingARRTransport struct {
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (t *blockingARRTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	t.once.Do(func() { close(t.started) })
	select {
	case <-t.release:
		return nil, errors.New("synthetic ARR request failure after release")
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

func waitForBlockedARRRequest(t *testing.T, transport *blockingARRTransport) {
	t.Helper()
	select {
	case <-transport.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ARR request did not reach blocking transport")
	}
}

func awaitRescanCall(done <-chan error, timeout time.Duration) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func pollRescanCall(done <-chan error) (error, bool) {
	select {
	case err := <-done:
		return err, true
	default:
		return nil, false
	}
}
