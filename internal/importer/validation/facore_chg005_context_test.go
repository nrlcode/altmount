package validation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type facoreCHG005OpenFastFailClient struct {
	*fakepool.Client
	source <-chan nntppool.StatManyResult
	called chan struct{}
	once   sync.Once
}

type facoreF026CancelOnHardAbsenceError struct {
	cancel    context.CancelFunc
	triggered bool
}

func (e *facoreF026CancelOnHardAbsenceError) Error() string {
	return "hard absence concurrent with cancellation"
}

func (e *facoreF026CancelOnHardAbsenceError) Is(target error) bool {
	if target != nntppool.ErrArticleNotFound {
		return false
	}
	e.triggered = true
	e.cancel()
	return true
}

func (c *facoreCHG005OpenFastFailClient) StatMany(
	context.Context,
	[]string,
	nntppool.StatManyOptions,
) <-chan nntppool.StatManyResult {
	c.once.Do(func() { close(c.called) })
	return c.source
}

func facoreCHG005AwaitFastFailSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestFACOREF026ReleaseProbeCancellationDominatesBufferedHardAbsence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	classificationErr := &facoreF026CancelOnHardAbsenceError{cancel: cancel}
	client := &scriptedFastFailClient{
		Client: fakepool.New(),
		results: []nntppool.StatManyResult{{
			MessageID: "cancelled-hard-absence-0",
			Err:       classificationErr,
		}},
	}

	missing, err := FastFailReleaseProbe(
		ctx,
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("cancelled-hard-absence", 1)}},
		fastFailPoolManager{client: client},
		100,
		1,
		time.Hour,
	)

	require.True(t, classificationErr.triggered,
		"the result must cancel during hard-absence classification")
	assert.False(t, missing, "cancelled classification must never become hard absence")
	require.Error(t, err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, err, &incomplete)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, incomplete.Expected)
	assert.Zero(t, incomplete.Completed)
}

func TestFACOREF026FileProbeCancellationDominatesBufferedHardAbsence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	classificationErr := &facoreF026CancelOnHardAbsenceError{cancel: cancel}
	client := &scriptedFastFailClient{
		Client: fakepool.New(),
		results: []nntppool.StatManyResult{{
			MessageID: "cancelled-hard-absence-0",
			Err:       classificationErr,
		}},
	}

	results, err := FastFailCheckFiles(
		ctx,
		[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("cancelled-hard-absence", 1)}},
		fastFailPoolManager{client: client},
		100,
		1,
		time.Hour,
		nil,
	)

	require.True(t, classificationErr.triggered,
		"the result must cancel during hard-absence classification")
	require.Error(t, err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, err, &incomplete)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, incomplete.Expected)
	assert.Zero(t, incomplete.Completed)
	require.Len(t, results, 1)
	assert.False(t, results[0].Broken,
		"cancelled classification must never mark a file broken")
	assert.Empty(t, results[0].MissingSegmentIDs)
}

func TestFACORECHG005ReleaseProbeCancellationInterruptsSilentStatMany(t *testing.T) {
	source := make(chan nntppool.StatManyResult)
	called := make(chan struct{})
	client := &facoreCHG005OpenFastFailClient{
		Client: fakepool.New(),
		source: source,
		called: called,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type callResult struct {
		missing bool
		err     error
	}
	done := make(chan callResult, 1)
	go func() {
		missing, err := FastFailReleaseProbe(
			ctx,
			[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("silent", 1)}},
			fastFailPoolManager{client: client},
			100,
			1,
			time.Hour,
		)
		done <- callResult{missing: missing, err: err}
	}()

	facoreCHG005AwaitFastFailSignal(t, called, "release-probe StatMany admission")
	cancel()

	var got callResult
	returnedBeforeClose := false
	select {
	case got = <-done:
		returnedBeforeClose = true
	case <-time.After(250 * time.Millisecond):
	}
	close(source)
	if !returnedBeforeClose {
		got = <-done
	}

	require.True(t, returnedBeforeClose,
		"release probe must select on context cancellation instead of waiting for StatMany to close")
	assert.False(t, got.missing, "cancelled work must never become hard absence")
	require.Error(t, got.err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, got.err, &incomplete)
	assert.ErrorIs(t, got.err, context.Canceled)
	assert.Equal(t, 1, incomplete.Expected)
	assert.Zero(t, incomplete.Completed)
}

func TestFACORECHG005ReleaseProbeTimeoutInterruptsSilentStatMany(t *testing.T) {
	source := make(chan nntppool.StatManyResult)
	called := make(chan struct{})
	client := &facoreCHG005OpenFastFailClient{
		Client: fakepool.New(),
		source: source,
		called: called,
	}

	type callResult struct {
		missing bool
		err     error
	}
	done := make(chan callResult, 1)
	go func() {
		missing, err := FastFailReleaseProbe(
			context.Background(),
			[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("timeout", 1)}},
			fastFailPoolManager{client: client},
			100,
			1,
			20*time.Millisecond,
		)
		done <- callResult{missing: missing, err: err}
	}()

	facoreCHG005AwaitFastFailSignal(t, called, "release-probe StatMany admission")
	var got callResult
	returnedBeforeClose := false
	select {
	case got = <-done:
		returnedBeforeClose = true
	case <-time.After(250 * time.Millisecond):
	}
	close(source)
	if !returnedBeforeClose {
		got = <-done
	}

	require.True(t, returnedBeforeClose,
		"configured STAT timeout must interrupt a silent open result channel")
	assert.False(t, got.missing, "timed-out work must never become hard absence")
	require.Error(t, got.err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, got.err, &incomplete)
	assert.ErrorIs(t, got.err, context.DeadlineExceeded)
	assert.Equal(t, 1, incomplete.Expected)
	assert.Zero(t, incomplete.Completed)
}

func TestFACORECHG005FileProbeCancellationInterruptsOpenStatMany(t *testing.T) {
	source := make(chan nntppool.StatManyResult)
	called := make(chan struct{})
	client := &facoreCHG005OpenFastFailClient{
		Client: fakepool.New(),
		source: source,
		called: called,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type callResult struct {
		results []FastFailFileResult
		err     error
	}
	done := make(chan callResult, 1)
	go func() {
		results, err := FastFailCheckFiles(
			ctx,
			[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("partial", 2)}},
			fastFailPoolManager{client: client},
			100,
			2,
			time.Hour,
			nil,
		)
		done <- callResult{results: results, err: err}
	}()

	facoreCHG005AwaitFastFailSignal(t, called, "per-file StatMany admission")
	delivered := make(chan struct{})
	go func() {
		source <- nntppool.StatManyResult{
			MessageID: "partial-0",
			Result:    &nntppool.StatResult{MessageID: "partial-0"},
		}
		close(delivered)
	}()
	facoreCHG005AwaitFastFailSignal(t, delivered, "first per-file STAT result consumption")
	cancel()

	var got callResult
	returnedBeforeClose := false
	select {
	case got = <-done:
		returnedBeforeClose = true
	case <-time.After(250 * time.Millisecond):
	}
	close(source)
	if !returnedBeforeClose {
		got = <-done
	}

	require.True(t, returnedBeforeClose,
		"per-file probe must select on context cancellation instead of waiting for StatMany to close")
	require.Error(t, got.err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, got.err, &incomplete)
	assert.ErrorIs(t, got.err, context.Canceled)
	assert.Equal(t, 2, incomplete.Expected)
	assert.Equal(t, 1, incomplete.Completed)
	require.Len(t, got.results, 1)
	assert.False(t, got.results[0].Broken,
		"cancelled or omitted work must never mark a file broken")
	assert.Empty(t, got.results[0].MissingSegmentIDs)
}

func TestFACORECHG005FileProbeTimeoutInterruptsSilentStatMany(t *testing.T) {
	source := make(chan nntppool.StatManyResult)
	called := make(chan struct{})
	client := &facoreCHG005OpenFastFailClient{
		Client: fakepool.New(),
		source: source,
		called: called,
	}

	type callResult struct {
		results []FastFailFileResult
		err     error
	}
	done := make(chan callResult, 1)
	go func() {
		results, err := FastFailCheckFiles(
			context.Background(),
			[]FastFailFile{{Filename: "movie.mkv", Segments: makeTestSegments("timeout", 1)}},
			fastFailPoolManager{client: client},
			100,
			1,
			20*time.Millisecond,
			nil,
		)
		done <- callResult{results: results, err: err}
	}()

	facoreCHG005AwaitFastFailSignal(t, called, "per-file StatMany admission")
	var got callResult
	returnedBeforeClose := false
	select {
	case got = <-done:
		returnedBeforeClose = true
	case <-time.After(250 * time.Millisecond):
	}
	close(source)
	if !returnedBeforeClose {
		got = <-done
	}

	require.True(t, returnedBeforeClose,
		"configured STAT timeout must interrupt a silent open per-file result channel")
	require.Error(t, got.err)
	var incomplete *usenet.IncompleteError
	require.ErrorAs(t, got.err, &incomplete)
	assert.ErrorIs(t, got.err, context.DeadlineExceeded)
	assert.Equal(t, 1, incomplete.Expected)
	assert.Zero(t, incomplete.Completed)
	require.Len(t, got.results, 1)
	assert.False(t, got.results[0].Broken,
		"timed-out or omitted work must never mark a file broken")
	assert.Empty(t, got.results[0].MissingSegmentIDs)
}
