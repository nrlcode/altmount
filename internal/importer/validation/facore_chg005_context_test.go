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
