package usenet

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type facoreCHG005OpenStatClient struct {
	*fakepool.Client
	source <-chan nntppool.StatManyResult
	called chan struct{}
	once   sync.Once
}

func (c *facoreCHG005OpenStatClient) StatMany(
	context.Context,
	[]string,
	nntppool.StatManyOptions,
) <-chan nntppool.StatManyResult {
	c.once.Do(func() { close(c.called) })
	return c.source
}

func facoreCHG005AwaitSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestFACORECHG005ValidationCancellationInterruptsOpenStatMany(t *testing.T) {
	source := make(chan nntppool.StatManyResult)
	called := make(chan struct{})
	client := &facoreCHG005OpenStatClient{
		Client: fakepool.New(),
		source: source,
		called: called,
	}
	mgr := &validationTestPoolManager{client: client}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type callResult struct {
		results []ValidationResult
		err     error
	}
	done := make(chan callResult, 1)
	go func() {
		results, err := ValidateSegmentAvailabilityBatch(
			ctx,
			[][]string{{"returned@test", "omitted@test"}},
			mgr,
			2,
			time.Hour,
		)
		done <- callResult{results: results, err: err}
	}()

	facoreCHG005AwaitSignal(t, called, "StatMany admission")
	delivered := make(chan struct{})
	go func() {
		source <- nntppool.StatManyResult{
			MessageID: "returned@test",
			Result:    &nntppool.StatResult{MessageID: "returned@test"},
		}
		close(delivered)
	}()
	facoreCHG005AwaitSignal(t, delivered, "first STAT result consumption")
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
		"validation must select on context cancellation instead of waiting for StatMany to close")
	require.Error(t, got.err)
	var incomplete *IncompleteError
	require.ErrorAs(t, got.err, &incomplete)
	assert.ErrorIs(t, got.err, context.Canceled)
	assert.Equal(t, 2, incomplete.Expected)
	assert.Equal(t, 1, incomplete.Completed)
	assert.True(t, incomplete.Global)
	require.Len(t, got.results, 1)
	assert.Zero(t, got.results[0].MissingCount,
		"cancelled or omitted work must never become hard absence")
	assert.Equal(t, 1, got.results[0].TotalChecked)
	assert.Equal(t, 1, got.results[0].IncompleteCount)
}
