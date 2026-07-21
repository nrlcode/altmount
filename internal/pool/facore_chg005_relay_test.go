package pool

import (
	"context"
	"testing"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
)

func TestFACORECHG005StatManyOmissionRetainsLeaseUntilSourceClose(t *testing.T) {
	source := make(chan nntppool.StatManyResult, 1)
	source <- nntppool.StatManyResult{
		MessageID: "returned@test",
		Result:    &nntppool.StatResult{MessageID: "returned@test"},
	}
	old := newFACOREGeneration("old", nntppool.ProviderStats{ProviderID: "shared"})
	old.statSource = source
	candidate := newFACOREGeneration("candidate", nntppool.ProviderStats{ProviderID: "shared"})
	m := facoreManager(t, nil, old, candidate)
	require.NoError(t, m.SetProviders(facoreProviders("shared")))
	facade, err := m.GetPool()
	require.NoError(t, err)

	out := facade.StatMany(
		context.Background(),
		[]string{"returned@test", "omitted@test"},
		nntppool.StatManyOptions{Concurrency: 1},
	)
	swapDone := make(chan error, 1)
	go func() { swapDone <- m.SetProviders(facoreProviders("shared")) }()
	facoreAwait(t, candidate.built, "candidate construction")
	facoreAwaitPaused(t, facade)
	facorePending(t, swapDone, "replacement before omitted-result source close")
	require.Zero(t, old.closeCalls.Load(), "old generation closed while StatMany source remained open")

	close(source)
	require.NoError(t, facoreAwait(t, swapDone, "replacement"))
	var results []nntppool.StatManyResult
	for result := range out {
		results = append(results, result)
	}
	require.Len(t, results, 1)
	require.Equal(t, "returned@test", results[0].MessageID)
}
