package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer/admissionctx"
	"github.com/javi11/altmount/internal/importer/validation"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/progress"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/testsupport/nzbbuild"
	"github.com/javi11/nntppool/v4"
	"github.com/javi11/nzbparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5AdmissionStub struct {
	result      validation.DurableFinalLayoutValidationResult
	err         error
	calls       int
	queueID     int64
	path        string
	provenance  validation.FinalLayoutProvenanceKind
	activateErr error
	activations []string
}

type pr5SelectiveAdmissionTransport struct{}

func (pr5SelectiveAdmissionTransport) TargetedSTAT(
	_ context.Context,
	_ validation.TargetedSTATProvider,
	requests []validation.TargetedSTATRequest,
) ([]validation.TargetedSTATObservation, error) {
	observations := make([]validation.TargetedSTATObservation, len(requests))
	for i, request := range requests {
		result := validation.TargetedSTATResult{
			Outcome:               nntppool.OutcomeSuccess,
			CompletionDisposition: validation.ImportCheckDispositionAttempted,
		}
		if request.MessageID == "fixture-missing" {
			result.Outcome = nntppool.OutcomeHardArticleAbsence
			result.ResponseCode = 430
		}
		observations[i] = validation.TargetedSTATObservation{
			Position: request.Position,
			Result:   result,
		}
	}
	return observations, nil
}

func (s *pr5AdmissionStub) ValidateFinalLayout(
	_ context.Context,
	queueID int64,
	path string,
	_ *metapb.FileMetadata,
	provenance validation.FinalLayoutProvenance,
) (validation.DurableFinalLayoutValidationResult, error) {
	s.calls++
	s.queueID = queueID
	s.path = path
	s.provenance = provenance.Kind
	result := s.result
	if result.FileRevisionID == "" &&
		(result.Status == validation.ImportAdmissionAccept || result.Status == validation.ImportAdmissionHealthPending) {
		result.FileRevisionID = "fixture-revision"
	}
	return result, s.err
}

func (s *pr5AdmissionStub) ActivateFileRevision(_ context.Context, _ int64, revisionID string) error {
	s.activations = append(s.activations, revisionID)
	return s.activateErr
}

func preparePR5MetadataWrite(
	t *testing.T,
	gate *durableImportWriteValidator,
	ctx context.Context,
	path string,
	meta *metapb.FileMetadata,
) error {
	t.Helper()
	permit, err := gate.PrepareMetadataWrite(ctx, path, meta)
	if err != nil || permit == nil {
		return err
	}
	return permit.FinalizeMetadataWrite(ctx)
}

func pr5WiringMetadata() *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize: 100,
		SegmentData: []*metapb.SegmentData{{
			Id: "fixture-article", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}
}

func TestPR5MetadataAdmissionRunsOnlyForQueueFinalLayoutContext(t *testing.T) {
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept,
	}}
	gate := newDurableImportWriteValidator(stub)
	meta := pr5WiringMetadata()

	require.NoError(t, preparePR5MetadataWrite(t, gate, context.Background(), "library/background.mkv", meta))
	assert.Zero(t, stub.calls, "ordinary metadata writes must retain source compatibility")

	ctx := withDurableImportIntent(
		context.Background(), 41, validation.FinalLayoutProvenanceStandalone,
	)
	require.NoError(t, preparePR5MetadataWrite(t, gate, ctx, "library/movie.mkv", meta))
	assert.Equal(t, 1, stub.calls)
	assert.Equal(t, int64(41), stub.queueID)
	assert.Equal(t, "library/movie.mkv", stub.path)
	assert.Equal(t, validation.FinalLayoutProvenanceStandalone, stub.provenance)
}

func TestPR5CommittedImportActivationBroadcastsSanitizedHealthInvalidation(t *testing.T) {
	broadcaster := progress.NewProgressBroadcaster()
	t.Cleanup(func() { require.NoError(t, broadcaster.Close()) })
	_, events := broadcaster.Subscribe()
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept,
		RunID:  "fixture-run",
	}}
	gate := newDurableImportWriteValidator(stub)
	gate.health = broadcaster
	require.NoError(t, preparePR5MetadataWrite(
		t, gate,
		withDurableImportIntent(
			context.Background(), 41, validation.FinalLayoutProvenanceStandalone,
		),
		"library/movie.mkv", pr5WiringMetadata(),
	))

	for range 1 {
		select {
		case event := <-events:
			assert.Equal(t, 0, event.QueueID)
			assert.Equal(t, "health_changed", event.Status)
			assert.Empty(t, event.Stage)
			assert.Empty(t, event.StoragePath)
		case <-time.After(time.Second):
			t.Fatal("missing committed health invalidation")
		}
	}
}

func TestPR5DurableFinalLayoutRequirementClassificationIsConservative(t *testing.T) {
	files := []nzbparser.NzbFile{
		{Filename: "Episode.One.mkv", Bytes: 500 * 1024 * 1024},
		{Filename: "Episode.One.nfo", Bytes: 1024},
		{Filename: "Episode.One.sample.mkv", Bytes: 20 * 1024 * 1024},
		{Filename: "Release.part02.rar", Bytes: 100 * 1024 * 1024},
		{Filename: "Release.7z.002", Bytes: 100 * 1024 * 1024},
		{Filename: "96f94d4bcf6043e3941c25f19b91eb03", Bytes: 500 * 1024 * 1024},
		{Filename: "Episode.Two.mkv", Bytes: 500 * 1024 * 1024},
		{Filename: "Episode.One.vol00+01.par2", Bytes: 1024},
	}

	optional := durableFinalLayoutOptionalFileIndexes(files, []string{".mkv"}, true)
	assert.NotContains(t, optional, 0, "eligible standalone output is required")
	assert.Contains(t, optional, 1, "excluded auxiliary file is optional")
	assert.Contains(t, optional, 2, "filtered sample is optional")
	assert.NotContains(t, optional, 3, "every RAR volume is a required dependency")
	assert.NotContains(t, optional, 4, "every 7z volume is a required dependency")
	assert.NotContains(t, optional, 5, "ambiguous/obfuscated payloads fail closed")
	assert.NotContains(t, optional, 6, "season-pack playback output is required")
	assert.Contains(t, optional, 7, "PAR2 recovery data is not a playback dependency")
}

func TestPR5MetadataAdmissionProvenanceFailsClosedForComplexLayouts(t *testing.T) {
	for _, tt := range []struct {
		name string
		base validation.FinalLayoutProvenanceKind
		meta func() *metapb.FileMetadata
		want validation.FinalLayoutProvenanceKind
	}{
		{
			name: "archive member", base: validation.FinalLayoutProvenanceArchiveMember,
			meta: pr5WiringMetadata, want: validation.FinalLayoutProvenanceArchiveMember,
		},
		{
			name: "nested archive", base: validation.FinalLayoutProvenanceArchiveMember,
			meta: func() *metapb.FileMetadata {
				meta := pr5WiringMetadata()
				meta.NestedSources = []*metapb.NestedSegmentSource{{InnerLength: 100}}
				return meta
			},
			want: validation.FinalLayoutProvenanceNestedArchive,
		},
		{
			name: "ISO expansion", base: validation.FinalLayoutProvenanceISOExpansion,
			meta: pr5WiringMetadata, want: validation.FinalLayoutProvenanceISOExpansion,
		},
		{
			name: "virtual concatenation", base: validation.FinalLayoutProvenanceISOExpansion,
			meta: func() *metapb.FileMetadata {
				meta := pr5WiringMetadata()
				meta.ClipBoundaries = []*metapb.ClipBoundary{{ByteLen: 100}}
				return meta
			},
			want: validation.FinalLayoutProvenanceVirtualConcatenation,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
				Status: validation.ImportAdmissionHealthPending,
			}}
			gate := newDurableImportWriteValidator(stub)
			ctx := withDurableImportIntent(context.Background(), 42, tt.base)
			require.NoError(t, preparePR5MetadataWrite(t, gate, ctx, "library/complex.mkv", tt.meta()))
			assert.Equal(t, tt.want, stub.provenance)
		})
	}
}

func TestPR5MetadataAdmissionReturnsTypedLifecycleBoundaries(t *testing.T) {
	retryAt := time.Date(2026, 7, 14, 12, 0, 30, 0, time.UTC)
	for _, tt := range []struct {
		name   string
		result validation.DurableFinalLayoutValidationResult
		hold   bool
		reject bool
	}{
		{
			name: "durable wait",
			result: validation.DurableFinalLayoutValidationResult{
				Status: validation.ImportAdmissionAwaitConfirmation, RetryAt: &retryAt,
			},
			hold: true,
		},
		{
			name: "incomplete immediate resume",
			result: validation.DurableFinalLayoutValidationResult{
				Status: validation.ImportAdmissionAwaitConfirmation, ResumeRequired: true,
			},
			hold: true,
		},
		{
			name: "strict rejection",
			result: validation.DurableFinalLayoutValidationResult{
				Status: validation.ImportAdmissionReject,
			},
			reject: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gate := newDurableImportWriteValidator(&pr5AdmissionStub{result: tt.result})
			err := preparePR5MetadataWrite(t, gate,
				withDurableImportIntent(context.Background(), 43, validation.FinalLayoutProvenanceStandalone),
				"library/movie.mkv", pr5WiringMetadata(),
			)
			if tt.hold {
				var hold *durableImportHoldError
				require.ErrorAs(t, err, &hold)
				assert.Equal(t, tt.result.RetryAt, hold.retryAt)
				assert.Equal(t, tt.result.ResumeRequired, hold.resumeRequired)
			}
			if tt.reject {
				var rejected *durableImportRejectedError
				require.ErrorAs(t, err, &rejected)
			}
		})
	}
}

type pr5STATPoolGetter struct {
	client pool.NntpClient
	err    error
}

func (g pr5STATPoolGetter) GetPool() (pool.NntpClient, error) { return g.client, g.err }

type pr5AdmittedSTATPoolGetter struct {
	client         pool.NntpClient
	budget         *pool.ImportBudget
	acquireStarted chan int
}

func (g *pr5AdmittedSTATPoolGetter) GetPool() (pool.NntpClient, error) { return g.client, nil }

func (g *pr5AdmittedSTATPoolGetter) AcquireImportConnections(
	ctx context.Context,
	maxSlots int,
) (func(), int, error) {
	if g.acquireStarted != nil {
		select {
		case g.acquireStarted <- maxSlots:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return g.budget.AcquireUpTo(ctx, maxSlots)
}

type pr5STATRegistry struct {
	providers   []database.HealthProvider
	generations map[string][]database.HealthProviderGeneration
}

func (r pr5STATRegistry) ListProviders(context.Context, bool) ([]database.HealthProvider, error) {
	return append([]database.HealthProvider(nil), r.providers...), nil
}

func (r pr5STATRegistry) ListProviderGenerations(_ context.Context, id string) ([]database.HealthProviderGeneration, error) {
	return append([]database.HealthProviderGeneration(nil), r.generations[id]...), nil
}

type pr5STATClient struct {
	results      []nntppool.StatManyResult
	providerName string
	concurrency  int
	requests     []string
}

func (c *pr5STATClient) Body(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	panic("unexpected Body")
}
func (c *pr5STATClient) BodyAsync(context.Context, string, io.Writer, ...func(nntppool.YEncMeta)) <-chan nntppool.BodyResult {
	panic("unexpected BodyAsync")
}
func (c *pr5STATClient) BodyPriority(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	panic("unexpected BodyPriority")
}
func (c *pr5STATClient) Stat(context.Context, string) (*nntppool.StatResult, error) {
	panic("unexpected Stat")
}
func (c *pr5STATClient) StatMany(_ context.Context, ids []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	c.providerName = opts.Provider
	c.concurrency = opts.Concurrency
	c.requests = append([]string(nil), ids...)
	result := make(chan nntppool.StatManyResult, len(c.results))
	for _, item := range c.results {
		result <- item
	}
	close(result)
	return result
}
func (c *pr5STATClient) Stats() nntppool.ClientStats { return nntppool.ClientStats{} }

type pr5BlockingSTATClient struct {
	*pr5STATClient
	mu            sync.Mutex
	calls         int
	concurrencies []int
	firstStarted  chan struct{}
	releaseFirst  chan struct{}
}

func (c *pr5BlockingSTATClient) StatMany(
	ctx context.Context,
	ids []string,
	opts nntppool.StatManyOptions,
) <-chan nntppool.StatManyResult {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.concurrencies = append(c.concurrencies, opts.Concurrency)
	c.mu.Unlock()
	if call == 1 {
		close(c.firstStarted)
	}

	results := make(chan nntppool.StatManyResult, len(ids))
	go func() {
		defer close(results)
		if call == 1 {
			select {
			case <-c.releaseFirst:
			case <-ctx.Done():
				return
			}
		}
		for _, id := range ids {
			select {
			case results <- nntppool.StatManyResult{
				MessageID: id,
				Result:    &nntppool.StatResult{ProviderID: "stable-primary"},
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return results
}

func (c *pr5BlockingSTATClient) callSnapshot() (int, []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, append([]int(nil), c.concurrencies...)
}

func pr5STATConfig() *config.Config {
	enabled := true
	backup := false
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{
		ID: "stable-primary", Name: "Primary", Host: "news.example.invalid", Port: 563,
		Username: "account", Enabled: &enabled, IsBackupProvider: &backup,
	}}
	return cfg
}

func pr5STATRegistryFixture() pr5STATRegistry {
	return pr5STATRegistry{
		providers: []database.HealthProvider{{
			ID: "stable-primary", Active: true, CurrentGeneration: 3, ActivationEpoch: 2,
		}},
		generations: map[string][]database.HealthProviderGeneration{
			"stable-primary": {{
				ProviderID: "stable-primary", Generation: 3,
				Endpoint: "news.example.invalid", Port: 563, Account: "account",
			}},
		},
	}
}

func TestPR5TargetedSTATTransportMapsStableProviderAndPreserves451(t *testing.T) {
	rawCause := &nntppool.Error{Code: 451, Message: "raw fixture article identity must not escape"}
	client := &pr5STATClient{results: []nntppool.StatManyResult{
		{
			MessageID: "second-article",
			Err: &nntppool.TransportError{
				Kind: nntppool.OutcomeTemporaryFailure, ProviderID: "stable-primary",
				ResponseCode: 451, Cause: rawCause,
				Attempts: []nntppool.AttemptEvidence{{
					ProviderID: "stable-primary", Operation: nntppool.OperationStat,
					Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451,
					Cause: rawCause, PoolQueueDuration: time.Millisecond,
				}},
			},
		},
		{
			MessageID: "first-article",
			Result: &nntppool.StatResult{
				ProviderID: "stable-primary",
				Attempts: []nntppool.AttemptEvidence{{
					ProviderID: "stable-primary", Operation: nntppool.OperationStat,
					Outcome: nntppool.OutcomeSuccess, ResponseCode: 223,
				}},
			},
		},
	}}
	cfg := pr5STATConfig()
	transport := newNntppoolTargetedSTATTransport(
		pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(), func() *config.Config { return cfg },
	)

	observations, err := transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
	}, []validation.TargetedSTATRequest{
		{Position: 7, MessageID: "first-article"},
		{Position: 8, MessageID: "second-article"},
	})
	require.NoError(t, err)
	assert.Equal(t, "news.example.invalid:563+account", client.providerName)
	assert.Equal(t, []string{"first-article", "second-article"}, client.requests)
	require.Len(t, observations, 2)
	assert.Equal(t, 7, observations[0].Position)
	assert.Equal(t, nntppool.OutcomeSuccess, observations[0].Result.Outcome)
	assert.Equal(t, 8, observations[1].Position)
	assert.Equal(t, nntppool.OutcomeTemporaryFailure, observations[1].Result.Outcome)
	assert.Equal(t, 451, observations[1].Result.ResponseCode, "451 remains temporary")
	assert.Equal(t, validation.TargetedSTATCauseUnknown, observations[1].Result.CauseClass)
	assert.NotContains(t, fmt.Sprintf("%#v", observations), rawCause.Message,
		"transport results may expose only allowlisted cause classes")
}

func TestPR5TargetedSTATTransportCapsConcurrencyAtOperatorHealthLimit(t *testing.T) {
	client := &pr5STATClient{results: []nntppool.StatManyResult{
		{MessageID: "first", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
		{MessageID: "second", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
		{MessageID: "third", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
	}}
	cfg := pr5STATConfig()
	cfg.Health.MaxConnectionsForHealthChecks = 2
	transport := newNntppoolTargetedSTATTransport(
		pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(), func() *config.Config { return cfg },
	)

	observations, err := transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
	}, []validation.TargetedSTATRequest{
		{Position: 0, MessageID: "first"},
		{Position: 1, MessageID: "second"},
		{Position: 2, MessageID: "third"},
	})
	require.NoError(t, err)
	require.Len(t, observations, 3)
	assert.Equal(t, 2, client.concurrency)
}

func TestPR5TargetedSTATTransportDeduplicatesWireIDsAndFansOutByPosition(t *testing.T) {
	temporary451 := &nntppool.TransportError{
		Kind: nntppool.OutcomeTemporaryFailure, ProviderID: "stable-primary", ResponseCode: 451,
	}
	client := &pr5STATClient{results: []nntppool.StatManyResult{
		// StatMany completion is deliberately out of request order.
		{MessageID: "other-article", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
		{MessageID: "repeated-article", Err: temporary451},
	}}
	cfg := pr5STATConfig()
	transport := newNntppoolTargetedSTATTransport(
		pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(), func() *config.Config { return cfg },
	)

	observations, err := transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
	}, []validation.TargetedSTATRequest{
		{Position: 7, MessageID: "repeated-article"},
		{Position: 8, MessageID: "other-article"},
		{Position: 9, MessageID: "repeated-article"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"repeated-article", "other-article"}, client.requests,
		"one provider/article observation must serve every canonical owner")
	assert.Equal(t, 2, client.concurrency, "wire width is based on unique article IDs")
	require.Len(t, observations, 3)
	assert.Equal(t, 7, observations[0].Position)
	assert.Equal(t, nntppool.OutcomeTemporaryFailure, observations[0].Result.Outcome)
	assert.Equal(t, 451, observations[0].Result.ResponseCode)
	assert.Equal(t, 8, observations[1].Position)
	assert.Equal(t, nntppool.OutcomeSuccess, observations[1].Result.Outcome)
	assert.Equal(t, 9, observations[2].Position)
	assert.Equal(t, observations[0].Result, observations[2].Result,
		"duplicate canonical positions must receive the same typed provider observation")
}

func TestPR5TargetedSTATTransportRejectsDuplicateOrOmittedWireResult(t *testing.T) {
	for _, tt := range []struct {
		name    string
		results []nntppool.StatManyResult
	}{
		{
			name: "duplicate result",
			results: []nntppool.StatManyResult{
				{MessageID: "repeated", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
				{MessageID: "repeated", Result: &nntppool.StatResult{ProviderID: "stable-primary"}},
			},
		},
		{name: "omitted result"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &pr5STATClient{results: tt.results}
			transport := newNntppoolTargetedSTATTransport(
				pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(),
				func() *config.Config { return pr5STATConfig() },
			)
			observations, err := transport.TargetedSTAT(
				context.Background(),
				validation.TargetedSTATProvider{
					ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
				},
				[]validation.TargetedSTATRequest{
					{Position: 0, MessageID: "repeated"},
					{Position: 1, MessageID: "repeated"},
				},
			)
			require.ErrorIs(t, err, errDurableImportSTATIncomplete)
			assert.Empty(t, observations,
				"protocol mismatch must not expose a partial positional batch for commit")
		})
	}
}

func TestPR5ConcurrentTargetedSTATValidationSharesImportWireBudgetAndCancelsWaiter(t *testing.T) {
	baseClient := &pr5STATClient{}
	client := &pr5BlockingSTATClient{
		pr5STATClient: baseClient,
		firstStarted:  make(chan struct{}),
		releaseFirst:  make(chan struct{}),
	}
	budget := pool.NewImportBudget()
	budget.SetCapacity(1)
	acquireStarted := make(chan int, 2)
	getter := &pr5AdmittedSTATPoolGetter{
		client: client, budget: budget, acquireStarted: acquireStarted,
	}
	cfg := pr5STATConfig()
	cfg.Health.MaxConnectionsForHealthChecks = 3
	transport := newNntppoolTargetedSTATTransport(
		getter, pr5STATRegistryFixture(), func() *config.Config { return cfg },
	)
	provider := validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := transport.TargetedSTAT(context.Background(), provider, []validation.TargetedSTATRequest{
			{Position: 0, MessageID: "first"},
			{Position: 1, MessageID: "second"},
			{Position: 2, MessageID: "third"},
		})
		firstDone <- err
	}()
	assert.Equal(t, 3, <-acquireStarted, "the transport must request its configured wire width")
	select {
	case <-client.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first validation never reached the NNTP client")
	}
	calls, concurrencies := client.callSnapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, []int{1}, concurrencies,
		"StatMany concurrency must use the shared budget's granted width")

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := transport.TargetedSTAT(secondCtx, provider, []validation.TargetedSTATRequest{
			{Position: 9, MessageID: "blocked"},
		})
		secondDone <- err
	}()
	assert.Equal(t, 1, <-acquireStarted)
	calls, _ = client.callSnapshot()
	assert.Equal(t, 1, calls,
		"a second validation may not reach StatMany while the shared wire slot is held")
	cancelSecond()
	select {
	case err := <-secondDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("blocked validation did not observe caller cancellation")
	}
	calls, _ = client.callSnapshot()
	assert.Equal(t, 1, calls, "canceled queued work must never dispatch")

	close(client.releaseFirst)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first validation did not finish after its wire work was released")
	}

	// A successful return must release all granted slots. A leak here would
	// leave this deterministic probe blocked behind the completed validation.
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	release, granted, err := budget.AcquireUpTo(probeCtx, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, granted)
	release()
}

func TestPR5TargetedSTATTransportRefusesMismatchedReturnedProviderIdentity(t *testing.T) {
	raw := errors.New("raw wrong-provider detail with credential=user:secret")
	for _, tt := range []struct {
		name   string
		result nntppool.StatManyResult
	}{
		{
			name: "success top-level mismatch",
			result: nntppool.StatManyResult{MessageID: "fixture", Result: &nntppool.StatResult{
				ProviderID: "different-provider",
			}},
		},
		{
			name: "success attempt mismatch",
			result: nntppool.StatManyResult{MessageID: "fixture", Result: &nntppool.StatResult{
				ProviderID: "stable-primary",
				Attempts: []nntppool.AttemptEvidence{{
					ProviderID: "different-provider", Operation: nntppool.OperationStat,
					Outcome: nntppool.OutcomeSuccess,
				}},
			}},
		},
		{
			name: "transport top-level mismatch",
			result: nntppool.StatManyResult{MessageID: "fixture", Err: &nntppool.TransportError{
				Kind: nntppool.OutcomeTemporaryFailure, ProviderID: "different-provider",
				ResponseCode: 451, Cause: raw,
				Attempts: []nntppool.AttemptEvidence{{
					ProviderID: "different-provider", Operation: nntppool.OperationStat,
					Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451, Cause: raw,
				}},
			}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &pr5STATClient{results: []nntppool.StatManyResult{tt.result}}
			cfg := pr5STATConfig()
			transport := newNntppoolTargetedSTATTransport(
				pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(), func() *config.Config { return cfg },
			)
			observations, err := transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
				ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
			}, []validation.TargetedSTATRequest{{Position: 0, MessageID: "fixture"}})
			require.NoError(t, err)
			require.Len(t, observations, 1)
			assert.Equal(t, validation.ImportCheckDispositionIncomplete,
				observations[0].Result.CompletionDisposition)
			assert.NotContains(t, fmt.Sprintf("%#v", observations), raw.Error())
			assert.NotContains(t, fmt.Sprintf("%#v", observations), "user:secret")
		})
	}
}

func TestPR5DurableAdmissionDemotesRawNZBFastFailAuthority(t *testing.T) {
	env := newBatteryEnv(t)
	content := []byte("deterministic final-layout content")
	segments := env.registerContent("pr5-final-layout", content, len(content), 1, &nntppool.YEncMeta{
		FileName: "Final.Layout.2026.mkv", FileSize: int64(len(content)),
	})
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Final.Layout.2026.mkv", Segments: segments})
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept,
	}}
	journal := newDurableImportRollbackJournal(env.svc, &pr5StoreRefOperationLedger{})
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	env.svc.SetWriteValidator(gate)
	env.proc.SetDurableRollbackJournal(journal)
	env.proc.SetDurableAdmissionEnabled(true)

	_, written, err := env.runImport(nzb, "Final.Layout.2026.mkv")
	require.NoError(t, err)
	require.Len(t, filePaths(written), 1)
	assert.Zero(t, env.client.StatCalls(),
		"the raw NZB fast-fail sweep must not run beside final-layout admission")
	assert.Equal(t, 1, stub.calls)
	assert.Equal(t, validation.FinalLayoutProvenanceStandalone, stub.provenance)
}

type pr5PartialAdmissionStub struct {
	mu            sync.Mutex
	holdSecond    bool
	firstObserved chan struct{}
	firstOnce     sync.Once
}

func (s *pr5PartialAdmissionStub) ValidateFinalLayout(
	_ context.Context,
	_ int64,
	path string,
	_ *metapb.FileMetadata,
	_ validation.FinalLayoutProvenance,
) (validation.DurableFinalLayoutValidationResult, error) {
	if strings.HasSuffix(path, "Episode.One.mkv") {
		s.firstOnce.Do(func() { close(s.firstObserved) })
		return validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionAccept, FileRevisionID: "episode-one-revision",
		}, nil
	}
	if strings.HasSuffix(path, "Episode.Two.mkv") {
		<-s.firstObserved
		s.mu.Lock()
		hold := s.holdSecond
		s.mu.Unlock()
		if hold {
			due := time.Now().UTC().Add(30 * time.Second)
			return validation.DurableFinalLayoutValidationResult{
				Status: validation.ImportAdmissionAwaitConfirmation, RetryAt: &due,
			}, nil
		}
	}
	return validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept, FileRevisionID: "episode-two-revision",
	}, nil
}

func (s *pr5PartialAdmissionStub) ActivateFileRevision(context.Context, int64, string) error {
	return nil
}

func TestPR5RestartedMultiFileAdmissionReusesAcceptedPathWithoutDuplicateSuffix(t *testing.T) {
	env := newBatteryEnv(t)
	firstContent := []byte("episode-one")
	secondContent := []byte("episode-two")
	firstSegments := env.registerContent("pr5-episode-one", firstContent, 11, 1, &nntppool.YEncMeta{
		FileName: "Episode.One.mkv", FileSize: int64(len(firstContent)),
	})
	secondSegments := env.registerContent("pr5-episode-two", secondContent, 11, 1, &nntppool.YEncMeta{
		FileName: "Episode.Two.mkv", FileSize: int64(len(secondContent)),
	})
	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "Episode.One.mkv", Segments: firstSegments},
		nzbbuild.File{Subject: "Episode.Two.mkv", Segments: secondSegments},
	)
	stub := &pr5PartialAdmissionStub{holdSecond: true, firstObserved: make(chan struct{})}
	journal := newDurableImportRollbackJournal(env.svc, &pr5StoreRefOperationLedger{})
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	env.svc.SetWriteValidator(gate)
	env.proc.SetDurableRollbackJournal(journal)
	env.proc.SetDurableAdmissionEnabled(true)

	firstPath := nzbbuild.WriteTemp(t, nzb, "Show.S01")
	_, firstWritten, err := env.proc.ProcessNzbFile(
		context.Background(), firstPath, filepath.Dir(firstPath),
		1, nil, nil, nil, nil, nil, nil,
	)
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	require.Len(t, filePaths(firstWritten), 1)
	acceptedPath := filePaths(firstWritten)[0]
	acceptedMeta := env.readMeta(acceptedPath)
	acceptedLayout, err := metadata.ResolveCanonicalSegmentLayout(acceptedMeta)
	require.NoError(t, err)

	stub.mu.Lock()
	stub.holdSecond = false
	stub.mu.Unlock()
	secondPath := nzbbuild.WriteTemp(t, nzb, "Show.S01")
	resumeContext := withDurableImportReusableLayouts(context.Background(), 1, map[string]string{
		acceptedPath: acceptedLayout.Fingerprint,
	})
	_, secondWritten, err := env.proc.ProcessNzbFile(
		resumeContext, secondPath, filepath.Dir(secondPath),
		1, nil, nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	paths := filePaths(secondWritten)
	require.Len(t, paths, 2)
	assert.Contains(t, paths, acceptedPath)
	for _, path := range paths {
		assert.NotContains(t, path, "Episode.One_1.mkv",
			"restart must reuse the exact prior queue/path/layout binding")
	}
}

func TestPR5TargetedSTATTransportFencesActivationAndLeavesIncompleteWorkUncommittable(t *testing.T) {
	breaker := &nntppool.CircuitBreakerError{ProviderID: "stable-primary"}
	client := &pr5STATClient{results: []nntppool.StatManyResult{{
		MessageID: "fixture-article",
		Err: &nntppool.TransportError{
			Kind: nntppool.OutcomeTemporaryFailure, ProviderID: "stable-primary", Cause: breaker,
			Attempts: []nntppool.AttemptEvidence{{
				ProviderID: "stable-primary", Operation: nntppool.OperationStat,
				Outcome: nntppool.OutcomeTemporaryFailure, Cause: breaker,
			}},
		},
	}}}
	cfg := pr5STATConfig()
	transport := newNntppoolTargetedSTATTransport(
		pr5STATPoolGetter{client: client}, pr5STATRegistryFixture(), func() *config.Config { return cfg },
	)

	observations, err := transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 2,
	}, []validation.TargetedSTATRequest{{Position: 0, MessageID: "fixture-article"}})
	require.NoError(t, err)
	require.Len(t, observations, 1)
	assert.Equal(t, validation.ImportCheckDispositionIncomplete,
		observations[0].Result.CompletionDisposition)
	assert.Equal(t, validation.TargetedSTATCauseBreakerOpen,
		observations[0].Result.CauseClass)

	client.requests = nil
	observations, err = transport.TargetedSTAT(context.Background(), validation.TargetedSTATProvider{
		ID: "stable-primary", Generation: 3, ActivationEpoch: 1,
	}, []validation.TargetedSTATRequest{{Position: 0, MessageID: "fixture-article"}})
	require.Error(t, err)
	assert.Empty(t, observations)
	assert.Empty(t, client.requests, "a stale activation must be refused before NNTP dispatch")
}

func TestPR5AdmissionHoldParksWithoutCleanupFailureAccountingOrFallback(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "import-hold.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (51, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)

	metadataService := metadata.NewMetadataService(t.TempDir())
	require.NoError(t, metadataService.WriteFileMetadata("library/already-accepted.mkv", pr5WiringMetadata()))
	service := &Service{
		database: db, metadataService: metadataService,
		log: slog.Default().With("component", "pr5-import-wiring-test"),
	}
	service.writtenPathsCache.Store(int64(51), []string{"library/already-accepted.mkv"})
	due := time.Now().UTC().Add(30 * time.Second)
	service.HandleFailure(context.Background(), &database.ImportQueueItem{ID: 51}, &durableImportHoldError{
		retryAt: &due,
	})

	var status database.QueueStatus
	var errorMessage *string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 51`,
	).Scan(&status, &errorMessage))
	assert.Equal(t, database.QueueStatusPaused, status)
	assert.Nil(t, errorMessage)
	_, err = metadataService.ReadFileMetadata("library/already-accepted.mkv")
	require.NoError(t, err, "a validation hold must not roll back already accepted files")
	_, cached := service.writtenPathsCache.Load(int64(51))
	assert.False(t, cached, "held path bookkeeping must not leak between attempts")
}

func TestPR5TerminalMultiFileFailureRollsBackPriorActivationAndRetainsEvidence(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "terminal-rollback.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (58, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)

	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	validator, err := validation.NewDurableFinalLayoutValidator(
		repository,
		pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary, Order: 0,
			}},
			DamagePolicy:      config.ImportDamagePolicyStrict,
			ConfirmationDelay: time.Millisecond,
		},
	)
	require.NoError(t, err)

	metadataService := metadata.NewMetadataService(t.TempDir())
	rollbackJournal := newDurableImportRollbackJournal(metadataService, nil)
	writeValidator := newDurableImportWriteValidator(validator)
	writeValidator.journal = rollbackJournal
	metadataService.SetWriteValidator(writeValidator)
	writeCtx := withDurableImportIntent(
		context.Background(), 58, validation.FinalLayoutProvenanceStandalone,
	)
	accepted := pr5WiringMetadata()
	accepted.SegmentData[0].Id = "fixture-present"
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		writeCtx, "library/episode-one.mkv", accepted, nil, "",
	))

	missing := pr5WiringMetadata()
	missing.SegmentData[0].Id = "fixture-missing"
	err = metadataService.WriteFileMetadataAuto(
		writeCtx, "library/episode-two.mkv", missing, nil, "",
	)
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	time.Sleep(2 * time.Millisecond)
	err = metadataService.WriteFileMetadataAuto(
		writeCtx, "library/episode-two.mkv", missing, nil, "",
	)
	var rejected *durableImportRejectedError
	require.ErrorAs(t, err, &rejected)

	service := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository:  repository,
		durableImportRollbackJournal:  rollbackJournal,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-terminal-rollback-test"),
	}
	// Exercise restart recovery: no transient written-path cache survives, so
	// the queue-owned active revision is the cleanup authority.
	require.NoError(t, service.cleanupTerminalImportArtifacts(context.Background(), 58))
	visible, err := metadataService.ReadFileMetadata("library/episode-one.mkv")
	require.NoError(t, err)
	assert.Nil(t, visible)

	var active bool
	require.NoError(t, db.Connection().QueryRow(`
		SELECT revision.active
		FROM health_file_revisions revision
		JOIN file_health health ON health.id = revision.file_health_id
		WHERE health.file_path = 'library/episode-one.mkv'
	`).Scan(&active))
	assert.False(t, active)

	for table, minimum := range map[string]int{
		"health_import_validations": 2,
		"health_runs":               2,
		"health_run_chunks":         3,
		"health_attempt_evidence":   3,
	} {
		var count int
		require.NoError(t, db.Connection().QueryRow(
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count))
		assert.GreaterOrEqual(t, count, minimum, "%s history was lost", table)
	}
}

func TestPR5TerminalFailedOverwriteRestoresExactPriorVisibility(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "overwrite-rollback.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (66, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)

	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	metadataService.SetStoreRefCounter(db.StoreRefRepo)
	path := "library/replacement.mkv"
	priorStoreRef := filepath.Join(t.TempDir(), "prior.nzbz")
	require.NoError(t, metadataService.Store().WriteStore(priorStoreRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{Segments: []*metapb.NzbSeg{{
			Id: "fixture-prior", Number: 1, Bytes: 100,
		}}}},
	}))
	prior := pr5WiringMetadata()
	prior.Status = metapb.FileStatus_FILE_STATUS_CORRUPTED
	prior.SegmentData[0].Id = "fixture-prior"
	priorLayout, err := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, err)
	priorRevision, err := repository.EnsureCandidateFileRevision(
		context.Background(), database.FileRevisionSpec{
			FilePath: path, LayoutFingerprint: priorLayout.Fingerprint,
			VirtualSize: priorLayout.VirtualSize, SegmentCount: int64(len(priorLayout.Segments)),
		},
	)
	require.NoError(t, err)
	_, err = db.Connection().Exec(`
		UPDATE health_file_revisions
		SET active = TRUE, activated_at = ?
		WHERE id = ?
	`, time.Now().UTC(), priorRevision.ID)
	require.NoError(t, err)
	require.NoError(t, metadataService.WriteFileMetadataV3(
		context.Background(), path, prior, map[string]int64{"fixture-prior": 0}, priorStoreRef,
	))

	validator, err := validation.NewDurableFinalLayoutValidator(
		repository, pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary, Order: 0,
			}},
			DamagePolicy: config.ImportDamagePolicyStrict,
		},
	)
	require.NoError(t, err)
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	gate := newDurableImportWriteValidator(validator)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)
	candidate := pr5WiringMetadata()
	candidate.SegmentData[0].Id = "fixture-present"
	candidateStoreRef := filepath.Join(t.TempDir(), "candidate.nzbz")
	require.NoError(t, metadataService.Store().WriteStore(candidateStoreRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{Segments: []*metapb.NzbSeg{{
			Id: "fixture-present", Number: 1, Bytes: 100,
		}}}},
	}))
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		withDurableImportIntent(
			context.Background(), 66, validation.FinalLayoutProvenanceStandalone,
		),
		path, candidate, map[string]int64{"fixture-present": 0}, candidateStoreRef,
	))
	candidateLayout, err := metadata.ResolveCanonicalSegmentLayout(candidate)
	require.NoError(t, err)
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, candidateLayout.Fingerprint, visible.LayoutFingerprint)

	service := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository: repository, durableImportRollbackJournal: journal,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-overwrite-rollback-test"),
	}
	restoreVisibility := journal.restore
	journal.restore = func(string, []byte, bool) error {
		return errors.New("injected rollback visibility failure")
	}
	require.ErrorIs(t, service.cleanupTerminalImportArtifacts(context.Background(), 66),
		errDurableImportActivationRollback)
	visible, err = metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, candidateLayout.Fingerprint, visible.LayoutFingerprint,
		"failed filesystem rollback must compensate the exact candidate revision")
	var compensatedFingerprint string
	require.NoError(t, db.Connection().QueryRow(`
		SELECT revision.layout_fingerprint
		FROM health_file_revisions revision
		JOIN file_health health ON health.id = revision.file_health_id
		WHERE health.file_path = ? AND revision.active = TRUE
	`, path).Scan(&compensatedFingerprint))
	assert.Equal(t, candidateLayout.Fingerprint, compensatedFingerprint)
	journal.restore = restoreVisibility
	require.NoError(t, service.cleanupTerminalImportArtifacts(context.Background(), 66))
	visible, err = metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.True(t, visible.Exists)
	assert.Equal(t, priorLayout.Fingerprint, visible.LayoutFingerprint)

	var activeFingerprint string
	require.NoError(t, db.Connection().QueryRow(`
		SELECT revision.layout_fingerprint
		FROM health_file_revisions revision
		JOIN file_health health ON health.id = revision.file_health_id
		WHERE health.file_path = ? AND revision.active = TRUE
	`, path).Scan(&activeFingerprint))
	assert.Equal(t, priorLayout.Fingerprint, activeFingerprint)
	priorCount, err := db.StoreRefRepo.GetStoreRefCount(context.Background(), priorStoreRef)
	require.NoError(t, err)
	candidateCount, err := db.StoreRefRepo.GetStoreRefCount(context.Background(), candidateStoreRef)
	require.NoError(t, err)
	assert.Equal(t, int64(1), priorCount)
	assert.Zero(t, candidateCount)
	_, err = os.Stat(journal.queueDirectory(66))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5CompletedReplacementStartupRecoveryCommitsStoreRefsExactlyOnce(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "replacement-success.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (67, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)

	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	metadataService.SetStoreRefCounter(db.StoreRefRepo)
	path := "library/store-replacement.mkv"
	priorStoreRef := filepath.Join(t.TempDir(), "prior.nzbz")
	require.NoError(t, metadataService.Store().WriteStore(priorStoreRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{Segments: []*metapb.NzbSeg{{
			Id: "fixture-prior", Number: 1, Bytes: 100,
		}}}},
	}))
	prior := pr5WiringMetadata()
	prior.Status = metapb.FileStatus_FILE_STATUS_CORRUPTED
	prior.SegmentData[0].Id = "fixture-prior"
	require.NoError(t, metadataService.WriteFileMetadataV3(
		context.Background(), path, prior, map[string]int64{"fixture-prior": 0}, priorStoreRef,
	))
	priorLayout, err := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, err)
	priorRevision, err := repository.EnsureCandidateFileRevision(
		context.Background(), database.FileRevisionSpec{
			FilePath: path, LayoutFingerprint: priorLayout.Fingerprint,
			VirtualSize: priorLayout.VirtualSize, SegmentCount: int64(len(priorLayout.Segments)),
		},
	)
	require.NoError(t, err)
	_, err = db.Connection().Exec(`
		UPDATE health_file_revisions SET active = TRUE, activated_at = ? WHERE id = ?
	`, time.Now().UTC(), priorRevision.ID)
	require.NoError(t, err)

	validator, err := validation.NewDurableFinalLayoutValidator(
		repository, pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary, Order: 0,
			}},
			DamagePolicy: config.ImportDamagePolicyStrict,
		},
	)
	require.NoError(t, err)
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	gate := newDurableImportWriteValidator(validator)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)
	candidateStoreRef := filepath.Join(t.TempDir(), "candidate.nzbz")
	require.NoError(t, metadataService.Store().WriteStore(candidateStoreRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{Segments: []*metapb.NzbSeg{{
			Id: "fixture-present", Number: 1, Bytes: 100,
		}}}},
	}))
	candidate := pr5WiringMetadata()
	candidate.SegmentData[0].Id = "fixture-present"
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		withDurableImportIntent(
			context.Background(), 67, validation.FinalLayoutProvenanceStandalone,
		),
		path, candidate, map[string]int64{"fixture-present": 0}, candidateStoreRef,
	))
	priorCount, err := db.StoreRefRepo.GetStoreRefCount(context.Background(), priorStoreRef)
	require.NoError(t, err)
	candidateCount, err := db.StoreRefRepo.GetStoreRefCount(context.Background(), candidateStoreRef)
	require.NoError(t, err)
	assert.Equal(t, int64(1), priorCount)
	assert.Equal(t, int64(1), candidateCount)

	require.NoError(t, db.Repository.UpdateQueueItemStatus(
		context.Background(), 67, database.QueueStatusCompleted, nil,
	))
	// Simulate a process stop after the SQL direction became committed but
	// before the private filesystem journal/ref transition was finalized.
	require.NoError(t, repository.CommitImportQueueActivations(
		context.Background(), 67, time.Now().UTC(),
	))
	restartedJournal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	restarted := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository:  repository,
		durableImportRollbackJournal:  restartedJournal,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-success-recovery-test"),
	}
	require.NoError(t, restarted.recoverDurableImportRollbackJournals(context.Background()))
	require.NoError(t, restarted.recoverDurableImportRollbackJournals(context.Background()))
	priorCount, err = db.StoreRefRepo.GetStoreRefCount(context.Background(), priorStoreRef)
	require.NoError(t, err)
	candidateCount, err = db.StoreRefRepo.GetStoreRefCount(context.Background(), candidateStoreRef)
	require.NoError(t, err)
	assert.Zero(t, priorCount)
	assert.Equal(t, int64(1), candidateCount)
	_, err = os.Stat(restartedJournal.queueDirectory(67))
	assert.ErrorIs(t, err, os.ErrNotExist)
	candidateLayout, err := metadata.ResolveCanonicalSegmentLayout(candidate)
	require.NoError(t, err)
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, candidateLayout.Fingerprint, visible.LayoutFingerprint)
}

func TestPR5CrashAfterCandidateRefAcquireBeforeRenameCleansExactReservation(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "pre-rename-crash.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (93, 'fixture.nzb', 'failed', 1)
	`)
	require.NoError(t, err)
	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	validator, err := validation.NewDurableFinalLayoutValidator(
		repository, pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary,
			}},
			DamagePolicy: config.ImportDamagePolicyStrict,
		},
	)
	require.NoError(t, err)
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	gate := newDurableImportWriteValidator(validator)
	gate.journal = journal
	path := "library/pre-rename.mkv"
	candidate := pr5WiringMetadata()
	candidate.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	candidate.SegmentData[0].Id = "fixture-present"
	ctx := withDurableImportIntent(
		context.Background(), 93, validation.FinalLayoutProvenanceStandalone,
	)
	permitValue, err := gate.PrepareMetadataWrite(ctx, path, candidate)
	require.NoError(t, err)
	permit := permitValue.(*durableImportWritePermit)
	require.NoError(t, permit.JournalPriorMetadata(ctx, path, nil, false, "", ""))
	storeRef := filepath.Join(t.TempDir(), "pre-rename.nzbz")
	require.NoError(t, metadataService.Store().WriteStoreDurable(storeRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{Segments: []*metapb.NzbSeg{{
			Id: "fixture-present", Number: 1, Bytes: 100,
		}}}},
	}))
	require.NoError(t, journal.RecordStoreIntent(ctx, 93, storeRef))
	require.NoError(t, permit.PrepareCandidateMetadata(ctx, path, storeRef))
	count, err := db.StoreRefRepo.GetStoreRefCount(ctx, storeRef)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.False(t, metadataService.FileExists(path), "the simulated crash precedes rename")

	service := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository: repository, durableImportRollbackJournal: journal,
		durableImportAdmissionEnabled: true,
	}
	require.NoError(t, service.cleanupTerminalImportArtifacts(context.Background(), 93))
	count, err = db.StoreRefRepo.GetStoreRefCount(ctx, storeRef)
	require.NoError(t, err)
	assert.Zero(t, count)
	_, err = os.Stat(storeRef)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(journal.queueDirectory(93))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5CrashedPreActivationCleanupPreservesForeignSharedRevisionOwner(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "shared-cleanup-owner.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority) VALUES
			(96, 'crashed.nzb', 'failed', 1),
			(97, 'foreign.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	validator, err := validation.NewDurableFinalLayoutValidator(
		repository, pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary,
			}},
			DamagePolicy: config.ImportDamagePolicyStrict,
		},
	)
	require.NoError(t, err)
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	gate := newDurableImportWriteValidator(validator)
	gate.journal = journal
	path := "library/shared-crash.mkv"
	candidate := pr5WiringMetadata()
	candidate.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	candidate.SegmentData[0].Id = "fixture-present"

	crashedCtx := withDurableImportIntent(
		context.Background(), 96, validation.FinalLayoutProvenanceStandalone,
	)
	permitValue, err := gate.PrepareMetadataWrite(crashedCtx, path, candidate)
	require.NoError(t, err)
	crashedPermit := permitValue.(*durableImportWritePermit)
	require.NoError(t, crashedPermit.JournalPriorMetadata(
		crashedCtx, path, nil, false, "", "",
	))
	require.NoError(t, crashedPermit.PrepareCandidateMetadata(crashedCtx, path, ""))
	require.NoError(t, metadataService.WriteFileMetadata(path, candidate),
		"simulate candidate rename followed by a crash before DB activation")

	foreignResult, err := validator.ValidateFinalLayout(
		context.Background(), 97, path, candidate,
		validation.FinalLayoutProvenance{Kind: validation.FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	require.Equal(t, validation.ImportAdmissionAccept, foreignResult.Status)
	_, err = repository.ActivateImportFileRevision(
		context.Background(), 97, foreignResult.FileRevisionID,
	)
	require.NoError(t, err)

	service := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository: repository, durableImportRollbackJournal: journal,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-shared-cleanup-owner-test"),
	}
	require.NoError(t, service.cleanupTerminalImportArtifacts(context.Background(), 96))
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.True(t, visible.Exists)
	layout, err := metadata.ResolveCanonicalSegmentLayout(candidate)
	require.NoError(t, err)
	assert.Equal(t, layout.Fingerprint, visible.LayoutFingerprint,
		"crashed queue cleanup must not delete metadata owned by the foreign activation")
	var activeID string
	require.NoError(t, db.Connection().QueryRow(`
		SELECT id FROM health_file_revisions
		WHERE file_health_id = (
			SELECT id FROM file_health WHERE file_path = ?
		) AND active = TRUE
	`, path).Scan(&activeID))
	assert.Equal(t, foreignResult.FileRevisionID, activeID)
	_, err = os.Stat(journal.queueDirectory(96))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5CrashedDifferentCandidateWaitsForForeignPriorOwnerBeforeRestore(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "different-cleanup-owner.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority) VALUES
			(98, 'foreign-prior.nzb', 'processing', 1),
			(99, 'crashed-candidate.nzb', 'failed', 1)
	`)
	require.NoError(t, err)
	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	validator, err := validation.NewDurableFinalLayoutValidator(
		repository, pr5SelectiveAdmissionTransport{},
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: []database.ProviderSpec{{
				StableID: "fixture-provider", DisplayName: "Fixture provider",
				Endpoint: "fixture.invalid", Port: 119,
				Role: database.ProviderRolePrimary,
			}},
			DamagePolicy: config.ImportDamagePolicyStrict,
		},
	)
	require.NoError(t, err)
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	gate := newDurableImportWriteValidator(validator)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)
	path := "library/different-crash.mkv"

	foreignPrior := pr5WiringMetadata()
	foreignPrior.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	foreignPrior.SegmentData[0].Id = "fixture-foreign-prior"
	foreignCtx := withDurableImportIntent(
		context.Background(), 98, validation.FinalLayoutProvenanceStandalone,
	)
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		foreignCtx, path, foreignPrior, nil, "",
	))
	foreignLayout, err := metadata.ResolveCanonicalSegmentLayout(foreignPrior)
	require.NoError(t, err)

	crashedCandidate := pr5WiringMetadata()
	crashedCandidate.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	crashedCandidate.SegmentData[0].Id = "fixture-crashed-candidate"
	crashedCtx := withDurableImportIntent(
		context.Background(), 99, validation.FinalLayoutProvenanceStandalone,
	)
	permitValue, err := gate.PrepareMetadataWrite(crashedCtx, path, crashedCandidate)
	require.NoError(t, err)
	crashedPermit := permitValue.(*durableImportWritePermit)
	priorBytes, priorState, err := metadataService.CaptureMetadataVisibilitySnapshot(path)
	require.NoError(t, err)
	require.True(t, priorState.Exists)
	require.NoError(t, crashedPermit.JournalPriorMetadata(
		crashedCtx, path, priorBytes, true,
		priorState.LayoutFingerprint, priorState.StoreRef,
	))
	require.NoError(t, crashedPermit.PrepareCandidateMetadata(crashedCtx, path, ""))
	require.NoError(t, metadataService.WriteFileMetadata(path, crashedCandidate),
		"simulate a different candidate rename followed by pre-activation crash")
	crashedLayout, err := metadata.ResolveCanonicalSegmentLayout(crashedCandidate)
	require.NoError(t, err)

	service := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository: repository, durableImportRollbackJournal: journal,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-different-cleanup-owner-test"),
	}
	require.ErrorIs(t, service.cleanupTerminalImportArtifacts(context.Background(), 99),
		errDurableImportActivationRollback)
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, crashedLayout.Fingerprint, visible.LayoutFingerprint,
		"cleanup must not restore across the unresolved foreign ownership window")
	_, err = os.Stat(journal.queueDirectory(99))
	require.NoError(t, err, "blocked cleanup must retain its private recovery journal")

	require.NoError(t, repository.CommitImportQueueActivations(
		context.Background(), 98, time.Now().UTC(),
	))
	require.NoError(t, service.cleanupTerminalImportArtifacts(context.Background(), 99))
	visible, err = metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, foreignLayout.Fingerprint, visible.LayoutFingerprint,
		"cleanup may restore the exact prior only after its foreign owner resolves")
	_, err = os.Stat(journal.queueDirectory(99))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5RestartCleansStoreCreatedBeforeFirstMetadataAdmission(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "pre-admission-store.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (94, 'fixture.nzb', 'failed', 1)
	`)
	require.NoError(t, err)
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	storeRef := filepath.Join(t.TempDir(), "pre-admission.nzbz")
	require.NoError(t, metadataService.Store().WriteStoreDurable(storeRef, &metapb.NzbStore{}))
	journal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	require.NoError(t, journal.RecordStoreIntent(context.Background(), 94, storeRef))
	restartedJournal := newDurableImportRollbackJournal(metadataService, db.StoreRefRepo)
	restarted := &Service{
		database: db, metadataService: metadataService,
		durableImportStateRepository: database.NewHealthStateRepository(db.Connection(), db.Dialect()),
		durableImportRollbackJournal: restartedJournal, durableImportAdmissionEnabled: true,
	}
	require.NoError(t, restarted.recoverDurableImportRollbackJournals(context.Background()))
	_, err = os.Stat(storeRef)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(restartedJournal.queueDirectory(94))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5ProcessorRecordsStoreIntentBeforeFirstRejectedMetadataWrite(t *testing.T) {
	env := newBatteryEnv(t)
	ledger := &pr5StoreRefOperationLedger{}
	journal := newDurableImportRollbackJournal(env.svc, ledger)
	gate := newDurableImportWriteValidator(&pr5AdmissionStub{
		result: validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionReject,
		},
	})
	gate.journal = journal
	env.svc.SetWriteValidator(gate)
	env.proc.SetDurableRollbackJournal(journal)
	env.proc.SetDurableAdmissionEnabled(true)
	segments := env.registerContent(
		"pre-admission-store", []byte("complete-body"), 64, 1,
		&nntppool.YEncMeta{FileName: "Rejected.mkv", FileSize: 13},
	)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Rejected.mkv", Segments: segments})
	_, written, err := env.runImport(nzb, "Rejected.mkv")
	require.Error(t, err)
	assert.Empty(t, written)
	stores, err := filepath.Glob(filepath.Join(env.configDir, ".nzbs", "*.nzbz"))
	require.NoError(t, err)
	require.Len(t, stores, 1)
	queueIDs, err := journal.PendingQueueIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{1}, queueIDs)
	require.NoError(t, journal.CleanupUnreferencedStores(context.Background(), 1))
	_, err = os.Stat(stores[0])
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPR5DurableStorePublicationFailureParksBeforeMetadataAndRetriesV3(t *testing.T) {
	env := newBatteryEnv(t)
	ledger := &pr5StoreRefOperationLedger{}
	journal := newDurableImportRollbackJournal(env.svc, ledger)
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionAccept, FileRevisionID: "store-retry-candidate",
	}}
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	env.svc.SetWriteValidator(gate)
	env.proc.SetDurableRollbackJournal(journal)
	env.proc.SetDurableAdmissionEnabled(true)

	segments := env.registerContent(
		"durable-store-retry", []byte("complete-body"), 64, 1,
		&nntppool.YEncMeta{FileName: "Store.Retry.mkv", FileSize: 13},
	)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "Store.Retry.mkv", Segments: segments})
	storeDir := filepath.Join(env.configDir, ".nzbs")
	require.NoError(t, os.MkdirAll(storeDir, 0o755))
	storeRef := filepath.Join(storeDir, "1-Store.Retry.nzbz")
	require.NoError(t, os.Mkdir(storeRef, 0o755),
		"a directory at the final store path deterministically fails atomic publication")

	_, written, err := env.runImport(nzb, "Store.Retry")
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	assert.True(t, hold.resumeRequired)
	assert.Empty(t, written)
	assert.Zero(t, stub.calls, "store failure must park before metadata admission")
	assert.False(t, env.svc.FileExists("Store.Retry.mkv"),
		"durable mode must never fall back to a v1 candidate")
	_, err = os.Stat(filepath.Join(
		journal.queueDirectory(1), durableRollbackStoreRecordName(storeRef),
	))
	require.NoError(t, err, "failed publication must retain its queue store intent")

	require.NoError(t, os.Remove(storeRef))
	_, written, err = env.runImport(nzb, "Store.Retry")
	require.NoError(t, err)
	require.Len(t, filePaths(written), 1)
	assert.Equal(t, 1, stub.calls)
	visible := env.readMeta(filePaths(written)[0])
	assert.Equal(t, storeRef, visible.StoreRef,
		"retry must preserve the durable v3 form instead of creating a retained v1 intent")
}

func TestPR5JournaledActivationFailureRetriesChangedTimestampsWithoutLivelock(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	journal := newDurableImportRollbackJournal(metadataService, nil)
	stub := &pr5AdmissionStub{
		result: validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
		},
		activateErr: errors.New("transient activation failure"),
	}
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)
	ctx := withDurableImportIntent(
		context.Background(), 95, validation.FinalLayoutProvenanceStandalone,
	)
	candidate := pr5WiringMetadata()
	candidate.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	firstModified := candidate.ModifiedAt
	err := metadataService.WriteFileMetadataAuto(ctx, "library/retry.mkv", candidate, nil, "")
	require.Error(t, err)
	assert.False(t, metadataService.FileExists("library/retry.mkv"))

	candidate.ModifiedAt = firstModified + 2
	stub.activateErr = nil
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		ctx, "library/retry.mkv", candidate, nil, "",
	))
	assert.True(t, metadataService.FileExists("library/retry.mkv"))
	assert.Equal(t, []string{"candidate-revision", "candidate-revision"}, stub.activations)
}

func TestPR5TerminalCleanupParksWhenDurableActivationRollbackCannotBeFenced(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "rollback-hold.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (59, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	metadataService := metadata.NewMetadataService(t.TempDir())
	require.NoError(t, metadataService.WriteFileMetadata(
		"library/visible-unfenced.mkv", pr5WiringMetadata(),
	))
	service := &Service{
		database: db, metadataService: metadataService,
		durableImportAdmissionEnabled: true,
		log:                           slog.Default().With("component", "pr5-rollback-hold-test"),
	}
	service.writtenPathsCache.Store(int64(59), []string{"library/visible-unfenced.mkv"})
	service.HandleFailure(
		context.Background(), &database.ImportQueueItem{ID: 59},
		&durableImportRejectedError{},
	)

	var status database.QueueStatus
	var marker string
	require.NoError(t, db.Connection().QueryRow(`
		SELECT status, error_message FROM import_queue WHERE id = 59
	`).Scan(&status, &marker))
	assert.Equal(t, database.QueueStatusPaused, status)
	assert.Equal(t, durableTerminalRollbackWaitingMarker, marker)
	_, err = metadataService.ReadFileMetadata("library/visible-unfenced.mkv")
	require.NoError(t, err, "metadata must remain visible while its activation cannot be rolled back")
}

func TestPR5IncompleteAdmissionReturnsQueueItemImmediatelyToPending(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "import-resume.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (52, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db,
		log:      slog.Default().With("component", "pr5-import-wiring-test"),
	}
	service.HandleFailure(context.Background(), &database.ImportQueueItem{ID: 52}, &durableImportHoldError{
		resumeRequired: true,
	})

	var status database.QueueStatus
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status FROM import_queue WHERE id = 52`,
	).Scan(&status))
	assert.Equal(t, database.QueueStatusPending, status)
}

func TestPR5IncompleteFinalLayoutAdmissionUsesBoundedRestartSafeDelay(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "import-incomplete-delay.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (57, 'fixture.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db,
		log:      slog.Default().With("component", "pr5-import-wiring-test"),
	}
	due := time.Now().UTC().Add(durablePrelayoutConfirmationDelay)
	service.HandleFailure(context.Background(), &database.ImportQueueItem{ID: 57}, &durableImportHoldError{
		resumeRequired: true, retryAt: &due,
	})

	var status database.QueueStatus
	var marker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 57`,
	).Scan(&status, &marker))
	assert.Equal(t, database.QueueStatusPaused, status)
	assert.Equal(t, durableFinalLayoutIncompleteWaitingMarker, marker)
	_, err = db.Connection().Exec(
		`UPDATE import_queue SET updated_at = ? WHERE id = 57`,
		time.Now().UTC().Add(-durablePrelayoutConfirmationDelay-time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, service.resumeDueImportValidations(context.Background()))
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 57`,
	).Scan(&status, &marker))
	assert.Equal(t, database.QueueStatusPending, status)
	assert.Equal(t, durableFinalLayoutIncompleteRetryDueMarker, marker)
}

type pr5DueValidationRepository struct {
	validations []database.ImportValidation
	err         error
}

func (r pr5DueValidationRepository) ListDueImportValidations(
	context.Context, time.Time, int,
) ([]database.ImportValidation, error) {
	return append([]database.ImportValidation(nil), r.validations...), r.err
}

func TestPR5DueConfirmationResumerIsRestartSafeAndCannotResurrectTerminalQueueItems(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "import-due.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority) VALUES
			(53, 'paused.nzb', 'paused', 1),
			(54, 'completed.nzb', 'completed', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db,
		durableImportRepository: pr5DueValidationRepository{validations: []database.ImportValidation{
			{QueueItemID: 53}, {QueueItemID: 54},
		}},
		log: slog.Default().With("component", "pr5-import-wiring-test"),
	}
	require.NoError(t, service.resumeDueImportValidations(context.Background()))

	for id, want := range map[int64]database.QueueStatus{
		53: database.QueueStatusPending,
		54: database.QueueStatusCompleted,
	} {
		var got database.QueueStatus
		require.NoError(t, db.Connection().QueryRow(
			`SELECT status FROM import_queue WHERE id = ?`, id,
		).Scan(&got))
		assert.Equal(t, want, got)
	}
	// A second post-restart poll is idempotent.
	require.NoError(t, service.resumeDueImportValidations(context.Background()))
}

func TestPR5NewServiceActivatesDurableAdmissionAgainstEnabledProviders(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "import-service.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	cfg := pr5STATConfig()
	cfg.Database.Path = filepath.Join(t.TempDir(), "altmount.db")
	metadataService := metadata.NewMetadataService(t.TempDir())
	service, err := NewService(
		ServiceConfig{Workers: 1}, metadataService, db,
		processorTestPoolManager{client: fakepool.New()}, nil,
		func() *config.Config { return cfg }, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.True(t, service.durableImportAdmissionEnabled)
	assert.True(t, service.processor.durableAdmissionEnabled.Load())
	assert.NotNil(t, service.durableImportStateRepository)
	assert.NotNil(t, service.durableImportRepository)
}

func TestPR5DurableImportProviderSpecsUseContiguousActiveOrder(t *testing.T) {
	enabled := true
	disabled := false
	primary := false
	backup := true
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{ID: "primary-a", Host: "a.invalid", Enabled: &enabled, IsBackupProvider: &primary},
		{ID: "disabled-gap", Host: "disabled.invalid", Enabled: &disabled, IsBackupProvider: &primary},
		{ID: "backup-a", Host: "b.invalid", Enabled: &enabled, IsBackupProvider: &backup},
	}

	specs := durableImportProviderSpecs(cfg)
	require.Len(t, specs, 2)
	assert.Equal(t, "primary-a", specs[0].StableID)
	assert.Equal(t, 0, specs[0].Order)
	assert.Equal(t, database.ProviderRolePrimary, specs[0].Role)
	assert.Equal(t, "backup-a", specs[1].StableID)
	assert.Equal(t, 1, specs[1].Order,
		"disabled config entries must not create registry-order gaps")
	assert.Equal(t, database.ProviderRoleBackup, specs[1].Role)
}

func TestPR5DurableImportLegacyProviderBackfillRetainsRegistryIdentityAcrossGeneration(t *testing.T) {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{{
		Name: "Legacy provider", Host: "first.invalid", Port: 119,
		Username: "synthetic-account-a", Enabled: &enabled,
	}}
	initialSpecs := durableImportProviderSpecs(cfg)
	require.Len(t, initialSpecs, 1)
	assert.Empty(t, initialSpecs[0].StableID,
		"legacy transport endpoint/account must not become durable identity")

	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "provider-identity.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repository := database.NewHealthStateRepository(db.Connection(), db.Dialect())
	initial, err := repository.ReconcileProviders(context.Background(), initialSpecs)
	require.NoError(t, err)
	require.Len(t, initial, 1)
	require.NotEmpty(t, initial[0].ID)
	assert.NotEqual(t, cfg.Providers[0].NNTPPoolName(), initial[0].ID)

	// Startup backfill persists this registry-issued ID before nntppool and the
	// import gate are built. Later endpoint/account edits are a new generation
	// of the same provider, never a new endpoint-derived durable identity.
	cfg.Providers[0].ID = initial[0].ID
	cfg.Providers[0].Host = "second.invalid"
	cfg.Providers[0].Username = "synthetic-account-b"
	changed, err := repository.ReconcileProviders(
		context.Background(), durableImportProviderSpecs(cfg),
	)
	require.NoError(t, err)
	require.Len(t, changed, 1)
	assert.Equal(t, initial[0].ID, changed[0].ID)
	assert.Equal(t, initial[0].CurrentGeneration+1, changed[0].CurrentGeneration)
}

func TestPR5ReusableLayoutLookupUsesDatabaseDialectPlaceholder(t *testing.T) {
	sqliteQuery := durableImportReusableLayoutsQuery(database.DialectSQLite)
	assert.Contains(t, sqliteQuery, "v.queue_item_id = ?")
	assert.NotContains(t, sqliteQuery, "$1")

	postgresQuery := durableImportReusableLayoutsQuery(database.DialectPostgres)
	assert.Contains(t, postgresQuery, "v.queue_item_id = $1")
	assert.NotContains(t, postgresQuery, "v.queue_item_id = ?")
}

func TestPR5ValidationInfrastructureErrorsRemainResumableUnlessCallerCanceled(t *testing.T) {
	gate := newDurableImportWriteValidator(&pr5AdmissionStub{err: errors.New("raw infrastructure detail")})
	err := preparePR5MetadataWrite(t, gate,
		withDurableImportIntent(context.Background(), 55, validation.FinalLayoutProvenanceStandalone),
		"library/movie.mkv", pr5WiringMetadata(),
	)
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	assert.True(t, hold.resumeRequired)
	assert.NotContains(t, err.Error(), "raw infrastructure detail")
}

func TestPR5PostWriteRevisionActivationFailureIsResumableAndIdempotent(t *testing.T) {
	metadataService := metadata.NewMetadataService(t.TempDir())
	fileMeta := pr5WiringMetadata()
	fileMeta.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	layout, err := metadata.ResolveCanonicalSegmentLayout(fileMeta)
	require.NoError(t, err)
	stub := &pr5AdmissionStub{
		result: validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
		},
		activateErr: errors.New("raw activation database detail"),
	}
	metadataService.SetWriteValidator(newDurableImportWriteValidator(stub))
	ctx := withDurableImportReusableLayoutBindings(
		context.Background(), 56, map[string]admissionctx.ReusableLayoutBinding{
			"library/movie.mkv": {
				Fingerprint: layout.Fingerprint, ActivationPending: true,
			},
		},
	)
	ctx = withDurableImportIntent(ctx, 56, validation.FinalLayoutProvenanceStandalone)

	err = metadataService.WriteFileMetadataAuto(
		ctx, "library/movie.mkv", fileMeta, nil, "",
	)
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	assert.True(t, hold.resumeRequired)
	assert.False(t, metadataService.FileExists("library/movie.mkv"),
		"failed candidate activation must roll back visibility")
	assert.Equal(t, []string{"candidate-revision"}, stub.activations)
	assert.NotContains(t, err.Error(), "raw activation database detail")

	// Simulate a process crash after atomic visibility but before activation.
	// Restart recovery must activate this exact candidate without rewriting it.
	visible := pr5WiringMetadata()
	visible.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	visible.SourceNzbPath = "preserve-visible-retry-marker"
	require.NoError(t, metadataService.WriteFileMetadata("library/movie.mkv", visible))

	stub.activateErr = nil
	require.NoError(t, metadataService.WriteFileMetadataAuto(
		ctx, "library/movie.mkv", fileMeta, nil, "",
	))
	assert.Equal(t, []string{"candidate-revision", "candidate-revision"}, stub.activations,
		"retry must finalize the same accepted candidate idempotently")
	visible, err = metadataService.ReadFileMetadata("library/movie.mkv")
	require.NoError(t, err)
	assert.Equal(t, "preserve-visible-retry-marker", visible.SourceNzbPath,
		"an activation-only retry must not rewrite already-visible metadata")
}

func TestPR5CrashVisibleActivationRetryFailureRemovesUnactivatedPath(t *testing.T) {
	metadataService := metadata.NewMetadataService(t.TempDir())
	fileMeta := pr5WiringMetadata()
	fileMeta.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	layout, err := metadata.ResolveCanonicalSegmentLayout(fileMeta)
	require.NoError(t, err)
	require.NoError(t, metadataService.WriteFileMetadata("library/movie.mkv", fileMeta))
	stub := &pr5AdmissionStub{
		result: validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
		},
		activateErr: errors.New("raw activation database detail"),
	}
	metadataService.SetWriteValidator(newDurableImportWriteValidator(stub))
	ctx := withDurableImportReusableLayoutBindings(
		context.Background(), 58, map[string]admissionctx.ReusableLayoutBinding{
			"library/movie.mkv": {
				Fingerprint: layout.Fingerprint, ActivationPending: true,
			},
		},
	)
	ctx = withDurableImportIntent(ctx, 58, validation.FinalLayoutProvenanceStandalone)

	err = metadataService.WriteFileMetadataAuto(ctx, "library/movie.mkv", fileMeta, nil, "")
	var hold *durableImportHoldError
	require.ErrorAs(t, err, &hold)
	assert.False(t, metadataService.FileExists("library/movie.mkv"),
		"failed crash-recovery activation must remove the still-unactivated path")
	assert.NotContains(t, err.Error(), "raw activation database detail")
}

func TestPR5CrashVisibleDurableActivationFailureRestoresJournaledPrior(t *testing.T) {
	metadataService := metadata.NewMetadataService(filepath.Join(t.TempDir(), "metadata"))
	path := "library/replacement.mkv"
	prior := pr5WiringMetadata()
	prior.Status = metapb.FileStatus_FILE_STATUS_CORRUPTED
	prior.SegmentData[0].Id = "fixture-prior"
	require.NoError(t, metadataService.WriteFileMetadata(path, prior))
	priorBytes, err := os.ReadFile(metadataService.GetMetadataFilePath(path))
	require.NoError(t, err)
	priorLayout, err := metadata.ResolveCanonicalSegmentLayout(prior)
	require.NoError(t, err)

	journal := newDurableImportRollbackJournal(metadataService, nil)
	require.NoError(t, journal.Record(
		context.Background(), 96, path, priorBytes, true, priorLayout.Fingerprint, "",
	))
	candidate := pr5WiringMetadata()
	candidate.Status = metapb.FileStatus_FILE_STATUS_HEALTHY
	candidate.SegmentData[0].Id = "fixture-candidate"
	candidateLayout, err := metadata.ResolveCanonicalSegmentLayout(candidate)
	require.NoError(t, err)
	require.NoError(t, metadataService.WriteFileMetadata(path, candidate))

	stub := &pr5AdmissionStub{
		result: validation.DurableFinalLayoutValidationResult{
			Status: validation.ImportAdmissionAccept, FileRevisionID: "candidate-revision",
		},
		activateErr: errors.New("transient activation failure"),
	}
	gate := newDurableImportWriteValidator(stub)
	gate.journal = journal
	metadataService.SetWriteValidator(gate)
	ctx := withDurableImportReusableLayoutBindings(
		context.Background(), 96, map[string]admissionctx.ReusableLayoutBinding{
			path: {Fingerprint: candidateLayout.Fingerprint, ActivationPending: true},
		},
	)
	ctx = withDurableImportIntent(ctx, 96, validation.FinalLayoutProvenanceStandalone)
	err = metadataService.WriteFileMetadataAuto(ctx, path, candidate, nil, "")
	require.Error(t, err)
	visible, err := metadataService.InspectMetadataVisibility(path)
	require.NoError(t, err)
	assert.Equal(t, priorLayout.Fingerprint, visible.LayoutFingerprint)
}

func TestPR5PrelayoutHardAbsenceWaitsOnceThenBecomesTerminal(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "prelayout-confirmation.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority) VALUES
			(61, 'first-pass.nzb', 'processing', 1),
			(62, 'not-due.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db,
		log:      slog.Default().With("component", "pr5-prelayout-test"),
	}
	failure := &durablePrelayoutValidationError{confirmationRequired: true}

	assert.True(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{ID: 61}, failure,
	))
	assert.True(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{ID: 62}, failure,
	))

	var firstStatus, firstMarker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 61`,
	).Scan(&firstStatus, &firstMarker))
	assert.Equal(t, string(database.QueueStatusPaused), firstStatus)
	assert.Equal(t, durablePrelayoutWaitingMarker, firstMarker)

	_, err = db.Connection().Exec(
		`UPDATE import_queue SET updated_at = ? WHERE id = 61`,
		time.Now().UTC().Add(-durablePrelayoutConfirmationDelay-time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, service.resumeDueImportValidations(context.Background()))

	var dueMarker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 61`,
	).Scan(&firstStatus, &dueMarker))
	assert.Equal(t, string(database.QueueStatusPending), firstStatus)
	assert.Equal(t, durablePrelayoutDueMarker, dueMarker)

	var secondStatus, secondMarker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 62`,
	).Scan(&secondStatus, &secondMarker))
	assert.Equal(t, string(database.QueueStatusPaused), secondStatus,
		"a confirmation must not run before the full delay")
	assert.Equal(t, durablePrelayoutWaitingMarker, secondMarker)

	assert.False(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{
			ID: 61, ErrorMessage: stringPointer(durablePrelayoutDueMarker),
		}, failure,
	), "the completed second hard-absence attempt must enter ordinary rejection handling")
}

func TestPR5PrelayoutIncompleteUsesBoundedResumableDelay(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "prelayout-incomplete.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (63, 'incomplete.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db,
		log:      slog.Default().With("component", "pr5-prelayout-test"),
	}

	assert.True(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{ID: 63},
		&durablePrelayoutValidationError{resumeRequired: true},
	))
	var status string
	var marker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 63`,
	).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPaused), status)
	assert.Equal(t, durablePrelayoutInitialIncompleteWaitingMarker, marker)
	assert.NotContains(t, marker, "article")
	assert.NotContains(t, marker, "provider")

	// It cannot hot-loop before the bounded delay.
	require.NoError(t, service.resumeDueImportValidations(context.Background()))
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 63`,
	).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPaused), status)

	_, err = db.Connection().Exec(
		`UPDATE import_queue SET updated_at = ? WHERE id = 63`,
		time.Now().UTC().Add(-durablePrelayoutConfirmationDelay-time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, service.resumeDueImportValidations(context.Background()))
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 63`,
	).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPending), status)
	assert.Equal(t, durablePrelayoutInitialRetryDueMarker, marker)
}

func TestPR5PrelayoutIncompleteConfirmationPreservesPhaseWithoutRejecting(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "prelayout-confirm-incomplete.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority, error_message)
		VALUES (64, 'confirm-incomplete.nzb', 'processing', 1, ?)
	`, durablePrelayoutDueMarker)
	require.NoError(t, err)
	service := &Service{
		database: db,
		log:      slog.Default().With("component", "pr5-prelayout-test"),
	}

	assert.True(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{
			ID: 64, ErrorMessage: stringPointer(durablePrelayoutDueMarker),
		}, &durablePrelayoutValidationError{resumeRequired: true},
	))
	var status, marker string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 64`,
	).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPaused), status)
	assert.Equal(t, durablePrelayoutConfirmIncompleteWaitingMarker, marker)

	_, err = db.Connection().Exec(
		`UPDATE import_queue SET updated_at = ? WHERE id = 64`,
		time.Now().UTC().Add(-durablePrelayoutConfirmationDelay-time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, service.resumeDueImportValidations(context.Background()))
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 64`,
	).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPending), status)
	assert.Equal(t, durablePrelayoutDueMarker, marker)

	assert.False(t, service.handleDurablePrelayoutFailure(
		context.Background(), &database.ImportQueueItem{
			ID: 64, ErrorMessage: stringPointer(durablePrelayoutDueMarker),
		}, &durablePrelayoutValidationError{confirmationRequired: true},
	), "only a later conclusive confirmation may become terminal")
}

func TestPR5DurablePrelayout451IsSanitizedAndRateLimited(t *testing.T) {
	env := newBatteryEnv(t)
	raw := "provider=user:super-secret message=<private-article@example>"
	env.client.SetBehavior("private-article@example", fakepool.SegmentBehavior{Err: &nntppool.TransportError{
		Kind: nntppool.OutcomeTemporaryFailure, ProviderID: "provider=user:super-secret",
		ResponseCode: 451, Cause: errors.New(raw),
	}})
	env.proc.SetDurableAdmissionEnabled(true)
	nzb := nzbbuild.Build(nzbbuild.File{
		Subject:  "Some.Show.S01E01.mkv",
		Segments: []nzbbuild.Segment{{ID: "private-article@example", Bytes: 1024}},
	})

	_, written, err := env.runImport(nzb, "Some.Show.S01E01.mkv")
	var failure *durablePrelayoutValidationError
	require.ErrorAs(t, err, &failure)
	assert.True(t, failure.resumeRequired)
	assert.False(t, failure.confirmationRequired)
	assert.Empty(t, written)
	assert.NotContains(t, err.Error(), raw)
	assert.NotContains(t, err.Error(), "private-article")
	assert.NotContains(t, err.Error(), "super-secret")

	db, dbErr := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "prelayout-sanitized.db"),
	})
	require.NoError(t, dbErr)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, dbErr = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (65, 'sanitized.nzb', 'processing', 1)
	`)
	require.NoError(t, dbErr)
	service := &Service{database: db, log: slog.Default().With("component", "pr5-prelayout-test")}
	service.HandleFailure(context.Background(), &database.ImportQueueItem{ID: 65}, err)
	var status, persisted string
	require.NoError(t, db.Connection().QueryRow(
		`SELECT status, error_message FROM import_queue WHERE id = 65`,
	).Scan(&status, &persisted))
	assert.Equal(t, string(database.QueueStatusPaused), status)
	assert.Equal(t, durablePrelayoutInitialIncompleteWaitingMarker, persisted)
	assert.NotContains(t, persisted, raw)
	assert.NotContains(t, persisted, "private-article")
	assert.NotContains(t, persisted, "super-secret")
}

func TestPR5DurablePrelayoutCompleteMixedAllProviderBODYPassRequestsConfirmation(t *testing.T) {
	env := newBatteryEnv(t)
	enabled := true
	env.cfg.Providers = []config.ProviderConfig{
		{ID: "fixture-primary", Enabled: &enabled},
		{ID: "fixture-backup", Enabled: &enabled},
	}
	env.client.SetBehavior("fixture-terminal-mixed", fakepool.SegmentBehavior{
		Err: &nntppool.TransportError{
			Kind: nntppool.OutcomeInconclusive,
			Attempts: []nntppool.AttemptEvidence{
				{ProviderID: "fixture-primary", Operation: nntppool.OperationBody,
					Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451},
				{ProviderID: "fixture-backup", Operation: nntppool.OperationBody,
					Outcome: nntppool.OutcomeProviderUnavailable},
			},
		},
	})
	env.proc.SetDurableAdmissionEnabled(true)
	nzb := nzbbuild.Build(nzbbuild.File{
		Subject:  "Some.Show.S01E01.mkv",
		Segments: []nzbbuild.Segment{{ID: "fixture-terminal-mixed", Bytes: 1024}},
	})

	_, written, err := env.runImport(nzb, "Some.Show.S01E01.mkv")
	var failure *durablePrelayoutValidationError
	require.ErrorAs(t, err, &failure)
	assert.True(t, failure.confirmationRequired)
	assert.False(t, failure.resumeRequired)
	assert.Empty(t, written)
}

func TestPR5SuccessFinalizationFailureParksWithoutTerminalRollback(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "success-finalization.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().Exec(`
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (92, 'finalization.nzb', 'processing', 1)
	`)
	require.NoError(t, err)
	service := &Service{
		database: db, durableImportAdmissionEnabled: true,
		log: slog.Default().With("component", "pr5-success-finalization-test"),
	}
	item := &database.ImportQueueItem{ID: 92}
	service.HandleFailure(
		context.Background(), item,
		service.durableSuccessFinalizationError(errors.New("private completion failure")),
	)
	var status, marker string
	require.NoError(t, db.Connection().QueryRow(`
		SELECT status, error_message FROM import_queue WHERE id = 92
	`).Scan(&status, &marker))
	assert.Equal(t, string(database.QueueStatusPaused), status)
	assert.Equal(t, durableSuccessFinalizationWaitingMarker, marker)
}

func TestPR5TolerantPolicyCannotBypassUnfingerprintableLayout(t *testing.T) {
	env := newBatteryEnv(t)
	env.cfg.Import.DamagePolicy = string(config.ImportDamagePolicyTolerant)
	env.client.SetBehavior("missing-layout@example", fakepool.SegmentBehavior{
		Err: nntppool.ErrArticleNotFound,
	})
	stub := &pr5AdmissionStub{result: validation.DurableFinalLayoutValidationResult{
		Status: validation.ImportAdmissionHealthPending,
	}}
	env.svc.SetWriteValidator(newDurableImportWriteValidator(stub))
	env.proc.SetDurableAdmissionEnabled(true)
	nzb := nzbbuild.Build(nzbbuild.File{
		Subject:  "Some.Show.S01E01.mkv",
		Segments: []nzbbuild.Segment{{ID: "missing-layout@example", Bytes: 1024}},
	})

	_, written, err := env.runImport(nzb, "Some.Show.S01E01.mkv")
	var failure *durablePrelayoutValidationError
	require.ErrorAs(t, err, &failure)
	assert.True(t, failure.confirmationRequired)
	assert.Empty(t, written)
	assert.Zero(t, stub.calls, "tolerant admission requires a canonical fingerprint first")
}

func stringPointer(value string) *string { return &value }
