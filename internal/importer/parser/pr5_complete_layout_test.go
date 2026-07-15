package parser

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v4"
	"github.com/javi11/nzbparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5CompleteAllProviderBODYFailureRequiresExactCompleteAttemptEvidence(t *testing.T) {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{ID: "provider-a", Enabled: &enabled},
		{ID: "provider-b", Enabled: &enabled},
	}
	dispatchProviders := captureBODYProviderSnapshot(cfg)
	complete := &nntppool.TransportError{
		Kind: nntppool.OutcomeInconclusive,
		Attempts: []nntppool.AttemptEvidence{
			{ProviderID: "provider-a", Operation: nntppool.OperationBody,
				Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451},
			{ProviderID: "provider-b", Operation: nntppool.OperationBody,
				Outcome: nntppool.OutcomeProviderUnavailable},
		},
	}
	assert.True(t, completeAllProviderBODYFailure(complete, dispatchProviders),
		"mixed terminal causes remain typed but complete one bounded import pass")

	omitted := *complete
	omitted.Attempts = omitted.Attempts[:1]
	assert.False(t, completeAllProviderBODYFailure(&omitted, dispatchProviders))
	canceled := *complete
	canceled.Attempts = append([]nntppool.AttemptEvidence(nil), complete.Attempts...)
	canceled.Attempts[1].Outcome = nntppool.OutcomeCancellation
	assert.False(t, completeAllProviderBODYFailure(&canceled, dispatchProviders))
	wrongOperation := *complete
	wrongOperation.Attempts = append([]nntppool.AttemptEvidence(nil), complete.Attempts...)
	wrongOperation.Attempts[1].Operation = nntppool.OperationStat
	assert.False(t, completeAllProviderBODYFailure(&wrongOperation, dispatchProviders))
	assert.False(t, completeAllProviderBODYFailure(stderrors.New("bare transport failure"), dispatchProviders))

	// The dispatch snapshot is immutable even if a new runtime config becomes
	// current before the transport reports its terminal attempts.
	cfg.Providers = []config.ProviderConfig{{ID: "provider-c", Enabled: &enabled}}
	assert.True(t, completeAllProviderBODYFailure(complete, dispatchProviders))
	assert.False(t, completeAllProviderBODYFailure(complete, captureBODYProviderSnapshot(cfg)))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, terminalAllProviderBODYFailure(canceledCtx, complete, dispatchProviders),
		"caller cancellation must outrank concurrently delivered terminal-looking evidence")
}

func TestPR5CompleteLayoutRejectsMissingFirstSegmentInsteadOfDroppingFile(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("healthy-first", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "Episode.One.mkv", PartSize: 1024},
	})
	fp.SetBehavior("missing-first", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{
			Filename: "Episode.One.mkv",
			Segments: nzbparser.NzbSegments{{Bytes: 1024, Number: 1, ID: "healthy-first"}},
		},
		{
			Filename: "Episode.Two.mkv",
			Segments: nzbparser.NzbSegments{{Bytes: 1024, Number: 1, ID: "missing-first"}},
		},
	}}

	parsed, err := p.ParseNzb(context.Background(), n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutConfirmationRequired(err))
	assert.False(t, IsFinalLayoutIncomplete(err))
	assert.NotContains(t, err.Error(), "missing-first", "article identities must not escape")
}

func TestPR5CompleteLayoutAllowsMissingExcludedAuxiliaryFiles(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("playback-first", fakepool.SegmentBehavior{
		Bytes: []byte("playback"),
		YEnc: nntppool.YEncMeta{
			FileName: "Episode.One.mkv", PartSize: 8, FileSize: 8,
		},
	})
	for _, id := range []string{"missing-par2", "missing-nfo", "missing-sample"} {
		fp.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{
			Filename: "Episode.One.mkv",
			Segments: nzbparser.NzbSegments{{Bytes: 8, Number: 1, ID: "playback-first"}},
		},
		{
			Filename: "Episode.One.vol00+01.par2",
			Segments: nzbparser.NzbSegments{{Bytes: 8, Number: 1, ID: "missing-par2"}},
		},
		{
			Filename: "Episode.One.nfo",
			Segments: nzbparser.NzbSegments{{Bytes: 8, Number: 1, ID: "missing-nfo"}},
		},
		{
			Filename: "Episode.One.sample.mkv",
			Segments: nzbparser.NzbSegments{{Bytes: 8, Number: 1, ID: "missing-sample"}},
		},
	}}

	parsed, err := p.ParseNzb(context.Background(), n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
		OptionalFileIndexes: map[int]struct{}{
			1: {}, 2: {}, 3: {},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Len(t, parsed.Files, 1)
	assert.Equal(t, "Episode.One.mkv", parsed.Files[0].Filename)
}

func TestPR5CompleteLayoutRejectsHardAbsentNormalizationInsteadOfDroppingFile(t *testing.T) {
	const (
		firstPartEncoded = 720000
		lastPartEncoded  = 51000
	)
	fp := fakepool.New()
	fp.SetBehavior("broken-first", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "Episode.One.mkv", PartSize: 700000},
	})
	fp.SetBehavior("broken-last", fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	fp.SetBehavior("healthy-first", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "Episode.Two.mkv", PartSize: 700000},
	})
	fp.SetBehavior("healthy-last", fakepool.SegmentBehavior{
		YEnc: nntppool.YEncMeta{FileName: "Episode.Two.mkv", PartSize: 50000},
	})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(4))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{
		{
			Filename: "Episode.One.mkv",
			Segments: nzbparser.NzbSegments{
				{Bytes: firstPartEncoded, Number: 1, ID: "broken-first"},
				{Bytes: lastPartEncoded, Number: 2, ID: "broken-last"},
			},
		},
		{
			Filename: "Episode.Two.mkv",
			Segments: nzbparser.NzbSegments{
				{Bytes: firstPartEncoded, Number: 1, ID: "healthy-first"},
				{Bytes: lastPartEncoded, Number: 2, ID: "healthy-last"},
			},
		},
	}}

	parsed, err := p.ParseNzb(context.Background(), n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutConfirmationRequired(err))
	assert.NotContains(t, err.Error(), "broken-last", "article identities must not escape")
}

func TestPR5CompleteLayoutRejectsUnnormalizedPresentArticle(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("present-without-yenc", fakepool.SegmentBehavior{Bytes: make([]byte, 16)})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(1))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: "Some.Show.S01E01.mkv",
		Segments: nzbparser.NzbSegments{{Bytes: 12345, Number: 1, ID: "present-without-yenc"}},
	}}}

	parsed, err := p.ParseNzb(context.Background(), n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutConfirmationRequired(err))
	assert.NotContains(t, err.Error(), "present-without-yenc", "article identities must not escape")
}

func TestPR5CompleteLayoutKeepsIncompleteFetchResumable(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("temporary-first", fakepool.SegmentBehavior{
		Err: stderrors.New("raw transient fixture detail"),
	})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(1))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: "Some.Show.S01E01.mkv",
		Segments: nzbparser.NzbSegments{{Bytes: 1024, Number: 1, ID: "temporary-first"}},
	}}}

	parsed, err := p.ParseNzb(context.Background(), n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutIncomplete(err))
	assert.False(t, IsFinalLayoutConfirmationRequired(err))
	assert.NotContains(t, err.Error(), "raw transient fixture detail")
	assert.NotContains(t, err.Error(), "temporary-first")
}

func TestPR5CompleteLayoutInternalSafetyDeadlineRemainsResumable(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("deadline-first", fakepool.SegmentBehavior{
		Bytes: []byte("first"),
		YEnc: nntppool.YEncMeta{
			FileName: "Some.Show.S01E01.mkv", PartSize: 100,
		},
	})
	fp.SetBehavior("deadline-last", fakepool.SegmentBehavior{
		Latency: time.Minute,
		YEnc: nntppool.YEncMeta{
			FileName: "Some.Show.S01E01.mkv", PartSize: 50,
		},
	})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(2))
	p.networkTimeout = 25 * time.Millisecond
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: "Some.Show.S01E01.mkv",
		Segments: nzbparser.NzbSegments{
			{Bytes: 120, Number: 1, ID: "deadline-first"},
			{Bytes: 70, Number: 2, ID: "deadline-last"},
		},
	}}}
	callerCtx := context.Background()

	parsed, err := p.ParseNzb(callerCtx, n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutIncomplete(err))
	assert.False(t, IsFinalLayoutConfirmationRequired(err))
	assert.NoError(t, callerCtx.Err(), "the caller remained live; only the internal safety deadline fired")
}

func TestPR5CompleteLayoutRejectsStructurallyDroppedFile(t *testing.T) {
	fp := fakepool.New()
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(1))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{Filename: "empty.mkv"}}}

	parsed, err := p.ParseNzb(context.Background(), n, "invalid.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.True(t, IsFinalLayoutInvalid(err))
	assert.False(t, IsFinalLayoutConfirmationRequired(err))
}

func TestPR5CompleteLayoutCallerCancellationCannotBecomeRejectionEvidence(t *testing.T) {
	fp := fakepool.New()
	fp.SetBehavior("canceled-first", fakepool.SegmentBehavior{
		Latency: time.Minute,
		YEnc:    nntppool.YEncMeta{FileName: "Some.Show.S01E01.mkv", PartSize: 1024},
	})
	p := NewParser(newFakeFullPoolManager(fp), stormConfigGetter(1))
	n := &nzbparser.Nzb{Files: nzbparser.NzbFiles{{
		Filename: "Some.Show.S01E01.mkv",
		Segments: nzbparser.NzbSegments{{Bytes: 1024, Number: 1, ID: "canceled-first"}},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	parsed, err := p.ParseNzb(ctx, n, "Show.S01.nzb", nil, ParseOptions{
		RequireCompleteFinalLayout: true,
	})
	require.Error(t, err)
	assert.Nil(t, parsed)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, IsFinalLayoutConfirmationRequired(err))
	assert.False(t, IsFinalLayoutIncomplete(err),
		"explicit caller cancellation remains a cancellation, never provider evidence")
}
