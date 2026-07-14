package parser

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
	"github.com/javi11/nzbparser"
	"github.com/stretchr/testify/require"
)

// provisionalMetadataClient reproduces the ordering that makes onMeta
// dangerous as final evidence: valid-looking yEnc headers arrive first, then
// complete BODY validation reports corruption.
type provisionalMetadataClient struct {
	*fakepool.Client
}

func (c *provisionalMetadataClient) BodyAsync(
	ctx context.Context,
	messageID string,
	w io.Writer,
	onMeta ...func(nntppool.YEncMeta),
) <-chan nntppool.BodyResult {
	meta := nntppool.YEncMeta{PartSize: 1024, FileSize: 1024}
	for _, callback := range onMeta {
		if callback != nil {
			callback(meta)
		}
	}

	results := make(chan nntppool.BodyResult, 1)
	go func() {
		defer close(results)
		select {
		case <-ctx.Done():
			results <- nntppool.BodyResult{Err: ctx.Err()}
		case <-time.After(10 * time.Millisecond):
			results <- nntppool.BodyResult{Err: &nntppool.TransportError{
				Kind:  nntppool.OutcomeCorruptBody,
				Cause: nntppool.ErrBodyCorrupt,
			}}
		}
	}()
	return results
}

func TestPR3FetchYencHeadersWaitsForValidatedBody(t *testing.T) {
	client := &provisionalMetadataClient{Client: fakepool.New()}
	parser := NewParser(newFakeFullPoolManager(client), stormConfigGetter(1))

	_, err := parser.fetchYencHeaders(
		context.Background(),
		nzbparser.NzbSegment{ID: "synthetic-provisional@test.invalid"},
		nil,
	)

	require.Error(t, err, "provisional yEnc metadata must not survive terminal BODY corruption")
	require.True(t, usenet.IsIncomplete(err))
	require.False(t, errors.Is(err, nntppool.ErrArticleNotFound),
		"corrupt BODY must not be converted to hard absence")
}
