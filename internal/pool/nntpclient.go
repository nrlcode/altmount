package pool

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/javi11/nntppool/v4"
)

// NntpClient is the narrow surface of the underlying nntppool.Client that the
// rest of AltMount calls through Manager.GetPool. Defining it here lets tests
// inject a deterministic fake (see internal/testsupport/fakepool) without
// standing up real NNTP connections, and pins exactly which operations the
// streaming, import, validation, and metrics paths depend on.
//
// Implementations must be safe for concurrent use. The production
// implementation is *nntppool.Client; the contract below intentionally mirrors
// its signatures so the existing client satisfies the interface without an
// adapter.
//
// Keep this interface small. Anything that needs a behavior not listed here
// should add the method explicitly so callers stay observable.
type NntpClient interface {
	// Body fetches an article body via the default (non-priority) lane.
	// Used by the importer to download NZB segments during scanning.
	Body(ctx context.Context, messageID string, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)

	// BodyAsync fetches an article body asynchronously, streaming the decoded
	// payload to w. The returned channel yields exactly one BodyResult.
	BodyAsync(ctx context.Context, messageID string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) <-chan nntppool.BodyResult

	// BodyPriority fetches an article body via the priority lane. Streaming
	// reads use this so live playback isn't queued behind a background import.
	BodyPriority(ctx context.Context, messageID string, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)

	// Stat checks whether an article exists on at least one provider without
	// downloading the body. Used by health checks and validation.
	Stat(ctx context.Context, messageID string) (*nntppool.StatResult, error)

	// StatMany checks the existence of many articles concurrently, streaming a
	// result per message-id as each completes. Used by health checks and
	// fast-fail import validation to batch existence sweeps instead of
	// issuing one Stat per segment.
	StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult

	// Stats returns a snapshot of pool/provider statistics used by the metrics
	// tracker and the system handlers.
	Stats() nntppool.ClientStats

	// SpeedTest measures provider throughput through the current pool.
	SpeedTest(ctx context.Context, opts nntppool.SpeedTestOptions) (*nntppool.SpeedTestResult, error)
}

// Compile-time assertion: the real client must satisfy the narrow interface.
// If nntppool changes a signature, this line will fail to build and the
// interface above must be updated to match.
var _ NntpClient = (*nntppool.Client)(nil)

// generationClient is the complete surface owned by one replaceable pool
// generation. Callers only receive the narrower, stable NntpClient facade.
type generationClient interface {
	NntpClient
	RestoreProviderQuotas(map[string]nntppool.ProviderQuotaState) error
	ResetProviderQuota(string) error
	Close() error
}

var _ generationClient = (*nntppool.Client)(nil)

// leasedClient is the single manager-owned facade. A handover pauses new
// admissions, waits for existing leases to settle, then publishes exactly one
// new generation (or reopens the old one on abort).
type leasedClient struct {
	mu      sync.Mutex
	current generationClient
	paused  bool
	resume  chan struct{}
	active  int
	drained chan struct{}
}

func (c *leasedClient) acquire(ctx context.Context) (generationClient, func(), error) {
	for {
		c.mu.Lock()
		if !c.paused {
			generation := c.current
			if generation == nil {
				c.mu.Unlock()
				return nil, nil, fmt.Errorf("NNTP connection pool not available - no providers configured")
			}
			c.active++
			c.mu.Unlock()

			var once sync.Once
			return generation, func() {
				once.Do(func() {
					c.mu.Lock()
					c.active--
					if c.paused && c.active == 0 && c.drained != nil {
						close(c.drained)
						c.drained = nil
					}
					c.mu.Unlock()
				})
			}, nil
		}
		resume := c.resume
		c.mu.Unlock()

		select {
		case <-resume:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// pause blocks new admissions and returns the settled-current notification.
// Handover serialization guarantees it is not called while already paused.
func (c *leasedClient) pause() (generationClient, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
	c.resume = make(chan struct{})
	drained := make(chan struct{})
	c.drained = drained
	if c.active == 0 {
		close(drained)
		c.drained = nil
	}
	return c.current, drained
}

func (c *leasedClient) publish(generation generationClient) {
	c.mu.Lock()
	c.current = generation
	resume := c.resume
	c.resume = nil
	c.drained = nil
	c.paused = false
	c.mu.Unlock()
	if resume != nil {
		close(resume)
	}
}

func (c *leasedClient) currentGeneration() generationClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *leasedClient) hasGeneration() bool {
	return c.currentGeneration() != nil
}

// isPaused is package-private test observability for deterministic handover
// assertions. It is not part of NntpClient's public surface.
func (c *leasedClient) isPaused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

func (c *leasedClient) Body(ctx context.Context, messageID string, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return generation.Body(ctx, messageID, onMeta...)
}

func (c *leasedClient) BodyPriority(ctx context.Context, messageID string, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return generation.BodyPriority(ctx, messageID, onMeta...)
}

func (c *leasedClient) Stat(ctx context.Context, messageID string) (*nntppool.StatResult, error) {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return generation.Stat(ctx, messageID)
}

func (c *leasedClient) Stats() nntppool.ClientStats {
	generation, release, err := c.acquire(context.Background())
	if err != nil {
		return nntppool.ClientStats{}
	}
	defer release()
	return generation.Stats()
}

func (c *leasedClient) SpeedTest(ctx context.Context, opts nntppool.SpeedTestOptions) (*nntppool.SpeedTestResult, error) {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return generation.SpeedTest(ctx, opts)
}

func (c *leasedClient) BodyAsync(ctx context.Context, messageID string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) <-chan nntppool.BodyResult {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		out := make(chan nntppool.BodyResult, 1)
		out <- nntppool.BodyResult{Err: err}
		close(out)
		return out
	}
	return relayGenerationResults(ctx, generation.BodyAsync(ctx, messageID, w, onMeta...), release, 1)
}

func (c *leasedClient) StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	generation, release, err := c.acquire(ctx)
	if err != nil {
		out := make(chan nntppool.StatManyResult, 1)
		out <- nntppool.StatManyResult{Err: err}
		close(out)
		return out
	}
	return relayGenerationResults(ctx, generation.StatMany(ctx, messageIDs, opts), release, len(messageIDs))
}

// relayGenerationResults drains the generation-owned source even when the
// caller stops consuming or cancels. The generation lease ends at source
// close, while any already-produced results may continue to drain afterward.
func relayGenerationResults[T any](ctx context.Context, source <-chan T, release func(), capacity int) <-chan T {
	out := make(chan T, max(capacity, 1))
	go func() {
		defer close(out)
		defer release()
		deliver := true
		for result := range source {
			if !deliver {
				continue
			}
			select {
			case out <- result:
			case <-ctx.Done():
				deliver = false
			}
		}
	}()
	return out
}
