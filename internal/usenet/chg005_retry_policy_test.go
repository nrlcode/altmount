package usenet

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/testsupport/segments"
	"github.com/javi11/nntppool/v4"
)

// TestCHG005TerminalBody451EvidenceIsNotRetried pins the boundary between
// nntppool's internal 451 replay and AltMount's outer retry loop. Once the
// pool returns at least two BODY/451 attempts, the result is terminal for this
// fetch and must be returned unchanged. Two attempts pin the lower bound;
// three attempts across mixed providers prove retained evidence is not limited
// to one exact provider pair. Both reader profiles use different request lanes.
func TestCHG005TerminalBody451EvidenceIsNotRetried(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		importMode       bool
		attemptProviders []string
		wantBody         int64
		wantPri          int64
	}{
		{
			name:             "streaming-priority/two-attempts",
			attemptProviders: []string{"provider-a", "provider-a"},
			wantPri:          1,
		},
		{
			name:             "streaming-priority/three-mixed-providers",
			attemptProviders: []string{"provider-a", "provider-b", "provider-a"},
			wantPri:          1,
		},
		{
			name:             "import-normal/two-attempts",
			importMode:       true,
			attemptProviders: []string{"provider-a", "provider-a"},
			wantBody:         1,
		},
		{
			name:             "import-normal/three-mixed-providers",
			importMode:       true,
			attemptProviders: []string{"provider-a", "provider-b", "provider-a"},
			wantBody:         1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			cause := &nntppool.Error{Code: 451, Message: "temporary article failure"}
			attempts := make([]nntppool.AttemptEvidence, len(tt.attemptProviders))
			for i, providerID := range tt.attemptProviders {
				attempts[i] = nntppool.AttemptEvidence{
					ProviderID:   providerID,
					Operation:    nntppool.OperationBody,
					Outcome:      nntppool.OutcomeTemporaryFailure,
					ResponseCode: 451,
					Cause:        cause,
				}
			}
			terminal := &nntppool.TransportError{
				Kind:         nntppool.OutcomeTemporaryFailure,
				ProviderID:   "provider-a",
				ResponseCode: 451,
				Attempts:     attempts,
				Cause:        cause,
			}

			fp := fakepool.New()
			segmentID := segments.MessageID(0)
			fp.SetBehavior(segmentID, fakepool.SegmentBehavior{Err: terminal})
			rg := buildEagerRange(ctx, t, 1, 32)

			var opts []ReaderOption
			if tt.importMode {
				opts = append(opts, WithImportProfile(nil))
			}
			ur, err := NewUsenetReader(
				ctx,
				func() (pool.NntpClient, error) { return fp, nil },
				rg,
				1,
				noopMetrics{},
				"chg005-terminal-451",
				nil,
				opts...,
			)
			if err != nil {
				t.Fatalf("NewUsenetReader: %v", err)
			}
			t.Cleanup(func() { _ = ur.Close() })

			_, gotErr := ur.downloadSegmentWithRetry(ctx, rg.segments[0])
			var gotTransport *nntppool.TransportError
			if !errors.As(gotErr, &gotTransport) {
				t.Fatalf("download error = %T %v, want *nntppool.TransportError", gotErr, gotErr)
			}
			if gotTransport != terminal {
				t.Fatalf("download error pointer = %p, want original terminal error %p", gotTransport, terminal)
			}
			if gotTransport.Kind != nntppool.OutcomeTemporaryFailure || gotTransport.ResponseCode != 451 {
				t.Fatalf("transport classification = %s/%d, want temporary_failure/451", gotTransport.Kind, gotTransport.ResponseCode)
			}
			if len(gotTransport.Attempts) != len(tt.attemptProviders) {
				t.Fatalf("transport attempts = %d, want %d provider attempts retained",
					len(gotTransport.Attempts), len(tt.attemptProviders))
			}
			for i, attempt := range gotTransport.Attempts {
				if attempt.Operation != nntppool.OperationBody ||
					attempt.Outcome != nntppool.OutcomeTemporaryFailure ||
					attempt.ResponseCode != 451 {
					t.Errorf("attempt %d = %+v, want BODY temporary_failure/451", i, attempt)
				}
			}

			if got := fp.BodyCalls(); got != tt.wantBody {
				t.Errorf("Body calls = %d, want %d", got, tt.wantBody)
			}
			if got := fp.BodyPriorityCalls(); got != tt.wantPri {
				t.Errorf("BodyPriority calls = %d, want %d", got, tt.wantPri)
			}
		})
	}
}

func TestCHG005TerminalBody451RequiresTwoMatchingAttempts(t *testing.T) {
	t.Parallel()

	matching := nntppool.AttemptEvidence{
		ProviderID:   "provider-a",
		Operation:    nntppool.OperationBody,
		Outcome:      nntppool.OutcomeTemporaryFailure,
		ResponseCode: 451,
	}
	tests := []struct {
		name     string
		attempts []nntppool.AttemptEvidence
	}{
		{name: "one-matching-attempt", attempts: []nntppool.AttemptEvidence{matching}},
		{
			name: "second-attempt-is-not-body",
			attempts: []nntppool.AttemptEvidence{
				matching,
				{
					ProviderID:   "provider-a",
					Operation:    nntppool.OperationStat,
					Outcome:      nntppool.OutcomeTemporaryFailure,
					ResponseCode: 451,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			cause := &nntppool.Error{Code: 451, Message: "temporary article failure"}
			first := &nntppool.TransportError{
				Kind:         nntppool.OutcomeTemporaryFailure,
				ProviderID:   "provider-a",
				ResponseCode: 451,
				Attempts:     tt.attempts,
				Cause:        cause,
			}
			client := &scriptedReaderClient{
				Client: fakepool.New(),
				results: []scriptedReaderResult{
					{err: first},
					{body: &nntppool.ArticleBody{Bytes: []byte("ok"), BytesDecoded: 2}},
				},
			}
			rg := buildEagerRange(ctx, t, 1, 32)
			ur, err := NewUsenetReader(
				ctx,
				func() (pool.NntpClient, error) { return client, nil },
				rg,
				1,
				noopMetrics{},
				"chg005-nonterminal-451",
				nil,
			)
			if err != nil {
				t.Fatalf("NewUsenetReader: %v", err)
			}
			t.Cleanup(func() { _ = ur.Close() })

			body, gotErr := ur.downloadSegmentWithRetry(ctx, rg.segments[0])
			if gotErr != nil {
				t.Fatalf("download error = %v, want success on outer retry", gotErr)
			}
			if string(body) != "ok" {
				t.Fatalf("download body = %q, want %q", body, "ok")
			}
			if got := client.totalCalls(); got != 2 {
				t.Errorf("reader calls = %d, want 2", got)
			}
		})
	}
}

// TestCHG005EligibleReaderErrorsStillRetry protects the other half of the
// policy: a deadline, a connection failure, and an ordinary transient error
// are still eligible for one fresh outer attempt. Each scripted failure is
// followed by a successful body so the expected two-call cardinality is
// independent of retry-go's final-error representation.
func TestCHG005EligibleReaderErrorsStillRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "deadline", err: context.DeadlineExceeded},
		{
			name: "connection",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: syscall.ECONNRESET,
			},
		},
		{name: "generic", err: errors.New("synthetic temporary provider failure")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client := &scriptedReaderClient{
				Client: fakepool.New(),
				results: []scriptedReaderResult{
					{err: tt.err},
					{body: &nntppool.ArticleBody{Bytes: []byte("ok"), BytesDecoded: 2}},
				},
			}
			rg := buildEagerRange(ctx, t, 1, 32)
			ur, err := NewUsenetReader(
				ctx,
				func() (pool.NntpClient, error) { return client, nil },
				rg,
				1,
				noopMetrics{},
				"chg005-eligible-retry",
				nil,
			)
			if err != nil {
				t.Fatalf("NewUsenetReader: %v", err)
			}
			t.Cleanup(func() { _ = ur.Close() })

			body, gotErr := ur.downloadSegmentWithRetry(ctx, rg.segments[0])
			if gotErr != nil {
				t.Fatalf("download error = %v, want success on the eligible retry", gotErr)
			}
			if string(body) != "ok" {
				t.Fatalf("download body = %q, want %q", body, "ok")
			}
			if got := client.totalCalls(); got != 2 {
				t.Errorf("reader calls = %d, want exactly two (initial + eligible retry)", got)
			}
			if got := client.bodyPriorityCalls(); got != 2 {
				t.Errorf("BodyPriority calls = %d, want 2", got)
			}
			if got := client.bodyCalls(); got != 0 {
				t.Errorf("Body calls = %d, want 0 for the default streaming profile", got)
			}
		})
	}
}

type scriptedReaderResult struct {
	body *nntppool.ArticleBody
	err  error
}

// scriptedReaderClient embeds fakepool for the non-body NntpClient methods
// and supplies a small, synchronized BODY script for retry cardinality tests.
type scriptedReaderClient struct {
	*fakepool.Client

	mu       sync.Mutex
	results  []scriptedReaderResult
	next     int
	bodyN    int
	priority int
}

func (c *scriptedReaderClient) nextResult() scriptedReaderResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls := c.next
	c.next++
	if len(c.results) == 0 {
		return scriptedReaderResult{}
	}
	if calls >= len(c.results) {
		calls = len(c.results) - 1
	}
	return c.results[calls]
}

func (c *scriptedReaderClient) Body(ctx context.Context, _ string, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	c.mu.Lock()
	c.bodyN++
	c.mu.Unlock()
	result := c.nextResult()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result.body, result.err
}

func (c *scriptedReaderClient) BodyPriority(ctx context.Context, _ string, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
	c.mu.Lock()
	c.priority++
	c.mu.Unlock()
	result := c.nextResult()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result.body, result.err
}

func (c *scriptedReaderClient) totalCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.next
}

func (c *scriptedReaderClient) bodyCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodyN
}

func (c *scriptedReaderClient) bodyPriorityCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.priority
}
