package pool

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
)

func responsiveStatFactory(t *testing.T, calls *atomic.Int32) nntppool.ConnFactory {
	t.Helper()
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_, _ = server.Write([]byte("200 synthetic provider ready\r\n"))
			reader := bufio.NewReader(server)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(line, "STAT ") {
					calls.Add(1)
					// Let the client register the pipelined request before the
					// in-memory server produces its response.
					time.Sleep(time.Millisecond)
					_, _ = server.Write([]byte("223 1 <synthetic@test.invalid> article exists\r\n"))
				}
			}
		}()
		return client, nil
	}
}

func failingStatFactory() nntppool.ConnFactory {
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_, _ = server.Write([]byte("200 synthetic provider ready\r\n"))
			_, _ = bufio.NewReader(server).ReadString('\n')
		}()
		return client, nil
	}
}

func TestPR3ManagerSelectsFIFOProviders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var firstCalls, secondCalls atomic.Int32
	mgr := NewManager(ctx, nil)
	require.NoError(t, mgr.SetProviders([]nntppool.Provider{
		{ID: "primary-a", Factory: responsiveStatFactory(t, &firstCalls), Connections: 1, StatInflight: 1, SkipPing: true},
		{ID: "primary-b", Factory: responsiveStatFactory(t, &secondCalls), Connections: 1, StatInflight: 1, SkipPing: true},
	}))
	t.Cleanup(func() { _ = mgr.ClearPool() })
	client, err := mgr.GetPool()
	require.NoError(t, err)

	// Let each provider establish its one cold connection. Capacity-aware FIFO
	// may use the second primary while the preferred primary's only connection
	// slot is being established; once both are hot, configured order must win.
	for range 2 {
		statCtx, statCancel := context.WithTimeout(ctx, time.Second)
		_, statErr := client.Stat(statCtx, "synthetic@test.invalid")
		statCancel()
		require.NoError(t, statErr)
	}
	firstCalls.Store(0)
	secondCalls.Store(0)

	for range 6 {
		statCtx, statCancel := context.WithTimeout(ctx, time.Second)
		result, statErr := client.Stat(statCtx, "synthetic@test.invalid")
		statCancel()
		require.NoError(t, statErr)
		require.Equal(t, "primary-a", result.ProviderID,
			"calls: primary-a=%d primary-b=%d attempts=%+v stats=%+v", firstCalls.Load(), secondCalls.Load(), result.Attempts, client.Stats().Providers)
	}
	require.Equal(t, int32(6), firstCalls.Load())
	require.Zero(t, secondCalls.Load())
}

func TestPR3ManagerEnablesProviderCircuitBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr := NewManager(ctx, nil)
	require.NoError(t, mgr.SetProviders([]nntppool.Provider{{
		ID: "primary-a", Factory: failingStatFactory(), Connections: 1, StatInflight: 1, SkipPing: true,
	}}))
	t.Cleanup(func() { _ = mgr.ClearPool() })
	client, err := mgr.GetPool()
	require.NoError(t, err)

	for range 3 {
		statCtx, statCancel := context.WithTimeout(ctx, time.Second)
		_, _ = client.Stat(statCtx, "synthetic@test.invalid")
		statCancel()
	}
	stats := client.Stats()
	require.Len(t, stats.Providers, 1)
	require.Equal(t, nntppool.CircuitBreakerOpen, stats.Providers[0].CircuitBreaker.State)
}
