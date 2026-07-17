//go:build linux

package metadata

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupAuthorityRetainsValidatedRootIdentity(t *testing.T) {
	base := t.TempDir()
	rootPath := filepath.Join(base, "root")
	parkedPath := filepath.Join(base, "parked")
	swapPath := filepath.Join(base, "swap")
	outsidePath := filepath.Join(base, "outside")
	require.NoError(t, os.Mkdir(rootPath, 0o755))
	require.NoError(t, os.Mkdir(outsidePath, 0o755))
	require.NoError(t, os.Symlink(outsidePath, swapPath))

	safe, err := os.Open(rootPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, safe.Close()) })
	safeInfo, err := safe.Stat()
	require.NoError(t, err)

	var stop atomic.Bool
	done := make(chan struct{})
	swapErr := make(chan error, 1)
	go func() {
		defer close(done)
		defer close(swapErr)
		for !stop.Load() {
			if err := os.Rename(rootPath, parkedPath); err != nil {
				swapErr <- err
				return
			}
			if err := os.Rename(swapPath, rootPath); err != nil {
				swapErr <- err
				return
			}
			runtime.Gosched()
			if err := os.Rename(rootPath, swapPath); err != nil {
				swapErr <- err
				return
			}
			if err := os.Rename(parkedPath, rootPath); err != nil {
				swapErr <- err
				return
			}
			runtime.Gosched()
		}
	}()
	t.Cleanup(func() {
		stop.Store(true)
		<-done
		if err, ok := <-swapErr; ok {
			require.NoError(t, err)
		}
	})

	for range 100_000 {
		planner := newCleanupPlanner()
		authority, openErr := planner.authority(rootPath)
		if openErr == nil && !authority.missing {
			got, statErr := authority.root.Stat(".")
			require.NoError(t, statErr)
			if !os.SameFile(safeInfo, got) {
				planner.close()
				t.Fatalf("cleanup authority retained a directory other than the one validated")
			}
		}
		planner.close()
	}
}
