//go:build linux

package utils

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveEmptyDirsSafeRetainsValidatedRootIdentity(t *testing.T) {
	base := t.TempDir()
	rootPath := filepath.Join(base, "root")
	parkedPath := filepath.Join(base, "parked")
	swapPath := filepath.Join(base, "swap")
	outsidePath := filepath.Join(base, "outside")
	relative := filepath.Join("one", "two")
	require.NoError(t, os.MkdirAll(filepath.Join(rootPath, relative), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(outsidePath, relative), 0o755))
	require.NoError(t, os.Symlink(outsidePath, swapPath))

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
		_ = RemoveEmptyDirsSafe(rootPath, filepath.Join(rootPath, relative))
		if _, err := os.Lstat(filepath.Join(outsidePath, relative)); err != nil {
			require.ErrorIs(t, err, os.ErrNotExist)
			t.Fatalf("empty-directory pruning escaped the validated root")
		}
	}
}
