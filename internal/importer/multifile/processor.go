package multifile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/altmount/internal/importer/admissionctx"
	"github.com/javi11/altmount/internal/importer/filesystem"
	"github.com/javi11/altmount/internal/importer/parser"
	"github.com/javi11/altmount/internal/importer/utils"
	"github.com/javi11/altmount/internal/importer/validation"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/progress"
	concpool "github.com/sourcegraph/conc/pool"
)

var ErrNoFilesProcessed = errors.New("no regular files were successfully processed (all files failed validation)")

type durableProcessingError struct {
	message string
	cause   error
}

func (e *durableProcessingError) Error() string { return e.message }
func (e *durableProcessingError) Unwrap() error { return e.cause }

// ProcessRegularFiles processes multiple regular files.
// Returns the virtual paths of all metadata files successfully written, plus any error.
// writtenPaths is populated even on partial failure (first-error mode).
func ProcessRegularFiles(
	ctx context.Context,
	virtualDir string,
	files []parser.ParsedFile,
	par2Files []parser.ParsedFile,
	nzbPath string,
	metadataService *metadata.MetadataService,
	allowedFileExtensions []string,
	filterSamples bool,
	tracker *progress.Tracker,
	storeIndex map[string]int64,
	storeRef string,
) ([]string, error) {
	_, durableAdmission := admissionctx.FromContext(ctx)
	if len(files) == 0 {
		return nil, nil
	}

	if !utils.HasAllowedFilesInRegular(files, allowedFileExtensions, filterSamples) {
		if !durableAdmission {
			slog.WarnContext(ctx, "No files with allowed extensions found",
				"allowed_extensions", allowedFileExtensions,
				"file_count", len(files))
		}
		if durableAdmission {
			return nil, fmt.Errorf("no files with allowed extensions found")
		}
		return nil, fmt.Errorf("no files with allowed extensions found (allowed: %v)", allowedFileExtensions)
	}

	var par2Refs []*metapb.Par2FileReference
	for _, par2File := range par2Files {
		par2Refs = append(par2Refs, &metapb.Par2FileReference{
			Filename:    par2File.Filename,
			FileSize:    par2File.Size,
			SegmentData: par2File.Segments,
		})
	}

	var writtenPaths []string
	var writtenPathsMu sync.Mutex

	// reserver hands out unique virtual paths across the concurrent batch.
	// Without it two goroutines could pick the same _N suffix (the on-disk
	// check alone can't see in-flight siblings) and race on the rename. It
	// assigns suffixes in O(1) amortized time even when many files collide.
	reserver := filesystem.NewPathReserver(metadataService)

	start := time.Now()
	pl := concpool.New().WithErrors().WithFirstError()

	// processed counts every file the pool has finished (written or skipped) so
	// the progress bar advances as writes complete in parallel. Without this the
	// bar sits frozen for the whole batch — slow-feeling on large multi-hundred
	// file releases (e.g. Blu-ray BDMV) even though writes run concurrently.
	var processed int64
	total := len(files)

	// Throttle progress broadcasts. The SSE subscriber channel is buffered and
	// drops on overflow, so firing one update per file (thousands, in well under
	// a second) floods it and nearly all are dropped — leaving the bar visually
	// stuck. Emit at most ~one update per percent of work, plus the final, so
	// the client actually receives a climbing bar.
	updateStep := max(int64(total)/100, 1)

	for _, file := range files {
		pl.Go(func() error {
			defer func() {
				done := atomic.AddInt64(&processed, 1)
				if done == int64(total) || done%updateStep == 0 {
					tracker.Update(int(done), total)
				}
			}()

			parentPath, filename := filesystem.DetermineFileLocation(file, virtualDir)

			if err := filesystem.EnsureDirectoryExists(parentPath, metadataService); err != nil {
				if durableAdmission {
					return &durableProcessingError{
						message: "failed to create final-layout directory", cause: err,
					}
				}
				return fmt.Errorf("failed to create parent directory %s: %w", parentPath, err)
			}

			virtualPath := filepath.Join(parentPath, filename)
			virtualPath = strings.ReplaceAll(virtualPath, string(filepath.Separator), "/")
			desiredVirtualPath := virtualPath

			if !utils.IsAllowedFile(filename, file.Size, allowedFileExtensions, filterSamples) {
				return nil
			}

			// Validate segments (local structural checks; network reachability confirmed at import start)
			if err := validation.ValidateSegmentsForFile(
				filename,
				file.Size,
				file.Segments,
				file.Encryption,
			); err != nil {
				if !durableAdmission {
					slog.WarnContext(ctx, "Skipping file due to segment validation error", "error", err, "file", filename)
				}
				return nil
			}

			fileMeta := metadataService.CreateFileMetadata(
				file.Size,
				nzbPath,
				metapb.FileStatus_FILE_STATUS_HEALTHY,
				file.Segments,
				file.Encryption,
				file.Password,
				file.Salt,
				file.AesKey,
				file.AesIv,
				file.ReleaseDate.Unix(),
				par2Refs,
				file.NzbdavID,
			)

			reuseAcceptedPath := false
			var reusableBinding admissionctx.ReusableLayoutBinding
			if binding, reusable := admissionctx.ReusableLayout(ctx, desiredVirtualPath); reusable {
				reusableBinding = binding
				layout, layoutErr := metadata.ResolveCanonicalSegmentLayout(fileMeta)
				if layoutErr == nil && layout.Fingerprint == binding.Fingerprint {
					virtualPath = reserver.ReserveReusable(desiredVirtualPath)
					reuseAcceptedPath = virtualPath == desiredVirtualPath
				} else {
					virtualPath = reserver.Reserve(desiredVirtualPath)
				}
			} else {
				virtualPath = reserver.Reserve(desiredVirtualPath)
			}
			defer reserver.Release(virtualPath)

			if reuseAcceptedPath && !reusableBinding.ActivationPending {
				if existing, readErr := metadataService.ReadFileMetadata(virtualPath); readErr == nil &&
					existing != nil && existing.Status == metapb.FileStatus_FILE_STATUS_HEALTHY {
					existingLayout, existingErr := metadata.ResolveCanonicalSegmentLayout(existing)
					if existingErr == nil && existingLayout.Fingerprint == reusableBinding.Fingerprint {
						writtenPathsMu.Lock()
						writtenPaths = append(writtenPaths, virtualPath)
						writtenPathsMu.Unlock()
						return nil
					}
				}
			}

			metadataPath := metadataService.GetMetadataFilePath(virtualPath)
			if !reuseAcceptedPath {
				if _, err := os.Stat(metadataPath); err == nil {
					_ = metadataService.DeleteFileMetadata(virtualPath)
				}
			} else if _, err := os.Stat(metadataPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("inspect reusable metadata path: %w", err)
			}

			if err := metadataService.WriteFileMetadataAuto(ctx, virtualPath, fileMeta, storeIndex, storeRef); err != nil {
				if durableAdmission {
					return &durableProcessingError{
						message: "failed to write admitted metadata", cause: err,
					}
				}
				return fmt.Errorf("failed to write metadata for file %s: %w", filename, err)
			}

			writtenPathsMu.Lock()
			writtenPaths = append(writtenPaths, virtualPath)
			writtenPathsMu.Unlock()

			if !durableAdmission {
				slog.DebugContext(ctx, "Created metadata file",
					"file", filename,
					"virtual_path", virtualPath,
					"size", file.Size)
			}
			return nil
		})
	}

	if err := pl.Wait(); err != nil {
		return writtenPaths, err
	}

	if len(writtenPaths) == 0 {
		return writtenPaths, ErrNoFilesProcessed
	}

	// Timing/count instrumentation: lets us see whether the write phase cost is
	// driven by file volume (high files/written) or per-file latency (high
	// duration with modest counts — e.g. slow metadata storage).
	elapsed := time.Since(start)
	perFile := time.Duration(0)
	if total > 0 {
		perFile = elapsed / time.Duration(total)
	}
	if !durableAdmission {
		slog.InfoContext(ctx, "Successfully processed regular files",
			"virtual_dir", virtualDir,
			"files", len(files),
			"written", len(writtenPaths),
			"duration", elapsed,
			"avg_per_file", perFile)
	}

	return writtenPaths, nil
}
