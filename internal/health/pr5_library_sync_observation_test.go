package health

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/require"
)

func newPR5ObservationDiscoveryFixture(t *testing.T) (
	*database.DB,
	*database.HealthRepository,
	*database.HealthStateRepository,
	*metadata.MetadataService,
	*config.Config,
) {
	t.Helper()
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "observation-discovery.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	metadataRoot := t.TempDir()
	mountRoot := t.TempDir()
	libraryRoot := t.TempDir()
	importRoot := t.TempDir()
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Metadata.RootPath = metadataRoot
	cfg.MountPath = mountRoot
	cfg.Health.Enabled = &enabled
	cfg.Health.CleanupOrphanedMetadata = &enabled
	cfg.Health.Repair.Enabled = &enabled
	cfg.Health.LibraryDir = &libraryRoot
	cfg.Import.ImportDir = &importRoot
	cfg.Import.ImportStrategy = config.ImportStrategySTRM
	return db,
		database.NewHealthRepository(db.Connection(), database.DialectSQLite),
		database.NewHealthStateRepository(db.Connection(), database.DialectSQLite),
		metadata.NewMetadataService(metadataRoot), cfg
}

func pr5WriteDiscoveryMetadata(
	t *testing.T,
	service *metadata.MetadataService,
	virtualPath string,
) string {
	t.Helper()
	fileMetadata := service.CreateFileMetadata(
		100, "synthetic.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, service.WriteFileMetadata(virtualPath, fileMetadata))
	return service.GetMetadataFilePath(virtualPath)
}

func pr5WriteSTRM(t *testing.T, root, name, virtualPath string) string {
	t.Helper()
	path := filepath.Join(root, name+".strm")
	require.NoError(t, os.WriteFile(
		path, []byte("http://localhost/files/stream?path="+virtualPath), 0o644,
	))
	return path
}

func TestPR5ObservationDiscoveryRetainsFilesAndDurableHistory(t *testing.T) {
	db, healthRepository, stateRepository, metadataService, cfg :=
		newPR5ObservationDiscoveryFixture(t)
	ctx := context.Background()

	// A record missing from both metadata and the library would be deleted by
	// legacy sync. Migration 035 makes that deletion cascade through its file
	// revision and every durable run, so retain-history mode must keep all three.
	require.NoError(t, healthRepository.AddFileToHealthCheck(
		ctx, "history/retained.mkv", nil, 3, 3, nil, database.HealthPriorityNormal,
	))
	revision, err := stateRepository.EnsureFileRevision(ctx, database.FileRevisionSpec{
		FilePath: "history/retained.mkv", LayoutFingerprint: "sha256:retained-layout",
		VirtualSize: 100, SegmentCount: 1,
	})
	require.NoError(t, err)
	_, err = stateRepository.ReconcileProviders(ctx, []database.ProviderSpec{{
		StableID: "synthetic-provider", DisplayName: "Synthetic",
		Endpoint: "synthetic.invalid", Port: 119, Account: "synthetic",
		Role: database.ProviderRolePrimary,
	}})
	require.NoError(t, err)
	snapshot, err := stateRepository.CaptureActiveProviderSnapshot(ctx, time.Now().UTC())
	require.NoError(t, err)
	run, err := stateRepository.CreateHealthRun(ctx, database.HealthRunSpec{
		ID: "retained-discovery-run", FileRevisionID: revision.ID,
		ProviderSnapshotID: snapshot.ID, Trigger: "ordinary", Mode: "observation",
		TotalSegments: 1, CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	// Four metadata/STRM pairs keep the legacy orphan-ratio guard below its
	// cutoff. The fifth metadata file and the extra STRM are pre-confirmed
	// orphans which legacy cleanup would delete on this pass.
	libraryRoot := *cfg.Health.LibraryDir
	for index := range 4 {
		virtualPath := "movies/kept-" + string(rune('a'+index)) + ".mkv"
		pr5WriteDiscoveryMetadata(t, metadataService, virtualPath)
		pr5WriteSTRM(t, libraryRoot, "kept-"+string(rune('a'+index)), virtualPath)
	}
	orphanMeta := pr5WriteDiscoveryMetadata(t, metadataService, "movies/orphan.mkv")
	orphanMetaBefore, err := os.ReadFile(orphanMeta)
	require.NoError(t, err)
	orphanSTRM := pr5WriteSTRM(t, libraryRoot, "ghost", "movies/ghost.mkv")
	emptyLibraryDir := filepath.Join(libraryRoot, "must-retain-empty-dir")
	emptyImportDir := filepath.Join(*cfg.Import.ImportDir, "must-retain-empty-dir")
	require.NoError(t, os.MkdirAll(emptyLibraryDir, 0o755))
	require.NoError(t, os.MkdirAll(emptyImportDir, 0o755))
	require.NoError(t, healthRepository.UpdateSystemState(
		ctx, "pending_metadata_deletions", `{"movies/orphan.mkv":true}`,
	))
	require.NoError(t, healthRepository.UpdateSystemState(
		ctx, "pending_library_deletions", `{"movies/ghost.mkv":true}`,
	))
	mountSentinel := filepath.Join(cfg.MountPath, "must-not-be-padded.mkv")
	sentinelBytes := []byte("unaltered-observation-bytes")
	require.NoError(t, os.WriteFile(mountSentinel, sentinelBytes, 0o644))

	worker := NewObservationLibrarySyncWorker(
		metadataService, healthRepository, func() *config.Config { return cfg },
	)
	require.Nil(t, worker.SyncLibrary(ctx, false))

	retained, err := healthRepository.GetFileHealth(ctx, "history/retained.mkv")
	require.NoError(t, err)
	require.NotNil(t, retained)
	retainedRun, err := stateRepository.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, retainedRun, "file-health cleanup must not cascade durable runs")
	var revisionCount int
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM health_file_revisions WHERE id = ?", revision.ID,
	).Scan(&revisionCount))
	require.Equal(t, 1, revisionCount)
	require.FileExists(t, orphanMeta)
	require.FileExists(t, orphanSTRM)
	require.DirExists(t, emptyLibraryDir)
	require.DirExists(t, emptyImportDir)
	orphanMetaAfter, err := os.ReadFile(orphanMeta)
	require.NoError(t, err)
	require.Equal(t, orphanMetaBefore, orphanMetaAfter,
		"observation discovery must not rewrite visible metadata")
	actualSentinel, err := os.ReadFile(mountSentinel)
	require.NoError(t, err)
	require.Equal(t, sentinelBytes, actualSentinel,
		"observation discovery must neither replace nor pad mounted data")

	metaPending, err := healthRepository.GetSystemState(ctx, "pending_metadata_deletions")
	require.NoError(t, err)
	require.JSONEq(t, `{"movies/orphan.mkv":true}`, metaPending,
		"observation mode must not mutate legacy cleanup intent")
	libraryPending, err := healthRepository.GetSystemState(ctx, "pending_library_deletions")
	require.NoError(t, err)
	require.JSONEq(t, `{"movies/ghost.mkv":true}`, libraryPending)

	for _, virtualPath := range []string{
		"movies/kept-a.mkv", "movies/kept-b.mkv", "movies/kept-c.mkv",
		"movies/kept-d.mkv", "movies/orphan.mkv",
	} {
		discovered, err := healthRepository.GetFileHealth(ctx, virtualPath)
		require.NoError(t, err)
		require.NotNil(t, discovered, "observation discovery may add safe state")
		require.Equal(t, database.HealthStatusPending, discovered.Status,
			"discovery must not synthesize a healthy provider result")
		require.Nil(t, discovered.LastChecked,
			"discovery must not synthesize provider-check evidence")
	}
}

func TestPR5ObservationDiscoveryDoesNotTurnMalformedMetadataIntoRepairWork(t *testing.T) {
	_, healthRepository, _, metadataService, cfg := newPR5ObservationDiscoveryFixture(t)
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.Health.LibraryDir = nil
	brokenPath := filepath.Join(cfg.Metadata.RootPath, "broken.mkv.meta")
	require.NoError(t, os.WriteFile(brokenPath, []byte("not protobuf metadata"), 0o644))

	worker := NewObservationLibrarySyncWorker(
		metadataService, healthRepository, func() *config.Config { return cfg },
	)
	require.Nil(t, worker.SyncLibrary(context.Background(), false))
	require.FileExists(t, brokenPath)
	repairCandidate, err := healthRepository.GetFileHealth(context.Background(), "broken.mkv")
	require.NoError(t, err)
	require.Nil(t, repairCandidate,
		"non-authoritative discovery must not register malformed metadata for repair")
}

func TestPR5LibrarySyncStopJoinsBeforeRestart(t *testing.T) {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.LibrarySyncIntervalMinutes = 60
	enteredFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var getterCalls atomic.Int32
	worker := &LibrarySyncWorker{
		configGetter: func() *config.Config {
			if getterCalls.Add(1) == 1 {
				close(enteredFirst)
				<-releaseFirst
			}
			return cfg
		},
		manualTrigger: make(chan struct{}, 1),
	}

	require.NoError(t, worker.StartLibrarySyncChecked(context.Background()))
	<-enteredFirst
	stopDone := make(chan error, 1)
	go func() { stopDone <- worker.StopAndWait(context.Background()) }()
	require.Eventually(t, func() bool {
		worker.mu.Lock()
		defer worker.mu.Unlock()
		return worker.stopping
	}, time.Second, time.Millisecond)

	require.ErrorIs(t, worker.StartLibrarySyncChecked(context.Background()), ErrLibrarySyncAlreadyRunning)
	require.Equal(t, int32(1), getterCalls.Load(),
		"a canceled generation must remain owned until its goroutine exits")
	select {
	case err := <-stopDone:
		t.Fatalf("stop returned before the active generation exited: %v", err)
	default:
	}

	close(releaseFirst)
	require.NoError(t, <-stopDone)
	require.False(t, worker.IsRunning())
	require.NoError(t, worker.StartLibrarySyncChecked(context.Background()))
	require.Eventually(t, func() bool { return getterCalls.Load() == 2 }, time.Second, time.Millisecond)
	require.NoError(t, worker.StopAndWait(context.Background()))
}

func TestPR5DiscoveryConsumerIsJoinedBeforeSyncReturns(t *testing.T) {
	flushEntered := make(chan struct{})
	releaseFlush := make(chan struct{})
	consumer := newDiscoveryBatchConsumer(
		context.Background(),
		1,
		false,
		func(context.Context, []database.AutomaticHealthCheckRecord) error {
			close(flushEntered)
			<-releaseFlush
			return context.Canceled
		},
	)
	consumer.Records() <- database.AutomaticHealthCheckRecord{FilePath: "synthetic.mkv"}
	<-flushEntered

	joined := make(chan struct{})
	go func() {
		consumer.CloseAndWait()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("discovery returned before its batch consumer exited")
	default:
	}
	close(releaseFlush)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("discovery consumer did not join after its flush completed")
	}
}

func TestPR5CancelActiveLibrarySyncKeepsSupervisorRestartable(t *testing.T) {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.LibrarySyncIntervalMinutes = 60
	firstEntered := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	var calls atomic.Int32
	worker := &LibrarySyncWorker{
		configGetter:  func() *config.Config { return cfg },
		manualTrigger: make(chan struct{}, 1),
		syncLibraryHook: func(ctx context.Context, _ bool) {
			switch calls.Add(1) {
			case 1:
				close(firstEntered)
				<-ctx.Done()
				close(firstCanceled)
				<-releaseFirst
			case 2:
				close(secondEntered)
			}
		},
	}

	require.NoError(t, worker.StartLibrarySyncChecked(context.Background()))
	require.NoError(t, worker.TriggerManualSync(context.Background()))
	<-firstEntered
	_, err := worker.RunLibrarySyncChecked(context.Background(), true)
	require.ErrorIs(t, err, ErrLibrarySyncScanAlreadyRunning,
		"dry-run/manual/scheduled scans must share one owned generation")
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- worker.CancelActiveSync(context.Background()) }()
	<-firstCanceled
	require.True(t, worker.IsRunning(), "canceling a scan must retain its periodic supervisor")
	select {
	case err := <-cancelDone:
		t.Fatalf("cancel returned before the active scan joined: %v", err)
	default:
	}
	close(releaseFirst)
	require.NoError(t, <-cancelDone)

	require.NoError(t, worker.TriggerManualSync(context.Background()))
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("manual sync could not restart after cancellation")
	}
	require.NoError(t, worker.StopAndWait(context.Background()))
	require.False(t, worker.IsRunning())
	require.NoError(t, worker.CancelActiveSync(context.Background()))
}

func TestPR5LibraryStopJoinsAPIScanWithoutSupervisor(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	worker := &LibrarySyncWorker{
		syncLibraryHook: func(ctx context.Context, _ bool) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
		},
	}
	go func() {
		_, err := worker.RunLibrarySyncChecked(context.Background(), true)
		scanDone <- err
	}()
	<-entered

	stopDone := make(chan error, 1)
	go func() { stopDone <- worker.StopAndWait(context.Background()) }()
	<-canceled
	select {
	case err := <-stopDone:
		t.Fatalf("runtime stop returned before the API scan joined: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-scanDone)
	require.NoError(t, <-stopDone)
}

func TestPR5MetadataOnlyCancellationJoinsStartedReaders(t *testing.T) {
	_, healthRepository, _, metadataService, cfg := newPR5ObservationDiscoveryFixture(t)
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.Health.LibraryDir = nil
	cfg.Health.LibrarySyncConcurrency = 1
	paths := make([]string, 0, 3)
	for i := range 3 {
		path := metadataService.GetMetadataFilePath("blocked/reader-" + string(rune('a'+i)))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, syscall.Mkfifo(path, 0o600))
		paths = append(paths, path)
	}
	worker := NewObservationLibrarySyncWorker(
		metadataService, healthRepository, func() *config.Config { return cfg },
	)

	scanDone := make(chan error, 1)
	go func() {
		_, err := worker.RunLibrarySyncChecked(context.Background(), false)
		scanDone <- err
	}()
	first, firstPath := pr5OpenWaitingFIFO(t, paths, "")
	// Let the scan submitter block behind the one occupied worker before it is
	// canceled. Releasing the first reader then starts the already-submitted
	// second read and exercises the early-cancel p.Wait path.
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan error, 1)
	go func() { stopDone <- worker.StopAndWait(context.Background()) }()
	require.NoError(t, pr5CloseFIFOWithInvalidMetadata(first))
	second, _ := pr5OpenWaitingFIFO(t, paths, firstPath)
	select {
	case err := <-stopDone:
		t.Fatalf("metadata-only stop returned before a started reader joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, pr5CloseFIFOWithInvalidMetadata(second))
	require.NoError(t, <-scanDone)
	require.NoError(t, <-stopDone)
}

func pr5OpenWaitingFIFO(t *testing.T, paths []string, skip string) (*os.File, string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, path := range paths {
			if path == skip {
				continue
			}
			file, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
			if err == nil {
				return file, path
			}
			if !errors.Is(err, syscall.ENXIO) {
				t.Fatalf("open FIFO writer: %v", err)
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("metadata reader did not open a FIFO")
	return nil, ""
}

func pr5CloseFIFOWithInvalidMetadata(file *os.File) error {
	if _, err := file.Write([]byte("invalid synthetic metadata")); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
