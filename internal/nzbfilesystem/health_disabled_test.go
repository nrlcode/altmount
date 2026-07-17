package nzbfilesystem

import (
	"context"
	"database/sql"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// setupStreamHealthEnv builds an in-memory health repository and a metadata service rooted
// in a temp dir, so the streaming-failure handler (updateFileHealthOnError) can be exercised
// end-to-end against real persistence.
func setupStreamHealthEnv(t *testing.T) (*database.HealthRepository, *sql.DB, *metadata.MetadataService) {
	t.Helper()

	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&mode=memory")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE file_health (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT NOT NULL UNIQUE,
			library_path TEXT,
			status TEXT NOT NULL,
			last_checked DATETIME,
			last_error TEXT,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			repair_retry_count INTEGER DEFAULT 0,
			max_repair_retries INTEGER DEFAULT 3,
			source_nzb_path TEXT,
			error_details TEXT,
			metadata TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			release_date DATETIME,
			scheduled_check_at DATETIME,
			priority INTEGER DEFAULT 0,
			streaming_failure_count INTEGER DEFAULT 0,
			is_masked BOOLEAN DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			download_id TEXT DEFAULT NULL
		);
	`)
	require.NoError(t, err)

	return database.NewHealthRepository(db, database.DialectSQLite), db, metadata.NewMetadataService(t.TempDir())
}

// newStreamFailureMVF wires a MetadataVirtualFile to the given real services with a nil
// repairCoalescer (ShouldTrigger returns true, EnqueueRefresh is a no-op).
func newStreamFailureMVF(ctx context.Context, name string, repo *database.HealthRepository, ms *metadata.MetadataService, seg []*metapb.SegmentData, cfg *config.Config) *MetadataVirtualFile {
	return &MetadataVirtualFile{
		name:             name,
		ctx:              ctx,
		meta:             &fileHandleMeta{FileSize: 1024, SegmentData: seg},
		metadataService:  ms,
		healthRepository: repo,
		configGetter:     func() *config.Config { return cfg },
	}
}

func writeStreamMeta(t *testing.T, ms *metadata.MetadataService, filePath string) []*metapb.SegmentData {
	t.Helper()
	meta := ms.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		[]*metapb.SegmentData{{Id: "a@b.example.com", SegmentSize: 1024, StartOffset: 0, EndOffset: 1023}},
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(filePath, meta))
	return meta.SegmentData
}
