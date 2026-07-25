package database

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type facoreCHG020QueueItemClaimer interface {
	ClaimQueueItemByID(context.Context, int64) (*ImportQueueItem, error)
}

func requireFACORECHG020QueueItemClaimer(
	t *testing.T,
	repo *QueueRepository,
) facoreCHG020QueueItemClaimer {
	t.Helper()
	claimer, ok := any(repo).(facoreCHG020QueueItemClaimer)
	require.True(t, ok,
		"QueueRepository must export ClaimQueueItemByID(context.Context,int64) (*ImportQueueItem,error)")
	return claimer
}

func TestFACORECHG020QueueClaimByIDSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db")+
		"?_journal_mode=WAL&_busy_timeout=30000&_txlock=immediate")
	require.NoError(t, err)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(3)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	setupQueueSchema(t, db)

	testFACORECHG020QueueClaimByID(t, NewQueueRepository(db, DialectSQLite), []QueueStatus{
		QueueStatusProcessing,
		QueueStatusFallback,
		QueueStatusPaused,
	})
}

func TestFACORECHG020QueueClaimByIDPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	backend := newFACORECHG009PostgresMigrationBackend(t, ctx)
	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect(backend.gooseDialect))
	require.NoError(t, goose.UpContext(ctx, backend.db, backend.migrationsDir))

	// The production PostgreSQL queue constraint does not currently admit the
	// model's paused status. Keep that schema concern outside this correction;
	// SQLite exercises paused admission rejection above.
	testFACORECHG020QueueClaimByID(t, NewQueueRepository(backend.db, DialectPostgres), []QueueStatus{
		QueueStatusProcessing,
		QueueStatusFallback,
	})
}

func testFACORECHG020QueueClaimByID(
	t *testing.T,
	repo *QueueRepository,
	ineligible []QueueStatus,
) {
	t.Helper()
	ctx := context.Background()

	for _, status := range []QueueStatus{
		QueueStatusPending,
		QueueStatusFailed,
		QueueStatusCompleted,
	} {
		status := status
		t.Run("claims_"+string(status), func(t *testing.T) {
			claimer := requireFACORECHG020QueueItemClaimer(t, repo)
			seed := seedFACORECHG020QueueItem(t, ctx, repo, status)

			claimed, err := claimer.ClaimQueueItemByID(ctx, seed.ID)
			require.NoError(t, err)
			require.NotNil(t, claimed)
			assert.Equal(t, seed.ID, claimed.ID)
			assert.Equal(t, QueueStatusProcessing, claimed.Status)
			assert.NotNil(t, claimed.StartedAt)
			stored, err := repo.GetQueueItem(ctx, seed.ID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, QueueStatusProcessing, stored.Status)
		})
	}

	for _, status := range ineligible {
		status := status
		t.Run("rejects_"+string(status), func(t *testing.T) {
			claimer := requireFACORECHG020QueueItemClaimer(t, repo)
			seed := seedFACORECHG020QueueItem(t, ctx, repo, status)

			claimed, err := claimer.ClaimQueueItemByID(ctx, seed.ID)
			require.Error(t, err, "an ineligible row must report an admission conflict")
			assert.Nil(t, claimed)
			stored, err := repo.GetQueueItem(ctx, seed.ID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, status, stored.Status)
		})
	}

	t.Run("missing_row_returns_nil", func(t *testing.T) {
		claimer := requireFACORECHG020QueueItemClaimer(t, repo)
		claimed, err := claimer.ClaimQueueItemByID(ctx, 1<<62)
		require.NoError(t, err)
		assert.Nil(t, claimed)
	})

	t.Run("manual_manual_has_one_winner", func(t *testing.T) {
		claimer := requireFACORECHG020QueueItemClaimer(t, repo)
		seed := seedFACORECHG020QueueItem(t, ctx, repo, QueueStatusPending)

		outcomes := runFACORECHG020Claims(
			func() (*ImportQueueItem, error) { return claimer.ClaimQueueItemByID(ctx, seed.ID) },
			func() (*ImportQueueItem, error) { return claimer.ClaimQueueItemByID(ctx, seed.ID) },
		)
		errorCount := assertFACORECHG020ClaimWinners(t, outcomes, 1, seed.ID)
		assert.Equal(t, 1, errorCount, "the losing by-ID claimant must receive a conflict")
	})

	t.Run("manual_and_next_claim_have_one_winner", func(t *testing.T) {
		claimer := requireFACORECHG020QueueItemClaimer(t, repo)
		seed := seedFACORECHG020QueueItem(t, ctx, repo, QueueStatusPending)

		outcomes := runFACORECHG020Claims(
			func() (*ImportQueueItem, error) { return claimer.ClaimQueueItemByID(ctx, seed.ID) },
			func() (*ImportQueueItem, error) { return repo.ClaimNextQueueItem(ctx) },
		)
		errorCount := assertFACORECHG020ClaimWinners(t, outcomes, 1, seed.ID)
		assert.LessOrEqual(t, errorCount, 1,
			"the automatic loser returns nil while the by-ID loser reports a conflict")
	})

	t.Run("distinct_ids_are_independently_claimable", func(t *testing.T) {
		claimer := requireFACORECHG020QueueItemClaimer(t, repo)
		first := seedFACORECHG020QueueItem(t, ctx, repo, QueueStatusPending)
		second := seedFACORECHG020QueueItem(t, ctx, repo, QueueStatusPending)

		outcomes := runFACORECHG020Claims(
			func() (*ImportQueueItem, error) { return claimer.ClaimQueueItemByID(ctx, first.ID) },
			func() (*ImportQueueItem, error) { return claimer.ClaimQueueItemByID(ctx, second.ID) },
		)
		errorCount := assertFACORECHG020ClaimWinners(t, outcomes, 2, first.ID, second.ID)
		assert.Zero(t, errorCount)
	})
}

func seedFACORECHG020QueueItem(
	t *testing.T,
	ctx context.Context,
	repo *QueueRepository,
	status QueueStatus,
) *ImportQueueItem {
	t.Helper()
	item := &ImportQueueItem{
		NzbPath:    fmt.Sprintf("facore/chg020/%s.nzb", uuid.NewString()),
		Status:     status,
		Priority:   QueuePriorityHigh,
		MaxRetries: 3,
	}
	require.NoError(t, repo.AddToQueue(ctx, item))
	require.NotZero(t, item.ID)
	t.Cleanup(func() { assert.NoError(t, repo.RemoveFromQueue(context.Background(), item.ID)) })
	return item
}

type facoreCHG020ClaimOutcome struct {
	item *ImportQueueItem
	err  error
}

func runFACORECHG020Claims(
	claims ...func() (*ImportQueueItem, error),
) []facoreCHG020ClaimOutcome {
	start := make(chan struct{})
	results := make(chan facoreCHG020ClaimOutcome, len(claims))
	var ready sync.WaitGroup
	ready.Add(len(claims))
	for _, claim := range claims {
		claim := claim
		go func() {
			ready.Done()
			<-start
			item, err := claim()
			results <- facoreCHG020ClaimOutcome{item: item, err: err}
		}()
	}
	ready.Wait()
	close(start)

	outcomes := make([]facoreCHG020ClaimOutcome, 0, len(claims))
	for range claims {
		outcomes = append(outcomes, <-results)
	}
	return outcomes
}

func assertFACORECHG020ClaimWinners(
	t *testing.T,
	outcomes []facoreCHG020ClaimOutcome,
	wantWinners int,
	wantIDs ...int64,
) int {
	t.Helper()
	winners := make(map[int64]int)
	errorCount := 0
	for _, outcome := range outcomes {
		if outcome.err != nil {
			errorCount++
			assert.Nil(t, outcome.item)
			continue
		}
		if outcome.item != nil {
			winners[outcome.item.ID]++
			assert.Equal(t, QueueStatusProcessing, outcome.item.Status)
		}
	}
	require.Len(t, winners, wantWinners)
	for _, id := range wantIDs {
		assert.Equalf(t, 1, winners[id], "queue item %d must be claimed exactly once", id)
	}
	return errorCount
}
