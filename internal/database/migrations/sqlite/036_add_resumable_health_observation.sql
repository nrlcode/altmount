-- +goose Up
-- +goose StatementBegin

-- PR5 import admission needs a durable queue hold. QueueStatusPaused predates
-- this migration in Go, but the original SQLite constraint never admitted it.
-- Rebuild the table before health_import_validations adds its foreign key.
-- Dropping a parent table applies import_history's ON DELETE SET NULL action,
-- so retain and restore those references explicitly inside this transaction.
CREATE TEMP TABLE import_history_queue_refs_pr5 AS
SELECT id, nzb_id FROM import_history WHERE nzb_id IS NOT NULL;
CREATE TABLE import_queue_pr5 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nzb_path TEXT NOT NULL,
    relative_path TEXT DEFAULT NULL,
    storage_path TEXT DEFAULT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
        'pending', 'processing', 'completed', 'failed', 'fallback', 'paused'
    )),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME DEFAULT NULL,
    completed_at DATETIME DEFAULT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    error_message TEXT DEFAULT NULL,
    batch_id TEXT DEFAULT NULL,
    metadata TEXT DEFAULT NULL,
    category TEXT DEFAULT NULL,
    file_size BIGINT DEFAULT NULL,
    target_path TEXT DEFAULT NULL,
    download_id TEXT DEFAULT NULL,
    skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
    skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
    indexer TEXT DEFAULT NULL,
    UNIQUE(nzb_path)
);
INSERT INTO import_queue_pr5 (
    id, nzb_path, relative_path, storage_path, priority, status, created_at,
    updated_at, started_at, completed_at, retry_count, max_retries,
    error_message, batch_id, metadata, category, file_size, target_path,
    download_id, skip_arr_notification, skip_post_import_links, indexer
)
SELECT id, nzb_path, relative_path, storage_path, priority, status, created_at,
       updated_at, started_at, completed_at, retry_count, max_retries,
       error_message, batch_id, metadata, category, file_size, target_path,
       download_id, skip_arr_notification, skip_post_import_links, indexer
FROM import_queue;
DROP TABLE import_queue;
ALTER TABLE import_queue_pr5 RENAME TO import_queue;
UPDATE import_history
SET nzb_id = (
    SELECT retained.nzb_id
    FROM import_history_queue_refs_pr5 retained
    WHERE retained.id = import_history.id
)
WHERE id IN (SELECT id FROM import_history_queue_refs_pr5);
DROP TABLE import_history_queue_refs_pr5;
CREATE INDEX idx_queue_status_priority ON import_queue(status, priority, created_at);
CREATE INDEX idx_queue_batch_id ON import_queue(batch_id);
CREATE INDEX idx_queue_status ON import_queue(status);
CREATE INDEX idx_queue_retry ON import_queue(status, retry_count, max_retries);
CREATE INDEX idx_queue_nzb_path ON import_queue(nzb_path);
CREATE INDEX idx_import_queue_category ON import_queue(category);
CREATE INDEX idx_queue_file_size ON import_queue(file_size);
CREATE INDEX idx_import_queue_nzbdav_id
    ON import_queue(json_extract(metadata, '$.nzbdav_id'));
CREATE INDEX idx_queue_download_id ON import_queue(download_id);
CREATE INDEX idx_queue_status_updated ON import_queue(status, updated_at);
CREATE INDEX idx_import_queue_indexer ON import_queue(indexer);

ALTER TABLE health_providers
    ADD COLUMN activation_epoch BIGINT NOT NULL DEFAULT 1 CHECK(activation_epoch >= 1);
-- SQLite forbids a non-constant CURRENT_TIMESTAMP default on ADD COLUMN. The
-- repository supplies activated_at on every provider insert; this sentinel is
-- used only to make the additive migration legal, and retained rows are
-- immediately backfilled below. Rebuilding this referenced root table solely
-- to mirror PostgreSQL's default expression would add avoidable migration risk.
ALTER TABLE health_providers
    ADD COLUMN activated_at DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00+00:00';
UPDATE health_providers SET activated_at = created_at;

ALTER TABLE health_provider_snapshot_entries
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);

ALTER TABLE health_runs ADD COLUMN last_error TEXT DEFAULT NULL;

-- Several PR4 tables are rebuilt below. Remove the file-root trigger first so
-- SQLite never reparses it while one of its referenced tables is temporarily
-- absent. The migration transaction recreates it after the complete PR5 tree.
DROP TRIGGER health_clear_durable_state_before_file_delete;

-- Freeze the provider activation boundary into every durable observation row.
-- PR4 rows predate activation epochs and are conservatively attributed to the
-- first epoch. Resolved bitmaps start empty for retained PR4 chunks so an
-- upgrade can never manufacture completed positional work.
ALTER TABLE health_run_chunks
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);
ALTER TABLE health_run_chunks
    ADD COLUMN resolved_bitmap BLOB NOT NULL DEFAULT X'';
UPDATE health_run_chunks
SET resolved_bitmap = zeroblob((segment_count + 7) / 8);

ALTER TABLE health_provider_coverage
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);
ALTER TABLE health_provider_coverage
    ADD COLUMN resolved_bitmap BLOB NOT NULL DEFAULT X'';
UPDATE health_provider_coverage
SET resolved_bitmap = zeroblob((segment_count + 7) / 8);

ALTER TABLE health_attempt_evidence
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);
ALTER TABLE health_confirmation_events
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);
ALTER TABLE health_retry_states
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);

DROP INDEX idx_health_provider_coverage_lookup;
CREATE INDEX idx_health_provider_coverage_lookup
    ON health_provider_coverage(
        file_revision_id, provider_id, provider_generation,
        provider_activation_epoch, segment_start
    );
DROP INDEX idx_health_attempt_evidence_lookup;
CREATE INDEX idx_health_attempt_evidence_lookup
    ON health_attempt_evidence(
        file_revision_id, provider_id, provider_generation,
        provider_activation_epoch, segment_index
    );

-- Segment exceptions are current routing materializations, but prior
-- activation epochs remain durable history. Include the epoch in the key so a
-- reactivated provider cannot overwrite or inherit the previous epoch.
DROP INDEX idx_health_segment_exceptions_outcome;
CREATE TABLE health_segment_exceptions_pr5 (
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    segment_index BIGINT NOT NULL CHECK(segment_index >= 0),
    outcome TEXT NOT NULL CHECK(outcome IN (
        'hard_absence', 'corrupt_body', 'temporary_failure', 'provider_unavailable',
        'transport_failure', 'canceled', 'inconclusive'
    )),
    source_chunk_id TEXT NOT NULL REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    observed_at DATETIME NOT NULL,
    next_retry_at DATETIME DEFAULT NULL,
    PRIMARY KEY(
        file_revision_id, provider_id, provider_generation,
        provider_activation_epoch, segment_index
    ),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
INSERT INTO health_segment_exceptions_pr5
    (file_revision_id, provider_id, provider_generation,
     provider_activation_epoch, segment_index, outcome, source_chunk_id,
     observed_at, next_retry_at)
SELECT file_revision_id, provider_id, provider_generation, 1, segment_index,
       outcome, source_chunk_id, observed_at, next_retry_at
FROM health_segment_exceptions;
DROP TABLE health_segment_exceptions;
ALTER TABLE health_segment_exceptions_pr5 RENAME TO health_segment_exceptions;
CREATE INDEX idx_health_segment_exceptions_outcome
    ON health_segment_exceptions(file_revision_id, outcome, segment_index);

-- SQLite cannot drop the PR4 inline natural-key UNIQUE constraint. Rebuild
-- the parent and both dependent tables in one transaction so existing gap
-- causes and synthetic-range history remain attached to their original IDs.
DROP INDEX idx_health_gap_ranges_active;
DROP INDEX idx_health_synthetic_unrecovered;

CREATE TABLE health_gap_ranges_pr5 (
    id TEXT NOT NULL PRIMARY KEY,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    kind TEXT NOT NULL CHECK(kind IN (
        'provisional', 'confirmed_absent', 'confirmed_unusable', 'legacy_unverified'
    )),
    start_segment BIGINT NOT NULL CHECK(start_segment >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count > 0),
    episode BIGINT NOT NULL DEFAULT 1 CHECK(episode >= 1),
    status TEXT NOT NULL CHECK(status IN ('active', 'cleared', 'dormant')),
    created_at DATETIME NOT NULL,
    confirmed_at DATETIME DEFAULT NULL,
    cleared_at DATETIME DEFAULT NULL,
    UNIQUE(file_revision_id, kind, start_segment, segment_count, episode)
);
INSERT INTO health_gap_ranges_pr5
    (id, file_revision_id, kind, start_segment, segment_count, episode, status,
     created_at, confirmed_at, cleared_at)
SELECT id, file_revision_id, kind, start_segment, segment_count, 1, status,
       created_at, confirmed_at, cleared_at
FROM health_gap_ranges;

CREATE TABLE health_gap_provider_causes_pr5 (
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges_pr5(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    cause TEXT NOT NULL CHECK(cause IN ('absent', 'corrupt')),
    confirmation_count INTEGER NOT NULL DEFAULT 0 CHECK(confirmation_count >= 0),
    confirmed_at DATETIME DEFAULT NULL,
    PRIMARY KEY(gap_id, provider_id, provider_generation, provider_activation_epoch),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
INSERT INTO health_gap_provider_causes_pr5
    (gap_id, provider_id, provider_generation, provider_activation_epoch,
     cause, confirmation_count, confirmed_at)
SELECT gap_id, provider_id, provider_generation, 1,
       cause, confirmation_count, confirmed_at
FROM health_gap_provider_causes;

CREATE TABLE health_synthetic_ranges_pr5 (
    id TEXT NOT NULL PRIMARY KEY,
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges_pr5(id),
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    byte_start BIGINT NOT NULL CHECK(byte_start >= 0),
    byte_end BIGINT NOT NULL CHECK(byte_end >= byte_start),
    emitted_at DATETIME NOT NULL,
    recovered_at DATETIME DEFAULT NULL,
    verified_at DATETIME DEFAULT NULL,
    UNIQUE(file_revision_id, byte_start, byte_end, emitted_at)
);
INSERT INTO health_synthetic_ranges_pr5
SELECT * FROM health_synthetic_ranges;

DROP TABLE health_gap_provider_causes;
DROP TABLE health_synthetic_ranges;
DROP TABLE health_gap_ranges;
ALTER TABLE health_gap_ranges_pr5 RENAME TO health_gap_ranges;
ALTER TABLE health_gap_provider_causes_pr5 RENAME TO health_gap_provider_causes;
ALTER TABLE health_synthetic_ranges_pr5 RENAME TO health_synthetic_ranges;

CREATE INDEX idx_health_gap_ranges_active
    ON health_gap_ranges(file_revision_id, status, start_segment);
CREATE UNIQUE INDEX idx_health_gap_ranges_one_active_exact
    ON health_gap_ranges(file_revision_id, kind, start_segment, segment_count)
    WHERE status = 'active';
CREATE INDEX idx_health_synthetic_unrecovered
    ON health_synthetic_ranges(file_revision_id, recovered_at);

CREATE TABLE health_run_schedule (
    run_id TEXT NOT NULL PRIMARY KEY REFERENCES health_runs(id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    target_provider_id TEXT DEFAULT NULL,
    target_provider_generation BIGINT DEFAULT NULL CHECK(target_provider_generation >= 1),
    target_provider_activation_epoch BIGINT DEFAULT NULL CHECK(target_provider_activation_epoch >= 1),
    target_gap_id TEXT DEFAULT NULL REFERENCES health_gap_ranges(id),
    priority INTEGER NOT NULL CHECK(priority >= 0 AND priority <= 2),
    not_before DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    CHECK(
        (target_provider_id IS NULL AND target_provider_generation IS NULL
         AND target_provider_activation_epoch IS NULL)
        OR
        (target_provider_id IS NOT NULL AND target_provider_generation IS NOT NULL
         AND target_provider_activation_epoch IS NOT NULL)
    ),
    FOREIGN KEY(target_provider_id, target_provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE UNIQUE INDEX idx_health_run_schedule_active_dedupe
    ON health_run_schedule(dedupe_key) WHERE active = 1;
CREATE INDEX idx_health_run_schedule_due
    ON health_run_schedule(active, not_before, priority DESC, created_at);

CREATE TABLE health_import_validations (
    id TEXT NOT NULL PRIMARY KEY,
    queue_item_id INTEGER NOT NULL REFERENCES import_queue(id) ON DELETE CASCADE,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    run_id TEXT NOT NULL REFERENCES health_runs(id),
    phase TEXT NOT NULL CHECK(phase IN (
        'initial_pass', 'confirmation_wait', 'confirmation_pass',
        'accepted', 'health_pending', 'rejected'
    )),
    damage_policy TEXT NOT NULL CHECK(damage_policy IN ('strict', 'tolerant')),
    confirmation_due_at DATETIME DEFAULT NULL,
    unresolved_segments BIGINT NOT NULL DEFAULT 0 CHECK(unresolved_segments >= 0),
    unresolved_bitmap BLOB NOT NULL DEFAULT X'',
    initial_pass_complete BOOLEAN NOT NULL DEFAULT 0 CHECK(initial_pass_complete IN (0, 1)),
    second_pass_complete BOOLEAN NOT NULL DEFAULT 0 CHECK(second_pass_complete IN (0, 1)),
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE(queue_item_id, file_revision_id),
    CHECK(phase <> 'confirmation_wait' OR confirmation_due_at IS NOT NULL),
    CHECK(second_pass_complete = 0 OR initial_pass_complete = 1)
);
CREATE UNIQUE INDEX idx_health_import_validations_run
    ON health_import_validations(run_id);
CREATE INDEX idx_health_import_validations_confirmation_due
    ON health_import_validations(phase, confirmation_due_at);

CREATE TRIGGER health_clear_durable_state_before_file_delete
BEFORE DELETE ON file_health
FOR EACH ROW
BEGIN
    DELETE FROM health_import_validations
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_run_schedule
      WHERE run_id IN (SELECT id FROM health_runs WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      )) OR target_gap_id IN (SELECT id FROM health_gap_ranges WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      ));
    DELETE FROM health_cache_recovery
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_synthetic_ranges
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_gap_provider_causes
      WHERE gap_id IN (SELECT id FROM health_gap_ranges WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      ));
    DELETE FROM health_gap_ranges
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_retry_states
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_confirmation_events
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_attempt_evidence
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_segment_exceptions
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_provider_coverage
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_run_chunks
      WHERE run_id IN (SELECT id FROM health_runs WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      ));
    DELETE FROM health_runs
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_file_revisions WHERE file_health_id = OLD.id;
END;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TRIGGER health_clear_durable_state_before_file_delete;
DROP TABLE health_import_validations;
DROP TABLE health_run_schedule;

-- PR4 cannot persist a paused import admission hold. Resume any retained hold
-- and restore the original queue constraint while preserving history links.
UPDATE import_queue SET status = 'pending' WHERE status = 'paused';
CREATE TEMP TABLE import_history_queue_refs_pr4 AS
SELECT id, nzb_id FROM import_history WHERE nzb_id IS NOT NULL;
CREATE TABLE import_queue_pr4 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nzb_path TEXT NOT NULL,
    relative_path TEXT DEFAULT NULL,
    storage_path TEXT DEFAULT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
        'pending', 'processing', 'completed', 'failed', 'fallback'
    )),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME DEFAULT NULL,
    completed_at DATETIME DEFAULT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    error_message TEXT DEFAULT NULL,
    batch_id TEXT DEFAULT NULL,
    metadata TEXT DEFAULT NULL,
    category TEXT DEFAULT NULL,
    file_size BIGINT DEFAULT NULL,
    target_path TEXT DEFAULT NULL,
    download_id TEXT DEFAULT NULL,
    skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
    skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
    indexer TEXT DEFAULT NULL,
    UNIQUE(nzb_path)
);
INSERT INTO import_queue_pr4 (
    id, nzb_path, relative_path, storage_path, priority, status, created_at,
    updated_at, started_at, completed_at, retry_count, max_retries,
    error_message, batch_id, metadata, category, file_size, target_path,
    download_id, skip_arr_notification, skip_post_import_links, indexer
)
SELECT id, nzb_path, relative_path, storage_path, priority, status, created_at,
       updated_at, started_at, completed_at, retry_count, max_retries,
       error_message, batch_id, metadata, category, file_size, target_path,
       download_id, skip_arr_notification, skip_post_import_links, indexer
FROM import_queue;
DROP TABLE import_queue;
ALTER TABLE import_queue_pr4 RENAME TO import_queue;
UPDATE import_history
SET nzb_id = (
    SELECT retained.nzb_id
    FROM import_history_queue_refs_pr4 retained
    WHERE retained.id = import_history.id
)
WHERE id IN (SELECT id FROM import_history_queue_refs_pr4);
DROP TABLE import_history_queue_refs_pr4;
CREATE INDEX idx_queue_status_priority ON import_queue(status, priority, created_at);
CREATE INDEX idx_queue_batch_id ON import_queue(batch_id);
CREATE INDEX idx_queue_status ON import_queue(status);
CREATE INDEX idx_queue_retry ON import_queue(status, retry_count, max_retries);
CREATE INDEX idx_queue_nzb_path ON import_queue(nzb_path);
CREATE INDEX idx_import_queue_category ON import_queue(category);
CREATE INDEX idx_queue_file_size ON import_queue(file_size);
CREATE INDEX idx_import_queue_nzbdav_id
    ON import_queue(json_extract(metadata, '$.nzbdav_id'));
CREATE INDEX idx_queue_download_id ON import_queue(download_id);
CREATE INDEX idx_queue_status_updated ON import_queue(status, updated_at);
CREATE INDEX idx_import_queue_indexer ON import_queue(indexer);

-- PR4 cannot encode provider activation epochs. Before removing that boundary,
-- move every currently active reactivated provider to a fresh representable
-- generation with the same transport identity. Historical observations stay
-- on their original generation, so a later PR5 upgrade cannot reinterpret
-- retained epoch-one coverage or negative evidence as current.
CREATE TEMP TABLE health_provider_downgrade_generations (
    provider_id TEXT NOT NULL PRIMARY KEY,
    generation BIGINT NOT NULL
);
INSERT INTO health_provider_downgrade_generations (provider_id, generation)
SELECT p.id, MAX(g.generation) + 1
FROM health_providers p
JOIN health_provider_generations g ON g.provider_id = p.id
WHERE p.active = TRUE AND p.activation_epoch > 1
GROUP BY p.id;
INSERT INTO health_provider_generations
    (provider_id, generation, endpoint, port, account,
     identity_fingerprint, created_at)
SELECT replacement.provider_id, replacement.generation,
       current.endpoint, current.port, current.account,
       current.identity_fingerprint, provider.activated_at
FROM health_provider_downgrade_generations replacement
JOIN health_providers provider ON provider.id = replacement.provider_id
JOIN health_provider_generations current
  ON current.provider_id = provider.id
 AND current.generation = provider.current_generation;
UPDATE health_providers
SET current_generation = (
    SELECT replacement.generation
    FROM health_provider_downgrade_generations replacement
    WHERE replacement.provider_id = health_providers.id
)
WHERE id IN (SELECT provider_id FROM health_provider_downgrade_generations);
DROP TABLE health_provider_downgrade_generations;

-- Downgrading removes episode history. Keep the currently active episode when
-- one exists, otherwise the latest episode, which is the single lifecycle row
-- representable by the PR4 schema.
DROP INDEX idx_health_gap_ranges_one_active_exact;
DROP INDEX idx_health_gap_ranges_active;
DROP INDEX idx_health_synthetic_unrecovered;

CREATE TABLE health_gap_ranges_pr4 (
    id TEXT NOT NULL PRIMARY KEY,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    kind TEXT NOT NULL CHECK(kind IN (
        'provisional', 'confirmed_absent', 'confirmed_unusable', 'legacy_unverified'
    )),
    start_segment BIGINT NOT NULL CHECK(start_segment >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count > 0),
    status TEXT NOT NULL CHECK(status IN ('active', 'cleared', 'dormant')),
    created_at DATETIME NOT NULL,
    confirmed_at DATETIME DEFAULT NULL,
    cleared_at DATETIME DEFAULT NULL,
    UNIQUE(file_revision_id, kind, start_segment, segment_count)
);
INSERT INTO health_gap_ranges_pr4
    (id, file_revision_id, kind, start_segment, segment_count, status,
     created_at, confirmed_at, cleared_at)
SELECT id, file_revision_id, kind, start_segment, segment_count, status,
       created_at, confirmed_at, cleared_at
FROM (
    SELECT g.*,
           ROW_NUMBER() OVER (
               PARTITION BY file_revision_id, kind, start_segment, segment_count
               ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                        episode DESC, created_at DESC, id DESC
           ) AS keep_rank
    FROM health_gap_ranges g
)
WHERE keep_rank = 1;

CREATE TABLE health_gap_provider_causes_pr4 (
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges_pr4(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    cause TEXT NOT NULL CHECK(cause IN ('absent', 'corrupt')),
    confirmation_count INTEGER NOT NULL DEFAULT 0 CHECK(confirmation_count >= 0),
    confirmed_at DATETIME DEFAULT NULL,
    PRIMARY KEY(gap_id, provider_id, provider_generation),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
INSERT INTO health_gap_provider_causes_pr4
    (gap_id, provider_id, provider_generation, cause,
     confirmation_count, confirmed_at)
SELECT gap_id, provider_id, provider_generation, cause,
       confirmation_count, confirmed_at
FROM (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY c.gap_id, c.provider_id, c.provider_generation
               ORDER BY c.provider_activation_epoch DESC,
                        c.confirmed_at DESC, c.cause DESC
           ) AS keep_rank
    FROM health_gap_provider_causes c
    JOIN health_gap_ranges_pr4 g ON g.id = c.gap_id
)
WHERE keep_rank = 1;

CREATE TABLE health_synthetic_ranges_pr4 (
    id TEXT NOT NULL PRIMARY KEY,
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges_pr4(id),
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    byte_start BIGINT NOT NULL CHECK(byte_start >= 0),
    byte_end BIGINT NOT NULL CHECK(byte_end >= byte_start),
    emitted_at DATETIME NOT NULL,
    recovered_at DATETIME DEFAULT NULL,
    verified_at DATETIME DEFAULT NULL,
    UNIQUE(file_revision_id, byte_start, byte_end, emitted_at)
);
INSERT INTO health_synthetic_ranges_pr4
    (id, gap_id, file_revision_id, byte_start, byte_end, emitted_at,
     recovered_at, verified_at)
SELECT s.id, retained.id, s.file_revision_id, s.byte_start, s.byte_end,
       s.emitted_at, s.recovered_at, s.verified_at
FROM health_synthetic_ranges s
JOIN health_gap_ranges source ON source.id = s.gap_id
JOIN health_gap_ranges_pr4 retained
  ON retained.file_revision_id = source.file_revision_id
 AND retained.kind = source.kind
 AND retained.start_segment = source.start_segment
 AND retained.segment_count = source.segment_count;

DROP TABLE health_gap_provider_causes;
DROP TABLE health_synthetic_ranges;
DROP TABLE health_gap_ranges;
ALTER TABLE health_gap_ranges_pr4 RENAME TO health_gap_ranges;
ALTER TABLE health_gap_provider_causes_pr4 RENAME TO health_gap_provider_causes;
ALTER TABLE health_synthetic_ranges_pr4 RENAME TO health_synthetic_ranges;

CREATE INDEX idx_health_gap_ranges_active
    ON health_gap_ranges(file_revision_id, status, start_segment);
CREATE INDEX idx_health_synthetic_unrecovered
    ON health_synthetic_ranges(file_revision_id, recovered_at);

-- PR4 has no activation boundary on segment exceptions. Keep the newest
-- activation's materialized outcome for each old natural key while retaining
-- all immutable chunk/evidence rows in their PR4 tables.
DROP INDEX idx_health_segment_exceptions_outcome;
CREATE TABLE health_segment_exceptions_pr4 (
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    segment_index BIGINT NOT NULL CHECK(segment_index >= 0),
    outcome TEXT NOT NULL CHECK(outcome IN (
        'hard_absence', 'corrupt_body', 'temporary_failure', 'provider_unavailable',
        'transport_failure', 'canceled', 'inconclusive'
    )),
    source_chunk_id TEXT NOT NULL REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    observed_at DATETIME NOT NULL,
    next_retry_at DATETIME DEFAULT NULL,
    PRIMARY KEY(file_revision_id, provider_id, provider_generation, segment_index),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
INSERT INTO health_segment_exceptions_pr4
    (file_revision_id, provider_id, provider_generation, segment_index,
     outcome, source_chunk_id, observed_at, next_retry_at)
SELECT file_revision_id, provider_id, provider_generation, segment_index,
       outcome, source_chunk_id, observed_at, next_retry_at
FROM (
    SELECT e.*,
           ROW_NUMBER() OVER (
               PARTITION BY e.file_revision_id, e.provider_id,
                            e.provider_generation, e.segment_index
               ORDER BY e.provider_activation_epoch DESC,
                        e.observed_at DESC, e.source_chunk_id DESC
           ) AS keep_rank
    FROM health_segment_exceptions e
)
WHERE keep_rank = 1;
DROP TABLE health_segment_exceptions;
ALTER TABLE health_segment_exceptions_pr4 RENAME TO health_segment_exceptions;
CREATE INDEX idx_health_segment_exceptions_outcome
    ON health_segment_exceptions(file_revision_id, outcome, segment_index);

DROP INDEX idx_health_provider_coverage_lookup;
ALTER TABLE health_provider_coverage DROP COLUMN resolved_bitmap;
ALTER TABLE health_provider_coverage DROP COLUMN provider_activation_epoch;
CREATE INDEX idx_health_provider_coverage_lookup
    ON health_provider_coverage(file_revision_id, provider_id, provider_generation, segment_start);

DROP INDEX idx_health_attempt_evidence_lookup;
ALTER TABLE health_attempt_evidence DROP COLUMN provider_activation_epoch;
CREATE INDEX idx_health_attempt_evidence_lookup
    ON health_attempt_evidence(file_revision_id, provider_id, provider_generation, segment_index);

ALTER TABLE health_confirmation_events DROP COLUMN provider_activation_epoch;
ALTER TABLE health_retry_states DROP COLUMN provider_activation_epoch;
ALTER TABLE health_run_chunks DROP COLUMN resolved_bitmap;
ALTER TABLE health_run_chunks DROP COLUMN provider_activation_epoch;

ALTER TABLE health_runs DROP COLUMN last_error;
ALTER TABLE health_provider_snapshot_entries DROP COLUMN provider_activation_epoch;
ALTER TABLE health_providers DROP COLUMN activated_at;
ALTER TABLE health_providers DROP COLUMN activation_epoch;

CREATE TRIGGER health_clear_durable_state_before_file_delete
BEFORE DELETE ON file_health
FOR EACH ROW
BEGIN
    DELETE FROM health_cache_recovery
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_synthetic_ranges
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_gap_provider_causes
      WHERE gap_id IN (SELECT id FROM health_gap_ranges WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      ));
    DELETE FROM health_gap_ranges
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_retry_states
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_confirmation_events
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_attempt_evidence
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_segment_exceptions
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_provider_coverage
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_run_chunks
      WHERE run_id IN (SELECT id FROM health_runs WHERE file_revision_id IN (
        SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id
      ));
    DELETE FROM health_runs
      WHERE file_revision_id IN (SELECT id FROM health_file_revisions WHERE file_health_id = OLD.id);
    DELETE FROM health_file_revisions WHERE file_health_id = OLD.id;
END;

-- +goose StatementEnd
