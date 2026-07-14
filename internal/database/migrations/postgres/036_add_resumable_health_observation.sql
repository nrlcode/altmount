-- +goose Up
-- +goose StatementBegin

ALTER TABLE import_queue DROP CONSTRAINT import_queue_status_check;
ALTER TABLE import_queue ADD CONSTRAINT import_queue_status_check
    CHECK(status IN (
        'pending', 'processing', 'completed', 'failed', 'fallback', 'paused'
    ));

ALTER TABLE health_providers
    ADD COLUMN activation_epoch BIGINT NOT NULL DEFAULT 1 CHECK(activation_epoch >= 1),
    ADD COLUMN activated_at TIMESTAMPTZ;
UPDATE health_providers SET activated_at = created_at;
ALTER TABLE health_providers
    ALTER COLUMN activated_at SET NOT NULL,
    ALTER COLUMN activated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE health_provider_snapshot_entries
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1);

ALTER TABLE health_runs ADD COLUMN last_error TEXT DEFAULT NULL;

-- Freeze activation identity into every durable observation. PR4 evidence is
-- attributed to epoch one. Retained PR4 chunks receive a zeroed resolved map,
-- which is conservative: an upgrade never invents completed positions.
ALTER TABLE health_run_chunks
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    ADD COLUMN resolved_bitmap BYTEA NOT NULL DEFAULT '\x'::bytea,
    ADD COLUMN fresh_transport BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE health_run_chunks
SET resolved_bitmap = decode(
    repeat('00', ((segment_count + 7) / 8)::INTEGER), 'hex'
);

ALTER TABLE health_provider_coverage
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    ADD COLUMN resolved_bitmap BYTEA NOT NULL DEFAULT '\x'::bytea;
UPDATE health_provider_coverage
SET resolved_bitmap = decode(
    repeat('00', ((segment_count + 7) / 8)::INTEGER), 'hex'
);

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

ALTER TABLE health_segment_exceptions
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    DROP CONSTRAINT health_segment_exceptions_pkey,
    ADD PRIMARY KEY(
        file_revision_id, provider_id, provider_generation,
        provider_activation_epoch, segment_index
    );

ALTER TABLE health_gap_ranges
    ADD COLUMN episode BIGINT NOT NULL DEFAULT 1 CHECK(episode >= 1);
DO $$
DECLARE
    natural_key_name TEXT;
BEGIN
    SELECT conname INTO natural_key_name
    FROM pg_constraint
    WHERE conrelid = 'health_gap_ranges'::regclass AND contype = 'u'
    LIMIT 1;
    IF natural_key_name IS NULL THEN
        RAISE EXCEPTION 'PR4 health gap natural-key constraint is missing';
    END IF;
    EXECUTE format('ALTER TABLE health_gap_ranges DROP CONSTRAINT %I', natural_key_name);
END;
$$;
ALTER TABLE health_gap_ranges
    ADD CONSTRAINT health_gap_ranges_episode_key
        UNIQUE(file_revision_id, kind, start_segment, segment_count, episode);
CREATE UNIQUE INDEX idx_health_gap_ranges_one_active_exact
    ON health_gap_ranges(file_revision_id, kind, start_segment, segment_count)
    WHERE status = 'active';

ALTER TABLE health_gap_ranges
    ADD COLUMN revalidation_step INTEGER NOT NULL DEFAULT 0
        CHECK(revalidation_step >= 0 AND revalidation_step <= 4),
    ADD COLUMN next_revalidation_at TIMESTAMPTZ DEFAULT NULL,
    ADD COLUMN last_revalidation_at TIMESTAMPTZ DEFAULT NULL;
UPDATE health_gap_ranges
SET next_revalidation_at = confirmed_at + INTERVAL '1 day'
WHERE status = 'active'
  AND kind IN ('confirmed_absent', 'confirmed_unusable')
  AND confirmed_at IS NOT NULL;
CREATE INDEX idx_health_gap_revalidation_due
    ON health_gap_ranges(status, next_revalidation_at, revalidation_step);

ALTER TABLE health_gap_provider_causes
    ADD COLUMN provider_activation_epoch BIGINT NOT NULL DEFAULT 1
        CHECK(provider_activation_epoch >= 1),
    DROP CONSTRAINT health_gap_provider_causes_pkey,
    ADD PRIMARY KEY(
        gap_id, provider_id, provider_generation, provider_activation_epoch
    );

CREATE TABLE health_run_schedule (
    run_id TEXT NOT NULL PRIMARY KEY REFERENCES health_runs(id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    target_provider_id TEXT DEFAULT NULL,
    target_provider_generation BIGINT DEFAULT NULL CHECK(target_provider_generation >= 1),
    target_provider_activation_epoch BIGINT DEFAULT NULL CHECK(target_provider_activation_epoch >= 1),
    target_gap_id TEXT DEFAULT NULL REFERENCES health_gap_ranges(id),
    priority INTEGER NOT NULL CHECK(priority >= 0 AND priority <= 2),
    not_before TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
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
    ON health_run_schedule(dedupe_key) WHERE active = TRUE;
CREATE INDEX idx_health_run_schedule_due
    ON health_run_schedule(active, not_before, priority DESC, created_at);
CREATE INDEX idx_health_run_schedule_activation_target
    ON health_run_schedule(
        target_provider_id, target_provider_generation,
        target_provider_activation_epoch, target_gap_id
    );

CREATE TABLE health_import_validations (
    id TEXT NOT NULL PRIMARY KEY,
    queue_item_id BIGINT NOT NULL REFERENCES import_queue(id) ON DELETE CASCADE,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    run_id TEXT NOT NULL REFERENCES health_runs(id),
    phase TEXT NOT NULL CHECK(phase IN (
        'initial_pass', 'confirmation_wait', 'confirmation_pass',
        'accepted', 'health_pending', 'rejected'
    )),
    damage_policy TEXT NOT NULL CHECK(damage_policy IN ('strict', 'tolerant')),
    confirmation_due_at TIMESTAMPTZ DEFAULT NULL,
    unresolved_segments BIGINT NOT NULL DEFAULT 0 CHECK(unresolved_segments >= 0),
    unresolved_bitmap BYTEA NOT NULL DEFAULT '\x'::bytea,
    initial_pass_complete BOOLEAN NOT NULL DEFAULT FALSE,
    second_pass_complete BOOLEAN NOT NULL DEFAULT FALSE,
    coverage_reused_at TIMESTAMPTZ DEFAULT NULL,
    health_pending_settled_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE(queue_item_id, file_revision_id),
    CHECK(phase <> 'confirmation_wait' OR confirmation_due_at IS NOT NULL),
    CHECK(NOT second_pass_complete OR initial_pass_complete)
);
CREATE UNIQUE INDEX idx_health_import_validations_run
    ON health_import_validations(run_id);
CREATE INDEX idx_health_import_validations_confirmation_due
    ON health_import_validations(phase, confirmation_due_at);

CREATE TABLE health_import_activation_journal (
    queue_item_id BIGINT NOT NULL REFERENCES import_queue(id) ON DELETE CASCADE,
    candidate_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    file_health_id BIGINT NOT NULL REFERENCES file_health(id),
    prior_revision_id TEXT DEFAULT NULL REFERENCES health_file_revisions(id),
    prior_status TEXT NOT NULL,
    prior_scheduled_check_at TIMESTAMPTZ DEFAULT NULL,
    prior_priority INTEGER NOT NULL,
    prior_retry_count INTEGER NOT NULL CHECK(prior_retry_count >= 0),
    prior_repair_retry_count INTEGER NOT NULL CHECK(prior_repair_retry_count >= 0),
    candidate_scheduled_check_at TIMESTAMPTZ NOT NULL,
    candidate_priority INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'active', 'committed', 'cleanup_pending', 'cleanup_completed', 'compensated'
    )),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY(queue_item_id, candidate_revision_id),
    UNIQUE(queue_item_id, file_health_id)
);
CREATE INDEX idx_health_import_activation_journal_state
    ON health_import_activation_journal(queue_item_id, state, updated_at);

CREATE TABLE nzb_store_ref_operations (
    operation_key TEXT NOT NULL PRIMARY KEY,
    store_path_hash TEXT NOT NULL,
    delta INTEGER NOT NULL CHECK(delta IN (-1, 1)),
    resulting_ref_count BIGINT NOT NULL CHECK(resulting_ref_count >= 0),
    applied_at TIMESTAMPTZ NOT NULL
);

CREATE OR REPLACE FUNCTION health_clear_durable_state_before_file_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM health_import_activation_journal
      WHERE file_health_id = OLD.id;
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
    RETURN OLD;
END;
$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TABLE nzb_store_ref_operations;
DROP TABLE health_import_activation_journal;
DROP TABLE health_import_validations;
DROP TABLE health_run_schedule;

UPDATE import_queue SET status = 'pending' WHERE status = 'paused';
ALTER TABLE import_queue DROP CONSTRAINT import_queue_status_check;
ALTER TABLE import_queue ADD CONSTRAINT import_queue_status_check
    CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'fallback'));

-- PR4 cannot encode provider activation epochs. Before removing that boundary,
-- move every currently active reactivated provider to a fresh representable
-- generation with the same transport identity. Historical observations stay
-- on their original generation, so a later PR5 upgrade cannot reinterpret
-- retained epoch-one coverage or negative evidence as current.
CREATE TEMP TABLE health_provider_downgrade_generations ON COMMIT DROP AS
SELECT p.id AS provider_id, MAX(g.generation) + 1 AS generation
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
UPDATE health_providers provider
SET current_generation = replacement.generation
FROM health_provider_downgrade_generations replacement
WHERE provider.id = replacement.provider_id;

-- PR4 can represent one row per exact range. Preserve the active episode when
-- present, otherwise the latest historical episode, and remove dependencies
-- belonging only to episode history before restoring the old constraint.
CREATE TEMP TABLE health_gap_ranges_pr4_keep ON COMMIT DROP AS
SELECT id
FROM (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY file_revision_id, kind, start_segment, segment_count
               ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                        episode DESC, created_at DESC, id DESC
           ) AS keep_rank
    FROM health_gap_ranges
) ranked
WHERE keep_rank = 1;

-- Synthetic rows describe bytes actually emitted and are immutable recovery
-- evidence. Rebind rows from discarded episodes to the one exact-range row
-- representable by PR4 instead of deleting that history.
UPDATE health_synthetic_ranges AS synthetic
SET gap_id = retained.id
FROM health_gap_ranges source
JOIN health_gap_ranges retained
  ON retained.file_revision_id = source.file_revision_id
 AND retained.kind = source.kind
 AND retained.start_segment = source.start_segment
 AND retained.segment_count = source.segment_count
JOIN health_gap_ranges_pr4_keep keep ON keep.id = retained.id
WHERE synthetic.gap_id = source.id
  AND synthetic.gap_id <> retained.id;

DELETE FROM health_gap_provider_causes
WHERE gap_id NOT IN (SELECT id FROM health_gap_ranges_pr4_keep);
DELETE FROM health_gap_ranges
WHERE id NOT IN (SELECT id FROM health_gap_ranges_pr4_keep);

-- PR4 has no activation dimension on materialized gap causes. Retain the
-- newest activation's cause for each old key while leaving immutable evidence
-- in its source tables.
DELETE FROM health_gap_provider_causes cause
USING (
    SELECT gap_id, provider_id, provider_generation,
           provider_activation_epoch,
           ROW_NUMBER() OVER (
               PARTITION BY gap_id, provider_id, provider_generation
               ORDER BY provider_activation_epoch DESC,
                        confirmed_at DESC NULLS LAST, cause DESC
           ) AS keep_rank
    FROM health_gap_provider_causes
) ranked
WHERE ranked.keep_rank > 1
  AND cause.gap_id = ranked.gap_id
  AND cause.provider_id = ranked.provider_id
  AND cause.provider_generation = ranked.provider_generation
  AND cause.provider_activation_epoch = ranked.provider_activation_epoch;

ALTER TABLE health_gap_provider_causes
    DROP CONSTRAINT health_gap_provider_causes_pkey,
    DROP COLUMN provider_activation_epoch,
    ADD PRIMARY KEY(gap_id, provider_id, provider_generation);

DROP INDEX idx_health_gap_ranges_one_active_exact;
DROP INDEX idx_health_gap_revalidation_due;
ALTER TABLE health_gap_ranges DROP CONSTRAINT health_gap_ranges_episode_key;
ALTER TABLE health_gap_ranges
    DROP COLUMN revalidation_step,
    DROP COLUMN next_revalidation_at,
    DROP COLUMN last_revalidation_at;
ALTER TABLE health_gap_ranges DROP COLUMN episode;
ALTER TABLE health_gap_ranges
    ADD CONSTRAINT health_gap_ranges_exact_key
        UNIQUE(file_revision_id, kind, start_segment, segment_count);

-- Collapse current exception materializations to the newest activation before
-- restoring the PR4 key. All source chunks/evidence remain retained.
DELETE FROM health_segment_exceptions exception
USING (
    SELECT file_revision_id, provider_id, provider_generation,
           provider_activation_epoch, segment_index,
           ROW_NUMBER() OVER (
               PARTITION BY file_revision_id, provider_id,
                            provider_generation, segment_index
               ORDER BY provider_activation_epoch DESC,
                        observed_at DESC, source_chunk_id DESC
           ) AS keep_rank
    FROM health_segment_exceptions
) ranked
WHERE ranked.keep_rank > 1
  AND exception.file_revision_id = ranked.file_revision_id
  AND exception.provider_id = ranked.provider_id
  AND exception.provider_generation = ranked.provider_generation
  AND exception.provider_activation_epoch = ranked.provider_activation_epoch
  AND exception.segment_index = ranked.segment_index;

ALTER TABLE health_segment_exceptions
    DROP CONSTRAINT health_segment_exceptions_pkey,
    DROP COLUMN provider_activation_epoch,
    ADD PRIMARY KEY(file_revision_id, provider_id, provider_generation, segment_index);

DROP INDEX idx_health_provider_coverage_lookup;
ALTER TABLE health_provider_coverage
    DROP COLUMN resolved_bitmap,
    DROP COLUMN provider_activation_epoch;
CREATE INDEX idx_health_provider_coverage_lookup
    ON health_provider_coverage(file_revision_id, provider_id, provider_generation, segment_start);

DROP INDEX idx_health_attempt_evidence_lookup;
ALTER TABLE health_attempt_evidence DROP COLUMN provider_activation_epoch;
CREATE INDEX idx_health_attempt_evidence_lookup
    ON health_attempt_evidence(file_revision_id, provider_id, provider_generation, segment_index);

ALTER TABLE health_confirmation_events DROP COLUMN provider_activation_epoch;
ALTER TABLE health_retry_states DROP COLUMN provider_activation_epoch;
ALTER TABLE health_run_chunks
    DROP COLUMN fresh_transport,
    DROP COLUMN resolved_bitmap,
    DROP COLUMN provider_activation_epoch;

ALTER TABLE health_runs DROP COLUMN last_error;
ALTER TABLE health_provider_snapshot_entries DROP COLUMN provider_activation_epoch;
ALTER TABLE health_providers
    DROP COLUMN activated_at,
    DROP COLUMN activation_epoch;

CREATE OR REPLACE FUNCTION health_clear_durable_state_before_file_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
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
    RETURN OLD;
END;
$$;

-- +goose StatementEnd
