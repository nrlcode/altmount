-- +goose Up
-- +goose StatementBegin

CREATE TABLE health_file_revisions (
    id TEXT NOT NULL PRIMARY KEY,
    file_health_id INTEGER NOT NULL REFERENCES file_health(id) ON DELETE CASCADE,
    layout_fingerprint TEXT NOT NULL,
    virtual_size BIGINT NOT NULL CHECK(virtual_size >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count >= 0),
    active BOOLEAN NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    created_at DATETIME NOT NULL,
    activated_at DATETIME NOT NULL,
    UNIQUE(file_health_id, layout_fingerprint)
);
CREATE UNIQUE INDEX idx_health_file_revisions_active
    ON health_file_revisions(file_health_id) WHERE active = 1;
CREATE INDEX idx_health_file_revisions_fingerprint
    ON health_file_revisions(layout_fingerprint);

CREATE TABLE health_providers (
    id TEXT NOT NULL PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('primary', 'backup')),
    configured_order INTEGER NOT NULL CHECK(configured_order >= 0),
    active BOOLEAN NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    current_generation BIGINT NOT NULL DEFAULT 1 CHECK(current_generation >= 1),
    tombstoned_at DATETIME DEFAULT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);
CREATE INDEX idx_health_providers_active_order
    ON health_providers(active, configured_order);

CREATE TABLE health_provider_generations (
    provider_id TEXT NOT NULL REFERENCES health_providers(id) ON DELETE RESTRICT,
    generation BIGINT NOT NULL CHECK(generation >= 1),
    endpoint TEXT NOT NULL,
    port INTEGER NOT NULL CHECK(port > 0 AND port <= 65535),
    account TEXT NOT NULL,
    identity_fingerprint TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY(provider_id, generation)
);
CREATE INDEX idx_health_provider_generation_identity
    ON health_provider_generations(identity_fingerprint);

CREATE TABLE health_provider_snapshots (
    id TEXT NOT NULL PRIMARY KEY,
    created_at DATETIME NOT NULL
);

CREATE TABLE health_provider_snapshot_entries (
    snapshot_id TEXT NOT NULL REFERENCES health_provider_snapshots(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('primary', 'backup')),
    configured_order INTEGER NOT NULL CHECK(configured_order >= 0),
    PRIMARY KEY(snapshot_id, provider_id),
    UNIQUE(snapshot_id, configured_order),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);

CREATE TABLE health_runs (
    id TEXT NOT NULL PRIMARY KEY,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_snapshot_id TEXT NOT NULL REFERENCES health_provider_snapshots(id),
    trigger TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'paused', 'canceled', 'completed', 'failed')),
    lease_owner TEXT DEFAULT NULL,
    lease_expires_at DATETIME DEFAULT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token >= 0),
    total_segments BIGINT NOT NULL CHECK(total_segments >= 0),
    resolved_segments BIGINT NOT NULL DEFAULT 0 CHECK(resolved_segments >= 0),
    provider_checks BIGINT NOT NULL DEFAULT 0 CHECK(provider_checks >= 0),
    missing_candidates BIGINT NOT NULL DEFAULT 0 CHECK(missing_candidates >= 0),
    inconclusive_count BIGINT NOT NULL DEFAULT 0 CHECK(inconclusive_count >= 0),
    stage TEXT NOT NULL DEFAULT '',
    current_provider_id TEXT DEFAULT NULL,
    current_provider_generation BIGINT DEFAULT NULL,
    cursor_segment BIGINT NOT NULL DEFAULT 0 CHECK(cursor_segment >= 0),
    pause_requested BOOLEAN NOT NULL DEFAULT 0 CHECK(pause_requested IN (0, 1)),
    cancel_requested BOOLEAN NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0, 1)),
    created_at DATETIME NOT NULL,
    started_at DATETIME DEFAULT NULL,
    updated_at DATETIME NOT NULL,
    completed_at DATETIME DEFAULT NULL
);
CREATE INDEX idx_health_runs_revision_status
    ON health_runs(file_revision_id, status);
CREATE INDEX idx_health_runs_lease
    ON health_runs(status, lease_expires_at);

CREATE TABLE health_run_chunks (
    id TEXT NOT NULL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES health_runs(id) ON DELETE RESTRICT,
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    stage TEXT NOT NULL,
    observation_kind TEXT NOT NULL CHECK(observation_kind IN ('stat', 'validated_body')),
    segment_start BIGINT NOT NULL CHECK(segment_start >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count > 0),
    tested_bitmap BLOB NOT NULL,
    present_bitmap BLOB NOT NULL,
    absent_bitmap BLOB NOT NULL,
    corrupt_bitmap BLOB NOT NULL,
    temporary_bitmap BLOB NOT NULL,
    inconclusive_bitmap BLOB NOT NULL,
    retry_state TEXT DEFAULT NULL,
    commit_digest TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    resolved_delta BIGINT NOT NULL DEFAULT 0 CHECK(resolved_delta >= 0),
    provider_checks_delta BIGINT NOT NULL DEFAULT 0 CHECK(provider_checks_delta >= 0),
    missing_candidates_delta BIGINT NOT NULL DEFAULT 0 CHECK(missing_candidates_delta >= 0),
    inconclusive_delta BIGINT NOT NULL DEFAULT 0 CHECK(inconclusive_delta >= 0),
    committed_at DATETIME NOT NULL,
    UNIQUE(run_id, id),
    UNIQUE(run_id, provider_id, provider_generation, stage, segment_start, segment_count),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE INDEX idx_health_run_chunks_run_range
    ON health_run_chunks(run_id, segment_start);

CREATE TABLE health_provider_coverage (
    id TEXT NOT NULL PRIMARY KEY,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    observation_kind TEXT NOT NULL CHECK(observation_kind IN ('stat', 'validated_body')),
    segment_start BIGINT NOT NULL CHECK(segment_start >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count > 0),
    tested_bitmap BLOB NOT NULL,
    present_bitmap BLOB NOT NULL,
    source_chunk_id TEXT NOT NULL UNIQUE REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    observed_at DATETIME NOT NULL,
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE INDEX idx_health_provider_coverage_lookup
    ON health_provider_coverage(file_revision_id, provider_id, provider_generation, segment_start);

CREATE TABLE health_segment_exceptions (
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
CREATE INDEX idx_health_segment_exceptions_outcome
    ON health_segment_exceptions(file_revision_id, outcome, segment_index);

CREATE TABLE health_attempt_evidence (
    idempotency_key TEXT NOT NULL PRIMARY KEY,
    source_chunk_id TEXT NOT NULL REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    segment_index BIGINT NOT NULL CHECK(segment_index >= 0),
    operation TEXT NOT NULL,
    outcome TEXT NOT NULL,
    response_code INTEGER DEFAULT NULL,
    body_validation TEXT NOT NULL,
    cause_class TEXT NOT NULL DEFAULT '',
    admission_wait_ns BIGINT NOT NULL DEFAULT 0 CHECK(admission_wait_ns >= 0),
    pool_queue_ns BIGINT NOT NULL DEFAULT 0 CHECK(pool_queue_ns >= 0),
    pipeline_wait_ns BIGINT NOT NULL DEFAULT 0 CHECK(pipeline_wait_ns >= 0),
    response_service_ns BIGINT NOT NULL DEFAULT 0 CHECK(response_service_ns >= 0),
    observed_at DATETIME NOT NULL,
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE INDEX idx_health_attempt_evidence_lookup
    ON health_attempt_evidence(file_revision_id, provider_id, provider_generation, segment_index);

CREATE TABLE health_confirmation_events (
    idempotency_key TEXT NOT NULL PRIMARY KEY,
    source_chunk_id TEXT NOT NULL REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    segment_index BIGINT NOT NULL CHECK(segment_index >= 0),
    cause TEXT NOT NULL CHECK(cause IN ('absent', 'corrupt')),
    observed_at DATETIME NOT NULL,
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE INDEX idx_health_confirmation_lookup
    ON health_confirmation_events(file_revision_id, segment_index, observed_at);

CREATE TABLE health_retry_states (
    retry_key TEXT NOT NULL PRIMARY KEY,
    source_chunk_id TEXT NOT NULL REFERENCES health_run_chunks(id) ON DELETE RESTRICT,
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    segment_start BIGINT NOT NULL CHECK(segment_start >= 0),
    segment_count BIGINT NOT NULL CHECK(segment_count > 0),
    outcome TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt >= 0),
    next_attempt_at DATETIME DEFAULT NULL,
    exhausted BOOLEAN NOT NULL DEFAULT 0 CHECK(exhausted IN (0, 1)),
    updated_at DATETIME NOT NULL,
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);
CREATE INDEX idx_health_retry_due
    ON health_retry_states(exhausted, next_attempt_at);

CREATE TABLE health_gap_ranges (
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
CREATE INDEX idx_health_gap_ranges_active
    ON health_gap_ranges(file_revision_id, status, start_segment);

CREATE TABLE health_gap_provider_causes (
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    provider_generation BIGINT NOT NULL,
    cause TEXT NOT NULL CHECK(cause IN ('absent', 'corrupt')),
    confirmation_count INTEGER NOT NULL DEFAULT 0 CHECK(confirmation_count >= 0),
    confirmed_at DATETIME DEFAULT NULL,
    PRIMARY KEY(gap_id, provider_id, provider_generation),
    FOREIGN KEY(provider_id, provider_generation)
        REFERENCES health_provider_generations(provider_id, generation)
);

CREATE TABLE health_synthetic_ranges (
    id TEXT NOT NULL PRIMARY KEY,
    gap_id TEXT NOT NULL REFERENCES health_gap_ranges(id),
    file_revision_id TEXT NOT NULL REFERENCES health_file_revisions(id),
    byte_start BIGINT NOT NULL CHECK(byte_start >= 0),
    byte_end BIGINT NOT NULL CHECK(byte_end >= byte_start),
    emitted_at DATETIME NOT NULL,
    recovered_at DATETIME DEFAULT NULL,
    verified_at DATETIME DEFAULT NULL,
    UNIQUE(file_revision_id, byte_start, byte_end, emitted_at)
);
CREATE INDEX idx_health_synthetic_unrecovered
    ON health_synthetic_ranges(file_revision_id, recovered_at);

CREATE TABLE health_cache_recovery (
    file_revision_id TEXT NOT NULL PRIMARY KEY REFERENCES health_file_revisions(id),
    status TEXT NOT NULL CHECK(status IN ('clean', 'synthetic', 'pending', 'in_progress', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    next_retry_at DATETIME DEFAULT NULL,
    last_error TEXT DEFAULT NULL,
    content_revision BIGINT NOT NULL DEFAULT 0 CHECK(content_revision >= 0),
    updated_at DATETIME NOT NULL
);

-- Deleting file_health is the existing explicit file-level health-clear
-- boundary. Preserve restrictive ordinary run deletion while allowing that
-- root operation to remove the complete file-scoped durable tree.
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

-- +goose Down
-- +goose StatementBegin

DROP TRIGGER IF EXISTS health_clear_durable_state_before_file_delete;
DROP TABLE IF EXISTS health_cache_recovery;
DROP TABLE IF EXISTS health_synthetic_ranges;
DROP TABLE IF EXISTS health_gap_provider_causes;
DROP TABLE IF EXISTS health_gap_ranges;
DROP TABLE IF EXISTS health_retry_states;
DROP TABLE IF EXISTS health_confirmation_events;
DROP TABLE IF EXISTS health_attempt_evidence;
DROP TABLE IF EXISTS health_segment_exceptions;
DROP TABLE IF EXISTS health_provider_coverage;
DROP TABLE IF EXISTS health_run_chunks;
DROP TABLE IF EXISTS health_runs;
DROP TABLE IF EXISTS health_provider_snapshot_entries;
DROP TABLE IF EXISTS health_provider_snapshots;
DROP TABLE IF EXISTS health_provider_generations;
DROP TABLE IF EXISTS health_providers;
DROP TABLE IF EXISTS health_file_revisions;

-- +goose StatementEnd
