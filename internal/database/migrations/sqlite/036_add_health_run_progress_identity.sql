-- +goose Up
-- +goose StatementBegin

ALTER TABLE health_runs
    ADD COLUMN cursor_sequence BIGINT NOT NULL DEFAULT 0 CHECK(cursor_sequence >= 0);

ALTER TABLE health_run_chunks
    ADD COLUMN resolved_bitmap BLOB DEFAULT NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE health_run_chunks DROP COLUMN resolved_bitmap;
ALTER TABLE health_runs DROP COLUMN cursor_sequence;

-- +goose StatementEnd
