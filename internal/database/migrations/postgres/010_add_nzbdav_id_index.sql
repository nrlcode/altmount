-- +goose Up
-- +goose StatementBegin

-- PostgreSQL requires the complete expression, including ->>, inside the
-- expression-index parentheses. The former `(metadata::jsonb)->>...` shape
-- failed on every fresh PostgreSQL migration chain before PR4 could run.
CREATE INDEX IF NOT EXISTS idx_import_queue_nzbdav_id ON import_queue ((metadata::jsonb ->> 'nzbdav_id'));

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_import_queue_nzbdav_id;

-- +goose StatementEnd
