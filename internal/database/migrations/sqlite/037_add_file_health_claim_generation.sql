-- +goose Up
ALTER TABLE file_health
ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0
CHECK (claim_generation >= 0);

-- +goose Down
ALTER TABLE file_health DROP COLUMN claim_generation;
