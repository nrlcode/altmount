-- +goose Up
ALTER TABLE file_health
ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT NULL;

-- +goose Down
-- Migration 027 owns metadata; preserve it when downgrading to version 37.
SELECT 1;
