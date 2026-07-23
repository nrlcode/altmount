-- +goose Up
-- PostgreSQL alone needs repair; SQLite migration 027 already adds metadata.
SELECT 1;

-- +goose Down
-- Migration 027 owns metadata; preserve it when downgrading to version 37.
SELECT 1;
