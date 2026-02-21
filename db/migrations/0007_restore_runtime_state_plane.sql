BEGIN;

CREATE TABLE IF NOT EXISTS rez_restore_index.rrs_state_snapshots (
    store_key TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_rrs_state_snapshots_updated_at
ON rez_restore_index.rrs_state_snapshots (updated_at DESC);

COMMIT;
