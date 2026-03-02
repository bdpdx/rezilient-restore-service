BEGIN;

CREATE TABLE IF NOT EXISTS rez_restore_index.source_progress_v2 (
    ingest_scope_id TEXT PRIMARY KEY,
    source_uri TEXT NOT NULL,
    cursor TEXT,
    last_batch_size INTEGER NOT NULL CHECK (last_batch_size >= 0),
    last_indexed_event_time TIMESTAMPTZ,
    last_indexed_offset BIGINT CHECK (last_indexed_offset >= 0),
    last_lag_seconds INTEGER CHECK (last_lag_seconds >= 0),
    processed_count BIGINT NOT NULL CHECK (processed_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    last_observed_tenant_id TEXT,
    last_observed_instance_id TEXT,
    last_observed_source TEXT
);

CREATE TABLE IF NOT EXISTS rez_restore_index.source_leader_leases_v2 (
    ingest_scope_id TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_source_progress_v2_updated_at
ON rez_restore_index.source_progress_v2 (updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_source_leader_leases_v2_active
ON rez_restore_index.source_leader_leases_v2 (lease_expires_at DESC);

COMMIT;
