BEGIN;

CREATE TABLE IF NOT EXISTS rez_restore_index.source_progress (
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cursor TEXT,
    last_batch_size INTEGER NOT NULL CHECK (last_batch_size >= 0),
    last_indexed_event_time TIMESTAMPTZ,
    last_indexed_offset BIGINT CHECK (last_indexed_offset >= 0),
    last_lag_seconds INTEGER CHECK (last_lag_seconds >= 0),
    processed_count BIGINT NOT NULL CHECK (processed_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, instance_id, source)
);

COMMIT;
