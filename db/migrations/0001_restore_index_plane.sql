BEGIN;

CREATE SCHEMA IF NOT EXISTS rez_restore_index;

CREATE TABLE IF NOT EXISTS rez_restore_index.index_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    app TEXT,
    table_name TEXT,
    record_sys_id TEXT,
    attachment_sys_id TEXT,
    media_id TEXT,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    operation TEXT CHECK (operation IN ('I', 'U', 'D')),
    schema_version INTEGER,
    sys_updated_on TEXT,
    sys_mod_count INTEGER CHECK (sys_mod_count >= 0),
    event_time TIMESTAMPTZ NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    content_type TEXT,
    size_bytes BIGINT CHECK (size_bytes >= 0),
    sha256_plain CHAR(64),
    artifact_key TEXT NOT NULL,
    manifest_key TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    generation_id TEXT NOT NULL,
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        tenant_id,
        instance_id,
        source,
        topic,
        kafka_partition,
        generation_id,
        event_id
    )
);

COMMENT ON TABLE rez_restore_index.index_events IS
'Allowlisted restore metadata only. Plaintext value fields are prohibited.';

CREATE INDEX IF NOT EXISTS ix_index_events_lookup
ON rez_restore_index.index_events (
    tenant_id,
    instance_id,
    source,
    table_name,
    record_sys_id,
    event_time
);

CREATE INDEX IF NOT EXISTS ix_index_events_partition_offsets
ON rez_restore_index.index_events (
    tenant_id,
    instance_id,
    topic,
    kafka_partition,
    kafka_offset DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.partition_watermarks (
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    generation_id TEXT NOT NULL,
    indexed_through_offset BIGINT NOT NULL CHECK (indexed_through_offset >= 0),
    indexed_through_time TIMESTAMPTZ NOT NULL,
    coverage_start TIMESTAMPTZ NOT NULL,
    coverage_end TIMESTAMPTZ NOT NULL,
    freshness TEXT NOT NULL CHECK (freshness IN ('fresh', 'stale', 'unknown')),
    executability TEXT NOT NULL CHECK (
        executability IN ('executable', 'preview_only', 'blocked')
    ),
    reason_code TEXT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, instance_id, topic, kafka_partition)
);

CREATE INDEX IF NOT EXISTS ix_partition_watermarks_source
ON rez_restore_index.partition_watermarks (
    tenant_id,
    instance_id,
    source,
    topic,
    kafka_partition
);

CREATE TABLE IF NOT EXISTS rez_restore_index.partition_generations (
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    generation_id TEXT NOT NULL,
    generation_started_at TIMESTAMPTZ NOT NULL,
    generation_ended_at TIMESTAMPTZ,
    max_indexed_offset BIGINT NOT NULL CHECK (max_indexed_offset >= 0),
    max_indexed_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        tenant_id,
        instance_id,
        topic,
        kafka_partition,
        generation_id
    )
);

CREATE TABLE IF NOT EXISTS rez_restore_index.source_coverage (
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    earliest_indexed_time TIMESTAMPTZ NOT NULL,
    latest_indexed_time TIMESTAMPTZ NOT NULL,
    generation_span JSONB NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, instance_id, source)
);

DO $$
BEGIN
    CREATE TYPE rez_restore_index.backfill_mode AS ENUM (
        'bootstrap',
        'gap_repair'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

DO $$
BEGIN
    CREATE TYPE rez_restore_index.backfill_status AS ENUM (
        'running',
        'paused',
        'completed',
        'failed'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

CREATE TABLE IF NOT EXISTS rez_restore_index.backfill_runs (
    run_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    mode rez_restore_index.backfill_mode NOT NULL,
    status rez_restore_index.backfill_status NOT NULL,
    pause_reason_code TEXT NOT NULL,
    throttle_batch_size INTEGER NOT NULL CHECK (throttle_batch_size > 0),
    max_realtime_lag_seconds INTEGER NOT NULL CHECK (
        max_realtime_lag_seconds >= 0
    ),
    last_cursor TEXT,
    rows_processed BIGINT NOT NULL DEFAULT 0 CHECK (rows_processed >= 0),
    started_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_backfill_runs_lookup
ON rez_restore_index.backfill_runs (
    tenant_id,
    instance_id,
    source,
    mode,
    status,
    updated_at DESC
);

COMMIT;
