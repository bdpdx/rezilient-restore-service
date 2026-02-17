BEGIN;

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_execution_checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES rez_restore_index.restore_jobs (job_id)
        ON DELETE CASCADE,
    plan_hash CHAR(64) NOT NULL,
    plan_checksum CHAR(64) NOT NULL,
    precondition_checksum CHAR(64) NOT NULL,
    next_chunk_index INTEGER NOT NULL CHECK (next_chunk_index >= 0),
    total_chunks INTEGER NOT NULL CHECK (total_chunks >= 0),
    last_chunk_id TEXT,
    row_attempt_by_row JSONB NOT NULL DEFAULT '{}'::jsonb,
    checkpoint_status TEXT NOT NULL CHECK (
        checkpoint_status IN ('paused', 'completed', 'failed')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id)
);

CREATE INDEX IF NOT EXISTS ix_restore_execution_checkpoints_lookup
ON rez_restore_index.restore_execution_checkpoints (
    job_id,
    updated_at DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_rollback_journal (
    journal_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES rez_restore_index.restore_jobs (job_id)
        ON DELETE CASCADE,
    plan_hash CHAR(64) NOT NULL,
    plan_row_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_sys_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (
        action IN ('update', 'insert', 'delete', 'skip')
    ),
    touched_fields TEXT[] NOT NULL CHECK (
        cardinality(touched_fields) > 0
    ),
    before_image_enc JSONB,
    chunk_id TEXT NOT NULL,
    row_attempt INTEGER NOT NULL CHECK (row_attempt > 0),
    row_attempt_key CHAR(64) NOT NULL,
    executed_by TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL CHECK (
        outcome IN ('applied', 'skipped', 'failed')
    ),
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (outcome <> 'applied') OR before_image_enc IS NOT NULL
    ),
    UNIQUE (job_id, row_attempt_key)
);

CREATE INDEX IF NOT EXISTS ix_restore_rollback_journal_lookup
ON rez_restore_index.restore_rollback_journal (
    job_id,
    executed_at DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_rollback_sn_mirror (
    mirror_id TEXT PRIMARY KEY,
    journal_id TEXT NOT NULL REFERENCES rez_restore_index.restore_rollback_journal (
        journal_id
    ) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES rez_restore_index.restore_jobs (job_id)
        ON DELETE CASCADE,
    plan_hash CHAR(64) NOT NULL,
    plan_row_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_sys_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (
        action IN ('update', 'insert', 'delete', 'skip')
    ),
    outcome TEXT NOT NULL CHECK (
        outcome IN ('applied', 'skipped', 'failed')
    ),
    reason_code TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    row_attempt INTEGER NOT NULL CHECK (row_attempt > 0),
    linked_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (journal_id)
);

CREATE INDEX IF NOT EXISTS ix_restore_rollback_sn_mirror_lookup
ON rez_restore_index.restore_rollback_sn_mirror (
    job_id,
    linked_at DESC
);

COMMIT;
