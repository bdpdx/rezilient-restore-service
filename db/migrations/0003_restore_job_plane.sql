BEGIN;

CREATE TABLE IF NOT EXISTS rez_restore_index.source_registry (
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, instance_id)
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_plans (
    plan_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    plan_hash CHAR(64) NOT NULL,
    plan_hash_algorithm TEXT NOT NULL,
    plan_hash_input_version TEXT NOT NULL,
    lock_scope_tables TEXT[] NOT NULL CHECK (
        cardinality(lock_scope_tables) > 0
    ),
    approval_required BOOLEAN NOT NULL DEFAULT FALSE,
    approval_state TEXT NOT NULL,
    approval_policy_id TEXT,
    approval_requested_at TIMESTAMPTZ,
    approval_requested_by TEXT,
    approval_decided_at TIMESTAMPTZ,
    approval_decided_by TEXT,
    approval_decision TEXT,
    approval_decision_reason TEXT,
    approval_external_ref TEXT,
    approval_snapshot_hash CHAR(64),
    approval_valid_until TIMESTAMPTZ,
    approval_revalidated_at TIMESTAMPTZ,
    approval_revalidation_result TEXT,
    approval_placeholder_mode TEXT NOT NULL DEFAULT 'mvp_not_enforced',
    metadata_allowlist_version TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, instance_id)
        REFERENCES rez_restore_index.source_registry (tenant_id, instance_id)
);

CREATE INDEX IF NOT EXISTS ix_restore_plans_lookup
ON rez_restore_index.restore_plans (
    tenant_id,
    instance_id,
    source,
    requested_at DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_jobs (
    job_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    source TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    plan_hash CHAR(64) NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled')
    ),
    status_reason_code TEXT NOT NULL,
    lock_scope_tables TEXT[] NOT NULL CHECK (
        cardinality(lock_scope_tables) > 0
    ),
    required_capabilities TEXT[] NOT NULL CHECK (
        cardinality(required_capabilities) > 0
    ),
    requested_by TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    queue_position INTEGER CHECK (queue_position >= 1),
    wait_reason_code TEXT,
    wait_tables TEXT[] NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    approval_required BOOLEAN NOT NULL DEFAULT FALSE,
    approval_state TEXT NOT NULL,
    approval_policy_id TEXT,
    approval_requested_at TIMESTAMPTZ,
    approval_requested_by TEXT,
    approval_decided_at TIMESTAMPTZ,
    approval_decided_by TEXT,
    approval_decision TEXT,
    approval_decision_reason TEXT,
    approval_external_ref TEXT,
    approval_snapshot_hash CHAR(64),
    approval_valid_until TIMESTAMPTZ,
    approval_revalidated_at TIMESTAMPTZ,
    approval_revalidation_result TEXT,
    approval_placeholder_mode TEXT NOT NULL DEFAULT 'mvp_not_enforced',
    metadata_allowlist_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (plan_id)
        REFERENCES rez_restore_index.restore_plans (plan_id),
    FOREIGN KEY (tenant_id, instance_id)
        REFERENCES rez_restore_index.source_registry (tenant_id, instance_id)
);

CREATE INDEX IF NOT EXISTS ix_restore_jobs_lookup
ON rez_restore_index.restore_jobs (
    tenant_id,
    instance_id,
    source,
    status,
    requested_at DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_job_locks (
    lock_id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES rez_restore_index.restore_jobs (job_id)
        ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    lock_state TEXT NOT NULL CHECK (lock_state IN ('queued', 'running')),
    reason_code TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, instance_id)
        REFERENCES rez_restore_index.source_registry (tenant_id, instance_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_restore_job_running_locks
ON rez_restore_index.restore_job_locks (
    tenant_id,
    instance_id,
    table_name
)
WHERE lock_state = 'running' AND released_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_restore_job_locks_job
ON rez_restore_index.restore_job_locks (
    job_id,
    lock_state,
    acquired_at DESC
);

CREATE TABLE IF NOT EXISTS rez_restore_index.restore_job_events (
    event_id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES rez_restore_index.restore_jobs (job_id)
        ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_restore_job_events_lookup
ON rez_restore_index.restore_job_events (
    job_id,
    created_at DESC
);

COMMIT;
