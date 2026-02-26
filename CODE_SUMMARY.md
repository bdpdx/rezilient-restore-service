# rezilient-restore-service

Purpose:
- Restore backend service for RS-07/RS-08/RS-09/RS-10/RS-11/RS-12 with:
  - auth-scoped restore job creation,
  - tenant/instance/source resolution checks,
  - dry-run planning with deterministic `plan_hash`,
  - freshness/deletion/conflict executability gating,
  - scoped lock queueing by `(tenant, instance, table)`,
  - durable Postgres-backed persistence for dry-run plans, job metadata,
    job-audit events, normalized cross-service audit events, lock queue
    ownership, execution checkpoints/journals, and evidence export/verification
    records across restart,
  - RS-09 execute orchestration with capability/conflict enforcement,
    chunk-first apply, and row-level fallback recording,
  - RS-10 checksum-gated resume checkpoints and authoritative rollback-journal
    linkage records (with compact SN mirror summaries),
  - RS-11 attachment/media candidate enforcement (explicit include/exclude),
    hard-cap override gating, parent/hash validation, bounded media retries,
    and per-item media outcome records,
  - RS-12 signed evidence package generation with canonical artifact/report
    hashing, signature verification state, immutable-storage metadata, and
    export endpoints,
  - RS-14 admin ops dashboards for queue/lock visibility, freshness/backfill
    summaries, and evidence verification status,
  - RS-15 admin SLO/burn-rate and GA-readiness/staging controls.

Entrypoints:
- `src/index.ts`: restore-service bootstrap and HTTP server startup.
- `src/server.ts`: HTTP API handlers for health, dry-run plans, jobs, and
  queue-state transitions, including evidence export/read APIs and
  cross-service job audit read path.
- `src/plans/models.ts`: RS-08 dry-run request/response/gate schemas.
- `src/plans/plan-service.ts`: RS-08 dry-run orchestration, deterministic hash
  generation, and freshness/deletion/conflict gate evaluation from
  authoritative restore-index state.
- `src/plans/plan-state-store.ts`: durable/in-memory state stores for
  persisted dry-run plan records.
- `src/restore-index/state-reader.ts`: authoritative restore-index watermark
  readers (in-memory + Postgres-backed) with freshness derivation policy.
- `src/execute/models.ts`: RS-09 execute request and result schemas.
- `src/execute/execute-service.ts`: RS-09 execute engine plus RS-10 resume flow
  enforcing plan immutability, capability checks, conflict matrix rules, chunk
  fallback, checkpoint persistence, rollback journal/mirror records, and RS-11
  media candidate execution outcomes.
- `src/execute/execute-state-store.ts`: durable/in-memory state stores for
  execution records, checkpoints, and rollback journal linkage.
- `src/evidence/signature.ts`: canonical payload signing and verification
  helpers for RS-12 evidence.
- `src/evidence/evidence-service.ts`: RS-12 evidence package builder and
  verifier (artifact hashes, report hash, signed manifest metadata).
- `src/evidence/evidence-state-store.ts`: durable/in-memory state stores for
  evidence export records and signature-verification metadata.
- `src/registry/acp-source-mapping-client.ts`: ACP internal source-mapping
  client used for staged migration away from static source mapping config.
- `src/registry/acp-source-mapping-provider.ts`: positive/negative TTL cache
  provider for ACP source-mapping resolve calls.
- `src/admin/ops-admin-service.ts`: RS-14/RS-15 admin ops summary service for
  queue/freshness/evidence plus SLO burn-rate and GA gate readiness checks.
- `src/jobs/job-service.ts`: restore job orchestration, legacy queue audit
  events, and normalized cross-service audit event emission.
- `src/jobs/job-state-store.ts`: durable/in-memory state stores for plan/job/
  event metadata and persisted lock queue snapshots.
- `src/locks/lock-manager.ts`: lock acquisition/release and queued-job
  promotion, plus import/export helpers for persisted lock-state recovery.
- `db/migrations/0001_restore_index_plane.sql`: RS-06 restore index tables.
- `db/migrations/0002_restore_index_roles.sql`: RS-06 index role grants.
- `db/migrations/0003_restore_job_plane.sql`: RS-07 job/plan/lock queue schema.
- `db/migrations/0004_restore_job_roles.sql`: RS-07 restore-service rw grants.
- `db/migrations/0005_restore_resume_journal_plane.sql`: RS-10 checkpoint and
  rollback-journal schema.
- `db/migrations/0006_restore_resume_journal_roles.sql`: RS-10 role grants for
  checkpoint and rollback-journal tables.
- `db/migrations/0007_restore_runtime_state_plane.sql`: RS-10 durable runtime
  snapshot state table for service-owned plan/job/execute/evidence state.
- `db/migrations/0008_restore_runtime_state_roles.sql`: RS-10 grants for
  runtime snapshot table access by restore-service roles.
- `db/migrations/0009_restore_index_source_progress_plane.sql`: RS-06 source
  progress checkpoint table in `rez_restore_index`.
- `db/migrations/0010_restore_index_source_progress_roles.sql`: RS-06 grants
  for source progress checkpoint table access.

Tests:
- `src/db-schema.test.ts`: migration contract checks for RS-06 + RS-07 schema,
  approval placeholders, and role grants.
- `src/locks/lock-manager.test.ts`: lock overlap arbitration tests.
- `src/jobs/job-service.test.ts`: parallel non-overlap and queued overlap tests.
- `src/core-state.durability.test.ts`: restart-survival and queue-fairness
  tests for durable plan/job/event/lock state.
- `src/restore-index/state-reader.test.ts`: authoritative in-memory/pg reader
  freshness derivation and unknown-partition behavior.
- `src/execution-evidence.durability.test.ts`: restart-survival tests for
  paused execution resume and evidence export/read verification state.
- `src/execute/execute-service.test.ts`: RS-09 conflict matrix and chunk
  fallback coverage, plus RS-10 checkpoint/resume and journal linkage tests,
  plus RS-11 media cap/parent/hash/retry behavior tests.
- `src/evidence/evidence-service.test.ts`: RS-12 canonicalization determinism
  and tamper detection tests.
- `src/server.integration.test.ts`: auth scope and source-mapping fail-closed
  tests, RS-08 dry-run coverage, RS-09 execute endpoint checks, RS-10
  resume/checkpoint/journal API checks, RS-11 media dry-run/execute API
  coverage, RS-12 evidence export/read API coverage, and RS-14/RS-15 admin ops
  endpoint coverage.
- `src/registry/acp-source-mapping-client.test.ts`: ACP client request, auth,
  not-found, and timeout/outage behavior tests.
- `src/registry/acp-source-mapping-provider.test.ts`: ACP cache TTL and
  negative-cache behavior tests.
