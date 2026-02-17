# rezilient-restore-service

Purpose:
- Restore backend service for RS-07/RS-08/RS-09/RS-10/RS-11/RS-12 with:
  - auth-scoped restore job creation,
  - tenant/instance/source resolution checks,
  - dry-run planning with deterministic `plan_hash`,
  - freshness/deletion/conflict executability gating,
  - scoped lock queueing by `(tenant, instance, table)`,
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
    summaries, and evidence verification status.

Entrypoints:
- `src/index.ts`: restore-service bootstrap and HTTP server startup.
- `src/server.ts`: HTTP API handlers for health, dry-run plans, jobs, and
  queue-state transitions, including evidence export/read APIs.
- `src/plans/models.ts`: RS-08 dry-run request/response/gate schemas.
- `src/plans/plan-service.ts`: RS-08 dry-run orchestration, deterministic hash
  generation, and freshness/deletion/conflict gate evaluation.
- `src/execute/models.ts`: RS-09 execute request and result schemas.
- `src/execute/execute-service.ts`: RS-09 execute engine plus RS-10 resume flow
  enforcing plan immutability, capability checks, conflict matrix rules, chunk
  fallback, checkpoint persistence, rollback journal/mirror records, and RS-11
  media candidate execution outcomes.
- `src/evidence/signature.ts`: canonical payload signing and verification
  helpers for RS-12 evidence.
- `src/evidence/evidence-service.ts`: RS-12 evidence package builder and
  verifier (artifact hashes, report hash, signed manifest metadata).
- `src/admin/ops-admin-service.ts`: RS-14 admin ops summary service for queue,
  freshness/backfill heuristic status, and evidence verification listings.
- `src/jobs/job-service.ts`: restore job orchestration and queue audit events.
- `src/locks/lock-manager.ts`: lock acquisition/release and queued-job
  promotion.
- `db/migrations/0001_restore_index_plane.sql`: RS-06 restore index tables.
- `db/migrations/0002_restore_index_roles.sql`: RS-06 index role grants.
- `db/migrations/0003_restore_job_plane.sql`: RS-07 job/plan/lock queue schema.
- `db/migrations/0004_restore_job_roles.sql`: RS-07 restore-service rw grants.
- `db/migrations/0005_restore_resume_journal_plane.sql`: RS-10 checkpoint and
  rollback-journal schema.
- `db/migrations/0006_restore_resume_journal_roles.sql`: RS-10 role grants for
  checkpoint and rollback-journal tables.

Tests:
- `src/db-schema.test.ts`: migration contract checks for RS-06 + RS-07 schema,
  approval placeholders, and role grants.
- `src/locks/lock-manager.test.ts`: lock overlap arbitration tests.
- `src/jobs/job-service.test.ts`: parallel non-overlap and queued overlap tests.
- `src/execute/execute-service.test.ts`: RS-09 conflict matrix and chunk
  fallback coverage, plus RS-10 checkpoint/resume and journal linkage tests,
  plus RS-11 media cap/parent/hash/retry behavior tests.
- `src/evidence/evidence-service.test.ts`: RS-12 canonicalization determinism
  and tamper detection tests.
- `src/server.integration.test.ts`: auth scope and source-mapping fail-closed
  tests, RS-08 dry-run coverage, RS-09 execute endpoint checks, RS-10
  resume/checkpoint/journal API checks, RS-11 media dry-run/execute API
  coverage, RS-12 evidence export/read API coverage, and RS-14 admin ops
  endpoint coverage.
