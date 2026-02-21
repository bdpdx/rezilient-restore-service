# src Index

- `index.ts`: service bootstrap and HTTP server startup.
- `constants.ts`: RS-06/RS-07 schema profile IDs and auth profile constants.
- `env.ts`: restore-service environment parsing.
- `server.ts`: HTTP routes for health, dry-run planning, job create/read, job
  events (`/events` and `/events/cross-service`), execute/resume/checkpoint/
  rollback-journal, signed evidence export, job completion (lock release), and
  RS-14/RS-15 admin ops APIs.
- `admin/ops-admin-service.ts`: RS-14/RS-15 admin summary builder for
  queue/locks, freshness/backfill heuristics, evidence verification listing,
  SLO burn-rate checks, and GA-readiness evaluation state.
- `auth/claims.ts`: token claim parsing/validation for RS-02 claim profile.
- `auth/jwt.ts`: HS256 JWT sign/verify helpers for scoped token checks.
- `auth/authenticator.ts`: auth middleware helper enforcing `rrs` scope.
- `execute/models.ts`: RS-09/RS-10 execute + resume request/response contracts,
  workflow policy controls, checkpoint, rollback-journal model schemas, and
  RS-11 media outcome/config contracts.
- `execute/execute-service.ts`: RS-09 execute orchestration and RS-10 resume
  handling with checksum-gated checkpoints, conflict/capability enforcement,
  chunk/row fallback result recording, rollback-journal mirror linkage, and
  RS-11 attachment/media selection + cap/hash/retry enforcement.
- `execute/execute-state-store.ts`: durable Postgres + in-memory state store
  adapters for execution records/checkpoints and rollback journal linkage.
- `evidence/models.ts`: RS-12 evidence export model and verification result
  contracts.
- `evidence/signature.ts`: RS-12 canonical payload signing/verification helpers
  (Ed25519).
- `evidence/evidence-service.ts`: RS-12 signed evidence package generation,
  deterministic artifact/report hashing, immutable-storage metadata capture,
  and tamper-verification logic.
- `evidence/evidence-state-store.ts`: durable Postgres + in-memory state store
  adapters for evidence export and signature-verification state.
- `registry/source-registry.ts`: tenant/instance/source mapping registry and
  fail-closed scope validation.
- `plans/models.ts`: RS-08 dry-run request/response and gate model contracts.
- `plans/plan-service.ts`: RS-08 deterministic dry-run plan generation and
  freshness/deletion/conflict executability gating from authoritative
  restore-index state.
- `plans/plan-state-store.ts`: durable Postgres + in-memory state store adapters
  for dry-run plans.
- `restore-index/state-reader.ts`: in-memory and Postgres readers for
  authoritative restore-index watermarks with freshness policy derivation.
- `locks/lock-manager.ts`: `(tenant, instance, table)` lock acquisition,
  queueing, release, queued-job promotion, and lock-state import/export.
- `jobs/models.ts`: request schemas and runtime record types for plans/jobs.
- `jobs/job-service.ts`: restore job orchestration with plan metadata,
  lock-state transitions, legacy queue audit events, normalized cross-service
  audit emission, and durable state-store writes.
- `jobs/job-state-store.ts`: durable Postgres + in-memory state store adapters
  for plan/job/event/cross-service-audit and lock-queue snapshots.
- `state/postgres-snapshot-store.ts`: shared Postgres JSON snapshot persistence
  primitive used by plan/job/execute/evidence state-store adapters.
- `test-helpers.ts`: scoped-token fixtures used by integration tests.
- `db-schema.test.ts`: migration contract tests for RS-06/RS-07/RS-10 schema/
  role expectations including runtime snapshot and source-progress table grants.
- `restore-index/state-reader.test.ts`: authoritative watermark reader tests
  for freshness/staleness and unknown-partition fail-closed behavior.
- `core-state.durability.test.ts`: stage-10 restart-survival and queue-fairness
  coverage for persisted plans/jobs/events/locks.
- `execution-evidence.durability.test.ts`: stage-11 restart-survival coverage
  for paused execution resume and evidence export/read consistency.
- `locks/lock-manager.test.ts`: unit tests for lock overlap and promotion.
- `jobs/job-service.test.ts`: concurrency tests for non-overlapping job runs and
  queued overlap promotion.
- `execute/execute-service.test.ts`: RS-09 conflict matrix/capability
  enforcement plus RS-10 checkpoint/resume/journal linkage coverage and RS-11
  media restore behavior tests.
- `evidence/evidence-service.test.ts`: RS-12 determinism and tamper-detection
  tests for report/artifact/signature integrity checks.
- `server.integration.test.ts`: auth + source-mapping fail-closed integration
  tests through HTTP endpoints, plus RS-08 dry-run, RS-09 execute endpoint
  coverage, RS-11 media API integration coverage, RS-12 evidence endpoint
  integration coverage, and RS-14/RS-15 admin ops endpoint coverage.
