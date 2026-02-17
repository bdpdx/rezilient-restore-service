# src Index

- `index.ts`: service bootstrap and HTTP server startup.
- `constants.ts`: RS-06/RS-07 schema profile IDs and auth profile constants.
- `env.ts`: restore-service environment parsing.
- `server.ts`: HTTP routes for health, dry-run planning, job create/read, job
  events, execute/resume/checkpoint/rollback-journal, signed evidence export,
  and job completion (lock release).
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
- `evidence/models.ts`: RS-12 evidence export model and verification result
  contracts.
- `evidence/signature.ts`: RS-12 canonical payload signing/verification helpers
  (Ed25519).
- `evidence/evidence-service.ts`: RS-12 signed evidence package generation,
  deterministic artifact/report hashing, immutable-storage metadata capture,
  and tamper-verification logic.
- `registry/source-registry.ts`: tenant/instance/source mapping registry and
  fail-closed scope validation.
- `plans/models.ts`: RS-08 dry-run request/response and gate model contracts.
- `plans/plan-service.ts`: RS-08 deterministic dry-run plan generation and
  freshness/deletion/conflict executability gating.
- `locks/lock-manager.ts`: `(tenant, instance, table)` lock acquisition,
  queueing, release, and queued-job promotion.
- `jobs/models.ts`: request schemas and runtime record types for plans/jobs.
- `jobs/job-service.ts`: restore job orchestration with plan metadata,
  lock-state transitions, and queue audit events.
- `test-helpers.ts`: scoped-token fixtures used by integration tests.
- `db-schema.test.ts`: migration contract tests for RS-06/RS-07/RS-10 schema/
  role expectations.
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
  coverage, RS-11 media API integration coverage, and RS-12 evidence endpoint
  integration coverage.
