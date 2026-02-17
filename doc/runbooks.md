# Restore Service Runbooks (RS-12)

## Queue Triage

1. List queued jobs with `GET /v1/jobs/{job_id}` and inspect:
   - `status`
   - `status_reason_code`
   - `wait_reason_code`
   - `wait_tables`
2. Confirm whether the blocking scope overlaps active running jobs on the same
   `(tenant_id, instance_id, table)` tuple.
3. Check `GET /v1/jobs/{job_id}/events` for transition history:
   - `job_queued`
   - `job_started`
   - terminal events
4. If queue depth is rising and no jobs are promoting, identify lock-holder
   jobs and verify they are still progressing.

## Lock Timeout Recovery

1. Identify running jobs that exceed expected execution window.
2. Validate worker status and downstream dependencies before intervention.
3. If a job is stuck, mark it terminal via `POST /v1/jobs/{job_id}/complete`
   with:
   - `status = failed`
   - `reason_code = failed_internal_error`
4. Confirm lock release and queued-job promotion in response payload
   (`promoted_job_ids`) and subsequent job events.
5. If queued jobs do not promote after lock release, treat as lock-manager
   defect and restart service with incident escalation.

## Stuck Job Cleanup

1. For queued jobs that should not execute (cancelled by operator intent), call
   `POST /v1/jobs/{job_id}/complete` with `status = cancelled`.
2. Verify the job moved to terminal state with cleared queue metadata:
   - `queue_position = null`
   - `wait_reason_code = null`
3. Confirm audit continuity by checking terminal event in
   `GET /v1/jobs/{job_id}/events`.
4. If duplicate `plan_id` requests repeatedly fail with
   `blocked_plan_hash_mismatch`, require explicit replan and new `plan_id`
   instead of forced overwrite.

## Dry-Run Preview-Only Plans

1. Create or inspect the dry-run plan using:
   - `POST /v1/plans/dry-run`
   - `GET /v1/plans/{plan_id}`
2. Review `gate.executability` and `gate.reason_code`.
3. If `executability = preview_only` and `reason_code = blocked_freshness_stale`:
   - Treat plan as non-executable.
   - Wait for index freshness recovery and rerun dry-run before execute.
4. Do not create execute-intent jobs from preview-only plans.

## Freshness Gate Remediation

1. If `reason_code = blocked_freshness_unknown`:
   - Treat as fail-closed.
   - Verify sidecar/index watermark publication for the affected partitions.
2. If `reason_code = blocked_freshness_stale`:
   - Confirm whether stale state is expected (for example backfill lag).
   - Re-run dry-run after freshness recovers to `fresh`.
3. Capture affected `(tenant_id, instance_id, topic, partition)` tuples from
   dry-run `watermarks` for handoff to sidecar operators.

## Dry-Run Revalidation

1. If `reason_code = blocked_unresolved_delete_candidates`, ensure every
   delete candidate has an explicit decision (`allow_deletion` or
   `skip_deletion`) and re-run dry-run.
2. If `reason_code = blocked_reference_conflict`, resolve dependency conflict
   conditions in the source/target state, then re-run dry-run.
3. If a repeated dry-run request reuses `plan_id` with changed inputs,
   expect `blocked_plan_hash_mismatch`; create a new `plan_id` for the
   revalidated scope.

## Execute Hard-Block Conflicts

1. Start execution with `POST /v1/jobs/{job_id}/execution`.
2. If response is `409` with `reason_code = blocked_reference_conflict`:
   - Treat as hard-blocked; do not retry execute with skip behavior.
   - Replan after resolving reference dependency conditions.
3. If response reason is `failed_schema_conflict` or
   `failed_permission_conflict`:
   - Treat as `abort_and_replan` path.
   - Re-run dry-run with updated schema/security context before creating a
     new execute attempt.

## Chunk Fallback Investigation

1. Query `GET /v1/jobs/{job_id}/execution`.
2. Inspect `chunks[*].status`:
   - `applied`: chunk transaction succeeded as a unit.
   - `row_fallback`: chunk isolated to row-level outcomes.
3. For fallback chunks, inspect:
   - `chunks[*].fallback_trigger_conflict_ids`
   - `row_outcomes[*].used_row_fallback`
4. Confirm non-conflicting rows were still applied and conflicting rows were
   skipped with explicit `reason_code`.

## Capability and Override Blocks

1. If execute returns `403` with `blocked_missing_capability`, inspect:
   - job required capabilities,
   - destructive-action requirements (`restore_delete`),
   - override requirements (`restore_override_caps`).
2. For high-risk execute scopes requiring elevated confirmation:
   - include `elevated_confirmation.confirmed = true`,
   - provide `confirmation` and `reason`,
   - ensure operator has `restore_override_caps`.
3. If capability changes are needed, re-auth with updated capabilities and
   retry execute on the same running job.

## Media Cap Enforcement (RS-11)

1. Review dry-run media selection counts (`media_candidates`) and planned
   attachment totals before execute.
2. If execute returns `403` with `blocked_missing_capability` and elevated
   confirmation messaging, verify whether media limits were exceeded:
   - `mediaMaxItems`
   - `mediaMaxBytes`
3. For approved exceptions, rerun execute with:
   - `restore_override_caps` capability
   - explicit `elevated_confirmation`
4. If override is not approved, reduce selected media candidates and rerun
   dry-run/execute.

## Media Verification Failures (RS-11)

1. Query `GET /v1/jobs/{job_id}/execution` and inspect `media_outcomes`.
2. Triage by `reason_code`:
   - `failed_media_parent_missing`: parent row missing; restore parent record
     first and replan.
   - `failed_media_hash_mismatch`: item hash mismatch; re-export candidate and
     validate source hash metadata.
   - `failed_media_retry_exhausted`: transfer retries exceeded; check network
     path and media store availability.
3. Confirm per-item outcomes are captured:
   - `candidate_id`
   - `attempt_count`
   - `expected_sha256_plain`
   - `observed_sha256_plain`
4. Re-run with corrected scope and maintain audit linkage in evidence exports.

## Resume Checkpoint Recovery

1. Fetch checkpoint state with `GET /v1/jobs/{job_id}/checkpoint`.
2. Confirm resume preconditions from latest execution:
   - `plan_checksum`
   - `precondition_checksum`
   - `checkpoint.next_chunk_index`
3. Resume with `POST /v1/jobs/{job_id}/resume` including:
   - `operator_id`
   - `operator_capabilities`
   - `expected_plan_checksum`
   - `expected_precondition_checksum`
4. If response is `202`, job is paused again at chunk boundary; repeat resume
   until terminal `200`.
5. Validate final execution with `GET /v1/jobs/{job_id}/execution` and confirm
   checkpoint reached `next_chunk_index = total_chunks`.

## Resume Failure (Checksum / Preconditions)

1. If resume returns `409` with
   `reason_code = blocked_resume_precondition_mismatch`:
   - Treat checkpoint as invalid for continuation.
   - Do not force resume.
2. Re-run dry-run for the same scope to generate a new immutable plan and
   hash set.
3. Create a new job referencing the new `plan_id`/`plan_hash`.
4. Record blocked resume event and mismatch reason in incident notes.

## Missing Checkpoint Handling

1. If resume/checkpoint endpoints return checkpoint-missing states:
   - `404` on `GET /checkpoint`, or
   - `409` with `blocked_resume_checkpoint_missing` on `/resume`,
   treat the prior execution state as non-recoverable.
2. Replan and create a new job rather than retrying the stale resume path.
3. Escalate if checkpoint loss is unexpected for the runtime mode.

## Rollback Journal Verification

1. Fetch rollback bundle using `GET /v1/jobs/{job_id}/rollback-journal`.
2. Validate linkage invariants:
   - every `sn_mirror[*].journal_id` exists in `rollback_journal[*].journal_id`,
   - every applied row outcome has a corresponding journal row.
3. Spot-check journal metadata:
   - `plan_hash`
   - `plan_row_id`
   - `chunk_id`
   - `row_attempt`
4. Confirm SN mirror sync consumers persist linkage IDs without exposing
   plaintext before-images.

## Journal Retention Governance

1. Monitor journal growth (`rows/job`, `bytes/job`, retention backlog).
2. Retain authoritative rollback journals for compliance window policy.
3. Prune only after:
   - retention window expires, and
   - required evidence exports are complete.
4. Ensure pruning keeps linkage metadata needed for evidence/audit references.

## Evidence Export and Verification (RS-12)

1. After terminal execute/resume, fetch evidence package:
   - `GET /v1/jobs/{job_id}/evidence`
   - or force generation/fetch:
     `POST /v1/jobs/{job_id}/evidence/export`
2. Verify response includes:
   - `evidence.plan_hash`
   - `evidence.report_hash`
   - `evidence.pit_algorithm_version`
   - `evidence.resume_metadata`
   - `verification.signature_verification`
3. If verification status is not `verified`, treat as compliance incident and
   block auditor handoff until resolved.

## Evidence Verification Failure Triage (RS-12)

1. Inspect `verification.reason_code`:
   - `failed_evidence_report_hash_mismatch`
   - `failed_evidence_artifact_hash_mismatch`
   - `failed_evidence_signature_verification`
2. For report-hash mismatch:
   - regenerate package with `POST /evidence/export`;
   - if mismatch persists, quarantine job artifacts and open integrity incident.
3. For artifact-hash mismatch:
   - confirm artifact payload source consistency (`plan`, `execution`,
     `rollback-journal`, `job-events`);
   - regenerate evidence after artifact source correction.
4. For signature-verification failure:
   - validate signer key config (`RRS_EVIDENCE_SIGNER_KEY_ID`,
     signing key PEM values);
   - rotate keys if key compromise/sync drift is suspected.

## Evidence Signing Key Rotation (RS-12)

1. Stage replacement signing material in environment:
   - `RRS_EVIDENCE_SIGNER_KEY_ID`
   - `RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM`
   - optional `RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM`
2. Restart restore-service and export evidence for a canary job.
3. Confirm new package shows:
   - updated `manifest_signature.signer_key_id`
   - `verification.signature_verification = verified`
4. Record rotation metadata in change log and retain prior key ID references
   for historical evidence audit traceability.

## Evidence Retrieval and Retention (RS-12)

1. Treat evidence payload as immutable audit output bound to `plan_hash`.
2. Preserve `manifest_sha256` and `artifact_hashes` in audit ticket artifacts.
3. Retain evidence exports per compliance retention class:
   - inspect `evidence.immutable_storage.retention_class`
   - verify `worm_enabled` policy state is explicit in export payload.
4. Do not delete rollback-journal linkage until required evidence exports are
   complete and retention criteria are met.

## SLO Burn-Rate Triage (RS-15)

1. Fetch SLO dashboard:
   - `GET /v1/admin/ops/slo`
2. Inspect `burn_rate`:
   - `status` (`within_budget`, `at_risk`, `breached`)
   - failed checks and observed vs target values.
3. Prioritize critical failures first:
   - `execute_success_rate`
   - `stale_source_count`
   - `evidence_verification_failed`
4. Route warning failures to ops queue with bounded mitigation windows:
   - queue wait p95
   - execute duration p95
   - auth pause rate.

## GA Readiness Gate (RS-15)

1. Fetch gate summary:
   - `GET /v1/admin/ops/ga-readiness`
2. Validate required checks:
   - staging mode enabled,
   - SLO burn-rate status is `within_budget`,
   - runbooks signed off,
   - required failure drills passed,
   - evidence verification failures are zero.
3. If gate is blocked, use `blocked_reasons` to drive corrective action.
4. Do not mark GA-ready until all checks pass in the agreed measurement window.

## Staging Mode and Drill Recording (RS-15)

1. Toggle staging mode for GA-targeted validation:
   - `POST /v1/admin/ops/staging-mode`
   - payload: `{"enabled": true|false, "actor": "<operator>"}`
2. Record runbook sign-off:
   - `POST /v1/admin/ops/runbooks-signoff`
   - payload: `{"signed_off": true|false, "actor": "<operator>"}`
3. Record failure drill outcomes:
   - `POST /v1/admin/ops/failure-drills`
   - payload:
     `{"drill_id":"auth_outage|sidecar_lag|pg_saturation|entitlement_disable|crash_resume|evidence_audit_export","status":"pass|fail|pending","actor":"<operator>","notes":"optional"}`
