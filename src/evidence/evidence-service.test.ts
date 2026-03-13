import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { RestoreExecutionService } from '../execute/execute-service';
import { NoopRestoreTargetWriter } from '../execute/models';
import { RestoreJobService } from '../jobs/job-service';
import { RestoreLockManager } from '../locks/lock-manager';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from '../plans/materialization-service';
import { RestorePlanService } from '../plans/plan-service';
import { NoopRestoreTargetStateLookup } from '../plans/target-reconciliation';
import { SourceRegistry } from '../registry/source-registry';
import { InMemoryRestoreIndexStateReader } from '../restore-index/state-reader';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from '../test-helpers';
import { RestoreEvidenceService } from './evidence-service';
import { InMemoryRestoreEvidenceStateStore } from './evidence-state-store';

const FIXED_NOW = new Date('2026-02-17T01:02:03.000Z');

function now(): Date {
    return new Date(FIXED_NOW.getTime());
}

function claims() {
    return {
        iss: 'rez-auth-control-plane',
        sub: 'client-1',
        aud: 'rezilient:rrs',
        jti: 'tok-1',
        iat: 100,
        exp: 200,
        service_scope: 'rrs' as const,
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
    };
}

function createAuthoritativeWatermark(): Record<string, unknown> {
    const indexedThrough = FIXED_NOW.toISOString();

    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: indexedThrough,
        coverage_start: '2026-02-16T00:00:00.000Z',
        coverage_end: indexedThrough,
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: indexedThrough,
    };
}

function createEvidenceServiceConfig() {
    return {
        signer: {
            signer_key_id: 'rrs-test-signer',
            private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
            public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        },
        immutable_storage: {
            worm_enabled: true,
            retention_class: 'compliance-7y',
        },
    };
}

function seedScopeMaterializationCandidate(input: {
    artifactReader: InMemoryRestoreArtifactBodyReader;
    indexReader: InMemoryRestoreIndexStateReader;
    offset: string;
    rowId: string;
}): void {
    const artifactKey = `rez/restore/event=evt-${input.rowId}.artifact.json`;
    const manifestKey = `rez/restore/event=evt-${input.rowId}.manifest.json`;

    input.indexReader.upsertIndexedEventCandidate({
        artifactKey,
        eventId: `evt-${input.rowId}`,
        eventTime: '2026-02-16T11:59:59.000Z',
        instanceId: 'sn-dev-01',
        manifestKey,
        offset: input.offset,
        partition: 1,
        recordSysId: `rec-${input.rowId}`,
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 2,
        sysUpdatedOn: '2026-02-16 11:59:59',
        table: 'incident',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    input.artifactReader.setArtifactBody({
        artifactKey,
        body: {
            __op: 'U',
            __schema_version: 3,
            __type: 'cdc.write',
            row_enc: {
                alg: 'AES-256-CBC',
                module: 'x_rezrp_rezilient.encrypter',
                format: 'kmf',
                compression: 'none',
                ciphertext: `cipher-${input.rowId}`,
            },
        },
        manifestKey,
    });
}

function createScopeDrivenDryRunRequest(
    planId: string,
    rowIds: string[],
    approval?: Record<string, unknown>,
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-16T12:00:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version:
                'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
            tie_breaker: [
                'sys_updated_on',
                'sys_mod_count',
                '__time',
                'event_id',
            ],
            tie_breaker_fallback: [
                'sys_updated_on',
                '__time',
                'event_id',
            ],
        },
        scope: {
            mode: 'record',
            tables: ['incident'],
            record_sys_ids: rowIds.map((rowId) => `rec-${rowId}`),
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        conflicts: [],
        delete_candidates: [],
        media_candidates: [],
        approval,
    };
}

async function createFixture(options?: {
    approval?: Record<string, unknown>;
}): Promise<{
    jobs: RestoreJobService;
    plans: RestorePlanService;
    execute: RestoreExecutionService;
    evidence: RestoreEvidenceService;
    jobId: string;
}> {
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();
    const artifactReader = new InMemoryRestoreArtifactBodyReader();
    const rowIds = ['row-01', 'row-02'];

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));
    for (let index = 0; index < rowIds.length; index += 1) {
        seedScopeMaterializationCandidate({
            artifactReader,
            indexReader: restoreIndexReader,
            offset: String(100 + index),
            rowId: rowIds[index],
        });
    }

    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        undefined,
        restoreIndexReader,
        undefined,
        new RestoreRowMaterializationService(
            artifactReader,
            new NoopRestoreTargetStateLookup(),
        ),
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        undefined,
        undefined,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await plans.getPlan(planId);

                if (!plan) {
                    return null;
                }

                return {
                    plan_id: plan.plan.plan_id,
                    plan_hash: plan.plan.plan_hash,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {
            executionProgressMode: 'legacy_apply',
        },
        now,
        undefined,
        new NoopRestoreTargetWriter(),
        new NoopRestoreTargetStateLookup(),
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        createEvidenceServiceConfig(),
        now,
    );
    const dryRun = await plans.createDryRunPlan(
        createScopeDrivenDryRunRequest(
            'plan-evidence-1',
            rowIds,
            options?.approval,
        ),
        claims(),
    );

    assert.equal(dryRun.success, true);
    if (!dryRun.success) {
        throw new Error('failed to create dry-run fixture');
    }
    assert.equal(dryRun.reconciliation_state, 'draft');

    const finalized = await plans.finalizeTargetReconciliation(
        'plan-evidence-1',
        {
            finalized_by: 'sn-worker',
            reconciled_records: rowIds.map((rowId) => ({
                table: 'incident',
                record_sys_id: `rec-${rowId}`,
                target_state: 'exists' as const,
            })),
        },
        claims(),
    );

    assert.equal(finalized.success, true);
    if (!finalized.success) {
        throw new Error('failed to finalize dry-run fixture');
    }

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: finalized.record.plan.plan_id,
            plan_hash: finalized.record.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );

    assert.equal(job.success, true);
    if (!job.success) {
        throw new Error('failed to create job fixture');
    }

    const executed = await execute.executeJob(
        job.job.job_id,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(executed.success, true);
    if (!executed.success) {
        throw new Error('failed to execute job fixture');
    }

    assert.equal(executed.record.status, 'completed');

    return {
        jobs,
        plans,
        execute,
        evidence,
        jobId: job.job.job_id,
    };
}

test('evidence export is deterministic and includes signed verification status', async () => {
    const fixture = await createFixture();
    const first = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(first.success, true);
    if (!first.success) {
        return;
    }

    assert.equal(first.statusCode, 201);
    assert.equal(first.reused, false);
    assert.equal(first.record.evidence.plan_hash.length, 64);
    assert.equal(first.record.evidence.report_hash.length, 64);
    assert.equal(
        first.record.evidence.manifest_signature.signature_verification,
        'verified',
    );
    assert.ok(first.record.evidence.artifact_hashes.length >= 3);
    assert.ok(first.record.evidence.resume_metadata.checkpoint_id);

    const second = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(second.success, true);
    if (!second.success) {
        return;
    }

    assert.equal(second.statusCode, 200);
    assert.equal(second.reused, true);
    assert.equal(
        second.record.evidence.evidence_id,
        first.record.evidence.evidence_id,
    );
    assert.equal(
        second.record.evidence.report_hash,
        first.record.evidence.report_hash,
    );
});

test('evidence export sanitizes caller approval metadata as unverified', async () => {
    const fixture = await createFixture({
        approval: {
            approval_required: true,
            approval_state: 'approved',
            approval_decided_by: 'manager@example.com',
            approval_decision: 'approve',
            approval_placeholder_mode: 'mvp_not_enforced',
        },
    });
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    assert.equal(
        exported.record.evidence.approval.approval_state,
        'placeholder_not_enforced',
    );
    assert.equal(exported.record.evidence.approval.approval_required, false);
    assert.equal(
        exported.record.evidence.approval.approval_decision,
        'placeholder',
    );
    assert.match(
        String(
            exported.record.evidence.approval.approval_decision_reason || '',
        ),
        /unverified/i,
    );
    assert.equal(
        exported.record.evidence.approval.approval_decided_by,
        undefined,
    );

    const planArtifact = exported.record.artifacts.find((artifact) => {
        return artifact.artifact_id === 'plan.json';
    });

    assert.ok(planArtifact);
    assert.match(String(planArtifact?.canonical_json || ''), /unverified/i);
    assert.doesNotMatch(
        String(planArtifact?.canonical_json || ''),
        /manager@example.com/i,
    );
});

test('evidence verification detects report-hash tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.evidence.execution_outcomes.rows_applied = 999;

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_report_hash_mismatch',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});

test('evidence verification detects artifact hash tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.artifacts[0].canonical_json =
        tampered.artifacts[0].canonical_json + ' ';

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_artifact_hash_mismatch',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});

test('evidence verification detects signature tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.evidence.manifest_signature.signature = 'not-a-valid-signature';

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_signature_verification',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});

test('ensureEvidence is idempotent', async () => {
    const fixture = await createFixture();
    const first = await fixture.evidence.exportEvidence(
        fixture.jobId,
    );
    const second = await fixture.evidence.exportEvidence(
        fixture.jobId,
    );

    assert.equal(first.success, true);
    assert.equal(second.success, true);

    if (first.success && second.success) {
        assert.equal(
            first.record.evidence.evidence_id,
            second.record.evidence.evidence_id,
        );
        assert.equal(second.reused, true);
    }
});

test(
    'evidence reads stay fresh across service instances sharing state',
    async () => {
        const fixture = await createFixture();
        const sharedStateStore = new InMemoryRestoreEvidenceStateStore();
        const primary = new RestoreEvidenceService(
            fixture.jobs,
            fixture.plans,
            fixture.execute,
            createEvidenceServiceConfig(),
            now,
            sharedStateStore,
        );
        const secondary = new RestoreEvidenceService(
            fixture.jobs,
            fixture.plans,
            fixture.execute,
            createEvidenceServiceConfig(),
            now,
            sharedStateStore,
        );
        const preWarmRead = await secondary.getEvidence(fixture.jobId);

        assert.equal(preWarmRead, null);

        const exported = await primary.exportEvidence(fixture.jobId);

        assert.equal(exported.success, true);
        if (!exported.success) {
            return;
        }

        assert.equal(exported.statusCode, 201);
        assert.equal(exported.reused, false);

        const visibleOnSecondary = await secondary.getEvidence(fixture.jobId);

        assert.notEqual(visibleOnSecondary, null);
        assert.equal(
            visibleOnSecondary?.evidence.evidence_id,
            exported.record.evidence.evidence_id,
        );

        const visibleByIdOnSecondary = await secondary.getEvidenceById(
            exported.record.evidence.evidence_id,
        );

        assert.notEqual(visibleByIdOnSecondary, null);
        assert.equal(
            visibleByIdOnSecondary?.evidence.job_id,
            fixture.jobId,
        );

        const listedOnSecondary = await secondary.listEvidence();

        assert.equal(listedOnSecondary.length, 1);
        assert.equal(
            listedOnSecondary[0]?.evidence.evidence_id,
            exported.record.evidence.evidence_id,
        );

        const reusedOnSecondary = await secondary.exportEvidence(
            fixture.jobId,
        );

        assert.equal(reusedOnSecondary.success, true);
        if (!reusedOnSecondary.success) {
            return;
        }

        assert.equal(reusedOnSecondary.statusCode, 200);
        assert.equal(reusedOnSecondary.reused, true);
        assert.equal(
            reusedOnSecondary.record.evidence.evidence_id,
            exported.record.evidence.evidence_id,
        );
    },
);

test('getEvidence returns null for unknown job', async () => {
    const fixture = await createFixture();
    const result = await fixture.evidence.getEvidence(
        'nonexistent',
    );

    assert.equal(result, null);
});

test('exportEvidence returns failure when job not found', async () => {
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader =
        new InMemoryRestoreIndexStateReader();
    restoreIndexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(
            createAuthoritativeWatermark(),
        ),
    );
    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        undefined,
        restoreIndexReader,
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        undefined,
        undefined,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await plans.getPlan(planId);

                if (!plan) {
                    return null;
                }

                return {
                    plan_id: plan.plan.plan_id,
                    plan_hash: plan.plan.plan_hash,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {},
        now,
        undefined,
        new NoopRestoreTargetWriter(),
        new NoopRestoreTargetStateLookup(),
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        createEvidenceServiceConfig(),
        now,
    );

    const result = await evidence.exportEvidence(
        'nonexistent-job',
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 404);
    }
});

test('exportEvidence returns failure when plan not found', async () => {
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader =
        new InMemoryRestoreIndexStateReader();
    const artifactReader = new InMemoryRestoreArtifactBodyReader();
    const rowIds = ['row-01'];
    restoreIndexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(
            createAuthoritativeWatermark(),
        ),
    );
    seedScopeMaterializationCandidate({
        artifactReader,
        indexReader: restoreIndexReader,
        offset: '100',
        rowId: rowIds[0],
    });
    const planServiceForJob = new RestorePlanService(
        sourceRegistry,
        now,
        undefined,
        restoreIndexReader,
        undefined,
        new RestoreRowMaterializationService(
            artifactReader,
            new NoopRestoreTargetStateLookup(),
        ),
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        undefined,
        undefined,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await planServiceForJob.getPlan(planId);

                if (!plan) {
                    return null;
                }

                return {
                    plan_id: plan.plan.plan_id,
                    plan_hash: plan.plan.plan_hash,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );
    const dryRun = await planServiceForJob.createDryRunPlan(
        createScopeDrivenDryRunRequest(
            'plan-orphaned',
            rowIds,
        ),
        claims(),
    );
    assert.equal(dryRun.success, true);
    if (!dryRun.success) {
        return;
    }
    assert.equal(dryRun.reconciliation_state, 'draft');

    const finalized = await planServiceForJob.finalizeTargetReconciliation(
        'plan-orphaned',
        {
            finalized_by: 'sn-worker',
            reconciled_records: [{
                table: 'incident',
                record_sys_id: 'rec-row-01',
                target_state: 'exists',
            }],
        },
        claims(),
    );

    assert.equal(finalized.success, true);
    if (!finalized.success) {
        return;
    }

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: finalized.record.plan.plan_id,
            plan_hash: finalized.record.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );
    assert.equal(job.success, true);
    if (!job.success) {
        return;
    }

    const emptyPlanService = new RestorePlanService(
        sourceRegistry,
        now,
    );
    const execute = new RestoreExecutionService(
        jobs,
        emptyPlanService,
        {},
        now,
        undefined,
        new NoopRestoreTargetWriter(),
        new NoopRestoreTargetStateLookup(),
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        emptyPlanService,
        execute,
        createEvidenceServiceConfig(),
        now,
    );

    const result = await evidence.exportEvidence(
        job.job.job_id,
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 409);
    }
});
