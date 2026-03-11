import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { newDb } from 'pg-mem';
import {
    RestoreEvidenceService,
} from './evidence/evidence-service';
import {
    PostgresRestoreEvidenceStateStore,
} from './evidence/evidence-state-store';
import {
    RestoreExecutionService,
} from './execute/execute-service';
import { NoopRestoreTargetWriter } from './execute/models';
import {
    PostgresRestoreExecutionStateStore,
} from './execute/execute-state-store';
import { PostgresRestoreJobStateStore } from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { RestoreLockManager } from './locks/lock-manager';
import { PostgresRestorePlanStateStore } from './plans/plan-state-store';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from './plans/materialization-service';
import { RestorePlanService } from './plans/plan-service';
import { NoopRestoreTargetStateLookup } from './plans/target-reconciliation';
import { SourceRegistry } from './registry/source-registry';
import { InMemoryRestoreIndexStateReader } from './restore-index/state-reader';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from './test-helpers';

type Fixture = {
    close: () => Promise<void>;
    jobs: RestoreJobService;
    plans: RestorePlanService;
    execute: RestoreExecutionService;
    evidence: RestoreEvidenceService;
    artifactReader: InMemoryRestoreArtifactBodyReader;
    restoreIndexReader: InMemoryRestoreIndexStateReader;
};

function now(): Date {
    return new Date('2026-02-18T18:00:00.000Z');
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

function createScopeDrivenDryRunRequest(
    planId: string,
    rowIds: string[],
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-18T18:00:00.000Z',
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
    };
}

function createJobRequest(
    planId: string,
    planHash: string,
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        plan_hash: planHash,
        lock_scope_tables: ['incident'],
        required_capabilities: ['restore_execute'],
        requested_by: 'operator@example.com',
    };
}

function createAuthoritativeWatermark(): Record<string, unknown> {
    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-18T17:59:59.000Z',
        coverage_start: '2026-02-18T00:00:00.000Z',
        coverage_end: '2026-02-18T17:59:59.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-18T18:00:00.000Z',
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
        eventTime: '2026-02-18T17:59:59.000Z',
        instanceId: 'sn-dev-01',
        manifestKey,
        offset: input.offset,
        partition: 1,
        recordSysId: `rec-${input.rowId}`,
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 2,
        sysUpdatedOn: '2026-02-18 17:59:59',
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

function createFixture(
    db: ReturnType<typeof newDb>,
    maxChunksPerAttempt: number,
): Fixture {
    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();
    const artifactReader = new InMemoryRestoreArtifactBodyReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));
    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        new PostgresRestorePlanStateStore('postgres://unused', {
            pool: pool as any,
        }),
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
        new PostgresRestoreJobStateStore('postgres://unused', {
            pool: pool as any,
        }),
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
                };
            },
        },
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {
            maxChunksPerAttempt,
        },
        now,
        new PostgresRestoreExecutionStateStore('postgres://unused', {
            pool: pool as any,
        }),
        new NoopRestoreTargetWriter(),
        new NoopRestoreTargetStateLookup(),
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        {
            signer: {
                signer_key_id: 'rrs-test-signer',
                private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
                public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
            },
            immutable_storage: {
                worm_enabled: true,
                retention_class: 'compliance-7y',
            },
        },
        now,
        new PostgresRestoreEvidenceStateStore('postgres://unused', {
            pool: pool as any,
        }),
    );

    return {
        close: async () => {
            await pool.end();
        },
        jobs,
        plans,
        execute,
        evidence,
        artifactReader,
        restoreIndexReader,
    };
}

async function createPlanAndJob(
    fixture: Fixture,
    planId: string,
    rowIds: string[],
): Promise<{
    jobId: string;
    planHash: string;
}> {
    for (let index = 0; index < rowIds.length; index += 1) {
        seedScopeMaterializationCandidate({
            artifactReader: fixture.artifactReader,
            indexReader: fixture.restoreIndexReader,
            offset: String(100 + index),
            rowId: rowIds[index],
        });
    }

    const dryRun = await fixture.plans.createDryRunPlan(
        createScopeDrivenDryRunRequest(planId, rowIds),
        claims(),
    );

    assert.equal(dryRun.success, true);
    if (!dryRun.success) {
        throw new Error('failed to create dry-run plan fixture');
    }
    assert.equal(dryRun.reconciliation_state, 'draft');

    const finalized = await fixture.plans.finalizeTargetReconciliation(
        planId,
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
        throw new Error('failed to finalize dry-run plan fixture');
    }

    const job = await fixture.jobs.createJob(
        createJobRequest(planId, finalized.record.plan.plan_hash),
        claims(),
    );

    assert.equal(job.success, true);
    if (!job.success) {
        throw new Error('failed to create restore job fixture');
    }

    return {
        jobId: job.job.job_id,
        planHash: finalized.record.plan.plan_hash,
    };
}

test('paused execution resumes from persisted checkpoint after restart', async () => {
    const db = newDb();
    db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
    const first = createFixture(db, 1);
    let restarted: Fixture | null = null;
    let restartedAgain: Fixture | null = null;

    try {
        const plan = await createPlanAndJob(first, 'plan-stage11-restart', [
            'row-01',
            'row-02',
            'row-03',
        ]);
        const firstAttempt = await first.execute.executeJob(
            plan.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                chunk_size: 1,
            },
            claims(),
        );

        assert.equal(firstAttempt.success, true);
        if (!firstAttempt.success) {
            return;
        }

        assert.equal(firstAttempt.statusCode, 202);
        assert.equal(firstAttempt.record.status, 'paused');
        assert.equal(firstAttempt.record.checkpoint.next_chunk_index, 1);

        restarted = createFixture(db, 1);
        const checkpointAfterRestart = await restarted.execute.getCheckpoint(plan.jobId);
        const journalAfterRestart = await restarted.execute.getRollbackJournal(plan.jobId);

        assert.notEqual(checkpointAfterRestart, null);
        assert.equal(checkpointAfterRestart?.next_chunk_index, 1);
        assert.equal(journalAfterRestart?.journal_entries.length, 1);
        assert.equal(journalAfterRestart?.sn_mirror_entries.length, 1);

        const resumeOne = await restarted.execute.resumeJob(
            plan.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: firstAttempt.record.plan_checksum,
                expected_precondition_checksum:
                    firstAttempt.record.precondition_checksum,
            },
            claims(),
        );

        assert.equal(resumeOne.success, true);
        if (!resumeOne.success) {
            return;
        }

        assert.equal(resumeOne.statusCode, 202);
        assert.equal(resumeOne.record.status, 'paused');
        assert.equal(resumeOne.record.checkpoint.next_chunk_index, 2);

        restartedAgain = createFixture(db, 1);
        const checkpointSecondRestart = await restartedAgain.execute.getCheckpoint(
            plan.jobId,
        );

        assert.notEqual(checkpointSecondRestart, null);
        assert.equal(checkpointSecondRestart?.next_chunk_index, 2);

        const resumeTwo = await restartedAgain.execute.resumeJob(
            plan.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: firstAttempt.record.plan_checksum,
                expected_precondition_checksum:
                    firstAttempt.record.precondition_checksum,
            },
            claims(),
        );

        assert.equal(resumeTwo.success, true);
        if (!resumeTwo.success) {
            return;
        }

        assert.equal(resumeTwo.statusCode, 200);
        assert.equal(resumeTwo.record.status, 'completed');
        assert.equal(resumeTwo.record.checkpoint.next_chunk_index, 3);
        assert.equal(resumeTwo.record.summary.applied_rows, 3);

        const journalFinal = await restartedAgain.execute.getRollbackJournal(plan.jobId);

        assert.notEqual(journalFinal, null);
        assert.equal(journalFinal?.journal_entries.length, 3);
        assert.equal(journalFinal?.sn_mirror_entries.length, 3);
        assert.equal(
            journalFinal?.journal_entries[0].journal_id,
            journalFinal?.sn_mirror_entries[0].journal_id,
        );
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }

        if (restartedAgain) {
            await restartedAgain.close();
        }
    }
});

test(
    'execute state remains consistent across concurrent service fixtures',
    async () => {
        const db = newDb();
        db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
        const first = createFixture(db, 1);
        const second = createFixture(db, 1);

        try {
            const plan = await createPlanAndJob(
                first,
                'plan-stage11-multi-instance',
                [
                    'row-aa',
                    'row-bb',
                ],
            );

            const secondInitial = await second.execute.listExecutions();

            assert.equal(secondInitial.length, 0);

            const firstAttempt = await first.execute.executeJob(
                plan.jobId,
                {
                    operator_id: 'operator@example.com',
                    operator_capabilities: ['restore_execute'],
                    chunk_size: 1,
                },
                claims(),
            );

            assert.equal(firstAttempt.success, true);
            if (!firstAttempt.success) {
                return;
            }

            assert.equal(firstAttempt.statusCode, 202);
            assert.equal(firstAttempt.record.status, 'paused');
            assert.equal(firstAttempt.record.checkpoint.next_chunk_index, 1);

            const checkpointOnSecond = await second.execute.getCheckpoint(
                plan.jobId,
            );

            assert.notEqual(checkpointOnSecond, null);
            assert.equal(checkpointOnSecond?.next_chunk_index, 1);

            const resumedOnSecond = await second.execute.resumeJob(
                plan.jobId,
                {
                    operator_id: 'operator@example.com',
                    operator_capabilities: ['restore_execute'],
                    expected_plan_checksum: firstAttempt.record.plan_checksum,
                    expected_precondition_checksum:
                        firstAttempt.record.precondition_checksum,
                },
                claims(),
            );

            assert.equal(resumedOnSecond.success, true);
            if (!resumedOnSecond.success) {
                return;
            }

            assert.equal(resumedOnSecond.statusCode, 200);
            assert.equal(resumedOnSecond.record.status, 'completed');
            assert.equal(resumedOnSecond.record.checkpoint.next_chunk_index, 2);

            const executionOnFirst = await first.execute.getExecution(plan.jobId);

            assert.notEqual(executionOnFirst, null);
            assert.equal(executionOnFirst?.status, 'completed');
            assert.equal(executionOnFirst?.summary.applied_rows, 2);

            const journalOnFirst = await first.execute.getRollbackJournal(
                plan.jobId,
            );

            assert.notEqual(journalOnFirst, null);
            assert.equal(journalOnFirst?.journal_entries.length, 2);
            assert.equal(journalOnFirst?.sn_mirror_entries.length, 2);
        } finally {
            await first.close();
            await second.close();
        }
    },
);

test(
    'evidence state stays visible across concurrent service fixtures',
    async () => {
        const db = newDb();
        db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
        const first = createFixture(db, 0);
        const second = createFixture(db, 0);

        try {
            const plan = await createPlanAndJob(
                first,
                'plan-stage11-evidence-multi-instance',
                [
                    'row-x',
                    'row-y',
                ],
            );
            const executed = await first.execute.executeJob(
                plan.jobId,
                {
                    operator_id: 'operator@example.com',
                    operator_capabilities: ['restore_execute'],
                },
                claims(),
            );

            assert.equal(executed.success, true);
            if (!executed.success) {
                return;
            }

            assert.equal(executed.record.status, 'completed');
            const preWarmOnSecond = await second.evidence.getEvidence(
                plan.jobId,
            );
            const preWarmListOnSecond = await second.evidence.listEvidence();

            assert.equal(preWarmOnSecond, null);
            assert.equal(preWarmListOnSecond.length, 0);

            const exportedOnFirst = await first.evidence.exportEvidence(
                plan.jobId,
            );

            assert.equal(exportedOnFirst.success, true);
            if (!exportedOnFirst.success) {
                return;
            }

            assert.equal(exportedOnFirst.statusCode, 201);
            assert.equal(exportedOnFirst.reused, false);
            const evidenceId = exportedOnFirst.record.evidence.evidence_id;

            const visibleOnSecond = await second.evidence.getEvidence(
                plan.jobId,
            );
            const visibleByIdOnSecond = await second.evidence.getEvidenceById(
                evidenceId,
            );
            const listedOnSecond = await second.evidence.listEvidence();

            assert.notEqual(visibleOnSecond, null);
            assert.equal(visibleOnSecond?.evidence.evidence_id, evidenceId);
            assert.notEqual(visibleByIdOnSecond, null);
            assert.equal(visibleByIdOnSecond?.evidence.job_id, plan.jobId);
            assert.equal(listedOnSecond.length, 1);
            assert.equal(listedOnSecond[0]?.evidence.evidence_id, evidenceId);

            const reusedOnSecond = await second.evidence.exportEvidence(
                plan.jobId,
            );

            assert.equal(reusedOnSecond.success, true);
            if (!reusedOnSecond.success) {
                return;
            }

            assert.equal(reusedOnSecond.statusCode, 200);
            assert.equal(reusedOnSecond.reused, true);
            assert.equal(
                reusedOnSecond.record.evidence.evidence_id,
                evidenceId,
            );
        } finally {
            await first.close();
            await second.close();
        }
    },
);

test('evidence export and verification remain consistent after restart', async () => {
    const db = newDb();
    db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
    const first = createFixture(db, 0);
    let restarted: Fixture | null = null;

    try {
        const plan = await createPlanAndJob(first, 'plan-stage11-evidence', [
            'row-a',
            'row-b',
        ]);
        const executed = await first.execute.executeJob(
            plan.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
            },
            claims(),
        );

        assert.equal(executed.success, true);
        if (!executed.success) {
            return;
        }

        assert.equal(executed.record.status, 'completed');

        const firstExport = await first.evidence.exportEvidence(plan.jobId);

        assert.equal(firstExport.success, true);
        if (!firstExport.success) {
            return;
        }

        assert.equal(firstExport.statusCode, 201);
        assert.equal(firstExport.reused, false);
        const firstEvidenceId = firstExport.record.evidence.evidence_id;
        const firstReportHash = firstExport.record.evidence.report_hash;

        restarted = createFixture(db, 0);
        const evidenceAfterRestart = await restarted.evidence.getEvidence(plan.jobId);

        assert.notEqual(evidenceAfterRestart, null);
        assert.equal(evidenceAfterRestart?.evidence.evidence_id, firstEvidenceId);
        assert.equal(evidenceAfterRestart?.evidence.report_hash, firstReportHash);
        assert.equal(
            evidenceAfterRestart?.verification.signature_verification,
            'verified',
        );

        const evidenceById = await restarted.evidence.getEvidenceById(firstEvidenceId);

        assert.notEqual(evidenceById, null);
        assert.equal(evidenceById?.evidence.report_hash, firstReportHash);

        const exportAfterRestart = await restarted.evidence.exportEvidence(plan.jobId);

        assert.equal(exportAfterRestart.success, true);
        if (!exportAfterRestart.success) {
            return;
        }

        assert.equal(exportAfterRestart.statusCode, 200);
        assert.equal(exportAfterRestart.reused, true);
        assert.equal(
            exportAfterRestart.record.evidence.evidence_id,
            firstEvidenceId,
        );
        const verification = restarted.evidence.validateEvidenceRecord(
            exportAfterRestart.record,
        );

        assert.equal(verification.signature_verification, 'verified');
        assert.equal(verification.reason_code, 'none');
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }
    }
});
