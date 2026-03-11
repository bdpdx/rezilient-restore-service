import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { newDb } from 'pg-mem';
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

type Fixture = {
    artifactReader: InMemoryRestoreArtifactBodyReader;
    close: () => Promise<void>;
    jobs: RestoreJobService;
    plans: RestorePlanService;
    restoreIndexReader: InMemoryRestoreIndexStateReader;
};

function now(): Date {
    return new Date('2026-02-18T15:30:00.000Z');
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
    rowId: string,
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-18T15:30:00.000Z',
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
            record_sys_ids: [`rec-${rowId}`],
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
        indexed_through_time: '2026-02-18T15:29:59.000Z',
        coverage_start: '2026-02-18T00:00:00.000Z',
        coverage_end: '2026-02-18T15:29:59.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-18T15:30:00.000Z',
    };
}

function createFixture(
    db: ReturnType<typeof newDb>,
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
                    plan_hash: plan.plan.plan_hash,
                    plan_id: plan.plan.plan_id,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );

    return {
        artifactReader,
        close: async () => {
            await pool.end();
        },
        jobs,
        plans,
        restoreIndexReader,
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
        eventTime: '2026-02-18T15:29:59.000Z',
        instanceId: 'sn-dev-01',
        manifestKey,
        offset: input.offset,
        partition: 1,
        recordSysId: `rec-${input.rowId}`,
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 2,
        sysUpdatedOn: '2026-02-18 15:29:59',
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

async function createPlanAndJob(
    fixture: Fixture,
    planId: string,
    offset: string,
): Promise<{
    jobId: string;
    planHash: string;
}> {
    seedScopeMaterializationCandidate({
        artifactReader: fixture.artifactReader,
        indexReader: fixture.restoreIndexReader,
        offset,
        rowId: planId,
    });

    const dryRun = await fixture.plans.createDryRunPlan(
        createScopeDrivenDryRunRequest(planId, planId),
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
            reconciled_records: [
                {
                    table: 'incident',
                    record_sys_id: `rec-${planId}`,
                    target_state: 'exists' as const,
                },
            ],
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

test('durable core state survives restart for plans/jobs/events/locks', async () => {
    const db = newDb();
    db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
    const first = createFixture(db);
    let restarted: Fixture | null = null;

    try {
        const running = await createPlanAndJob(
            first,
            'plan-stage10-restart',
            '101',
        );
        const queued = await createPlanAndJob(
            first,
            'plan-stage10-queued',
            '102',
        );

        restarted = createFixture(db);
        const planAfterRestart = await restarted.plans.getPlan(
            'plan-stage10-restart',
        );
        const runningAfterRestart = await restarted.jobs.getJob(running.jobId);
        const queuedAfterRestart = await restarted.jobs.getJob(queued.jobId);

        assert.notEqual(planAfterRestart, null);
        assert.notEqual(runningAfterRestart, null);
        assert.notEqual(queuedAfterRestart, null);
        assert.equal(planAfterRestart?.tenant_id, 'tenant-acme');
        assert.equal(planAfterRestart?.instance_id, 'sn-dev-01');
        assert.equal(planAfterRestart?.source, 'sn://acme-dev.service-now.com');
        assert.equal(runningAfterRestart?.tenant_id, 'tenant-acme');
        assert.equal(runningAfterRestart?.instance_id, 'sn-dev-01');
        assert.equal(runningAfterRestart?.source, 'sn://acme-dev.service-now.com');
        assert.equal(queuedAfterRestart?.tenant_id, 'tenant-acme');
        assert.equal(queuedAfterRestart?.instance_id, 'sn-dev-01');
        assert.equal(queuedAfterRestart?.source, 'sn://acme-dev.service-now.com');
        assert.equal(runningAfterRestart?.status, 'running');
        assert.equal(queuedAfterRestart?.status, 'queued');

        const runningEvents = await restarted.jobs.listJobEvents(running.jobId);
        const queuedEvents = await restarted.jobs.listJobEvents(queued.jobId);

        assert.equal(runningEvents.length >= 2, true);
        assert.equal(queuedEvents.length >= 2, true);

        const lockSnapshot = await restarted.jobs.getLockSnapshot();

        assert.deepEqual(
            lockSnapshot.running.map((entry) => entry.jobId),
            [running.jobId],
        );
        assert.deepEqual(
            lockSnapshot.queued.map((entry) => entry.jobId),
            [queued.jobId],
        );

        const completion = await restarted.jobs.completeJob(running.jobId, {
            status: 'completed',
        });

        assert.equal(completion.success, true);
        if (!completion.success) {
            return;
        }

        assert.deepEqual(completion.promoted_job_ids, [queued.jobId]);
        const promoted = await restarted.jobs.getJob(queued.jobId);

        assert.equal(promoted?.status, 'running');
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }
    }
});

test('lock queue preserves FIFO promotion order across restart', async () => {
    const db = newDb();
    db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
    const first = createFixture(db);
    let restartedOnce: Fixture | null = null;
    let restartedTwice: Fixture | null = null;

    try {
        const firstJob = await createPlanAndJob(first, 'plan-fair-01', '201');
        const secondJob = await createPlanAndJob(first, 'plan-fair-02', '202');
        const thirdJob = await createPlanAndJob(first, 'plan-fair-03', '203');

        const firstJobRecord = await first.jobs.getJob(firstJob.jobId);
        const secondJobRecord = await first.jobs.getJob(secondJob.jobId);
        const thirdJobRecord = await first.jobs.getJob(thirdJob.jobId);

        assert.equal(firstJobRecord?.status, 'running');
        assert.equal(secondJobRecord?.status, 'queued');
        assert.equal(thirdJobRecord?.status, 'queued');

        restartedOnce = createFixture(db);
        const firstCompletion = await restartedOnce.jobs.completeJob(firstJob.jobId, {
            status: 'completed',
        });

        assert.equal(firstCompletion.success, true);
        if (!firstCompletion.success) {
            return;
        }

        assert.deepEqual(firstCompletion.promoted_job_ids, [secondJob.jobId]);

        restartedTwice = createFixture(db);
        const secondCompletion = await restartedTwice.jobs.completeJob(secondJob.jobId, {
            status: 'completed',
        });

        assert.equal(secondCompletion.success, true);
        if (!secondCompletion.success) {
            return;
        }

        assert.deepEqual(secondCompletion.promoted_job_ids, [thirdJob.jobId]);
        const thirdAfterPromotion = await restartedTwice.jobs.getJob(thirdJob.jobId);

        assert.equal(thirdAfterPromotion?.status, 'running');
    } finally {
        await first.close();

        if (restartedOnce) {
            await restartedOnce.close();
        }

        if (restartedTwice) {
            await restartedTwice.close();
        }
    }
});
