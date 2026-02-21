import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { newDb } from 'pg-mem';
import { PostgresRestoreJobStateStore } from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { RestoreLockManager } from './locks/lock-manager';
import { PostgresRestorePlanStateStore } from './plans/plan-state-store';
import { RestorePlanService } from './plans/plan-service';
import { SourceRegistry } from './registry/source-registry';
import { InMemoryRestoreIndexStateReader } from './restore-index/state-reader';

type Fixture = {
    close: () => Promise<void>;
    jobs: RestoreJobService;
    plans: RestorePlanService;
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

function createDryRunRequest(planId: string): Record<string, unknown> {
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
            encoded_query: 'active=true',
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        rows: [
            {
                row_id: `row-${planId}`,
                table: 'incident',
                record_sys_id: `rec-${planId}`,
                action: 'update',
                precondition_hash: 'a'.repeat(64),
                metadata: {
                    allowlist_version: 'rrs.metadata.allowlist.v1',
                    metadata: {
                        tenant_id: 'tenant-acme',
                        instance_id: 'sn-dev-01',
                        source: 'sn://acme-dev.service-now.com',
                        table: 'incident',
                        record_sys_id: `rec-${planId}`,
                        event_id: `evt-${planId}`,
                        event_type: 'cdc.write',
                        operation: 'U',
                        schema_version: 3,
                        sys_updated_on: '2026-02-18 15:29:59',
                        sys_mod_count: 2,
                        __time: '2026-02-18T15:29:59.000Z',
                        topic: 'rez.cdc',
                        partition: 1,
                        offset: '42',
                    },
                },
                values: {
                    diff_enc: {
                        alg: 'AES-256-CBC',
                        module: 'x_rezrp_rezilient.encrypter',
                        format: 'kmf',
                        compression: 'none',
                        ciphertext: `cipher-${planId}`,
                    },
                },
            },
        ],
        conflicts: [],
        delete_candidates: [],
        media_candidates: [],
        watermarks: [
            {
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
            },
        ],
        pit_candidates: [],
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
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        new PostgresRestoreJobStateStore('postgres://unused', {
            pool: pool as any,
        }),
    );
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();

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
    );

    return {
        close: async () => {
            await pool.end();
        },
        jobs,
        plans,
    };
}

test('durable core state survives restart for plans/jobs/events/locks', async () => {
    const db = newDb();
    db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');
    const first = createFixture(db);
    let restarted: Fixture | null = null;

    try {
        const dryRun = await first.plans.createDryRunPlan(
            createDryRunRequest('plan-stage10-restart'),
            claims(),
        );

        assert.equal(dryRun.success, true);
        if (!dryRun.success) {
            return;
        }

        const running = await first.jobs.createJob(
            createJobRequest(
                'plan-stage10-restart',
                dryRun.record.plan.plan_hash,
            ),
            claims(),
        );
        const queued = await first.jobs.createJob(
            createJobRequest(
                'plan-stage10-queued',
                'b'.repeat(64),
            ),
            claims(),
        );

        assert.equal(running.success, true);
        assert.equal(queued.success, true);
        if (!running.success || !queued.success) {
            return;
        }

        restarted = createFixture(db);
        const planAfterRestart = await restarted.plans.getPlan('plan-stage10-restart');
        const runningAfterRestart = await restarted.jobs.getJob(running.job.job_id);
        const queuedAfterRestart = await restarted.jobs.getJob(queued.job.job_id);

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

        const runningEvents = await restarted.jobs.listJobEvents(running.job.job_id);
        const queuedEvents = await restarted.jobs.listJobEvents(queued.job.job_id);

        assert.equal(runningEvents.length >= 2, true);
        assert.equal(queuedEvents.length >= 2, true);

        const lockSnapshot = await restarted.jobs.getLockSnapshot();

        assert.deepEqual(
            lockSnapshot.running.map((entry) => entry.jobId),
            [running.job.job_id],
        );
        assert.deepEqual(
            lockSnapshot.queued.map((entry) => entry.jobId),
            [queued.job.job_id],
        );

        const completion = await restarted.jobs.completeJob(running.job.job_id, {
            status: 'completed',
        });

        assert.equal(completion.success, true);
        if (!completion.success) {
            return;
        }

        assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);
        const promoted = await restarted.jobs.getJob(queued.job.job_id);

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
        const firstJob = await first.jobs.createJob(
            createJobRequest('plan-fair-01', 'c'.repeat(64)),
            claims(),
        );
        const secondJob = await first.jobs.createJob(
            createJobRequest('plan-fair-02', 'd'.repeat(64)),
            claims(),
        );
        const thirdJob = await first.jobs.createJob(
            createJobRequest('plan-fair-03', 'e'.repeat(64)),
            claims(),
        );

        assert.equal(firstJob.success, true);
        assert.equal(secondJob.success, true);
        assert.equal(thirdJob.success, true);
        if (!firstJob.success || !secondJob.success || !thirdJob.success) {
            return;
        }

        assert.equal(firstJob.job.status, 'running');
        assert.equal(secondJob.job.status, 'queued');
        assert.equal(thirdJob.job.status, 'queued');

        restartedOnce = createFixture(db);
        const firstCompletion = await restartedOnce.jobs.completeJob(firstJob.job.job_id, {
            status: 'completed',
        });

        assert.equal(firstCompletion.success, true);
        if (!firstCompletion.success) {
            return;
        }

        assert.deepEqual(firstCompletion.promoted_job_ids, [secondJob.job.job_id]);

        restartedTwice = createFixture(db);
        const secondCompletion = await restartedTwice.jobs.completeJob(secondJob.job.job_id, {
            status: 'completed',
        });

        assert.equal(secondCompletion.success, true);
        if (!secondCompletion.success) {
            return;
        }

        assert.deepEqual(secondCompletion.promoted_job_ids, [thirdJob.job.job_id]);
        const thirdAfterPromotion = await restartedTwice.jobs.getJob(thirdJob.job.job_id);

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
