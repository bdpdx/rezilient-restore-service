import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SqliteRestoreJobStateStore } from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { RestoreLockManager } from './locks/lock-manager';
import { SqliteRestorePlanStateStore } from './plans/plan-state-store';
import { RestorePlanService } from './plans/plan-service';
import { SourceRegistry } from './registry/source-registry';

function createTempDbPath(name: string): string {
    const directory = mkdtempSync(join(tmpdir(), 'rrs-stage10-'));

    return join(directory, `${name}.sqlite`);
}

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

function createFixture(dbPath: string): {
    jobs: RestoreJobService;
    plans: RestorePlanService;
} {
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
        new SqliteRestoreJobStateStore(dbPath),
    );
    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        new SqliteRestorePlanStateStore(dbPath),
    );

    return {
        jobs,
        plans,
    };
}

test('durable core state survives restart for plans/jobs/events/locks', () => {
    const dbPath = createTempDbPath('core-restart-survival');
    const first = createFixture(dbPath);
    const dryRun = first.plans.createDryRunPlan(
        createDryRunRequest('plan-stage10-restart'),
        claims(),
    );

    assert.equal(dryRun.success, true);
    if (!dryRun.success) {
        return;
    }

    const running = first.jobs.createJob(
        createJobRequest(
            'plan-stage10-restart',
            dryRun.record.plan.plan_hash,
        ),
        claims(),
    );
    const queued = first.jobs.createJob(
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

    const restarted = createFixture(dbPath);
    const planAfterRestart = restarted.plans.getPlan('plan-stage10-restart');
    const runningAfterRestart = restarted.jobs.getJob(running.job.job_id);
    const queuedAfterRestart = restarted.jobs.getJob(queued.job.job_id);

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

    const runningEvents = restarted.jobs.listJobEvents(running.job.job_id);
    const queuedEvents = restarted.jobs.listJobEvents(queued.job.job_id);

    assert.equal(runningEvents.length >= 2, true);
    assert.equal(queuedEvents.length >= 2, true);

    const lockSnapshot = restarted.jobs.getLockSnapshot();

    assert.deepEqual(
        lockSnapshot.running.map((entry) => entry.jobId),
        [running.job.job_id],
    );
    assert.deepEqual(
        lockSnapshot.queued.map((entry) => entry.jobId),
        [queued.job.job_id],
    );

    const completion = restarted.jobs.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);
    if (!completion.success) {
        return;
    }

    assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);
    const promoted = restarted.jobs.getJob(queued.job.job_id);

    assert.equal(promoted?.status, 'running');
});

test('lock queue preserves FIFO promotion order across restart', () => {
    const dbPath = createTempDbPath('lock-fairness-restart');
    const first = createFixture(dbPath);
    const firstJob = first.jobs.createJob(
        createJobRequest('plan-fair-01', 'c'.repeat(64)),
        claims(),
    );
    const secondJob = first.jobs.createJob(
        createJobRequest('plan-fair-02', 'd'.repeat(64)),
        claims(),
    );
    const thirdJob = first.jobs.createJob(
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

    const restartedOnce = createFixture(dbPath);
    const firstCompletion = restartedOnce.jobs.completeJob(firstJob.job.job_id, {
        status: 'completed',
    });

    assert.equal(firstCompletion.success, true);
    if (!firstCompletion.success) {
        return;
    }

    assert.deepEqual(firstCompletion.promoted_job_ids, [secondJob.job.job_id]);

    const restartedTwice = createFixture(dbPath);
    const secondCompletion = restartedTwice.jobs.completeJob(secondJob.job.job_id, {
        status: 'completed',
    });

    assert.equal(secondCompletion.success, true);
    if (!secondCompletion.success) {
        return;
    }

    assert.deepEqual(secondCompletion.promoted_job_ids, [thirdJob.job.job_id]);
    const thirdAfterPromotion = restartedTwice.jobs.getJob(thirdJob.job.job_id);

    assert.equal(thirdAfterPromotion?.status, 'running');
});
