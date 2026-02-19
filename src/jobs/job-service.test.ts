import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreJobService } from './job-service';
import { SourceRegistry } from '../registry/source-registry';
import { RestoreLockManager } from '../locks/lock-manager';

const FIXED_NOW = new Date('2026-02-16T12:00:00.000Z');

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

function baseRequest(planId: string, table: string) {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        plan_hash: 'a'.repeat(64),
        lock_scope_tables: [table],
        required_capabilities: ['restore_execute'],
        requested_by: 'operator@example.com',
    };
}

test('parallel non-overlapping jobs run immediately', () => {
    const service = new RestoreJobService(
        new RestoreLockManager(),
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
    );

    const first = service.createJob(baseRequest('plan-1', 'incident'), claims());
    const second = service.createJob(baseRequest('plan-2', 'cmdb_ci'), claims());

    assert.equal(first.success, true);
    assert.equal(second.success, true);

    if (!first.success || !second.success) {
        return;
    }

    assert.equal(first.job.status, 'running');
    assert.equal(second.job.status, 'running');
    assert.equal(first.job.status_reason_code, 'none');
    assert.equal(second.job.status_reason_code, 'none');
});

test('queued job is promoted after overlapping running job completes', () => {
    const service = new RestoreJobService(
        new RestoreLockManager(),
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
    );

    const running = service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    assert.equal(queued.job.status, 'queued');
    assert.equal(queued.job.status_reason_code, 'queued_scope_lock');

    const completion = service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    if (!completion.success) {
        return;
    }

    assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);

    const promoted = service.getJob(queued.job.job_id);

    assert.notEqual(promoted, null);
    assert.equal(promoted?.status, 'running');
    assert.equal(promoted?.status_reason_code, 'none');
    assert.equal(promoted?.queue_position, null);
});

test('running job can pause and resume without releasing scope lock', () => {
    const service = new RestoreJobService(
        new RestoreLockManager(),
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
    );

    const running = service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const pause = service.pauseJob(
        running.job.job_id,
        'paused_token_refresh_grace_exhausted',
    );

    assert.equal(pause.success, true);

    if (!pause.success) {
        return;
    }

    assert.equal(pause.job.status, 'paused');
    assert.equal(queued.job.status, 'queued');

    const resumed = service.resumePausedJob(running.job.job_id);

    assert.equal(resumed.success, true);

    if (!resumed.success) {
        return;
    }

    assert.equal(resumed.job.status, 'running');

    const completion = service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    if (!completion.success) {
        return;
    }

    assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);
});

test('job lifecycle events emit normalized cross-service audit events', () => {
    const service = new RestoreJobService(
        new RestoreLockManager(),
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
    );

    const running = service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const completion = service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    const runningCrossService = service.listCrossServiceJobEvents(
        running.job.job_id,
    );
    const queuedCrossService = service.listCrossServiceJobEvents(
        queued.job.job_id,
    );

    assert.ok(runningCrossService.length >= 2);
    assert.equal(runningCrossService[0].contract_version, 'audit.contracts.v1');
    assert.equal(runningCrossService[0].schema_version, 'audit.event.v1');
    assert.equal(runningCrossService[0].service, 'rrs');
    assert.equal(runningCrossService[0].tenant_id, 'tenant-acme');
    assert.equal(runningCrossService[0].instance_id, 'sn-dev-01');
    assert.equal(
        runningCrossService[0].source,
        'sn://acme-dev.service-now.com',
    );
    assert.equal(runningCrossService[0].plan_id, 'plan-1');
    assert.equal(runningCrossService[0].job_id, running.job.job_id);

    const hasPlanCreated = runningCrossService.some((event) =>
        event.lifecycle === 'plan' &&
        event.action === 'job_created' &&
        event.outcome === 'accepted'
    );
    const hasCompleted = runningCrossService.some((event) =>
        event.lifecycle === 'execute' &&
        event.action === 'completed' &&
        event.outcome === 'completed'
    );

    assert.equal(hasPlanCreated, true);
    assert.equal(hasCompleted, true);

    const hasQueued = queuedCrossService.some((event) =>
        event.lifecycle === 'execute' &&
        event.action === 'queued_for_lock' &&
        event.outcome === 'queued'
    );

    assert.equal(hasQueued, true);
});
