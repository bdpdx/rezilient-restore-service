import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreLockManager } from './lock-manager';

test('overlapping table scopes are queued with explicit reason code', async () => {
    const locks = new RestoreLockManager();
    const first = locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });

    assert.equal(first.state, 'running');
    assert.equal(first.reasonCode, 'none');

    const second = locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident', 'task'],
    });

    assert.equal(second.state, 'queued');
    assert.equal(second.reasonCode, 'queued_scope_lock');
    assert.deepEqual(second.blockedTables, ['incident']);
    assert.equal(second.queuePosition, 1);
});

test('release promotes queued overlapping job once scope lock frees', async () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });

    const released = locks.release('job-1');

    assert.equal(released.released, true);
    assert.equal(released.promoted.length, 1);
    assert.equal(released.promoted[0].jobId, 'job-2');
    assert.equal(released.promoted[0].reasonCode, 'none');

    const snapshot = locks.snapshot();

    assert.deepEqual(
        snapshot.running.map((entry) => entry.jobId).sort(),
        ['job-2'],
    );
    assert.equal(snapshot.queued.length, 0);
});

test('non-overlapping scopes can run in parallel', async () => {
    const locks = new RestoreLockManager();

    const first = locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    const second = locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['cmdb_ci'],
    });

    assert.equal(first.state, 'running');
    assert.equal(second.state, 'running');
    assert.equal(second.reasonCode, 'none');

    const snapshot = locks.snapshot();

    assert.deepEqual(
        snapshot.running.map((entry) => entry.jobId).sort(),
        ['job-1', 'job-2'],
    );
    assert.equal(snapshot.queued.length, 0);
});

test('same table can run in parallel when restore sources differ', async () => {
    const locks = new RestoreLockManager();

    const first = locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tables: ['incident'],
    });
    const second = locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        source: 'sn://acme-prod.service-now.com',
        tables: ['incident'],
    });

    assert.equal(first.state, 'running');
    assert.equal(second.state, 'running');
    assert.equal(second.reasonCode, 'none');

    const snapshot = locks.snapshot();

    assert.deepEqual(
        snapshot.running.map((entry) => entry.jobId).sort(),
        ['job-1', 'job-2'],
    );
    assert.equal(snapshot.queued.length, 0);
});

test('getBlockingLocks returns blocked tables and blocker job IDs', () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['task'],
    });

    const blockers = locks.getBlockingLocks({
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident', 'task', 'cmdb_ci'],
    });

    assert.deepEqual(blockers.blockedTables, ['incident', 'task']);
    assert.deepEqual(blockers.blockerJobIds, ['job-1', 'job-2']);
});

test('acquire with empty table array throws', () => {
    const locks = new RestoreLockManager();

    assert.throws(
        () =>
            locks.acquire({
                jobId: 'job-1',
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                tables: [],
            }),
        /at least one table/,
    );
});

test('getQueuePosition returns position for queued job', () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });

    assert.equal(locks.getQueuePosition('job-2'), 1);
});

test('getQueuePosition returns null for unknown job', () => {
    const locks = new RestoreLockManager();

    assert.equal(
        locks.getQueuePosition('nonexistent'),
        null,
    );
});

test('loadState / exportState round-trips', () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-1',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-2',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });

    const exported = locks.exportState();
    const restored = new RestoreLockManager(exported);
    const snapshot = restored.snapshot();

    assert.equal(snapshot.running.length, 1);
    assert.equal(snapshot.running[0].jobId, 'job-1');
    assert.equal(snapshot.queued.length, 1);
    assert.equal(snapshot.queued[0].jobId, 'job-2');
});

test('release promotes multiple queued jobs when scopes free', () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-a',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident', 'task'],
    });
    locks.acquire({
        jobId: 'job-b',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-c',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['task'],
    });

    const released = locks.release('job-a');

    assert.equal(released.released, true);
    assert.equal(released.promoted.length, 2);

    const promotedIds = released.promoted.map(
        (entry) => entry.jobId,
    );

    assert.ok(promotedIds.includes('job-b'));
    assert.ok(promotedIds.includes('job-c'));

    const snapshot = locks.snapshot();

    assert.equal(snapshot.queued.length, 0);
    assert.equal(snapshot.running.length, 2);
});

test('release removes queued job without forcing unrelated promotion', () => {
    const locks = new RestoreLockManager();

    locks.acquire({
        jobId: 'job-running',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-stale-queued',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });
    locks.acquire({
        jobId: 'job-waiting',
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        tables: ['incident'],
    });

    const removedQueued = locks.release('job-stale-queued');

    assert.equal(removedQueued.released, false);
    assert.deepEqual(removedQueued.promoted, []);
    assert.equal(locks.getQueuePosition('job-waiting'), 1);

    const releasedRunning = locks.release('job-running');

    assert.equal(releasedRunning.released, true);
    assert.deepEqual(
        releasedRunning.promoted.map((entry) => entry.jobId),
        ['job-waiting'],
    );
});
