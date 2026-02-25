import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestoreJobStateStore,
    PostgresRestoreJobStateStore,
} from './job-state-store';

describe('InMemoryRestoreJobStateStore', () => {
    test('read returns empty initial state', async () => {
        const store = new InMemoryRestoreJobStateStore();
        const state = await store.read();
        assert.deepEqual(state.plans_by_id, {});
        assert.deepEqual(state.jobs_by_id, {});
        assert.deepEqual(state.events_by_job_id, {});
        assert.deepEqual(state.lock_state.running_jobs, []);
        assert.deepEqual(state.lock_state.queued_jobs, []);
    });

    test('mutate persists jobs, events, lock state', async () => {
        const store = new InMemoryRestoreJobStateStore();
        await store.mutate((state) => {
            state.jobs_by_id['job-01'] = {
                job_id: 'job-01',
                status: 'running',
            } as any;
            state.events_by_job_id['job-01'] = [
                {
                    event_id: 'evt-01',
                    event_type: 'job_created',
                } as any,
            ];
            state.lock_state.running_jobs.push({
                job_id: 'job-01',
                tables: ['x_app.ticket'],
            } as any);
        });
        const state = await store.read();
        assert.ok(state.jobs_by_id['job-01']);
        assert.equal(
            state.events_by_job_id['job-01'].length,
            1,
        );
        assert.equal(
            state.lock_state.running_jobs.length,
            1,
        );
    });
});

describe('PostgresRestoreJobStateStore', () => {
    test('round-trip for jobs and events', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestoreJobStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            await store.mutate((state) => {
                state.jobs_by_id['job-pg'] = {
                    job_id: 'job-pg',
                    status: 'queued',
                } as any;
                state.events_by_job_id['job-pg'] = [];
            });
            const state = await store.read();
            assert.ok(state.jobs_by_id['job-pg']);
        } finally {
            await store.close();
        }
    });

    test('preserves lock state across mutations', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestoreJobStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            await store.mutate((state) => {
                state.lock_state.running_jobs.push({
                    job_id: 'job-pg',
                    tables: ['t1'],
                } as any);
            });
            await store.mutate((state) => {
                state.lock_state.queued_jobs.push({
                    job_id: 'job-q',
                    tables: ['t1'],
                } as any);
            });
            const state = await store.read();
            assert.equal(
                state.lock_state.running_jobs.length,
                1,
            );
            assert.equal(
                state.lock_state.queued_jobs.length,
                1,
            );
        } finally {
            await store.close();
        }
    });
});

describe('parseState', () => {
    test('handles missing optional fields', async () => {
        const store = new InMemoryRestoreJobStateStore();
        const state = await store.read();
        assert.ok(state.cross_service_events_by_job_id);
    });
});
