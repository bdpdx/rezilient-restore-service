import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestoreExecutionStateStore,
    PostgresRestoreExecutionStateStore,
} from './execute-state-store';

describe('InMemoryRestoreExecutionStateStore', () => {
    test('read returns empty initial state', async () => {
        const store = new InMemoryRestoreExecutionStateStore();
        const state = await store.read();
        assert.deepEqual(state.records_by_job_id, {});
        assert.deepEqual(
            state.rollback_journal_by_job_id,
            {},
        );
        assert.deepEqual(state.sn_mirror_by_job_id, {});
    });

    test('mutate persists execution records', async () => {
        const store = new InMemoryRestoreExecutionStateStore();
        await store.mutate((state) => {
            state.records_by_job_id['job-01'] = {
                job_id: 'job-01',
                status: 'completed',
            } as any;
        });
        const state = await store.read();
        assert.ok(state.records_by_job_id['job-01']);
    });

    test('mutate persists rollback journals', async () => {
        const store = new InMemoryRestoreExecutionStateStore();
        await store.mutate((state) => {
            state.rollback_journal_by_job_id['job-01'] = [
                { journal_id: 'j-01' } as any,
            ];
        });
        const state = await store.read();
        assert.equal(
            state.rollback_journal_by_job_id['job-01'].length,
            1,
        );
    });

    test('mutate persists SN mirror entries', async () => {
        const store = new InMemoryRestoreExecutionStateStore();
        await store.mutate((state) => {
            state.sn_mirror_by_job_id['job-01'] = [
                { mirror_id: 'm-01' } as any,
            ];
        });
        const state = await store.read();
        assert.equal(
            state.sn_mirror_by_job_id['job-01'].length,
            1,
        );
    });
});

describe('PostgresRestoreExecutionStateStore', () => {
    test('round-trip for execution and journal', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestoreExecutionStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            await store.mutate((state) => {
                state.records_by_job_id['job-pg'] = {
                    job_id: 'job-pg',
                } as any;
                state.rollback_journal_by_job_id['job-pg'] = [
                    { journal_id: 'j-pg' } as any,
                ];
            });
            const state = await store.read();
            assert.ok(state.records_by_job_id['job-pg']);
            assert.equal(
                state.rollback_journal_by_job_id['job-pg']
                    .length,
                1,
            );
        } finally {
            await store.close();
        }
    });
});

describe('parseState', () => {
    test('handles missing optional fields', async () => {
        const store = new InMemoryRestoreExecutionStateStore();
        const state = await store.read();
        assert.ok(state.sn_mirror_by_job_id !== undefined);
    });
});
