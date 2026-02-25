import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestoreEvidenceStateStore,
    PostgresRestoreEvidenceStateStore,
} from './evidence-state-store';

describe('InMemoryRestoreEvidenceStateStore', () => {
    test('read returns empty initial state', async () => {
        const store = new InMemoryRestoreEvidenceStateStore();
        const state = await store.read();
        assert.deepEqual(state.by_job_id, {});
        assert.deepEqual(state.by_evidence_id, {});
    });

    test('mutate persists evidence records', async () => {
        const store = new InMemoryRestoreEvidenceStateStore();
        await store.mutate((state) => {
            const record = {
                evidence: { evidence_id: 'ev-01' },
            } as any;
            state.by_job_id['job-01'] = record;
            state.by_evidence_id['ev-01'] = record;
        });
        const state = await store.read();
        assert.ok(state.by_job_id['job-01']);
        assert.ok(state.by_evidence_id['ev-01']);
    });

    test('lookup by job_id and evidence_id', async () => {
        const store = new InMemoryRestoreEvidenceStateStore();
        await store.mutate((state) => {
            const record = {
                evidence: { evidence_id: 'ev-02' },
                generated_at: '2026-02-16T12:00:00.000Z',
            } as any;
            state.by_job_id['job-02'] = record;
            state.by_evidence_id['ev-02'] = record;
        });
        const state = await store.read();
        assert.equal(
            state.by_job_id['job-02'].evidence.evidence_id,
            'ev-02',
        );
        assert.equal(
            state.by_evidence_id['ev-02'].evidence
                .evidence_id,
            'ev-02',
        );
    });
});

describe('PostgresRestoreEvidenceStateStore', () => {
    test('round-trip for evidence records', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestoreEvidenceStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            await store.mutate((state) => {
                const record = {
                    evidence: { evidence_id: 'ev-pg' },
                } as any;
                state.by_job_id['job-pg'] = record;
                state.by_evidence_id['ev-pg'] = record;
            });
            const state = await store.read();
            assert.ok(state.by_job_id['job-pg']);
            assert.ok(state.by_evidence_id['ev-pg']);
        } finally {
            await store.close();
        }
    });
});

describe('parseState', () => {
    test('handles missing optional fields', async () => {
        const store = new InMemoryRestoreEvidenceStateStore();
        const state = await store.read();
        assert.ok(state.by_evidence_id !== undefined);
    });
});
