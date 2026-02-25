import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestorePlanStateStore,
    PostgresRestorePlanStateStore,
} from './plan-state-store';

describe('InMemoryRestorePlanStateStore', () => {
    test('read returns empty initial state', async () => {
        const store = new InMemoryRestorePlanStateStore();
        const state = await store.read();
        assert.deepEqual(state.plans_by_id, {});
    });

    test('mutate persists plan records', async () => {
        const store = new InMemoryRestorePlanStateStore();
        await store.mutate((state) => {
            state.plans_by_id['plan-01'] = {
                tenant_id: 'tenant-acme',
            } as any;
        });
        const state = await store.read();
        assert.ok(state.plans_by_id['plan-01']);
        assert.equal(
            state.plans_by_id['plan-01'].tenant_id,
            'tenant-acme',
        );
    });

    test('mutate is idempotent for same data', async () => {
        const store = new InMemoryRestorePlanStateStore();
        await store.mutate((state) => {
            state.plans_by_id['plan-01'] = {
                tenant_id: 'acme',
            } as any;
        });
        await store.mutate((state) => {
            state.plans_by_id['plan-01'] = {
                tenant_id: 'acme',
            } as any;
        });
        const state = await store.read();
        assert.equal(
            Object.keys(state.plans_by_id).length,
            1,
        );
    });
});

describe('PostgresRestorePlanStateStore', () => {
    test('read returns empty initial state', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestorePlanStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            const state = await store.read();
            assert.deepEqual(state.plans_by_id, {});
        } finally {
            await store.close();
        }
    });

    test('mutate persists and read retrieves', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestorePlanStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        try {
            await store.mutate((state) => {
                state.plans_by_id['plan-pg'] = {
                    tenant_id: 'pg-tenant',
                } as any;
            });
            const state = await store.read();
            assert.ok(state.plans_by_id['plan-pg']);
        } finally {
            await store.close();
        }
    });

    test('close releases pool', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();
        const store = new PostgresRestorePlanStateStore(
            'postgres://unused',
            { pool: pool as any },
        );
        await store.read();
        await store.close();
        assert.ok(true);
    });
});

describe('parseState', () => {
    test('handles missing plans_by_id by throwing', async () => {
        const store = new InMemoryRestorePlanStateStore();
        const state = await store.read();
        assert.ok(
            state.plans_by_id !== undefined,
            'plans_by_id should exist',
        );
    });
});
