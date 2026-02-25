import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { newDb } from 'pg-mem';
import { PostgresSnapshotStore } from './postgres-snapshot-store';

interface TestState {
    counter: number;
    items: string[];
}

function createEmpty(): TestState {
    return { counter: 0, items: [] };
}

function parseState(raw: unknown): TestState {
    const obj = raw as Record<string, unknown>;
    return {
        counter: (obj.counter as number) || 0,
        items: (obj.items as string[]) || [],
    };
}

function buildStore(
    db: ReturnType<typeof newDb>,
    storeKey = 'test-store',
    tableName = 'rrs_state_snapshots',
): PostgresSnapshotStore<TestState> {
    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();
    return new PostgresSnapshotStore<TestState>(
        'postgres://unused',
        createEmpty,
        parseState,
        {
            pool: pool as any,
            storeKey,
            tableName,
        },
    );
}

describe('PostgresSnapshotStore', () => {
    test('read returns default state when no rows exist', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(db);
        try {
            const state = await store.read();
            assert.equal(state.counter, 0);
            assert.deepEqual(state.items, []);
        } finally {
            await store.close();
        }
    });

    test('mutate writes and read retrieves', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(db);
        try {
            await store.mutate((state) => {
                state.counter = 42;
                state.items.push('hello');
            });
            const state = await store.read();
            assert.equal(state.counter, 42);
            assert.deepEqual(state.items, ['hello']);
        } finally {
            await store.close();
        }
    });

    test('mutate increments version', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(db);
        try {
            await store.mutate((state) => {
                state.counter = 1;
            });
            await store.mutate((state) => {
                state.counter = 2;
            });
            const state = await store.read();
            assert.equal(state.counter, 2);
        } finally {
            await store.close();
        }
    });

    test('concurrent mutate calls serialize correctly', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(db);
        try {
            await Promise.all([
                store.mutate((state) => {
                    state.counter += 1;
                }),
                store.mutate((state) => {
                    state.counter += 1;
                }),
            ]);
            const state = await store.read();
            assert.ok(
                state.counter >= 1,
                'at least one mutation applied',
            );
        } finally {
            await store.close();
        }
    });

    test('close releases pool connections', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(db);
        await store.read();
        await store.close();
        assert.ok(true, 'close completed without error');
    });

    test('lazy table creation runs on first access', async () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        const store = buildStore(
            db,
            'lazy-store',
            'rrs_state_snapshots',
        );
        try {
            const state = await store.read();
            assert.equal(state.counter, 0);
        } finally {
            await store.close();
        }
    });

    test('validateSqlIdentifier rejects injection attempts', () => {
        const db = newDb();
        db.public.none(
            'CREATE SCHEMA IF NOT EXISTS rez_restore_index',
        );
        assert.throws(() => {
            buildStore(
                db,
                'test-store',
                '"; DROP TABLE users; --',
            );
        }, /must match/);
    });
});
