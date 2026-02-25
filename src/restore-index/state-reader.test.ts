import assert from 'node:assert/strict';
import { test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestoreIndexStateReader,
    PostgresRestoreIndexStateReader,
} from './state-reader';

test('in-memory reader derives freshness from authoritative indexed-through time', async () => {
    const reader = new InMemoryRestoreIndexStateReader({
        staleAfterSeconds: 120,
    });

    reader.upsertWatermark({
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-21T10:00:00.000Z',
        coverage_start: '2026-02-21T09:00:00.000Z',
        coverage_end: '2026-02-21T10:00:00.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-21T10:00:00.000Z',
    });

    const fresh = await reader.readWatermarksForPartitions({
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        measuredAt: '2026-02-21T10:01:00.000Z',
        partitions: [{
            topic: 'rez.cdc',
            partition: 1,
        }],
    });

    assert.equal(fresh.length, 1);
    assert.equal(fresh[0].freshness, 'fresh');
    assert.equal(fresh[0].executability, 'executable');

    const unknown = await reader.readWatermarksForPartitions({
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        measuredAt: '2026-02-21T10:01:00.000Z',
        partitions: [{
            topic: 'rez.cdc',
            partition: 9,
        }],
    });

    assert.equal(unknown.length, 1);
    assert.equal(unknown[0].freshness, 'unknown');
    assert.equal(unknown[0].executability, 'blocked');
    assert.equal(unknown[0].reason_code, 'blocked_freshness_unknown');
});

test('postgres reader maps authoritative rows and stale gating', async () => {
    const db = newDb();

    db.public.none(`
        CREATE SCHEMA IF NOT EXISTS rez_restore_index;

        CREATE TABLE rez_restore_index.partition_watermarks (
            tenant_id TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            source TEXT NOT NULL,
            topic TEXT NOT NULL,
            kafka_partition INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            indexed_through_offset BIGINT NOT NULL,
            indexed_through_time TIMESTAMPTZ NOT NULL,
            coverage_start TIMESTAMPTZ NOT NULL,
            coverage_end TIMESTAMPTZ NOT NULL,
            freshness TEXT NOT NULL,
            executability TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            measured_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (tenant_id, instance_id, topic, kafka_partition)
        );

        INSERT INTO rez_restore_index.partition_watermarks (
            tenant_id,
            instance_id,
            source,
            topic,
            kafka_partition,
            generation_id,
            indexed_through_offset,
            indexed_through_time,
            coverage_start,
            coverage_end,
            freshness,
            executability,
            reason_code,
            measured_at
        ) VALUES (
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
            'rez.cdc',
            1,
            'gen-01',
            100,
            '2026-02-21T09:50:00.000Z',
            '2026-02-21T09:00:00.000Z',
            '2026-02-21T09:50:00.000Z',
            'fresh',
            'executable',
            'none',
            '2026-02-21T09:50:00.000Z'
        );
    `);

    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();
    const reader = new PostgresRestoreIndexStateReader(pool as any, {
        staleAfterSeconds: 120,
    });

    try {
        const watermarks = await reader.readWatermarksForPartitions({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:00:00.000Z',
            partitions: [{
                topic: 'rez.cdc',
                partition: 1,
            }],
        });

        assert.equal(watermarks.length, 1);
        assert.equal(watermarks[0].freshness, 'stale');
        assert.equal(watermarks[0].executability, 'preview_only');
        assert.equal(watermarks[0].reason_code, 'blocked_freshness_stale');
        assert.equal(watermarks[0].indexed_through_offset, '100');

        const listed = await reader.listWatermarksForSource({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:00:00.000Z',
        });

        assert.equal(listed.length, 1);
        assert.equal(listed[0].topic, 'rez.cdc');
        assert.equal(listed[0].partition, 1);
    } finally {
        await pool.end();
    }
});

test('mixed fresh/stale partitions returns correct per-partition status', async () => {
    const reader = new InMemoryRestoreIndexStateReader({
        staleAfterSeconds: 120,
    });

    reader.upsertWatermark({
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 0,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-21T10:00:00.000Z',
        coverage_start: '2026-02-21T09:00:00.000Z',
        coverage_end: '2026-02-21T10:00:00.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-21T10:00:00.000Z',
    });
    reader.upsertWatermark({
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '50',
        indexed_through_time: '2026-02-21T09:50:00.000Z',
        coverage_start: '2026-02-21T09:00:00.000Z',
        coverage_end: '2026-02-21T09:50:00.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-21T09:50:00.000Z',
    });

    const watermarks =
        await reader.readWatermarksForPartitions({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:01:00.000Z',
            partitions: [
                { topic: 'rez.cdc', partition: 0 },
                { topic: 'rez.cdc', partition: 1 },
            ],
        });

    assert.equal(watermarks.length, 2);
    const p0 = watermarks.find(
        (watermark) => watermark.partition === 0,
    );
    const p1 = watermarks.find(
        (watermark) => watermark.partition === 1,
    );
    assert.equal(p0?.freshness, 'fresh');
    assert.equal(p1?.freshness, 'stale');
});

test('missing partition returns unknown freshness', async () => {
    const reader = new InMemoryRestoreIndexStateReader();

    const watermarks =
        await reader.readWatermarksForPartitions({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:00:00.000Z',
            partitions: [
                { topic: 'rez.cdc', partition: 99 },
            ],
        });

    assert.equal(watermarks.length, 1);
    assert.equal(watermarks[0].freshness, 'unknown');
    assert.equal(watermarks[0].executability, 'blocked');
    assert.equal(
        watermarks[0].reason_code,
        'blocked_freshness_unknown',
    );
});

test('boundary stale threshold (exactly at threshold)', async () => {
    const reader = new InMemoryRestoreIndexStateReader({
        staleAfterSeconds: 120,
    });

    reader.upsertWatermark({
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 0,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-21T09:58:00.000Z',
        coverage_start: '2026-02-21T09:00:00.000Z',
        coverage_end: '2026-02-21T09:58:00.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-21T09:58:00.000Z',
    });

    const atBoundary =
        await reader.readWatermarksForPartitions({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:00:00.000Z',
            partitions: [
                { topic: 'rez.cdc', partition: 0 },
            ],
        });

    assert.equal(atBoundary[0].freshness, 'fresh');

    const pastBoundary =
        await reader.readWatermarksForPartitions({
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            measuredAt: '2026-02-21T10:00:01.000Z',
            partitions: [
                { topic: 'rez.cdc', partition: 0 },
            ],
        });

    assert.equal(pastBoundary[0].freshness, 'stale');
});
