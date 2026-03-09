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

test('in-memory reader lookupIndexedEventCandidates filters scope and PIT',
async () => {
    const reader = new InMemoryRestoreIndexStateReader({
        staleAfterSeconds: 120,
    });

    reader.upsertIndexedEventCandidate({
        artifactKey: 'rez/restore/event=evt-a1.artifact.json',
        eventId: 'evt-a1',
        eventTime: '2026-02-21T11:00:00.000Z',
        instanceId: 'sn-dev-01',
        manifestKey: 'rez/restore/event=evt-a1.manifest.json',
        offset: '101',
        partition: 0,
        recordSysId: 'alpha',
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 11:00:00',
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    reader.upsertIndexedEventCandidate({
        artifactKey: 'rez/restore/event=evt-a2.artifact.json',
        eventId: 'evt-a2',
        eventTime: '2026-02-21T11:00:00.000Z',
        instanceId: 'sn-dev-01',
        manifestKey: 'rez/restore/event=evt-a2.manifest.json',
        offset: '102',
        partition: 0,
        recordSysId: 'alpha',
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 2,
        sysUpdatedOn: '2026-02-21 11:00:00',
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    reader.upsertIndexedEventCandidate({
        artifactKey: 'rez/restore/event=evt-b1.artifact.json',
        eventId: 'evt-b1',
        eventTime: '2026-02-21T12:00:00.000Z',
        instanceId: 'sn-dev-01',
        manifestKey: 'rez/restore/event=evt-b1.manifest.json',
        offset: '103',
        partition: 0,
        recordSysId: 'bravo',
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 12:00:00',
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    reader.upsertIndexedEventCandidate({
        artifactKey: 'rez/restore/event=evt-a3-after.artifact.json',
        eventId: 'evt-a3-after',
        eventTime: '2026-02-21T13:00:00.000Z',
        instanceId: 'sn-dev-01',
        manifestKey: 'rez/restore/event=evt-a3-after.manifest.json',
        offset: '104',
        partition: 0,
        recordSysId: 'alpha',
        source: 'sn://acme-dev.service-now.com',
        sysModCount: 3,
        sysUpdatedOn: '2026-02-21 13:00:00',
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    reader.upsertIndexedEventCandidate({
        artifactKey: 'rez/restore/event=evt-other-source.artifact.json',
        eventId: 'evt-other-source',
        eventTime: '2026-02-21T11:00:00.000Z',
        instanceId: 'sn-dev-01',
        manifestKey: 'rez/restore/event=evt-other-source.manifest.json',
        offset: '105',
        partition: 0,
        recordSysId: 'alpha',
        source: 'sn://other.service-now.com',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 11:00:00',
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });

    const scoped = await reader.lookupIndexedEventCandidates({
        instanceId: 'sn-dev-01',
        pitCutoff: '2026-02-21T12:30:00.000Z',
        recordSysIds: ['alpha'],
        source: 'sn://acme-dev.service-now.com',
        tables: ['x_app.ticket'],
        tenantId: 'tenant-acme',
    });

    assert.equal(scoped.coverage, 'covered');
    assert.deepEqual(
        scoped.candidates.map((candidate) => candidate.eventId),
        [
            'evt-a2',
            'evt-a1',
        ],
    );

    const tableScoped = await reader.lookupIndexedEventCandidates({
        instanceId: 'sn-dev-01',
        pitCutoff: '2026-02-21T12:30:00.000Z',
        source: 'sn://acme-dev.service-now.com',
        tables: ['x_app.ticket'],
        tenantId: 'tenant-acme',
    });

    assert.equal(tableScoped.coverage, 'covered');
    assert.deepEqual(
        tableScoped.candidates.map((candidate) => candidate.eventId),
        [
            'evt-a2',
            'evt-a1',
            'evt-b1',
        ],
    );

    const noCoverage = await reader.lookupIndexedEventCandidates({
        instanceId: 'sn-dev-01',
        pitCutoff: '2026-02-21T12:30:00.000Z',
        source: 'sn://acme-dev.service-now.com',
        tables: ['x_app.unknown'],
        tenantId: 'tenant-acme',
    });

    assert.equal(noCoverage.coverage, 'no_indexed_coverage');
    assert.equal(noCoverage.candidates.length, 0);
});

test('postgres reader lookupIndexedEventCandidates maps and orders rows',
async () => {
    const db = newDb();

    db.public.none(`
        CREATE SCHEMA IF NOT EXISTS rez_restore_index;

        CREATE TABLE rez_restore_index.index_events (
            tenant_id TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            source TEXT NOT NULL,
            table_name TEXT,
            record_sys_id TEXT,
            event_id TEXT,
            sys_updated_on TEXT,
            sys_mod_count INTEGER,
            event_time TIMESTAMPTZ NOT NULL,
            topic TEXT NOT NULL,
            kafka_partition INTEGER NOT NULL,
            kafka_offset TEXT NOT NULL,
            artifact_key TEXT,
            manifest_key TEXT
        );

        INSERT INTO rez_restore_index.index_events (
            tenant_id,
            instance_id,
            source,
            table_name,
            record_sys_id,
            event_id,
            sys_updated_on,
            sys_mod_count,
            event_time,
            topic,
            kafka_partition,
            kafka_offset,
            artifact_key,
            manifest_key
        ) VALUES
            (
                'tenant-acme',
                'sn-dev-01',
                'sn://acme-dev.service-now.com',
                'x_app.ticket',
                'alpha',
                'evt-pg-a1',
                '2026-02-21 11:00:00',
                1,
                '2026-02-21T11:00:00.000Z',
                'rez.cdc',
                0,
                '201',
                'rez/restore/event=evt-pg-a1.artifact.json',
                'rez/restore/event=evt-pg-a1.manifest.json'
            ),
            (
                'tenant-acme',
                'sn-dev-01',
                'sn://acme-dev.service-now.com',
                'x_app.ticket',
                'alpha',
                'evt-pg-a2',
                '2026-02-21 11:00:00',
                2,
                '2026-02-21T11:00:00.000Z',
                'rez.cdc',
                0,
                '202',
                'rez/restore/event=evt-pg-a2.artifact.json',
                'rez/restore/event=evt-pg-a2.manifest.json'
            ),
            (
                'tenant-acme',
                'sn-dev-01',
                'sn://acme-dev.service-now.com',
                'x_app.ticket',
                'bravo',
                'evt-pg-b1',
                '2026-02-21 12:00:00',
                1,
                '2026-02-21T12:00:00.000Z',
                'rez.cdc',
                0,
                '203',
                'rez/restore/event=evt-pg-b1.artifact.json',
                'rez/restore/event=evt-pg-b1.manifest.json'
            ),
            (
                'tenant-acme',
                'sn-dev-01',
                'sn://acme-dev.service-now.com',
                'x_app.ticket',
                'alpha',
                'evt-pg-after-cutoff',
                '2026-02-21 13:00:00',
                3,
                '2026-02-21T13:00:00.000Z',
                'rez.cdc',
                0,
                '204',
                'rez/restore/event=evt-pg-after-cutoff.artifact.json',
                'rez/restore/event=evt-pg-after-cutoff.manifest.json'
            ),
            (
                'tenant-acme',
                'sn-dev-01',
                'sn://other.service-now.com',
                'x_app.ticket',
                'alpha',
                'evt-pg-other-source',
                '2026-02-21 11:00:00',
                1,
                '2026-02-21T11:00:00.000Z',
                'rez.cdc',
                0,
                '205',
                'rez/restore/event=evt-pg-other-source.artifact.json',
                'rez/restore/event=evt-pg-other-source.manifest.json'
            );
    `);

    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();
    const reader = new PostgresRestoreIndexStateReader(pool as any, {
        staleAfterSeconds: 120,
    });

    try {
        const result = await reader.lookupIndexedEventCandidates({
            instanceId: 'sn-dev-01',
            pitCutoff: '2026-02-21T12:30:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tables: ['x_app.ticket'],
            tenantId: 'tenant-acme',
        });

        assert.equal(result.coverage, 'covered');
        assert.deepEqual(
            result.candidates.map((candidate) => candidate.eventId),
            [
                'evt-pg-a2',
                'evt-pg-a1',
                'evt-pg-b1',
            ],
        );

        const noCoverage = await reader.lookupIndexedEventCandidates({
            instanceId: 'sn-dev-01',
            pitCutoff: '2026-02-21T12:30:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tables: ['x_app.unknown'],
            tenantId: 'tenant-acme',
        });

        assert.equal(noCoverage.coverage, 'no_indexed_coverage');
        assert.equal(noCoverage.candidates.length, 0);
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
