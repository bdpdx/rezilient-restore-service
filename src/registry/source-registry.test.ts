import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { SourceRegistry } from './source-registry';

describe('SourceRegistry', () => {
    test('constructor initializes from mapping array', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://a.service-now.com',
            },
            {
                tenantId: 't2',
                instanceId: 'i2',
                source: 'sn://b.service-now.com',
            },
        ]);
        assert.equal(registry.list().length, 2);
    });

    test('resolve returns source for known tenant/instance', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://a.service-now.com',
            },
        ]);
        const result = registry.resolve('t1', 'i1');
        assert.ok(result);
        assert.equal(result.source, 'sn://a.service-now.com');
    });

    test('resolve returns null for unknown tenant/instance', () => {
        const registry = new SourceRegistry();
        const result = registry.resolve('t-unknown', 'i-unknown');
        assert.equal(result, null);
    });

    test('upsert adds new mapping', () => {
        const registry = new SourceRegistry();
        registry.upsert({
            tenantId: 't1',
            instanceId: 'i1',
            source: 'sn://new.service-now.com',
        });
        const result = registry.resolve('t1', 'i1');
        assert.ok(result);
        assert.equal(result.source, 'sn://new.service-now.com');
    });

    test('upsert overwrites existing mapping', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://old.service-now.com',
            },
        ]);
        registry.upsert({
            tenantId: 't1',
            instanceId: 'i1',
            source: 'sn://updated.service-now.com',
        });
        const result = registry.resolve('t1', 'i1');
        assert.ok(result);
        assert.equal(
            result.source,
            'sn://updated.service-now.com',
        );
    });

    test('validateScope succeeds for matching triple', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://a.service-now.com',
            },
        ]);
        const validation = registry.validateScope({
            tenantId: 't1',
            instanceId: 'i1',
            source: 'sn://a.service-now.com',
        });
        assert.equal(validation.allowed, true);
    });

    test('validateScope fails for mismatched source', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://a.service-now.com',
            },
        ]);
        const validation = registry.validateScope({
            tenantId: 't1',
            instanceId: 'i1',
            source: 'sn://wrong.service-now.com',
        });
        assert.equal(validation.allowed, false);
        assert.equal(
            validation.reasonCode,
            'blocked_unknown_source_mapping',
        );
    });

    test('validateScope fails for unknown tenant/instance', () => {
        const registry = new SourceRegistry();
        const validation = registry.validateScope({
            tenantId: 't-missing',
            instanceId: 'i-missing',
            source: 'sn://a.service-now.com',
        });
        assert.equal(validation.allowed, false);
        assert.equal(
            validation.reasonCode,
            'blocked_unknown_source_mapping',
        );
    });

    test('list returns all mappings', () => {
        const registry = new SourceRegistry([
            {
                tenantId: 't1',
                instanceId: 'i1',
                source: 'sn://a.service-now.com',
            },
            {
                tenantId: 't2',
                instanceId: 'i2',
                source: 'sn://b.service-now.com',
            },
            {
                tenantId: 't3',
                instanceId: 'i3',
                source: 'sn://c.service-now.com',
            },
        ]);
        assert.equal(registry.list().length, 3);
    });

    test('list returns empty array when no mappings', () => {
        const registry = new SourceRegistry();
        assert.deepEqual(registry.list(), []);
    });
});
