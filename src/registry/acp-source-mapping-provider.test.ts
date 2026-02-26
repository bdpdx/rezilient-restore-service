import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
    AcpListSourceMappingsResult,
    AcpResolveSourceMappingResult,
    AcpSourceMappingRecord,
} from './acp-source-mapping-client';
import { CachedAcpSourceMappingProvider } from './acp-source-mapping-provider';

function buildMapping(
    source: string,
): AcpSourceMappingRecord {
    return {
        tenantId: 'tenant-acme',
        instanceId: 'instance-acme',
        source,
        tenantState: 'active',
        entitlementState: 'active',
        instanceState: 'active',
        allowedServices: ['rrs'],
        updatedAt: '2026-02-26T00:00:00.000Z',
    };
}

describe('CachedAcpSourceMappingProvider', () => {
    test('caches found mappings within positive TTL', async () => {
        let resolveCalls = 0;
        let listCalls = 0;
        let nowMs = Date.parse('2026-02-26T12:00:00.000Z');
        const provider = new CachedAcpSourceMappingProvider({
            async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
                resolveCalls += 1;

                return {
                    status: 'found',
                    mapping: {
                        ...buildMapping('sn://acme-v1.service-now.com'),
                        requestedServiceScope: 'rrs',
                        serviceAllowed: true,
                    },
                };
            },
            async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                listCalls += 1;

                return {
                    status: 'ok',
                    mappings: [buildMapping('sn://list.service-now.com')],
                };
            },
        }, {
            positiveTtlSeconds: 30,
            negativeTtlSeconds: 5,
            now: () => new Date(nowMs),
        });

        const first = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });
        const second = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });

        assert.equal(resolveCalls, 1);
        assert.equal(first.status, 'found');
        assert.equal(first.cacheHit, false);
        assert.equal(second.status, 'found');
        assert.equal(second.cacheHit, true);
        assert.equal(provider.getResolveCacheSize(), 1);
        const listResult = await provider.listSourceMappings();
        assert.equal(listCalls, 1);
        assert.equal(listResult.status, 'ok');

        nowMs += 31_000;

        const afterExpiry = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });
        assert.equal(afterExpiry.status, 'found');
        assert.equal(afterExpiry.cacheHit, false);
        assert.equal(resolveCalls, 2);
    });

    test('caches not_found results with negative TTL', async () => {
        let resolveCalls = 0;
        let nowMs = Date.parse('2026-02-26T12:00:00.000Z');
        const provider = new CachedAcpSourceMappingProvider({
            async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
                resolveCalls += 1;

                if (resolveCalls === 1) {
                    return {
                        status: 'not_found',
                    };
                }

                return {
                    status: 'found',
                    mapping: {
                        ...buildMapping('sn://acme-v2.service-now.com'),
                        requestedServiceScope: 'rrs',
                        serviceAllowed: true,
                    },
                };
            },
            async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                return {
                    status: 'ok',
                    mappings: [],
                };
            },
        }, {
            positiveTtlSeconds: 30,
            negativeTtlSeconds: 5,
            now: () => new Date(nowMs),
        });

        const first = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });
        const second = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });

        assert.equal(first.status, 'not_found');
        assert.equal(first.cacheHit, false);
        assert.equal(second.status, 'not_found');
        assert.equal(second.cacheHit, true);
        assert.equal(resolveCalls, 1);

        nowMs += 6_000;

        const third = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });
        assert.equal(resolveCalls, 2);
        assert.equal(third.status, 'found');
        assert.equal(third.cacheHit, false);
    });

    test('does not cache outage responses', async () => {
        let resolveCalls = 0;
        const provider = new CachedAcpSourceMappingProvider({
            async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
                resolveCalls += 1;

                if (resolveCalls === 1) {
                    return {
                        status: 'outage',
                        message: 'dependency unavailable',
                    };
                }

                return {
                    status: 'found',
                    mapping: {
                        ...buildMapping('sn://acme-v3.service-now.com'),
                        requestedServiceScope: 'rrs',
                        serviceAllowed: true,
                    },
                };
            },
            async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                return {
                    status: 'ok',
                    mappings: [],
                };
            },
        }, {
            positiveTtlSeconds: 30,
            negativeTtlSeconds: 5,
        });

        const first = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });
        const second = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
        });

        assert.equal(first.status, 'outage');
        assert.equal(first.cacheHit, false);
        assert.equal(second.status, 'found');
        assert.equal(second.cacheHit, false);
        assert.equal(resolveCalls, 2);
    });

    test('separates cache entries by service scope', async () => {
        let resolveCalls = 0;
        const provider = new CachedAcpSourceMappingProvider({
            async resolveSourceMapping(input): Promise<AcpResolveSourceMappingResult> {
                resolveCalls += 1;
                const serviceScope = input.serviceScope || 'rrs';

                return {
                    status: 'found',
                    mapping: {
                        ...buildMapping(
                            `sn://scope-${serviceScope}.service-now.com`,
                        ),
                        requestedServiceScope: serviceScope,
                        serviceAllowed: true,
                    },
                };
            },
            async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                return {
                    status: 'ok',
                    mappings: [],
                };
            },
        }, {
            positiveTtlSeconds: 30,
            negativeTtlSeconds: 5,
        });

        const rrs = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
            serviceScope: 'rrs',
        });
        const reg = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
            serviceScope: 'reg',
        });
        const rrsCached = await provider.resolveSourceMapping({
            tenantId: 'tenant-acme',
            instanceId: 'instance-acme',
            serviceScope: 'rrs',
        });

        assert.equal(rrs.status, 'found');
        assert.equal(reg.status, 'found');
        assert.equal(rrsCached.status, 'found');
        assert.equal(rrsCached.cacheHit, true);
        assert.equal(resolveCalls, 2);
    });

    test('rejects negative cache TTL configuration', () => {
        assert.throws(() => {
            new CachedAcpSourceMappingProvider({
                async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
                    return {
                        status: 'not_found',
                    };
                },
                async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                    return {
                        status: 'ok',
                        mappings: [],
                    };
                },
            }, {
                positiveTtlSeconds: 30,
                negativeTtlSeconds: -1,
            });
        }, /negativeTtlSeconds must be a non-negative integer/);
    });
});
