import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { AuthTokenClaims } from '../auth/claims';
import {
    AcpResolveSourceMappingResult,
    ResolveSourceMappingInput,
} from '../registry/acp-source-mapping-client';
import {
    SourceMappingResolver,
} from '../registry/source-mapping-resolver';
import { SourceRegistry } from '../registry/source-registry';
import { InMemoryRestoreIndexStateReader } from '../restore-index/state-reader';
import { InMemoryRestorePlanStateStore } from './plan-state-store';
import { RestorePlanService } from './plan-service';

const PIT_ALGORITHM_VERSION =
    'pit.v1.sys_updated_on-sys_mod_count-__time-event_id';
const NOW = new Date('2026-02-18T15:30:00.000Z');

function fixedNow(): Date {
    return new Date(NOW);
}

function claims(): AuthTokenClaims {
    return {
        iss: 'rez-auth-control-plane',
        sub: 'client-1',
        aud: 'rezilient:rrs',
        jti: 'tok-1',
        iat: 100,
        exp: 200,
        service_scope: 'rrs',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
    };
}

function buildWatermark(
    overrides: Record<string, unknown> = {},
) {
    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 0,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-18T15:29:30.000Z',
        coverage_start: '2026-02-18T00:00:00.000Z',
        coverage_end: '2026-02-18T15:29:30.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-18T15:30:00.000Z',
        ...overrides,
    };
}

function buildRow(
    overrides: Record<string, unknown> = {},
) {
    return {
        row_id: 'row-01',
        table: 'x_app.ticket',
        record_sys_id: 'rec-01',
        action: 'skip',
        precondition_hash: 'a'.repeat(64),
        metadata: {
            allowlist_version: 'rrs.metadata.allowlist.v1',
            metadata: {
                table: 'x_app.ticket',
                record_sys_id: 'rec-01',
            },
        },
        ...overrides,
    };
}

function buildDryRunRequest(
    planId = 'plan-01',
    overrides: Record<string, unknown> = {},
) {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-18T15:30:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version: PIT_ALGORITHM_VERSION,
            tie_breaker: [
                'sys_updated_on',
                'sys_mod_count',
                '__time',
                'event_id',
            ],
            tie_breaker_fallback: [
                'sys_updated_on',
                '__time',
                'event_id',
            ],
        },
        scope: {
            mode: 'table',
            tables: ['x_app.ticket'],
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        rows: [buildRow()],
        watermarks: [buildWatermark()],
        ...overrides,
    };
}

type Fixture = {
    service: RestorePlanService;
    indexReader: InMemoryRestoreIndexStateReader;
};

type FoundMapping = Extract<
    AcpResolveSourceMappingResult,
    { status: 'found' }
>;

function createResolveResult(
    overrides: Partial<FoundMapping['mapping']> = {},
): FoundMapping {
    return {
        status: 'found',
        mapping: {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantState: 'active',
            entitlementState: 'active',
            instanceState: 'active',
            allowedServices: ['rrs'],
            updatedAt: '2026-02-18T15:30:00.000Z',
            requestedServiceScope: 'rrs',
            serviceAllowed: true,
            ...overrides,
        },
    };
}

function createResolver(
    resolveHandler?: (
        input: ResolveSourceMappingInput,
    ) => Promise<AcpResolveSourceMappingResult>,
): SourceMappingResolver {
    return {
        async resolveSourceMapping(
            input: ResolveSourceMappingInput,
        ): Promise<AcpResolveSourceMappingResult> {
            if (resolveHandler) {
                return resolveHandler(input);
            }

            return createResolveResult();
        },
    };
}

function createFixture(options?: {
    resolveHandler?: (
        input: ResolveSourceMappingInput,
    ) => Promise<AcpResolveSourceMappingResult>;
}): Fixture {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const indexReader = new InMemoryRestoreIndexStateReader();
    indexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(buildWatermark()),
    );
    const service = new RestorePlanService(
        registry,
        fixedNow,
        new InMemoryRestorePlanStateStore(),
        indexReader,
        createResolver(options?.resolveHandler),
    );
    return { service, indexReader };
}

describe('RestorePlanService', () => {
    test('createDryRunPlan succeeds with executable gate', async () => {
        const { service } = createFixture();
        const result = await service.createDryRunPlan(
            buildDryRunRequest(),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'executable',
            );
            assert.equal(
                result.record.gate.reason_code,
                'none',
            );
        }
    });

    test('createDryRunPlan returns blocked gate for unresolved deletes', async () => {
        const { service } = createFixture();
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-del', {
                delete_candidates: [{
                    candidate_id: 'dc-01',
                    row_id: 'row-01',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                }],
            }),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'blocked',
            );
            assert.equal(
                result.record.gate.reason_code,
                'blocked_unresolved_delete_candidates',
            );
        }
    });

    test('createDryRunPlan returns blocked gate for hard-block conflicts', async () => {
        const { service } = createFixture();
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-conflict', {
                conflicts: [{
                    conflict_id: 'c-01',
                    class: 'reference_conflict',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    reason_code: 'blocked_reference_conflict',
                    reason: 'ref missing',
                    observed_at: '2026-02-18T15:00:00.000Z',
                }],
            }),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'blocked',
            );
            assert.equal(
                result.record.gate.reason_code,
                'blocked_reference_conflict',
            );
        }
    });

    test('createDryRunPlan returns blocked gate for unresolved media', async () => {
        const { service } = createFixture();
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-media', {
                media_candidates: [{
                    candidate_id: 'mc-01',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    media_id: 'media-01',
                    size_bytes: 128,
                    sha256_plain: 'b'.repeat(64),
                }],
            }),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'blocked',
            );
            assert.equal(
                result.record.gate.reason_code,
                'blocked_unresolved_media_candidates',
            );
        }
    });

    test(
        'createDryRunPlan derives non-zero partition from authoritative '
        + 'source watermarks when rows omit partition metadata',
        async () => {
            const registry = new SourceRegistry([
                {
                    tenantId: 'tenant-acme',
                    instanceId: 'sn-dev-01',
                    source: 'sn://acme-dev.service-now.com',
                },
            ]);
            const indexReader =
                new InMemoryRestoreIndexStateReader();
            indexReader.upsertWatermark(
                RestoreWatermarkSchema.parse(
                    buildWatermark({
                        partition: 7,
                    }),
                ),
            );
            const service = new RestorePlanService(
                registry,
                fixedNow,
                new InMemoryRestorePlanStateStore(),
                indexReader,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-derived-non-zero', {
                    watermarks: [buildWatermark({
                        partition: 0,
                        freshness: 'unknown',
                        executability: 'blocked',
                        reason_code: 'blocked_freshness_unknown',
                    })],
                }),
                claims(),
            );
            assert.equal(result.success, true);
            if (result.success) {
                assert.equal(
                    result.record.gate.executability,
                    'executable',
                );
                assert.equal(
                    result.record.gate.reason_code,
                    'none',
                );
                assert.equal(result.record.watermarks.length, 1);
                assert.equal(result.record.watermarks[0].partition, 7);
            }
        },
    );

    test('createDryRunPlan returns blocked gate for unknown freshness', async () => {
        const registry = new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]);
        const indexReader =
            new InMemoryRestoreIndexStateReader();
        const service = new RestorePlanService(
            registry,
            fixedNow,
            new InMemoryRestorePlanStateStore(),
            indexReader,
        );
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-unknown', {
                watermarks: [buildWatermark({
                    partition: 99,
                    freshness: 'unknown',
                    executability: 'blocked',
                    reason_code: 'blocked_freshness_unknown',
                })],
            }),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'blocked',
            );
            assert.equal(
                result.record.gate.reason_code,
                'blocked_freshness_unknown',
            );
        }
    });

    test(
        'createDryRunPlan remains fail-closed when authoritative source '
        + 'watermarks are absent and rows omit partition metadata',
        async () => {
            const registry = new SourceRegistry([
                {
                    tenantId: 'tenant-acme',
                    instanceId: 'sn-dev-01',
                    source: 'sn://acme-dev.service-now.com',
                },
            ]);
            const indexReader =
                new InMemoryRestoreIndexStateReader();
            const service = new RestorePlanService(
                registry,
                fixedNow,
                new InMemoryRestorePlanStateStore(),
                indexReader,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-derived-missing-authoritative', {
                    watermarks: [buildWatermark({
                        partition: 0,
                        freshness: 'fresh',
                        executability: 'executable',
                        reason_code: 'none',
                    })],
                }),
                claims(),
            );
            assert.equal(result.success, true);
            if (result.success) {
                assert.equal(
                    result.record.gate.executability,
                    'blocked',
                );
                assert.equal(
                    result.record.gate.reason_code,
                    'blocked_freshness_unknown',
                );
            }
        },
    );

    test('createDryRunPlan returns preview_only for stale partitions', async () => {
        const registry = new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]);
        const indexReader =
            new InMemoryRestoreIndexStateReader();
        indexReader.upsertWatermark(
            RestoreWatermarkSchema.parse(
                buildWatermark({
                    indexed_through_time:
                        '2026-02-18T15:27:00.000Z',
                    freshness: 'stale',
                    executability: 'preview_only',
                    reason_code: 'blocked_freshness_stale',
                }),
            ),
        );
        const service = new RestorePlanService(
            registry,
            fixedNow,
            new InMemoryRestorePlanStateStore(),
            indexReader,
        );
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-stale'),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.record.gate.executability,
                'preview_only',
            );
        }
    });

    test('createDryRunPlan validates scope against ACP mapping resolver', async () => {
        const { service } = createFixture();
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-scope'),
            claims(),
        );
        assert.equal(result.success, true);
    });

    test('createDryRunPlan rejects when ACP mapping is missing', async () => {
        const { service } = createFixture({
            resolveHandler: async () => ({
                status: 'not_found',
            }),
        });
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-missing-mapping'),
            claims(),
        );

        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(result.statusCode, 403);
            assert.equal(
                result.reasonCode,
                'blocked_unknown_source_mapping',
            );
        }
    });

    test('createDryRunPlan rejects mismatched ACP canonical source', async () => {
        const { service } = createFixture({
            resolveHandler: async () =>
                createResolveResult({
                    source: 'sn://different.service-now.com',
                }),
        });
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-acp-source-mismatch'),
            claims(),
        );

        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(result.statusCode, 403);
            assert.equal(
                result.reasonCode,
                'blocked_unknown_source_mapping',
            );
        }
    });

    test('createDryRunPlan rejects inactive ACP mapping', async () => {
        const { service } = createFixture({
            resolveHandler: async () =>
                createResolveResult({
                    instanceState: 'suspended',
                }),
        });
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-acp-inactive'),
            claims(),
        );

        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(result.statusCode, 403);
            assert.equal(
                result.reasonCode,
                'blocked_unknown_source_mapping',
            );
        }
    });

    test('createDryRunPlan rejects ACP outages explicitly', async () => {
        const { service } = createFixture({
            resolveHandler: async () => ({
                status: 'outage',
                message: 'ACP timeout',
            }),
        });
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-acp-outage'),
            claims(),
        );

        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(result.statusCode, 503);
            assert.equal(
                result.reasonCode,
                'blocked_auth_control_plane_outage',
            );
        }
    });

    test('createDryRunPlan rejects mismatched scope', async () => {
        const { service } = createFixture();
        const mismatchedClaims = claims();
        mismatchedClaims.tenant_id = 'tenant-wrong';
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-mismatch'),
            mismatchedClaims,
        );
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'blocked_unknown_source_mapping',
            );
        }
    });

    test('createDryRunPlan persists plan to state store', async () => {
        const { service } = createFixture();
        await service.createDryRunPlan(
            buildDryRunRequest('plan-persist'),
            claims(),
        );
        const plan = await service.getPlan('plan-persist');
        assert.ok(plan);
        assert.equal(plan.plan.plan_id, 'plan-persist');
    });

    test('createDryRunPlan generates deterministic plan_hash', async () => {
        const fixture1 = createFixture();
        const result1 = await fixture1.service.createDryRunPlan(
            buildDryRunRequest('plan-det-1'),
            claims(),
        );

        const fixture2 = createFixture();
        const result2 = await fixture2.service.createDryRunPlan(
            buildDryRunRequest('plan-det-1'),
            claims(),
        );

        assert.equal(result1.success, true);
        assert.equal(result2.success, true);
        if (result1.success && result2.success) {
            assert.equal(
                result1.record.plan.plan_hash,
                result2.record.plan.plan_hash,
            );
        }
    });

    test('getPlan returns plan by ID', async () => {
        const { service } = createFixture();
        await service.createDryRunPlan(
            buildDryRunRequest('plan-get'),
            claims(),
        );
        const plan = await service.getPlan('plan-get');
        assert.ok(plan);
        assert.equal(plan.plan.plan_id, 'plan-get');
    });

    test('getPlan returns null for unknown ID', async () => {
        const { service } = createFixture();
        const plan = await service.getPlan('nonexistent');
        assert.equal(plan, null);
    });

    test('listPlans returns all plans sorted by generation time', async () => {
        const { service } = createFixture();
        await service.createDryRunPlan(
            buildDryRunRequest('plan-a'),
            claims(),
        );
        await service.createDryRunPlan(
            buildDryRunRequest('plan-b'),
            claims(),
        );
        await service.createDryRunPlan(
            buildDryRunRequest('plan-c'),
            claims(),
        );
        const plans = await service.listPlans();
        assert.equal(plans.length, 3);
    });

    test('listPlans returns empty array when no plans exist', async () => {
        const { service } = createFixture();
        const plans = await service.listPlans();
        assert.deepEqual(plans, []);
    });

    test('freshness summary aggregates partition states correctly', async () => {
        const registry = new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]);
        const indexReader =
            new InMemoryRestoreIndexStateReader();

        indexReader.upsertWatermark(
            RestoreWatermarkSchema.parse(buildWatermark({
                partition: 0,
                freshness: 'fresh',
                executability: 'executable',
                reason_code: 'none',
            })),
        );
        indexReader.upsertWatermark(
            RestoreWatermarkSchema.parse(buildWatermark({
                partition: 1,
                indexed_through_time:
                    '2026-02-18T15:27:00.000Z',
                freshness: 'stale',
                executability: 'preview_only',
                reason_code: 'blocked_freshness_stale',
            })),
        );
        const service = new RestorePlanService(
            registry,
            fixedNow,
            new InMemoryRestorePlanStateStore(),
            indexReader,
        );
        const result = await service.createDryRunPlan(
            buildDryRunRequest('plan-mixed', {
                watermarks: [
                    buildWatermark({ partition: 0 }),
                    buildWatermark({
                        partition: 1,
                        indexed_through_time:
                            '2026-02-18T15:27:00.000Z',
                        freshness: 'stale',
                        executability: 'preview_only',
                        reason_code: 'blocked_freshness_stale',
                    }),
                ],
            }),
            claims(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.ok(
                result.record.gate.stale_partition_count >= 1,
            );
        }
    });
});
