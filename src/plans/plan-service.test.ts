import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import type { RestoreWatermark } from '@rezilient/types';
import { AuthTokenClaims } from '../auth/claims';
import {
    AcpResolveSourceMappingResult,
    ResolveSourceMappingInput,
} from '../registry/acp-source-mapping-client';
import {
    SourceMappingResolver,
} from '../registry/source-mapping-resolver';
import { SourceRegistry } from '../registry/source-registry';
import {
    InMemoryRestoreIndexStateReader,
    RestoreIndexStateReader,
} from '../restore-index/state-reader';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from './materialization-service';
import { InMemoryRestorePlanStateStore } from './plan-state-store';
import { RestorePlanService } from './plan-service';
import type { FinalizeTargetReconciliationRequest } from './models';

const PIT_ALGORITHM_VERSION =
    'pit.v1.sys_updated_on-sys_mod_count-__time-event_id';
const NOW = new Date('2026-02-18T15:30:00.000Z');
const LEGACY_ROWS_COMPAT_OPTIONS = {
    allowLegacyRowsCompat: true,
} as const;

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

function buildWatermarkHint(
    overrides: Record<string, unknown> = {},
) {
    return {
        topic: 'rez.cdc',
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

function buildScopeDrivenDryRunRequest(
    planId = 'plan-scope-01',
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
        ...overrides,
    };
}

function buildArtifactBody(
    overrides: Record<string, unknown> = {},
): Record<string, unknown> {
    return {
        __op: 'U',
        __schema_version: 3,
        __type: 'cdc.write',
        row_enc: {
            alg: 'AES-256-GCM',
            ciphertext: 'cipher-row',
        },
        ...overrides,
    };
}

function buildFinalizeRequest(
    records: Array<{
        table: string;
        record_sys_id: string;
        target_state: 'exists' | 'missing';
    }>,
): FinalizeTargetReconciliationRequest {
    return {
        finalized_by: 'sn-worker',
        reconciled_records: records,
    };
}

function seedScopeMaterializationRecord(input: {
    artifactReader: InMemoryRestoreArtifactBodyReader;
    eventId: string;
    eventTime: string;
    indexReader: InMemoryRestoreIndexStateReader;
    offset: string;
    partition: number;
    recordSysId: string;
    sysModCount: number;
    sysUpdatedOn: string;
    table: string;
    sourceOperation?: 'I' | 'U' | 'D';
}): void {
    const artifactKey = `rez/restore/event=${input.eventId}.artifact.json`;
    const manifestKey = `rez/restore/event=${input.eventId}.manifest.json`;

    input.indexReader.upsertIndexedEventCandidate({
        artifactKey,
        eventId: input.eventId,
        eventTime: input.eventTime,
        instanceId: 'sn-dev-01',
        manifestKey,
        offset: input.offset,
        partition: input.partition,
        recordSysId: input.recordSysId,
        source: 'sn://acme-dev.service-now.com',
        sysModCount: input.sysModCount,
        sysUpdatedOn: input.sysUpdatedOn,
        table: input.table,
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    input.artifactReader.setArtifactBody({
        artifactKey,
        body: buildArtifactBody({
            __op: input.sourceOperation || 'U',
            row_enc: {
                alg: 'AES-256-GCM',
                ciphertext: `cipher-${input.recordSysId}`,
            },
        }),
        manifestKey,
    });
}

type Fixture = {
    service: RestorePlanService;
    indexReader: InMemoryRestoreIndexStateReader;
};

type ScopeDrivenFixture = Fixture & {
    artifactReader: InMemoryRestoreArtifactBodyReader;
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
        undefined,
        LEGACY_ROWS_COMPAT_OPTIONS,
    );
    return { service, indexReader };
}

function createScopeDrivenFixture(options?: {
    resolveHandler?: (
        input: ResolveSourceMappingInput,
    ) => Promise<AcpResolveSourceMappingResult>;
}): ScopeDrivenFixture {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const indexReader = new InMemoryRestoreIndexStateReader();
    const artifactReader = new InMemoryRestoreArtifactBodyReader();

    indexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(buildWatermark()),
    );

    const service = new RestorePlanService(
        registry,
        fixedNow,
        new InMemoryRestorePlanStateStore(),
        indexReader,
        createResolver(options?.resolveHandler),
        new RestoreRowMaterializationService(
            artifactReader,
        ),
    );

    return {
        service,
        indexReader,
        artifactReader,
    };
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

    test(
        'createDryRunPlan returns draft reconciliation request for scope-driven '
        + 'materialization',
        async () => {
            const fixture = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixture.artifactReader,
                eventId: 'evt-scope-01',
                eventTime: '2026-02-18T15:20:00.000Z',
                indexReader: fixture.indexReader,
                offset: '100',
                partition: 0,
                recordSysId: 'rec-01',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-18 15:20:00',
                table: 'x_app.ticket',
            });

            const result = await fixture.service.createDryRunPlan(
                buildScopeDrivenDryRunRequest('plan-scope-driven', {
                    scope: {
                        mode: 'record',
                        tables: ['x_app.ticket'],
                        record_sys_ids: ['rec-01'],
                    },
                }),
                claims(),
            );

            assert.equal(result.success, true);
            if (!result.success) {
                return;
            }

            assert.equal(result.statusCode, 202);
            assert.equal(result.reconciliation_state, 'draft');
            assert.equal(result.target_reconciliation_records.length, 1);
            assert.equal(result.record.plan_hash_input.rows.length, 1);
            assert.equal(result.record.plan_hash_input.rows[0].action, 'update');
            assert.equal(
                result.record.plan_hash_input.rows[0].metadata.metadata.event_id,
                'evt-scope-01',
            );
            assert.equal(
                result.target_reconciliation_records[0].record_sys_id,
                'rec-01',
            );
            assert.equal(result.target_reconciliation_records[0].table, 'x_app.ticket');
            assert.equal(
                result.target_reconciliation_records[0].source_operation,
                'U',
            );
            assert.equal(
                result.target_reconciliation_records[0].source_action,
                'update',
            );
            assert.equal(
                await fixture.service.getPlan('plan-scope-driven'),
                null,
            );
        },
    );

    test(
        'finalizeTargetReconciliation reconciles source insert into update '
        + 'when target record exists',
        async () => {
            const fixture = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixture.artifactReader,
                eventId: 'evt-scope-insert',
                eventTime: '2026-02-18T15:22:00.000Z',
                indexReader: fixture.indexReader,
                offset: '111',
                partition: 0,
                recordSysId: 'rec-01',
                sourceOperation: 'I',
                sysModCount: 3,
                sysUpdatedOn: '2026-02-18 15:22:00',
                table: 'x_app.ticket',
            });

            const draft = await fixture.service.createDryRunPlan(
                buildScopeDrivenDryRunRequest('plan-scope-insert-reconciled', {
                    scope: {
                        mode: 'record',
                        tables: ['x_app.ticket'],
                        record_sys_ids: ['rec-01'],
                    },
                }),
                claims(),
            );

            assert.equal(draft.success, true);
            if (!draft.success) {
                return;
            }

            assert.equal(draft.reconciliation_state, 'draft');
            assert.equal(draft.record.plan_hash_input.rows[0].action, 'insert');

            const finalized = await fixture.service.finalizeTargetReconciliation(
                'plan-scope-insert-reconciled',
                buildFinalizeRequest([{
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    target_state: 'exists',
                }]),
                claims(),
            );

            assert.equal(finalized.success, true);
            if (!finalized.success) {
                return;
            }

            assert.equal(finalized.statusCode, 201);
            assert.equal(finalized.record.plan_hash_input.rows.length, 1);
            assert.equal(finalized.record.plan_hash_input.rows[0].action, 'update');
            assert.equal(finalized.record.plan.action_counts.update, 1);
            assert.equal(finalized.record.plan.action_counts.insert, 0);
            assert.equal(
                finalized.record.plan_hash_input.rows[0].metadata.metadata.operation,
                'I',
            );
            assert.equal(finalized.reused_existing_plan, false);
        },
    );

    test(
        'finalizeTargetReconciliation blocks when source update points to '
        + 'missing target record',
        async () => {
            const fixture = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixture.artifactReader,
                eventId: 'evt-scope-update',
                eventTime: '2026-02-18T15:23:00.000Z',
                indexReader: fixture.indexReader,
                offset: '112',
                partition: 0,
                recordSysId: 'rec-01',
                sourceOperation: 'U',
                sysModCount: 4,
                sysUpdatedOn: '2026-02-18 15:23:00',
                table: 'x_app.ticket',
            });

            const draft = await fixture.service.createDryRunPlan(
                buildScopeDrivenDryRunRequest('plan-scope-update-missing', {
                    scope: {
                        mode: 'record',
                        tables: ['x_app.ticket'],
                        record_sys_ids: ['rec-01'],
                    },
                }),
                claims(),
            );

            assert.equal(draft.success, true);
            if (!draft.success) {
                return;
            }

            const finalized = await fixture.service.finalizeTargetReconciliation(
                'plan-scope-update-missing',
                buildFinalizeRequest([{
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    target_state: 'missing',
                }]),
                claims(),
            );

            assert.equal(finalized.success, false);
            if (finalized.success) {
                return;
            }

            assert.equal(finalized.statusCode, 409);
            assert.equal(finalized.error, 'restore_plan_materialization_failed');
            assert.equal(finalized.reasonCode, 'blocked_reference_conflict');
        },
    );

    test(
        'finalizeTargetReconciliation enforces exact target reconciliation '
        + 'request set',
        async () => {
            const fixture = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixture.artifactReader,
                eventId: 'evt-scope-validate',
                eventTime: '2026-02-18T15:21:00.000Z',
                indexReader: fixture.indexReader,
                offset: '111',
                partition: 0,
                recordSysId: 'rec-01',
                sourceOperation: 'I',
                sysModCount: 3,
                sysUpdatedOn: '2026-02-18 15:21:00',
                table: 'x_app.ticket',
            });

            const draft = await fixture.service.createDryRunPlan(
                buildScopeDrivenDryRunRequest('plan-scope-validate', {
                    scope: {
                        mode: 'record',
                        tables: ['x_app.ticket'],
                        record_sys_ids: ['rec-01'],
                    },
                }),
                claims(),
            );

            assert.equal(draft.success, true);
            if (!draft.success) {
                return;
            }

            const invalid = await fixture.service.finalizeTargetReconciliation(
                'plan-scope-validate',
                buildFinalizeRequest([{
                    table: 'x_app.other',
                    record_sys_id: 'rec-99',
                    target_state: 'exists',
                }]),
                claims(),
            );

            assert.equal(invalid.success, false);
            if (invalid.success) {
                return;
            }

            assert.equal(invalid.statusCode, 400);
            assert.equal(invalid.error, 'invalid_request');
        },
    );

    test(
        'finalizeTargetReconciliation keeps deterministic plan_hash with '
        + 'target-aware reconciliation',
        async () => {
            const fixtureA = createScopeDrivenFixture();
            const fixtureB = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixtureA.artifactReader,
                eventId: 'evt-alpha',
                eventTime: '2026-02-18T15:24:00.000Z',
                indexReader: fixtureA.indexReader,
                offset: '121',
                partition: 0,
                recordSysId: 'rec-alpha',
                sourceOperation: 'I',
                sysModCount: 4,
                sysUpdatedOn: '2026-02-18 15:24:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureA.artifactReader,
                eventId: 'evt-bravo',
                eventTime: '2026-02-18T15:25:00.000Z',
                indexReader: fixtureA.indexReader,
                offset: '122',
                partition: 0,
                recordSysId: 'rec-bravo',
                sourceOperation: 'I',
                sysModCount: 1,
                sysUpdatedOn: '2026-02-18 15:25:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureB.artifactReader,
                eventId: 'evt-bravo',
                eventTime: '2026-02-18T15:25:00.000Z',
                indexReader: fixtureB.indexReader,
                offset: '122',
                partition: 0,
                recordSysId: 'rec-bravo',
                sourceOperation: 'I',
                sysModCount: 1,
                sysUpdatedOn: '2026-02-18 15:25:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureB.artifactReader,
                eventId: 'evt-alpha',
                eventTime: '2026-02-18T15:24:00.000Z',
                indexReader: fixtureB.indexReader,
                offset: '121',
                partition: 0,
                recordSysId: 'rec-alpha',
                sourceOperation: 'I',
                sysModCount: 4,
                sysUpdatedOn: '2026-02-18 15:24:00',
                table: 'x_app.ticket',
            });

            const request = buildScopeDrivenDryRunRequest(
                'plan-scope-target-det',
                {
                    scope: {
                        mode: 'record',
                        tables: ['x_app.ticket'],
                        record_sys_ids: ['rec-alpha', 'rec-bravo'],
                    },
                },
            );
            const draftA = await fixtureA.service.createDryRunPlan(
                request,
                claims(),
            );
            const draftB = await fixtureB.service.createDryRunPlan(
                request,
                claims(),
            );

            assert.equal(draftA.success, true);
            assert.equal(draftB.success, true);
            if (!draftA.success || !draftB.success) {
                return;
            }

            const finalizedA = await fixtureA.service.finalizeTargetReconciliation(
                'plan-scope-target-det',
                buildFinalizeRequest([
                    {
                        table: 'x_app.ticket',
                        record_sys_id: 'rec-alpha',
                        target_state: 'exists',
                    },
                    {
                        table: 'x_app.ticket',
                        record_sys_id: 'rec-bravo',
                        target_state: 'missing',
                    },
                ]),
                claims(),
            );
            const finalizedB = await fixtureB.service.finalizeTargetReconciliation(
                'plan-scope-target-det',
                buildFinalizeRequest([
                    {
                        table: 'x_app.ticket',
                        record_sys_id: 'rec-bravo',
                        target_state: 'missing',
                    },
                    {
                        table: 'x_app.ticket',
                        record_sys_id: 'rec-alpha',
                        target_state: 'exists',
                    },
                ]),
                claims(),
            );

            assert.equal(finalizedA.success, true);
            assert.equal(finalizedB.success, true);
            if (!finalizedA.success || !finalizedB.success) {
                return;
            }

            assert.equal(
                finalizedA.record.plan.plan_hash,
                finalizedB.record.plan.plan_hash,
            );
            assert.deepEqual(
                finalizedA.record.plan_hash_input.rows,
                finalizedB.record.plan_hash_input.rows,
            );
            const actionByRecord = new Map(
                finalizedA.record.plan_hash_input.rows.map((row) => {
                    return [row.record_sys_id, row.action];
                }),
            );
            assert.equal(actionByRecord.get('rec-alpha'), 'update');
            assert.equal(actionByRecord.get('rec-bravo'), 'insert');
        },
    );

    test(
        'createDryRunPlan keeps deterministic plan_hash for '
        + 'scope-driven materialization',
        async () => {
            const fixtureA = createScopeDrivenFixture();
            const fixtureB = createScopeDrivenFixture();

            seedScopeMaterializationRecord({
                artifactReader: fixtureA.artifactReader,
                eventId: 'evt-alpha',
                eventTime: '2026-02-18T15:20:00.000Z',
                indexReader: fixtureA.indexReader,
                offset: '101',
                partition: 0,
                recordSysId: 'rec-alpha',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-18 15:20:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureA.artifactReader,
                eventId: 'evt-bravo',
                eventTime: '2026-02-18T15:21:00.000Z',
                indexReader: fixtureA.indexReader,
                offset: '102',
                partition: 0,
                recordSysId: 'rec-bravo',
                sysModCount: 1,
                sysUpdatedOn: '2026-02-18 15:21:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureB.artifactReader,
                eventId: 'evt-bravo',
                eventTime: '2026-02-18T15:21:00.000Z',
                indexReader: fixtureB.indexReader,
                offset: '102',
                partition: 0,
                recordSysId: 'rec-bravo',
                sysModCount: 1,
                sysUpdatedOn: '2026-02-18 15:21:00',
                table: 'x_app.ticket',
            });
            seedScopeMaterializationRecord({
                artifactReader: fixtureB.artifactReader,
                eventId: 'evt-alpha',
                eventTime: '2026-02-18T15:20:00.000Z',
                indexReader: fixtureB.indexReader,
                offset: '101',
                partition: 0,
                recordSysId: 'rec-alpha',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-18 15:20:00',
                table: 'x_app.ticket',
            });

            const request = buildScopeDrivenDryRunRequest('plan-scope-det');
            const resultA = await fixtureA.service.createDryRunPlan(
                request,
                claims(),
            );
            const resultB = await fixtureB.service.createDryRunPlan(
                request,
                claims(),
            );

            assert.equal(resultA.success, true);
            assert.equal(resultB.success, true);
            if (!resultA.success || !resultB.success) {
                return;
            }

            assert.equal(resultA.reconciliation_state, 'draft');
            assert.equal(resultB.reconciliation_state, 'draft');
            assert.equal(resultA.target_reconciliation_records.length, 2);
            assert.equal(resultB.target_reconciliation_records.length, 2);

            assert.equal(
                resultA.record.plan.plan_hash,
                resultB.record.plan.plan_hash,
            );
            assert.deepEqual(
                resultA.record.plan_hash_input.rows,
                resultB.record.plan_hash_input.rows,
            );
            assert.equal(
                resultA.record.plan_hash_input.rows[0].row_id <
                resultA.record.plan_hash_input.rows[1].row_id,
                true,
            );
        },
    );

    test('createDryRunPlan fails closed when scope lookup has no coverage', async () => {
        const fixture = createScopeDrivenFixture();
        const result = await fixture.service.createDryRunPlan(
            buildScopeDrivenDryRunRequest('plan-scope-no-coverage'),
            claims(),
        );

        assert.equal(result.success, false);
        if (result.success) {
            return;
        }

        assert.equal(result.statusCode, 409);
        assert.equal(result.error, 'restore_plan_materialization_failed');
        assert.equal(result.reasonCode, 'blocked_freshness_unknown');
        assert.equal(result.freshnessUnknownDetail, 'no_indexed_coverage');
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
                undefined,
                undefined,
                LEGACY_ROWS_COMPAT_OPTIONS,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-derived-non-zero', {
                    watermarks: [buildWatermarkHint()],
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

    test(
        'createDryRunPlan uses explicit partition hint fallback when source '
        + 'watermarks are absent',
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
                undefined,
                undefined,
                LEGACY_ROWS_COMPAT_OPTIONS,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-unknown', {
                    watermarks: [buildWatermarkHint({
                        partition: 99,
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
                assert.equal(
                    result.record.gate.freshness_unknown_detail,
                    'partition_not_indexed',
                );
                assert.equal(result.record.watermarks.length, 1);
                assert.equal(result.record.watermarks[0].topic, 'rez.cdc');
                assert.equal(result.record.watermarks[0].partition, 99);
            }
        },
    );

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
                undefined,
                undefined,
                LEGACY_ROWS_COMPAT_OPTIONS,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-derived-missing-authoritative', {
                    watermarks: [buildWatermarkHint()],
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
                assert.equal(
                    result.record.gate.freshness_unknown_detail,
                    'no_indexed_coverage',
                );
                assert.equal(result.record.watermarks.length, 0);
            }
        },
    );

    test(
        'createDryRunPlan surfaces invalid_authoritative_timestamp detail '
        + 'for unknown freshness from malformed authoritative timestamps',
        async () => {
            const registry = new SourceRegistry([
                {
                    tenantId: 'tenant-acme',
                    instanceId: 'sn-dev-01',
                    source: 'sn://acme-dev.service-now.com',
                },
            ]);
            const malformedTimestampReader: RestoreIndexStateReader = {
                async listWatermarksForSource() {
                    return [
                        buildWatermark({
                            indexed_through_time: 'not-a-timestamp',
                            freshness: 'unknown',
                            executability: 'blocked',
                            reason_code: 'blocked_freshness_unknown',
                        }) as RestoreWatermark,
                    ];
                },
                async readWatermarksForPartitions() {
                    return [];
                },
            };
            const service = new RestorePlanService(
                registry,
                fixedNow,
                new InMemoryRestorePlanStateStore(),
                malformedTimestampReader,
                undefined,
                undefined,
                LEGACY_ROWS_COMPAT_OPTIONS,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-invalid-authoritative-timestamp', {
                    watermarks: [buildWatermarkHint()],
                }),
                claims(),
            );

            assert.equal(result.success, true);
            if (result.success) {
                assert.equal(result.record.gate.executability, 'blocked');
                assert.equal(
                    result.record.gate.reason_code,
                    'blocked_freshness_unknown',
                );
                assert.equal(
                    result.record.gate.freshness_unknown_detail,
                    'invalid_authoritative_timestamp',
                );
            }
        },
    );

    test(
        'createDryRunPlan returns restore_index_unavailable detail '
        + 'when authoritative reads fail',
        async () => {
            const registry = new SourceRegistry([
                {
                    tenantId: 'tenant-acme',
                    instanceId: 'sn-dev-01',
                    source: 'sn://acme-dev.service-now.com',
                },
            ]);
            const failingReader: RestoreIndexStateReader = {
                async listWatermarksForSource() {
                    throw new Error('reader unavailable');
                },
                async readWatermarksForPartitions() {
                    throw new Error('reader unavailable');
                },
            };
            const service = new RestorePlanService(
                registry,
                fixedNow,
                new InMemoryRestorePlanStateStore(),
                failingReader,
                undefined,
                undefined,
                LEGACY_ROWS_COMPAT_OPTIONS,
            );
            const result = await service.createDryRunPlan(
                buildDryRunRequest('plan-index-unavailable', {
                    watermarks: [buildWatermarkHint()],
                }),
                claims(),
            );

            assert.equal(result.success, false);
            if (!result.success) {
                assert.equal(result.statusCode, 503);
                assert.equal(result.error, 'restore_index_unavailable');
                assert.equal(result.reasonCode, 'blocked_freshness_unknown');
                assert.equal(
                    result.freshnessUnknownDetail,
                    'restore_index_unavailable',
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
            undefined,
            undefined,
            LEGACY_ROWS_COMPAT_OPTIONS,
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
            undefined,
            undefined,
            LEGACY_ROWS_COMPAT_OPTIONS,
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
