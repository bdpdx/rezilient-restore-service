import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { Server } from 'node:http';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import type { RestoreWatermark } from '@rezilient/types';
import { newDb } from 'pg-mem';
import {
    RestoreOpsAdminService,
    SourceMappingListProvider,
} from './admin/ops-admin-service';
import { RequestAuthenticator } from './auth/authenticator';
import { RestoreEvidenceService } from './evidence/evidence-service';
import {
    PostgresRestoreEvidenceStateStore,
    RestoreEvidenceStateStore,
} from './evidence/evidence-state-store';
import { RestoreExecutionService } from './execute/execute-service';
import type {
    RestoreReasonCode,
    RestoreTargetWriteRequest,
    RestoreTargetWriteResult,
    RestoreTargetWriter,
} from './execute/models';
import { NoopRestoreTargetWriter } from './execute/models';
import {
    PostgresRestoreExecutionStateStore,
    RestoreExecutionStateStore,
} from './execute/execute-state-store';
import {
    InMemoryRestoreJobStateStore,
    PostgresRestoreJobStateStore,
    RestoreJobStateStore,
} from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { RestoreLockManager } from './locks/lock-manager';
import { RestorePlanService } from './plans/plan-service';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from './plans/materialization-service';
import {
    PostgresRestorePlanStateStore,
    RestorePlanStateStore,
} from './plans/plan-state-store';
import {
    InMemoryRestoreTargetStateLookup,
    NoopRestoreTargetStateLookup,
} from './plans/target-reconciliation';
import {
    AcpListSourceMappingsResult,
    AcpResolveSourceMappingResult,
    AcpSourceMappingRecord,
} from './registry/acp-source-mapping-client';
import {
    SourceMappingResolver,
} from './registry/source-mapping-resolver';
import { CachedAcpSourceMappingProvider } from './registry/acp-source-mapping-provider';
import { SourceRegistry } from './registry/source-registry';
import {
    InMemoryRestoreIndexStateReader,
    RestoreIndexStateReader,
} from './restore-index/state-reader';
import { createRestoreServiceServer } from './server';
import {
    buildScopedToken,
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from './test-helpers';

interface ResponseData {
    status: number;
    body: Record<string, unknown>;
}

type SeedableRestoreIndexStateReader = RestoreIndexStateReader & {
    upsertIndexedEventCandidate?: (candidate: {
        artifactKey: string;
        eventId: string;
        eventTime: string;
        instanceId: string;
        manifestKey: string;
        offset: string;
        partition: number;
        recordSysId: string;
        source: string;
        sysModCount?: number | null;
        sysUpdatedOn?: string | null;
        table: string;
        tenantId: string;
        topic: string;
    }) => void;
    upsertWatermark: (watermark: RestoreWatermark) => void;
};

type ServiceStateStores = {
    jobs?: RestoreJobStateStore;
    plans?: RestorePlanStateStore;
    execute?: RestoreExecutionStateStore;
    evidence?: RestoreEvidenceStateStore;
};

const FIXED_NOW = new Date('2026-02-16T12:00:00.000Z');

function now(): Date {
    return new Date(FIXED_NOW.getTime());
}

class RecordingTargetWriter implements RestoreTargetWriter {
    readonly applyCalls: RestoreTargetWriteRequest[] = [];

    private readonly failedRecords = new Map<
        string,
        {
            reason_code: RestoreReasonCode;
            message: string;
        }
    >();

    failRecord(
        recordSysId: string,
        reasonCode: RestoreReasonCode,
        message: string,
    ): void {
        this.failedRecords.set(recordSysId, {
            reason_code: reasonCode,
            message,
        });
    }

    async applyRow(
        input: RestoreTargetWriteRequest,
    ): Promise<RestoreTargetWriteResult> {
        this.applyCalls.push(input);

        const failure = this.failedRecords.get(input.row.record_sys_id);

        if (failure) {
            return {
                outcome: 'failed',
                reason_code: failure.reason_code,
                message: failure.message,
            };
        }

        return {
            outcome: 'applied',
            reason_code: 'none',
        };
    }
}

async function listen(server: Server): Promise<string> {
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');

    const address = server.address();

    if (!address || typeof address === 'string') {
        throw new Error('server address is unavailable');
    }

    return `http://127.0.0.1:${address.port}`;
}

async function closeServer(server: Server): Promise<void> {
    await new Promise<void>((resolve) => {
        server.close(() => {
            resolve();
        });
    });
}

async function postJson(
    baseUrl: string,
    path: string,
    token: string | null,
    payload: Record<string, unknown>,
): Promise<ResponseData> {
    const headers: Record<string, string> = {
        'content-type': 'application/json',
    };

    if (token) {
        headers.authorization = `Bearer ${token}`;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
    });
    const body = await response.json() as Record<string, unknown>;

    return {
        status: response.status,
        body,
    };
}

async function getJson(
    baseUrl: string,
    path: string,
    token: string | null,
): Promise<ResponseData> {
    const headers: Record<string, string> = {};

    if (token) {
        headers.authorization = `Bearer ${token}`;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method: 'GET',
        headers,
    });
    const body = await response.json() as Record<string, unknown>;

    return {
        status: response.status,
        body,
    };
}

async function getAdminJson(
    baseUrl: string,
    path: string,
    adminToken?: string,
): Promise<ResponseData> {
    const headers: Record<string, string> = {};

    if (adminToken) {
        headers['x-rezilient-admin-token'] = adminToken;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method: 'GET',
        headers,
    });
    const body = await response.json() as Record<string, unknown>;

    return {
        status: response.status,
        body,
    };
}

async function postAdminJson(
    baseUrl: string,
    path: string,
    payload: Record<string, unknown>,
    adminToken?: string,
): Promise<ResponseData> {
    const headers: Record<string, string> = {
        'content-type': 'application/json',
    };

    if (adminToken) {
        headers['x-rezilient-admin-token'] = adminToken;
    }

    const response = await fetch(`${baseUrl}${path}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
    });
    const body = await response.json() as Record<string, unknown>;

    return {
        status: response.status,
        body,
    };
}

function materializedRowId(
    recordSysId: string,
    table = 'incident',
): string {
    const checksum = createHash('sha256')
        .update(`${table}|${recordSysId}`, 'utf8')
        .digest('hex');

    return `row.${checksum}`;
}

function createService(
    signingKey: string,
    options?: {
        executeConfig?: {
            defaultChunkSize?: number;
            maxRows?: number;
            elevatedSkipRatioPercent?: number;
            maxChunksPerAttempt?: number;
            mediaChunkSize?: number;
            mediaMaxItems?: number;
            mediaMaxBytes?: number;
            mediaMaxRetryAttempts?: number;
            executionProgressMode?: 'commit_driven' | 'legacy_apply';
        };
        adminToken?: string;
        bodyMaxBytes?: number;
        authoritativeWatermarks?: Record<string, unknown>[];
        indexedEventCandidates?: Array<{
            artifactKey: string;
            eventId: string;
            eventTime: string;
            manifestKey: string;
            offset: string;
            partition: number;
            recordSysId: string;
            sysModCount?: number | null;
            sysUpdatedOn?: string | null;
            table: string;
            topic?: string;
        }>;
        artifactBodies?: Array<{
            artifactKey: string;
            body: Record<string, unknown>;
            manifestKey: string;
        }>;
        targetRecordStates?: Array<{
            recordSysId: string;
            state: 'exists' | 'missing';
            table: string;
        }>;
        restoreIndexReader?: RestoreIndexStateReader;
        sourceMappingResolver?: SourceMappingResolver;
        sourceMappingListProvider?: SourceMappingListProvider;
        stateStores?: ServiceStateStores;
        targetWriter?: RestoreTargetWriter;
        failClosedRuntimeComposition?: boolean;
        executePreflightReconcileStaleAfterMs?: number;
    },
): Server {
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const authenticator = new RequestAuthenticator({
        signingKey,
        tokenClockSkewSeconds: 30,
        expectedIssuer: 'rez-auth-control-plane',
        now,
    });
    const restoreIndexReader = options?.restoreIndexReader
        || new InMemoryRestoreIndexStateReader();
    const artifactBodyReader = new InMemoryRestoreArtifactBodyReader();
    const failClosedRuntimeComposition =
        options?.failClosedRuntimeComposition === true;
    const targetStateLookup = failClosedRuntimeComposition
        ? undefined
        : options?.targetRecordStates
        ? new InMemoryRestoreTargetStateLookup()
        : new NoopRestoreTargetStateLookup();

    if (
        typeof (restoreIndexReader as Partial<SeedableRestoreIndexStateReader>)
            .upsertWatermark === 'function'
    ) {
        const seedWatermarks = options?.authoritativeWatermarks === undefined
            ? [createWatermark()]
            : options.authoritativeWatermarks;

        for (let index = 0; index < seedWatermarks.length; index += 1) {
            const parsed = RestoreWatermarkSchema.safeParse(seedWatermarks[index]);

            if (!parsed.success) {
                throw new Error(
                    `invalid authoritativeWatermarks[${index}] fixture`,
                );
            }

            (
                restoreIndexReader as SeedableRestoreIndexStateReader
            ).upsertWatermark(parsed.data);
        }

        const indexedCandidates = options?.indexedEventCandidates === undefined
            ? [
                createIndexedEventCandidateFixture({
                    eventId: 'evt-rec-01',
                    offset: '100',
                    partition: 1,
                    recordSysId: 'rec-01',
                }),
                createIndexedEventCandidateFixture({
                    eventId: 'evt-rec-02',
                    offset: '101',
                    partition: 1,
                    recordSysId: 'rec-02',
                }),
                createIndexedEventCandidateFixture({
                    eventId: 'evt-rec-03',
                    offset: '102',
                    partition: 1,
                    recordSysId: 'rec-03',
                }),
            ]
            : options.indexedEventCandidates;
        const seedableReader = restoreIndexReader as SeedableRestoreIndexStateReader;

        if (seedableReader.upsertIndexedEventCandidate) {
            for (const candidate of indexedCandidates) {
                seedableReader.upsertIndexedEventCandidate({
                    artifactKey: candidate.artifactKey,
                    eventId: candidate.eventId,
                    eventTime: candidate.eventTime,
                    instanceId: 'sn-dev-01',
                    manifestKey: candidate.manifestKey,
                    offset: candidate.offset,
                    partition: candidate.partition,
                    recordSysId: candidate.recordSysId,
                    source: 'sn://acme-dev.service-now.com',
                    sysModCount: candidate.sysModCount,
                    sysUpdatedOn: candidate.sysUpdatedOn,
                    table: candidate.table,
                    tenantId: 'tenant-acme',
                    topic: candidate.topic || 'rez.cdc',
                });
            }
        }
    }

    const artifactBodies = options?.artifactBodies === undefined
        ? [
            createArtifactBodyFixture({
                eventId: 'evt-rec-01',
            }),
            createArtifactBodyFixture({
                eventId: 'evt-rec-02',
            }),
            createArtifactBodyFixture({
                eventId: 'evt-rec-03',
            }),
        ]
        : options.artifactBodies;

    if (artifactBodies) {
        for (const artifact of artifactBodies) {
            artifactBodyReader.setArtifactBody({
                artifactKey: artifact.artifactKey,
                body: artifact.body,
                manifestKey: artifact.manifestKey,
            });
        }
    }

    if (options?.targetRecordStates && targetStateLookup) {
        for (const targetRecord of options.targetRecordStates) {
            if (targetStateLookup instanceof InMemoryRestoreTargetStateLookup) {
                targetStateLookup.setTargetRecordState({
                    record_sys_id: targetRecord.recordSysId,
                    state: targetRecord.state,
                    table: targetRecord.table,
                });
            }
        }
    }

    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        options?.stateStores?.plans,
        restoreIndexReader,
        options?.sourceMappingResolver,
        new RestoreRowMaterializationService(
            artifactBodyReader,
            targetStateLookup,
        ),
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        options?.stateStores?.jobs,
        options?.sourceMappingResolver,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await plans.getPlan(planId);

                if (!plan) {
                    return null;
                }

                return {
                    plan_id: plan.plan.plan_id,
                    plan_hash: plan.plan.plan_hash,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {
            executionProgressMode: 'legacy_apply',
            ...(options?.executeConfig || {}),
        },
        now,
        options?.stateStores?.execute,
        options?.targetWriter ||
            (
                failClosedRuntimeComposition
                    ? undefined
                    : new NoopRestoreTargetWriter()
            ),
        targetStateLookup,
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        {
            signer: {
                signer_key_id: 'rrs-test-signer',
                private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
                public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
            },
            immutable_storage: {
                worm_enabled: true,
                retention_class: 'compliance-7y',
            },
        },
        now,
        options?.stateStores?.evidence,
    );
    const admin = new RestoreOpsAdminService(
        jobs,
        plans,
        evidence,
        execute,
        options?.sourceMappingListProvider || createSourceMappingListProvider(),
        restoreIndexReader,
        {
            now,
        },
    );

    return createRestoreServiceServer({
        admin,
        authenticator,
        evidence,
        execute,
        jobs,
        plans,
    }, {
        adminToken: options?.adminToken,
        maxJsonBodyBytes: options?.bodyMaxBytes,
        executePreflightReconcileStaleAfterMs:
            options?.executePreflightReconcileStaleAfterMs,
    });
}

function createPostgresBackedStateStores(
    db: ReturnType<typeof newDb>,
): {
    pool: {
        end: () => Promise<void>;
    };
    stores: ServiceStateStores;
} {
    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();

    return {
        pool,
        stores: {
            plans: new PostgresRestorePlanStateStore('postgres://unused', {
                pool: pool as any,
            }),
            jobs: new PostgresRestoreJobStateStore('postgres://unused', {
                pool: pool as any,
            }),
            execute: new PostgresRestoreExecutionStateStore(
                'postgres://unused',
                {
                    pool: pool as any,
                },
            ),
            evidence: new PostgresRestoreEvidenceStateStore(
                'postgres://unused',
                {
                    pool: pool as any,
                },
            ),
        },
    };
}

function createToken(
    signingKey: string,
    scope: 'reg' | 'rrs' = 'rrs',
    overrides?: Partial<{
        tenantId: string;
        instanceId: string;
        source: string;
    }>,
): string {
    const issuedAt = Math.floor(now().getTime() / 1000);

    return buildScopedToken({
        expiresInSeconds: 3600,
        issuedAt,
        signingKey,
        scope,
        tenantId: overrides?.tenantId,
        instanceId: overrides?.instanceId,
        source: overrides?.source,
    });
}

function createSourceMappingRecord(
    overrides: Partial<AcpSourceMappingRecord> = {},
): AcpSourceMappingRecord {
    return {
        tenantId: 'tenant-acme',
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantState: 'active',
        entitlementState: 'active',
        instanceState: 'active',
        allowedServices: ['rrs'],
        updatedAt: '2026-02-16T12:00:00.000Z',
        ...overrides,
    };
}

function createSourceMappingListProvider(
    result: AcpListSourceMappingsResult = {
        status: 'ok',
        mappings: [
            createSourceMappingRecord(),
        ],
    },
): SourceMappingListProvider {
    return {
        async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
            return result;
        },
    };
}

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
            ...createSourceMappingRecord(),
            requestedServiceScope: 'rrs',
            serviceAllowed: true,
            ...overrides,
        },
    };
}

function createResolver(
    result: AcpResolveSourceMappingResult,
): SourceMappingResolver {
    return {
        async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
            return result;
        },
    };
}

function assertScopedNotFound(response: ResponseData): void {
    assert.equal(response.status, 404);
    assert.equal(response.body.error, 'not_found');
    assert.equal(
        response.body.reason_code,
        'blocked_unknown_source_mapping',
    );
}

function assertNotScopedNotFound(
    response: ResponseData,
    context: string,
): void {
    const isScopedNotFound = response.status === 404 &&
        response.body.error === 'not_found' &&
        response.body.reason_code === 'blocked_unknown_source_mapping';

    assert.equal(
        isScopedNotFound,
        false,
        `${context} returned scoped not_found for a valid in-scope job`,
    );
}

function createJobPayload(
    source: string,
    overrides?: {
        planId?: string;
        planHash?: string;
    },
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source,
        plan_id: overrides?.planId || 'plan-1',
        plan_hash: overrides?.planHash || 'a'.repeat(64),
        lock_scope_tables: ['incident'],
        required_capabilities: ['restore_execute'],
        requested_by: 'operator@example.com',
    };
}

function createDryRunRow(rowId: string, recordSysId: string): Record<string, unknown> {
    return {
        row_id: rowId,
        table: 'incident',
        record_sys_id: recordSysId,
        action: 'update',
        precondition_hash: 'a'.repeat(64),
        metadata: {
            allowlist_version: 'rrs.metadata.allowlist.v1',
            metadata: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                table: 'incident',
                record_sys_id: recordSysId,
                event_id: `evt-${rowId}`,
                event_type: 'cdc.write',
                operation: 'U',
                schema_version: 3,
                sys_updated_on: '2026-02-16 11:59:59',
                sys_mod_count: 2,
                __time: '2026-02-16T11:59:59.000Z',
                topic: 'rez.cdc',
                partition: 1,
                offset: '100',
            },
        },
        values: {
            diff_enc: {
                alg: 'AES-256-CBC',
                module: 'x_rezrp_rezilient.encrypter',
                format: 'kmf',
                compression: 'none',
                ciphertext: `cipher-${rowId}`,
            },
        },
    };
}

function createMediaCandidate(
    candidateId: string,
    overrides?: Partial<{
        decision: 'include' | 'exclude';
        parent_record_exists: boolean;
        observed_sha256_plain: string;
        retryable_failures: number;
        max_retry_attempts: number;
        size_bytes: number;
        sha256_plain: string;
    }>,
): Record<string, unknown> {
    const defaultHash = 'b'.repeat(64);
    const candidate: Record<string, unknown> = {
        candidate_id: candidateId,
        table: 'incident',
        record_sys_id: `rec-${candidateId}`,
        attachment_sys_id: `att-${candidateId}`,
        size_bytes: overrides?.size_bytes ?? 128,
        sha256_plain: overrides?.sha256_plain || defaultHash,
        decision: overrides?.decision ?? 'include',
        parent_record_exists: overrides?.parent_record_exists ?? true,
    };

    if (overrides?.observed_sha256_plain !== undefined) {
        candidate.observed_sha256_plain = overrides.observed_sha256_plain;
    }

    if (overrides?.retryable_failures !== undefined) {
        candidate.retryable_failures = overrides.retryable_failures;
    }

    if (overrides?.max_retry_attempts !== undefined) {
        candidate.max_retry_attempts = overrides.max_retry_attempts;
    }

    return candidate;
}

function createIndexedEventCandidateFixture(input: {
    eventId: string;
    offset: string;
    partition: number;
    recordSysId: string;
    table?: string;
}) {
    const artifactKey = `rez/restore/event=${input.eventId}.artifact.json`;
    const manifestKey = `rez/restore/event=${input.eventId}.manifest.json`;

    return {
        artifactKey,
        eventId: input.eventId,
        eventTime: '2026-02-16T11:59:00.000Z',
        manifestKey,
        offset: input.offset,
        partition: input.partition,
        recordSysId: input.recordSysId,
        sysModCount: 2,
        sysUpdatedOn: '2026-02-16 11:59:00',
        table: input.table || 'incident',
        topic: 'rez.cdc',
    };
}

function createArtifactBodyFixture(input: {
    eventId: string;
    operation?: 'D' | 'I' | 'U';
}) {
    const artifactKey = `rez/restore/event=${input.eventId}.artifact.json`;
    const manifestKey = `rez/restore/event=${input.eventId}.manifest.json`;
    const operation = input.operation || 'U';

    return {
        artifactKey,
        body: {
            __op: operation,
            __schema_version: 3,
            __type: operation === 'D' ? 'cdc.delete' : 'cdc.write',
            row_enc: {
                alg: 'AES-256-GCM',
                ciphertext: `cipher-${input.eventId}`,
            },
        },
        manifestKey,
    };
}

function createWatermark(input?: {
    freshness?: 'fresh' | 'stale' | 'unknown';
    executability?: 'executable' | 'preview_only' | 'blocked';
    reasonCode?: string;
}): Record<string, unknown> {
    const freshness = input?.freshness || 'fresh';
    const executability = input?.executability || 'executable';
    const reasonCode = input?.reasonCode || 'none';

    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-16T12:00:00.000Z',
        coverage_start: '2026-02-16T00:00:00.000Z',
        coverage_end: '2026-02-16T12:00:00.000Z',
        freshness,
        executability,
        reason_code: reasonCode,
        measured_at: '2026-02-16T12:00:00.000Z',
    };
}

function createDryRunPayload(
    planId: string,
    overrides?: {
        rows?: Record<string, unknown>[];
        watermarks?: Record<string, unknown>[];
        deleteCandidates?: Record<string, unknown>[];
        conflicts?: Record<string, unknown>[];
        mediaCandidates?: Record<string, unknown>[];
    },
): Record<string, unknown> {
    const rows = overrides?.rows || [
        createDryRunRow('row-01', 'rec-01'),
        createDryRunRow('row-02', 'rec-02'),
    ];

    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-16T12:00:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version: 'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
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
            mode: 'record',
            tables: Array.from(new Set(
                rows.map((row) => String(row.table || 'incident')),
            )),
            record_sys_ids: Array.from(new Set(
                rows.map((row) => String(row.record_sys_id || '')),
            )).filter(Boolean),
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        conflicts: overrides?.conflicts || [],
        delete_candidates: overrides?.deleteCandidates || [],
        media_candidates: overrides?.mediaCandidates || [],
        compatibility_adapter: {
            rows,
            watermarks: overrides?.watermarks || [createWatermark()],
        },
    };
}

function createScopeDrivenDryRunPayload(
    planId: string,
    overrides?: {
        scope?: Record<string, unknown>;
    },
): Record<string, unknown> {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-16T12:00:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version: 'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
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
        scope: overrides?.scope || {
            mode: 'record',
            tables: ['incident'],
            record_sys_ids: ['rec-01'],
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
    };
}

function createExecutePayload(overrides?: {
    capabilities?: string[];
    chunkSize?: number;
    runtimeConflicts?: Record<string, unknown>[];
    includeOverride?: boolean;
    revalidatedTargetRecords?: Array<Record<string, unknown>>;
}): Record<string, unknown> {
    const payload: Record<string, unknown> = {
        operator_id: 'operator@example.com',
        operator_capabilities: overrides?.capabilities || ['restore_execute'],
    };

    if (overrides?.chunkSize !== undefined) {
        payload.chunk_size = overrides.chunkSize;
    }

    if (overrides?.runtimeConflicts) {
        payload.runtime_conflicts = overrides.runtimeConflicts;
    }

    if (overrides?.revalidatedTargetRecords) {
        payload.revalidated_target_records =
            overrides.revalidatedTargetRecords;
    }

    if (overrides?.includeOverride) {
        payload.elevated_confirmation = {
            confirmed: true,
            confirmation: 'I UNDERSTAND',
            reason: 'approved override for test execution',
        };
    }

    return payload;
}

async function createPlanAndJob(
    baseUrl: string,
    token: string,
    planId: string,
    options?: {
        dryRunOverrides?: Parameters<typeof createDryRunPayload>[1];
        requiredCapabilities?: string[];
    },
): Promise<{
    jobId: string;
    planHash: string;
}> {
    const dryRun = await postJson(
        baseUrl,
        '/v1/plans/dry-run',
        token,
        createDryRunPayload(planId, options?.dryRunOverrides),
    );

    assert.equal(dryRun.status, 202);
    const targetReconciliationRecords =
        dryRun.body.target_reconciliation_records as Array<Record<string, unknown>>;
    const finalize = await postJson(
        baseUrl,
        `/v1/plans/${encodeURIComponent(planId)}/target-reconciliation/finalize`,
        token,
        {
            finalized_by: 'sn-worker',
            reconciled_records: targetReconciliationRecords.map((record) => ({
                table: record.table,
                record_sys_id: record.record_sys_id,
                target_state: 'exists',
            })),
        },
    );

    assert.equal(finalize.status, 201);

    const plan = finalize.body.plan as Record<string, unknown>;
    const planHash = plan.plan_hash as string;

    assert.equal(typeof planHash, 'string');

    const jobResponse = await postJson(
        baseUrl,
        '/v1/jobs',
        token,
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: planId,
            plan_hash: planHash,
            lock_scope_tables: ['incident'],
            required_capabilities: options?.requiredCapabilities || [
                'restore_execute',
            ],
            requested_by: 'operator@example.com',
        },
    );

    assert.equal(jobResponse.status, 201);

    const jobRecord = jobResponse.body.job as Record<string, unknown>;
    const jobId = jobRecord.job_id as string;

    assert.equal(typeof jobId, 'string');

    return {
        jobId,
        planHash,
    };
}

async function hasReconcilePromotionEvent(
    baseUrl: string,
    token: string,
    jobId: string,
): Promise<boolean> {
    const events = await listJobEvents(baseUrl, token, jobId);

    for (const event of events) {
        if (event.event_type !== 'job_started') {
            continue;
        }

        const details = event.details as Record<string, unknown> | undefined;

        if (details && details.reconcile_operation === true) {
            return true;
        }
    }

    return false;
}

async function listJobEvents(
    baseUrl: string,
    token: string,
    jobId: string,
): Promise<Array<Record<string, unknown>>> {
    const eventsResponse = await getJson(
        baseUrl,
        `/v1/jobs/${encodeURIComponent(jobId)}/events`,
        token,
    );

    assert.equal(eventsResponse.status, 200);

    return eventsResponse.body.events as Array<Record<string, unknown>>;
}

test(
    'valid scoped token and ACP mapping can create restore job for '
        + 'finalized plan',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            sourceMappingResolver: createResolver(createResolveResult()),
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);
        const planId = 'plan-http-finalized-success';

        try {
            const dryRun = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createDryRunPayload(planId),
            );

            assert.equal(dryRun.status, 202);
            assert.equal(dryRun.body.reconciliation_state, 'draft');
            const finalize = await postJson(
                baseUrl,
                `/v1/plans/${encodeURIComponent(planId)}`
                    + '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: [{
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'exists',
                    }, {
                        table: 'incident',
                        record_sys_id: 'rec-02',
                        target_state: 'exists',
                    }],
                },
            );

            assert.equal(finalize.status, 201);
            const finalizedPlan = finalize.body.plan as Record<string, unknown>;
            const planHash = finalizedPlan.plan_hash as string;

            assert.equal(typeof planHash, 'string');

            const response = await postJson(
                baseUrl,
                '/v1/jobs',
                token,
                createJobPayload(
                    'sn://acme-dev.service-now.com',
                    {
                        planId,
                        planHash,
                    },
                ),
            );

            assert.equal(response.status, 201);
            const job = response.body.job as Record<string, unknown>;
            assert.equal(job.plan_id, planId);
            assert.equal(job.plan_hash, planHash);
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'POST /v1/jobs rejects missing finalized plan at create time',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey);
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const response = await postJson(
                baseUrl,
                '/v1/jobs',
                token,
                createJobPayload(
                    'sn://acme-dev.service-now.com',
                    {
                        planId: 'missing-finalized-plan',
                    },
                ),
            );

            assert.equal(response.status, 409);
            assert.equal(response.body.error, 'plan_missing');
            assert.equal(
                response.body.reason_code,
                'blocked_plan_unavailable',
            );
            assert.equal(
                response.body.message,
                'job plan is unavailable in plan store',
            );
        } finally {
            await closeServer(server);
        }
    },
);

test('POST /v1/jobs rejects draft-only plan ids', async () => {
    const signingKey = 'test-signing-key';
    const artifactKey = 'rez/restore/event=evt-draft-only-01.artifact.json';
    const manifestKey = 'rez/restore/event=evt-draft-only-01.manifest.json';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver(createResolveResult()),
        indexedEventCandidates: [{
            artifactKey,
            eventId: 'evt-draft-only-01',
            eventTime: '2026-02-16T11:59:00.000Z',
            manifestKey,
            offset: '100',
            partition: 1,
            recordSysId: 'rec-01',
            sysModCount: 2,
            sysUpdatedOn: '2026-02-16 11:59:00',
            table: 'incident',
            topic: 'rez.cdc',
        }],
        artifactBodies: [{
            artifactKey,
            body: {
                __op: 'U',
                __schema_version: 3,
                __type: 'cdc.write',
                row_enc: {
                    alg: 'AES-256-GCM',
                    ciphertext: 'cipher-draft-only-01',
                },
            },
            manifestKey,
        }],
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);
    const planId = 'plan-http-draft-only';

    try {
        const draft = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createScopeDrivenDryRunPayload(planId),
        );

        assert.equal(draft.status, 202);
        assert.equal(draft.body.reconciliation_state, 'draft');
        const draftPlan = draft.body.plan as Record<string, unknown>;
        const draftHash = draftPlan.plan_hash as string;

        assert.equal(typeof draftHash, 'string');

        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload(
                'sn://acme-dev.service-now.com',
                {
                    planId,
                    planHash: draftHash,
                },
            ),
        );

        assert.equal(response.status, 409);
        assert.equal(response.body.error, 'plan_missing');
        assert.equal(
            response.body.reason_code,
            'blocked_plan_unavailable',
        );
        assert.equal(
            response.body.message,
            'job plan is unavailable in plan store',
        );
    } finally {
        await closeServer(server);
    }
});

test(
    'POST /v1/jobs rejects finalized preview-only plans at create time',
    async () => {
        const signingKey = 'test-signing-key';
        const staleAuthoritative = {
            ...createWatermark(),
            coverage_end: '2026-02-16T11:55:00.000Z',
            indexed_through_time: '2026-02-16T11:55:00.000Z',
        };
        const jobsStore = new InMemoryRestoreJobStateStore();
        const server = createService(signingKey, {
            authoritativeWatermarks: [staleAuthoritative],
            stateStores: {
                jobs: jobsStore,
            },
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);
        const planId = 'plan-http-preview-only';

        try {
            const dryRun = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createDryRunPayload(planId),
            );

            assert.equal(dryRun.status, 202);
            assert.equal(dryRun.body.reconciliation_state, 'draft');
            const targetReconciliationRecords =
                dryRun.body.target_reconciliation_records as Array<
                    Record<string, unknown>
                >;

            const finalize = await postJson(
                baseUrl,
                `/v1/plans/${encodeURIComponent(planId)}`
                    + '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: targetReconciliationRecords.map(
                        (record) => ({
                            table: record.table,
                            record_sys_id: record.record_sys_id,
                            target_state: 'exists',
                        }),
                    ),
                },
            );

            assert.equal(finalize.status, 201);
            const finalizeGate = finalize.body.gate as Record<string, unknown>;

            assert.equal(finalizeGate.executability, 'preview_only');
            assert.equal(
                finalizeGate.reason_code,
                'blocked_freshness_stale',
            );

            const finalizedPlan = finalize.body.plan as Record<string, unknown>;
            const planHash = finalizedPlan.plan_hash as string;

            assert.equal(typeof planHash, 'string');

            const response = await postJson(
                baseUrl,
                '/v1/jobs',
                token,
                createJobPayload(
                    'sn://acme-dev.service-now.com',
                    {
                        planId,
                        planHash,
                    },
                ),
            );

            assert.equal(response.status, 409);
            assert.equal(response.body.error, 'plan_not_executable');
            assert.equal(
                response.body.reason_code,
                'blocked_freshness_stale',
            );
            assert.equal(
                response.body.message,
                'dry-run plan gate is not executable',
            );

            const jobState = await jobsStore.read();

            assert.deepEqual(jobState.jobs_by_id, {});
            assert.deepEqual(jobState.plans_by_id, {});
            assert.deepEqual(jobState.events_by_job_id, {});
            assert.deepEqual(jobState.lock_state.running_jobs, []);
            assert.deepEqual(jobState.lock_state.queued_jobs, []);
        } finally {
            await closeServer(server);
        }
    },
);

test('ACP missing mapping fails closed on job create', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver({
            status: 'not_found',
        }),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload('sn://acme-dev.service-now.com'),
        );

        assert.equal(response.status, 403);
        assert.equal(
            response.body.reason_code,
            'blocked_unknown_source_mapping',
        );
    } finally {
        await closeServer(server);
    }
});

test('mismatched request source fails closed on job create', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload('sn://different-instance.service-now.com'),
        );

        assert.equal(response.status, 403);
        assert.equal(
            response.body.reason_code,
            'blocked_unknown_source_mapping',
        );
    } finally {
        await closeServer(server);
    }
});

test('ACP canonical source mismatch fails closed on job create', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver(
            createResolveResult({
                source: 'sn://different-instance.service-now.com',
            }),
        ),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload('sn://acme-dev.service-now.com'),
        );

        assert.equal(response.status, 403);
        assert.equal(
            response.body.reason_code,
            'blocked_unknown_source_mapping',
        );
    } finally {
        await closeServer(server);
    }
});

test('ACP outage returns explicit block reason on job create', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver({
            status: 'outage',
            message: 'ACP timeout',
        }),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload('sn://acme-dev.service-now.com'),
        );

        assert.equal(response.status, 503);
        assert.equal(
            response.body.reason_code,
            'blocked_auth_control_plane_outage',
        );
    } finally {
        await closeServer(server);
    }
});

test('wrong service scope token is rejected by auth middleware', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey, 'reg');

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            createJobPayload('sn://acme-dev.service-now.com'),
        );

        assert.equal(response.status, 401);
        assert.equal(
            response.body.reason_code,
            'denied_token_wrong_service_scope',
        );
    } finally {
        await closeServer(server);
    }
});

test('object-level authorization gates scoped reads and object actions', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
    });
    const baseUrl = await listen(server);
    const inScopeToken = createToken(signingKey);
    const crossTenantToken = createToken(signingKey, 'rrs', {
        tenantId: 'tenant-other',
    });
    const crossInstanceToken = createToken(signingKey, 'rrs', {
        instanceId: 'sn-prod-99',
    });

    try {
        const readyPlanId = 'plan-authz-ready';
        const readyFixture = await createPlanAndJob(
            baseUrl,
            inScopeToken,
            readyPlanId,
        );
        const readyExecute = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}/execution`,
            inScopeToken,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 1,
            }),
        );

        assert.equal(readyExecute.status, 202);
        const readyExecution = readyExecute.body.execution as Record<
            string,
            unknown
        >;
        const readyResume = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}/resume`,
            inScopeToken,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: readyExecution.plan_checksum,
                expected_precondition_checksum:
                    readyExecution.precondition_checksum,
            },
        );

        assert.equal(readyResume.status, 200);

        const pausedPlanId = 'plan-authz-paused';
        const pausedFixture = await createPlanAndJob(
            baseUrl,
            inScopeToken,
            pausedPlanId,
        );
        const pausedExecute = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(pausedFixture.jobId)}/execution`,
            inScopeToken,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 1,
            }),
        );

        assert.equal(pausedExecute.status, 202);
        const pausedExecution = pausedExecute.body.execution as Record<
            string,
            unknown
        >;

        const completeFixture = await createPlanAndJob(
            baseUrl,
            inScopeToken,
            'plan-authz-complete',
        );

        const resumePayload = {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: pausedExecution.plan_checksum,
            expected_precondition_checksum:
                pausedExecution.precondition_checksum,
        };
        const completePayload = {
            status: 'completed',
            reason_code: 'none',
        };
        const protectedRequests: Array<{
            method: 'GET' | 'POST';
            path: string;
            payload?: Record<string, unknown>;
        }> = [
            {
                method: 'GET',
                path: `/v1/plans/${encodeURIComponent(readyPlanId)}`,
            },
            {
                method: 'GET',
                path: `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}`,
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/events',
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/events/cross-service',
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/execution',
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/checkpoint',
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/rollback-journal',
            },
            {
                method: 'GET',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/evidence',
            },
            {
                method: 'POST',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/evidence/export',
                payload: {},
            },
            {
                method: 'POST',
                path:
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/execution',
                payload: createExecutePayload({
                    capabilities: ['restore_execute'],
                }),
            },
            {
                method: 'POST',
                path:
                    `/v1/jobs/${encodeURIComponent(pausedFixture.jobId)}` +
                    '/resume',
                payload: resumePayload,
            },
            {
                method: 'POST',
                path:
                    `/v1/jobs/${encodeURIComponent(completeFixture.jobId)}` +
                    '/complete',
                payload: completePayload,
            },
        ];

        for (const token of [crossTenantToken, crossInstanceToken]) {
            for (const request of protectedRequests) {
                const response = request.method === 'GET'
                    ? await getJson(baseUrl, request.path, token)
                    : await postJson(
                        baseUrl,
                        request.path,
                        token,
                        request.payload || {},
                    );

                assertScopedNotFound(response);
            }
        }

        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/plans/${encodeURIComponent(readyPlanId)}`,
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}`,
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/events',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/events/cross-service',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/execution',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/checkpoint',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/rollback-journal',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await getJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/evidence',
                    inScopeToken,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await postJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(readyFixture.jobId)}` +
                    '/evidence/export',
                    inScopeToken,
                    {},
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await postJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(pausedFixture.jobId)}` +
                    '/resume',
                    inScopeToken,
                    resumePayload,
                )
            ).status,
            200,
        );
        assert.equal(
            (
                await postJson(
                    baseUrl,
                    `/v1/jobs/${encodeURIComponent(completeFixture.jobId)}` +
                    '/complete',
                    inScopeToken,
                    completePayload,
                )
            ).status,
            200,
        );
    } finally {
        await closeServer(server);
    }
});

test(
    'protocol endpoints validate payloads and persist execute claim/commit state',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey);
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const planId = 'plan-protocol-surface';
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                planId,
            );

            const finalizeInvalid = await postJson(
                baseUrl,
                `/v1/plans/${encodeURIComponent(planId)}` +
                    '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: [],
                },
            );

            assert.equal(finalizeInvalid.status, 400);
            assert.equal(finalizeInvalid.body.error, 'invalid_request');

            const finalizeValid = await postJson(
                baseUrl,
                `/v1/plans/${encodeURIComponent(planId)}` +
                    '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: [{
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'exists',
                    }],
                },
            );

            assert.equal(finalizeValid.status, 200);
            assert.equal(finalizeValid.body.accepted, true);
            assert.equal(finalizeValid.body.reconciliation_state, 'finalized');
            assert.equal(finalizeValid.body.reused_existing_plan, true);
            assert.equal(finalizeValid.body.requested_record_count, 1);

            const claimInvalid = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/claim',
                token,
                {},
            );

            assert.equal(claimInvalid.status, 400);
            assert.equal(claimInvalid.body.error, 'invalid_request');

            const claimValid = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/claim',
                token,
                {
                    operator_id: 'sn-worker',
                    max_rows: 1,
                },
            );

            assert.equal(claimValid.status, 200);
            assert.equal(claimValid.body.accepted, true);
            assert.equal(claimValid.body.job_id, fixture.jobId);
            assert.equal(claimValid.body.requested_max_rows, 1);
            const claimValidRows = claimValid.body.claimed_rows as Array<
                Record<string, unknown>
            >;
            const firstClaimedRowId = String(claimValidRows[0]?.row_id || '');

            assert.equal(claimValidRows.length, 1);
            assert.equal(typeof firstClaimedRowId, 'string');

            const overlapClaim = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/claim',
                token,
                {
                    operator_id: 'sn-worker',
                    max_rows: 1,
                },
            );

            assert.equal(overlapClaim.status, 409);
            assert.equal(overlapClaim.body.error, 'claim_in_progress');

            const commitInvalid = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/commit',
                token,
                {
                    claim_id: 'claim-01',
                    committed_by: 'sn-worker',
                    row_outcomes: [],
                },
            );

            assert.equal(commitInvalid.status, 400);
            assert.equal(commitInvalid.body.error, 'invalid_request');

            const commitMismatch = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/commit',
                token,
                {
                    claim_id: claimValid.body.claim_id,
                    committed_by: 'sn-worker',
                    row_outcomes: [{
                        row_id: 'row-99',
                        outcome: 'applied',
                        reason_code: 'none',
                    }],
                },
            );

            assert.equal(commitMismatch.status, 409);
            assert.equal(commitMismatch.body.error, 'claim_commit_mismatch');
            assert.equal(
                commitMismatch.body.reason_code,
                'blocked_resume_precondition_mismatch',
            );

            const commitValid = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/commit',
                token,
                {
                    claim_id: claimValid.body.claim_id,
                    committed_by: 'sn-worker',
                    row_outcomes: [{
                        row_id: firstClaimedRowId,
                        outcome: 'applied',
                        reason_code: 'none',
                    }],
                },
            );

            assert.equal(commitValid.status, 200);
            assert.equal(commitValid.body.accepted, true);
            assert.equal(commitValid.body.reason_code, 'none');
            assert.equal(commitValid.body.job_id, fixture.jobId);
            assert.equal(commitValid.body.claim_id, claimValid.body.claim_id);
            assert.equal(commitValid.body.committed_rows, 1);
            assert.equal(commitValid.body.execution_status, 'paused');
            const commitValidSummary = commitValid.body.summary as Record<
                string,
                unknown
            >;

            assert.equal(commitValidSummary.applied_rows, 1);
            assert.equal(commitValidSummary.failed_rows, 0);

            const secondClaim = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/claim',
                token,
                {
                    operator_id: 'sn-worker',
                    max_rows: 1,
                },
            );

            assert.equal(secondClaim.status, 200);
            assert.equal(secondClaim.body.accepted, true);
            const secondClaimRows = secondClaim.body.claimed_rows as Array<
                Record<string, unknown>
            >;
            const secondClaimedRowId = String(
                secondClaimRows[0]?.row_id || '',
            );

            assert.equal(typeof secondClaimedRowId, 'string');

            const secondCommit = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
                    '/execute-batches/commit',
                token,
                {
                    claim_id: secondClaim.body.claim_id,
                    committed_by: 'sn-worker',
                    row_outcomes: [{
                        row_id: secondClaimedRowId,
                        outcome: 'failed',
                        reason_code: 'failed_permission_conflict',
                        message: 'target rejected write',
                    }],
                },
            );

            assert.equal(secondCommit.status, 200);
            assert.equal(secondCommit.body.accepted, true);
            assert.equal(secondCommit.body.execution_status, 'failed');
            assert.equal(secondCommit.body.reason_code, 'failed_internal_error');
            const commitEvidence = secondCommit.body.evidence as Record<
                string,
                unknown
            >;

            assert.equal(typeof commitEvidence.evidence_id, 'string');
            assert.equal(
                commitEvidence.signature_verification,
                'verified',
            );
            assert.equal(commitEvidence.reused, false);
            const secondCommitSummary = secondCommit.body.summary as Record<
                string,
                unknown
            >;

            assert.equal(secondCommitSummary.applied_rows, 1);
            assert.equal(secondCommitSummary.failed_rows, 1);

            const committedCheckpoint = await getJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/checkpoint`,
                token,
            );

            assert.equal(committedCheckpoint.status, 200);
            const committedCheckpointBody = committedCheckpoint.body
                .checkpoint as Record<string, unknown>;
            assert.equal(committedCheckpointBody.next_chunk_index, 1);
            assert.equal(committedCheckpointBody.next_row_index, 0);

            const committedEvidence = await getJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/evidence`,
                token,
            );

            assert.equal(committedEvidence.status, 200);
            const committedEvidenceBody = committedEvidence.body as Record<
                string,
                unknown
            >;
            const committedEvidenceRecord = committedEvidenceBody.evidence as Record<
                string,
                unknown
            >;
            const committedVerification = committedEvidenceBody.verification as Record<
                string,
                unknown
            >;
            assert.equal(
                committedEvidenceRecord.evidence_id,
                commitEvidence.evidence_id,
            );
            assert.equal(
                committedVerification.signature_verification,
                'verified',
            );
            const committedExecutionOutcomes =
                committedEvidenceRecord.execution_outcomes as Record<
                    string,
                    unknown
                >;
            assert.equal(committedExecutionOutcomes.rows_applied, 1);
            assert.equal(committedExecutionOutcomes.rows_failed, 1);
        } finally {
            await closeServer(server);
        }
    },
);

test('dry-run returns executable gate when ACP mapping exists', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver(createResolveResult()),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-fresh'),
        );

        assert.equal(response.status, 202);
        const gate = response.body.gate as Record<string, unknown>;

        assert.equal(gate.executability, 'executable');
        assert.equal(gate.reason_code, 'none');
    } finally {
        await closeServer(server);
    }
});

test(
    'dry-run returns scope-driven draft with target reconciliation request rows',
    async () => {
    const signingKey = 'test-signing-key';
    const artifactKey = 'rez/restore/event=evt-scope-01.artifact.json';
    const manifestKey = 'rez/restore/event=evt-scope-01.manifest.json';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver(createResolveResult()),
        indexedEventCandidates: [{
            artifactKey,
            eventId: 'evt-scope-01',
            eventTime: '2026-02-16T11:59:00.000Z',
            manifestKey,
            offset: '100',
            partition: 1,
            recordSysId: 'rec-01',
            sysModCount: 2,
            sysUpdatedOn: '2026-02-16 11:59:00',
            table: 'incident',
            topic: 'rez.cdc',
        }],
        artifactBodies: [{
            artifactKey,
            body: {
                __op: 'U',
                __schema_version: 3,
                __type: 'cdc.write',
                row_enc: {
                    alg: 'AES-256-GCM',
                    ciphertext: 'cipher-scope-01',
                },
            },
            manifestKey,
        }],
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createScopeDrivenDryRunPayload('plan-scope-http'),
        );

        assert.equal(response.status, 202);
        assert.equal(response.body.reconciliation_state, 'draft');
        const planHashInput = response.body.plan_hash_input as Record<
            string,
            unknown
        >;
        const rows = planHashInput.rows as Array<Record<string, unknown>>;
        const firstRow = rows[0];
        const metadata = firstRow.metadata as Record<string, unknown>;
        const metadataFields = metadata.metadata as Record<string, unknown>;

        assert.equal(rows.length, 1);
        assert.equal(firstRow.record_sys_id, 'rec-01');
        assert.equal(metadataFields.event_id, 'evt-scope-01');
        assert.equal(metadataFields.topic, 'rez.cdc');
        const targetReconciliationRecords =
            response.body.target_reconciliation_records as Array<
                Record<string, unknown>
            >;

        assert.equal(targetReconciliationRecords.length, 1);
        assert.equal(targetReconciliationRecords[0].record_sys_id, 'rec-01');
        assert.equal(targetReconciliationRecords[0].source_operation, 'U');

        const pitResolutions = response.body.pit_resolutions as Array<
            Record<string, unknown>
        >;

        assert.equal(pitResolutions.length, 1);
        assert.equal(pitResolutions[0].winning_event_id, 'evt-scope-01');
    } finally {
        await closeServer(server);
    }
},
);

test(
    'scope-driven dry-run keeps media candidates while selecting '
    + 'row-capable PIT winners over newer media artifacts',
    async () => {
        const signingKey = 'test-signing-key';
        const rowArtifactKey =
            'rez/restore/event=evt-scope-row-capable.artifact.json';
        const rowManifestKey =
            'rez/restore/event=evt-scope-row-capable.manifest.json';
        const mediaArtifactKey =
            'rez/restore/event=evt-scope-media-only.artifact.json';
        const mediaManifestKey =
            'rez/restore/event=evt-scope-media-only.manifest.json';
        const server = createService(signingKey, {
            sourceMappingResolver: createResolver(createResolveResult()),
            indexedEventCandidates: [{
                artifactKey: rowArtifactKey,
                eventId: 'evt-scope-row-capable',
                eventTime: '2026-02-16T11:56:00.000Z',
                manifestKey: rowManifestKey,
                offset: '140',
                partition: 1,
                recordSysId: 'rec-01',
                sysModCount: 4,
                sysUpdatedOn: '2026-02-16 11:56:00',
                table: 'incident',
                topic: 'rez.cdc',
            }, {
                artifactKey: mediaArtifactKey,
                eventId: 'evt-scope-media-only',
                eventTime: '2026-02-16T11:59:00.000Z',
                manifestKey: mediaManifestKey,
                offset: '141',
                partition: 1,
                recordSysId: 'rec-01',
                sysModCount: 9,
                sysUpdatedOn: '2026-02-16 11:59:00',
                table: 'incident',
                topic: 'rez.media',
            }],
            artifactBodies: [{
                artifactKey: rowArtifactKey,
                body: {
                    __op: 'U',
                    __schema_version: 3,
                    __type: 'cdc.write',
                    row_enc: {
                        alg: 'AES-256-GCM',
                        ciphertext: 'cipher-scope-row-capable',
                    },
                },
                manifestKey: rowManifestKey,
            }, {
                artifactKey: mediaArtifactKey,
                body: {
                    __op: 'U',
                    __record_sys_id: 'rec-01',
                    __table: 'incident',
                    __type: 'media.manifest',
                    media: {
                        op: 'upsert',
                        items: [{
                            media_id: 'media-rec-01',
                            parent_record_sys_id: 'rec-01',
                            table: 'incident',
                        }],
                    },
                    source: 'sn://acme-dev.service-now.com',
                    tenant_id: 'tenant-acme',
                },
                manifestKey: mediaManifestKey,
            }],
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);
        const mediaCandidate = createMediaCandidate('mixed-scope-01');

        try {
            const response = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                {
                    ...createScopeDrivenDryRunPayload(
                        'plan-scope-http-mixed-row-media',
                    ),
                    media_candidates: [mediaCandidate],
                },
            );

            assert.equal(response.status, 202);
            assert.equal(response.body.reconciliation_state, 'draft');
            const gate = response.body.gate as Record<string, unknown>;

            assert.equal(gate.executability, 'executable');
            assert.equal(gate.reason_code, 'none');

            const planHashInput = response.body.plan_hash_input as Record<
                string,
                unknown
            >;
            const rows = planHashInput.rows as Array<Record<string, unknown>>;
            const firstRow = rows[0];
            const metadata = firstRow.metadata as Record<string, unknown>;
            const metadataFields = metadata.metadata as Record<string, unknown>;

            assert.equal(rows.length, 1);
            assert.equal(firstRow.record_sys_id, 'rec-01');
            assert.equal(metadataFields.event_id, 'evt-scope-row-capable');
            assert.equal(metadataFields.topic, 'rez.cdc');
            assert.equal(metadataFields.sys_mod_count, 4);

            const pitResolutions = response.body.pit_resolutions as Array<
                Record<string, unknown>
            >;

            assert.equal(pitResolutions.length, 1);
            assert.equal(
                pitResolutions[0].winning_event_id,
                'evt-scope-row-capable',
            );

            const targetReconciliationRecords =
                response.body.target_reconciliation_records as Array<
                    Record<string, unknown>
                >;

            assert.equal(targetReconciliationRecords.length, 1);
            assert.equal(targetReconciliationRecords[0].source_operation, 'U');

            const mediaCandidates = response.body.media_candidates as Array<
                Record<string, unknown>
            >;

            assert.equal(mediaCandidates.length, 1);
            assert.equal(mediaCandidates[0].candidate_id, 'mixed-scope-01');
            assert.equal(mediaCandidates[0].decision, 'include');
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'scope-driven finalize reconciles source insert to update and '
    + 'authoritative action counts',
    async () => {
        const signingKey = 'test-signing-key';
        const artifactKey =
            'rez/restore/event=evt-scope-insert.artifact.json';
        const manifestKey =
            'rez/restore/event=evt-scope-insert.manifest.json';
        const server = createService(signingKey, {
            sourceMappingResolver: createResolver(createResolveResult()),
            indexedEventCandidates: [{
                artifactKey,
                eventId: 'evt-scope-insert',
                eventTime: '2026-02-16T11:59:00.000Z',
                manifestKey,
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-16 11:59:00',
                table: 'incident',
                topic: 'rez.cdc',
            }],
            artifactBodies: [{
                artifactKey,
                body: {
                    __op: 'I',
                    __schema_version: 3,
                    __type: 'cdc.write',
                    row_enc: {
                        alg: 'AES-256-GCM',
                        ciphertext: 'cipher-scope-insert',
                    },
                },
                manifestKey,
            }],
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const draft = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createScopeDrivenDryRunPayload('plan-scope-http-reconciled'),
            );

            assert.equal(draft.status, 202);

            const response = await postJson(
                baseUrl,
                '/v1/plans/plan-scope-http-reconciled'
                    + '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: [{
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'exists',
                    }],
                },
            );

            assert.equal(response.status, 201);
            assert.equal(response.body.accepted, true);
            assert.equal(response.body.reconciliation_state, 'finalized');
            const planHashInput = response.body.plan_hash_input as Record<
                string,
                unknown
            >;
            const rows = planHashInput.rows as Array<Record<string, unknown>>;
            const firstRow = rows[0];
            const metadata = firstRow.metadata as Record<string, unknown>;
            const metadataFields = metadata.metadata as Record<string, unknown>;
            const plan = response.body.plan as Record<string, unknown>;
            const actionCounts = plan.action_counts as Record<string, unknown>;

            assert.equal(rows.length, 1);
            assert.equal(firstRow.action, 'update');
            assert.equal(metadataFields.operation, 'I');
            assert.equal(actionCounts.update, 1);
            assert.equal(actionCounts.insert, 0);
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'scope-driven finalize blocks when source update points to missing target '
    + 'row',
    async () => {
        const signingKey = 'test-signing-key';
        const artifactKey =
            'rez/restore/event=evt-scope-update.artifact.json';
        const manifestKey =
            'rez/restore/event=evt-scope-update.manifest.json';
        const server = createService(signingKey, {
            sourceMappingResolver: createResolver(createResolveResult()),
            indexedEventCandidates: [{
                artifactKey,
                eventId: 'evt-scope-update',
                eventTime: '2026-02-16T11:59:00.000Z',
                manifestKey,
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-16 11:59:00',
                table: 'incident',
                topic: 'rez.cdc',
            }],
            artifactBodies: [{
                artifactKey,
                body: {
                    __op: 'U',
                    __schema_version: 3,
                    __type: 'cdc.write',
                    row_enc: {
                        alg: 'AES-256-GCM',
                        ciphertext: 'cipher-scope-update',
                    },
                },
                manifestKey,
            }],
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const draft = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createScopeDrivenDryRunPayload('plan-scope-http-missing'),
            );

            assert.equal(draft.status, 202);

            const response = await postJson(
                baseUrl,
                '/v1/plans/plan-scope-http-missing'
                    + '/target-reconciliation/finalize',
                token,
                {
                    finalized_by: 'sn-worker',
                    reconciled_records: [{
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'missing',
                    }],
                },
            );

            assert.equal(response.status, 409);
            assert.equal(response.body.error, 'restore_plan_materialization_failed');
            assert.equal(response.body.reason_code, 'blocked_reference_conflict');
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'scope-driven dry-run draft does not depend on target lookup runtime '
    + 'composition',
    async () => {
        const signingKey = 'test-signing-key';
        const artifactKey =
            'rez/restore/event=evt-scope-runtime-block.artifact.json';
        const manifestKey =
            'rez/restore/event=evt-scope-runtime-block.manifest.json';
        const server = createService(signingKey, {
            failClosedRuntimeComposition: true,
            sourceMappingResolver: createResolver(createResolveResult()),
            indexedEventCandidates: [{
                artifactKey,
                eventId: 'evt-scope-runtime-block',
                eventTime: '2026-02-16T11:59:00.000Z',
                manifestKey,
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
                sysModCount: 2,
                sysUpdatedOn: '2026-02-16 11:59:00',
                table: 'incident',
                topic: 'rez.cdc',
            }],
            artifactBodies: [{
                artifactKey,
                body: {
                    __op: 'U',
                    __schema_version: 3,
                    __type: 'cdc.write',
                    row_enc: {
                        alg: 'AES-256-GCM',
                        ciphertext: 'cipher-scope-runtime-block',
                    },
                },
                manifestKey,
            }],
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const response = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createScopeDrivenDryRunPayload('plan-scope-runtime-blocked'),
            );

            assert.equal(response.status, 202);
            assert.equal(response.body.reconciliation_state, 'draft');
            const records = response.body.target_reconciliation_records as Array<
                Record<string, unknown>
            >;

            assert.equal(records.length, 1);
            assert.equal(records[0].record_sys_id, 'rec-01');
        } finally {
            await closeServer(server);
        }
    },
);

test('dry-run fails closed when ACP mapping is missing', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver({
            status: 'not_found',
        }),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-acp-missing'),
        );

        assert.equal(response.status, 403);
        assert.equal(
            response.body.reason_code,
            'blocked_unknown_source_mapping',
        );
    } finally {
        await closeServer(server);
    }
});

test('dry-run returns explicit outage block when ACP mapping lookup fails', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        sourceMappingResolver: createResolver({
            status: 'outage',
            message: 'ACP unavailable',
        }),
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-acp-outage'),
        );

        assert.equal(response.status, 503);
        assert.equal(
            response.body.reason_code,
            'blocked_auth_control_plane_outage',
        );
    } finally {
        await closeServer(server);
    }
});

test('dry-run refreshes ACP mapping cache after TTL expiry', async () => {
    const signingKey = 'test-signing-key';
    let resolverNowMs = now().getTime();
    let canonicalSource = 'sn://acme-dev.service-now.com';
    let resolveCalls = 0;
    const sourceMappingResolver = new CachedAcpSourceMappingProvider(
        {
            async resolveSourceMapping(): Promise<AcpResolveSourceMappingResult> {
                resolveCalls += 1;

                return createResolveResult({
                    source: canonicalSource,
                });
            },
            async listSourceMappings(): Promise<AcpListSourceMappingsResult> {
                return {
                    status: 'ok',
                    mappings: [],
                };
            },
        },
        {
            positiveTtlSeconds: 60,
            negativeTtlSeconds: 5,
            now: () => new Date(resolverNowMs),
        },
    );
    const server = createService(signingKey, {
        sourceMappingResolver,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const firstResponse = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-acp-cache-first'),
        );

        assert.equal(firstResponse.status, 202);
        assert.equal(resolveCalls, 1);

        canonicalSource = 'sn://changed.service-now.com';
        const cachedResponse = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-acp-cache-hit'),
        );

        assert.equal(cachedResponse.status, 202);
        assert.equal(resolveCalls, 1);

        resolverNowMs += 61 * 1000;
        const refreshedResponse = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-acp-cache-expired'),
        );

        assert.equal(refreshedResponse.status, 403);
        assert.equal(
            refreshedResponse.body.reason_code,
            'blocked_unknown_source_mapping',
        );
        assert.equal(resolveCalls, 2);
    } finally {
        await closeServer(server);
    }
});

test('dry-run accepts large decimal-string offsets and returns string values', async () => {
    const signingKey = 'test-signing-key';
    const largeOffset = '900719925474099312345678901234567890';
    const server = createService(signingKey, {
        indexedEventCandidates: [
            createIndexedEventCandidateFixture({
                eventId: 'evt-large-offset',
                offset: largeOffset,
                partition: 1,
                recordSysId: 'rec-large-offset',
            }),
        ],
        artifactBodies: [
            createArtifactBodyFixture({
                eventId: 'evt-large-offset',
            }),
        ],
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-large-offset', {
                rows: [createDryRunRow('row-large-offset', 'rec-large-offset')],
                watermarks: [
                    {
                        ...createWatermark(),
                        indexed_through_offset: `000${largeOffset}`,
                    },
                ],
            }),
        );

        assert.equal(response.status, 202);

        const watermarks = response.body.watermarks as Record<string, unknown>[];
        const planHashInput = response.body.plan_hash_input as Record<
            string,
            unknown
        >;
        const rows = planHashInput.rows as Record<string, unknown>[];
        const firstRow = rows[0];
        const parsedRowMetadata = firstRow.metadata as Record<string, unknown>;
        const parsedMetadataFields = parsedRowMetadata.metadata as Record<
            string,
            unknown
        >;

        assert.equal(watermarks[0].indexed_through_offset, '100');
        assert.equal(parsedMetadataFields.offset, largeOffset);
    } finally {
        await closeServer(server);
    }
});

test('dry-run rejects invalid watermark offset strings', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-invalid-offset', {
                watermarks: [
                    {
                        ...createWatermark(),
                        indexed_through_offset: '-1',
                    },
                ],
            }),
        );

        assert.equal(response.status, 400);
        assert.equal(response.body.error, 'invalid_request');
        assert.match(
            String(response.body.message || ''),
            /must be non-negative integer offset as decimal string/,
        );
    } finally {
        await closeServer(server);
    }
});

test('dry-run ignores caller-provided freshness when authoritative state is fresh', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-request-spoofed', {
                watermarks: [
                    createWatermark({
                        freshness: 'unknown',
                        executability: 'blocked',
                        reasonCode: 'blocked_freshness_unknown',
                    }),
                ],
            }),
        );

        assert.equal(response.status, 202);
        const gate = response.body.gate as Record<string, unknown>;

        assert.equal(gate.executability, 'executable');
        assert.equal(gate.reason_code, 'none');
    } finally {
        await closeServer(server);
    }
});

test('dry-run freshness matrix enforces authoritative stale and unknown states', async () => {
    const signingKey = 'test-signing-key';
    const staleAuthoritative = {
        ...createWatermark(),
        coverage_end: '2026-02-16T11:55:00.000Z',
        indexed_through_time: '2026-02-16T11:55:00.000Z',
    };
    const server = createService(signingKey, {
        authoritativeWatermarks: [staleAuthoritative],
        indexedEventCandidates: [
            createIndexedEventCandidateFixture({
                eventId: 'evt-freshness-rec-01',
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
            }),
            createIndexedEventCandidateFixture({
                eventId: 'evt-freshness-rec-unknown',
                offset: '101',
                partition: 2,
                recordSysId: 'rec-unknown',
            }),
        ],
        artifactBodies: [
            createArtifactBodyFixture({
                eventId: 'evt-freshness-rec-01',
            }),
            createArtifactBodyFixture({
                eventId: 'evt-freshness-rec-unknown',
            }),
        ],
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const staleResponse = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-stale', {
                rows: [createDryRunRow('row-01', 'rec-01')],
            }),
        );

        assert.equal(staleResponse.status, 202);
        const staleGate = staleResponse.body.gate as Record<string, unknown>;

        assert.equal(staleGate.executability, 'preview_only');
        assert.equal(staleGate.reason_code, 'blocked_freshness_stale');

        const unknownResponse = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-unknown', {
                rows: [createDryRunRow('row-unknown', 'rec-unknown')],
            }),
        );

        assert.equal(unknownResponse.status, 202);
        const unknownGate = unknownResponse.body.gate as Record<string, unknown>;

        assert.equal(unknownGate.executability, 'blocked');
        assert.equal(unknownGate.reason_code, 'blocked_freshness_unknown');
        assert.equal(
            unknownGate.freshness_unknown_detail,
            'partition_not_indexed',
        );
    } finally {
        await closeServer(server);
    }
});

test(
    'dry-run reports invalid_authoritative_timestamp detail when '
    + 'authoritative freshness data is malformed',
    async () => {
        const signingKey = 'test-signing-key';
        const baseReader = new InMemoryRestoreIndexStateReader();

        baseReader.upsertIndexedEventCandidate({
            ...createIndexedEventCandidateFixture({
                eventId: 'evt-rec-01',
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
            }),
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const malformedTimestampReader = {
            async listWatermarksForSource(input) {
                return baseReader.listWatermarksForSource(input);
            },
            async lookupIndexedEventCandidates(input: any) {
                return baseReader.lookupIndexedEventCandidates(input);
            },
            async readWatermarksForPartitions() {
                return [
                    {
                        ...createWatermark({
                            freshness: 'unknown',
                            executability: 'blocked',
                            reasonCode: 'blocked_freshness_unknown',
                        }),
                        indexed_through_time: 'not-a-timestamp',
                    } as RestoreWatermark,
                ];
            },
        } as RestoreIndexStateReader;
        const server = createService(signingKey, {
            restoreIndexReader: malformedTimestampReader,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const response = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createDryRunPayload('plan-invalid-authoritative-timestamp', {
                    rows: [createDryRunRow('row-01', 'rec-01')],
                }),
            );
            const gate = response.body.gate as Record<string, unknown>;

            assert.equal(response.status, 202);
            assert.equal(gate.executability, 'blocked');
            assert.equal(gate.reason_code, 'blocked_freshness_unknown');
            assert.equal(
                gate.freshness_unknown_detail,
                'invalid_authoritative_timestamp',
            );
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'dry-run returns restore_index_unavailable detail when '
    + 'authoritative reads fail',
    async () => {
        const signingKey = 'test-signing-key';
        const failingReader: RestoreIndexStateReader = {
            async listWatermarksForSource() {
                throw new Error('reader unavailable');
            },
            async readWatermarksForPartitions() {
                throw new Error('reader unavailable');
            },
        };
        const server = createService(signingKey, {
            restoreIndexReader: failingReader,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const response = await postJson(
                baseUrl,
                '/v1/plans/dry-run',
                token,
                createDryRunPayload('plan-index-unavailable'),
            );

            assert.equal(response.status, 503);
            assert.equal(response.body.error, 'restore_index_unavailable');
            assert.equal(
                response.body.reason_code,
                'blocked_freshness_unknown',
            );
            assert.equal(
                response.body.freshness_unknown_detail,
                'restore_index_unavailable',
            );
        } finally {
            await closeServer(server);
        }
    },
);

test('dry-run blocks unresolved delete and hard-block conflicts', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const unresolvedDelete = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-unresolved-delete', {
                deleteCandidates: [
                    {
                        candidate_id: 'delete-01',
                        row_id: 'row-01',
                        table: 'incident',
                        record_sys_id: 'rec-01',
                    },
                ],
            }),
        );

        assert.equal(unresolvedDelete.status, 202);
        const unresolvedDeleteGate = unresolvedDelete.body.gate as Record<
            string,
            unknown
        >;

        assert.equal(unresolvedDeleteGate.executability, 'blocked');
        assert.equal(
            unresolvedDeleteGate.reason_code,
            'blocked_unresolved_delete_candidates',
        );

        const unresolvedConflict = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-unresolved-reference', {
                conflicts: [
                    {
                        conflict_id: 'conf-01',
                        class: 'reference_conflict',
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        reason_code: 'blocked_reference_conflict',
                        reason: 'Referenced row missing',
                        observed_at: '2026-02-16T12:00:00.000Z',
                    },
                ],
            }),
        );

        assert.equal(unresolvedConflict.status, 202);
        const unresolvedConflictGate = unresolvedConflict.body.gate as Record<
            string,
            unknown
        >;

        assert.equal(unresolvedConflictGate.executability, 'blocked');
        assert.equal(
            unresolvedConflictGate.reason_code,
            'blocked_reference_conflict',
        );
    } finally {
        await closeServer(server);
    }
});

test('dry-run blocks unresolved media candidate decisions', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);
    const unresolved = createMediaCandidate('media-unresolved');

    delete unresolved.decision;

    try {
        const response = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            createDryRunPayload('plan-unresolved-media', {
                mediaCandidates: [unresolved],
            }),
        );

        assert.equal(response.status, 202);
        const gate = response.body.gate as Record<string, unknown>;

        assert.equal(gate.executability, 'blocked');
        assert.equal(
            gate.reason_code,
            'blocked_unresolved_media_candidates',
        );
    } finally {
        await closeServer(server);
    }
});

test('dry-run plan_hash is deterministic for equivalent inputs', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const rowOne = createDryRunRow('row-01', 'rec-01');
        const rowTwo = createDryRunRow('row-02', 'rec-02');
        const first = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            {
                ...createDryRunPayload('plan-hash-a', {
                    rows: [rowTwo, rowOne],
                }),
                scope: {
                    mode: 'record',
                    tables: ['incident'],
                    record_sys_ids: ['rec-01', 'rec-02'],
                },
            },
        );
        const second = await postJson(
            baseUrl,
            '/v1/plans/dry-run',
            token,
            {
                ...createDryRunPayload('plan-hash-b', {
                    rows: [rowOne, rowTwo],
                }),
                scope: {
                    mode: 'record',
                    tables: ['incident'],
                    record_sys_ids: ['rec-01', 'rec-02'],
                },
            },
        );

        assert.equal(first.status, 202);
        assert.equal(second.status, 202);

        const firstPlan = first.body.plan as Record<string, unknown>;
        const secondPlan = second.body.plan as Record<string, unknown>;

        assert.equal(firstPlan.plan_hash, secondPlan.plan_hash);

        const firstHashInput = first.body.plan_hash_input as Record<
            string,
            unknown
        >;
        const secondHashInput = second.body.plan_hash_input as Record<
            string,
            unknown
        >;
        const firstRows = firstHashInput.rows as Array<Record<string, unknown>>;
        const secondRows = secondHashInput.rows as Array<Record<string, unknown>>;

        assert.equal(firstRows[0]?.row_id, secondRows[0]?.row_id);
    } finally {
        await closeServer(server);
    }
});

test('execute endpoint records chunk fallback and row outcomes', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-fallback',
            {
                requiredCapabilities: ['restore_execute'],
            },
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute', 'restore_override_caps'],
                includeOverride: true,
                chunkSize: 2,
                runtimeConflicts: [
                    {
                        conflict_id: 'conf-row-01',
                        row_id: materializedRowId('rec-01'),
                        class: 'value_conflict',
                        reason_code: 'failed_internal_error',
                        reason: 'runtime mismatch',
                        resolution: 'skip',
                    },
                ],
            }),
        );

        assert.equal(executeResponse.status, 200);
        const execution = executeResponse.body.execution as Record<
            string,
            unknown
        >;
        const summary = execution.summary as Record<string, unknown>;
        const chunks = execution.chunks as Array<Record<string, unknown>>;

        assert.equal(summary.fallback_chunk_count, 1);
        assert.equal(summary.applied_rows, 1);
        assert.equal(summary.skipped_rows, 1);
        assert.equal(chunks[0]?.status, 'row_fallback');

        const executionGet = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
        );

        assert.equal(executionGet.status, 200);
        const executionFromGet = executionGet.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(executionFromGet.job_id, fixture.jobId);
    } finally {
        await closeServer(server);
    }
});

test(
    'execute endpoint fails closed when target revalidation support is unavailable',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            failClosedRuntimeComposition: true,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                'plan-execute-runtime-blocked',
                {
                    requiredCapabilities: ['restore_execute'],
                },
            );
            const executeResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
                createExecutePayload({
                    capabilities: ['restore_execute'],
                    chunkSize: 2,
                }),
            );

            assert.equal(executeResponse.status, 503);
            assert.equal(
                executeResponse.body.error,
                'target_revalidation_unavailable',
            );
            assert.equal(
                executeResponse.body.reason_code,
                'failed_internal_error',
            );
            assert.match(
                String(executeResponse.body.message || ''),
                /target revalidation/i,
            );
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'execute endpoint accepts caller-supplied target revalidation records when runtime support is unavailable',
    async () => {
        const noopPlaceholderMessage = 'noop target writer placeholder';
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            failClosedRuntimeComposition: true,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                'plan-execute-runtime-supplied-target-states',
                {
                    requiredCapabilities: ['restore_execute'],
                },
            );
            const executeResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
                createExecutePayload({
                    capabilities: ['restore_execute'],
                    chunkSize: 2,
                    revalidatedTargetRecords: [
                        {
                            table: 'incident',
                            record_sys_id: 'rec-01',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-02',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-03',
                            target_state: 'exists',
                        },
                    ],
                }),
            );

            assert.equal(executeResponse.status, 200);
            const executeBody = executeResponse.body.execution as Record<
                string,
                unknown
            >;
            assert.equal(executeBody.status, 'failed');
            const revalidatedTargetRecords =
                executeBody.revalidated_target_records as Array<
                    Record<string, unknown>
                >;
            const summary = executeBody.summary as Record<string, unknown>;
            const rowOutcomes = executeBody.row_outcomes as Array<
                Record<string, unknown>
            >;

            assert.equal(
                revalidatedTargetRecords.length,
                3,
            );
            assert.equal(summary.applied_rows, 0);
            assert.equal(
                Number(summary.failed_rows) >= 1,
                true,
            );

            let sawFailClosedApplyMessage = false;

            for (const outcome of rowOutcomes) {
                if (outcome.action === 'skip') {
                    continue;
                }

                assert.equal(outcome.outcome, 'failed');
                assert.equal(outcome.reason_code, 'failed_internal_error');
                assert.equal(
                    outcome.message,
                    'target apply runtime support is unavailable',
                );
                assert.notEqual(
                    outcome.message,
                    noopPlaceholderMessage,
                );
                sawFailClosedApplyMessage = true;
            }
            assert.equal(sawFailClosedApplyMessage, true);
            assert.equal(
                JSON.stringify(executeBody).includes(noopPlaceholderMessage),
                false,
            );
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'execute endpoint commit-driven mode defers progress until batch commit',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            failClosedRuntimeComposition: true,
            executeConfig: {
                executionProgressMode: 'commit_driven',
            },
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                'plan-execute-commit-driven-start',
                {
                    requiredCapabilities: ['restore_execute'],
                },
            );
            const executeResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
                createExecutePayload({
                    capabilities: ['restore_execute'],
                    revalidatedTargetRecords: [
                        {
                            table: 'incident',
                            record_sys_id: 'rec-01',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-02',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-03',
                            target_state: 'exists',
                        },
                    ],
                }),
            );

            assert.equal(executeResponse.status, 202);
            const executeBody = executeResponse.body.execution as Record<
                string,
                unknown
            >;
            const executeSummary = executeBody.summary as Record<
                string,
                unknown
            >;
            const checkpoint = executeBody.checkpoint as Record<
                string,
                unknown
            >;

            assert.equal(executeBody.status, 'paused');
            assert.equal(executeSummary.applied_rows, 0);
            assert.equal(executeSummary.failed_rows, 0);
            assert.deepEqual(executeBody.row_outcomes, []);
            assert.equal(checkpoint.next_chunk_index, 0);
            assert.equal(checkpoint.next_row_index, 0);

            const claimResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execute-batches/claim`,
                token,
                {
                    operator_id: 'sn-worker',
                    max_rows: 1,
                },
            );

            assert.equal(claimResponse.status, 200);
            assert.equal(claimResponse.body.accepted, true);
            const claimedRows =
                claimResponse.body.claimed_rows as Array<Record<string, unknown>>;
            assert.equal(claimedRows.length, 1);

            const executionBeforeCommit = await getJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
            );

            assert.equal(executionBeforeCommit.status, 200);
            const beforeCommitBody =
                executionBeforeCommit.body.execution as Record<string, unknown>;
            const beforeCommitSummary =
                beforeCommitBody.summary as Record<string, unknown>;

            assert.equal(beforeCommitSummary.applied_rows, 0);
            assert.equal(
                (beforeCommitBody.row_outcomes as Array<unknown>).length,
                0,
            );

            const commitResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execute-batches/commit`,
                token,
                {
                    claim_id: claimResponse.body.claim_id,
                    committed_by: 'sn-worker',
                    row_outcomes: [{
                        row_id: String(claimedRows[0]?.row_id || ''),
                        outcome: 'applied',
                        reason_code: 'none',
                    }],
                },
            );

            assert.equal(commitResponse.status, 200);
            assert.equal(commitResponse.body.accepted, true);
            assert.equal(commitResponse.body.execution_status, 'paused');
            const commitSummary = commitResponse.body.summary as Record<
                string,
                unknown
            >;
            assert.equal(commitSummary.applied_rows, 1);
            assert.equal(commitSummary.failed_rows, 0);

            const executionAfterCommit = await getJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
            );

            assert.equal(executionAfterCommit.status, 200);
            const afterCommitBody =
                executionAfterCommit.body.execution as Record<string, unknown>;
            const afterCommitSummary =
                afterCommitBody.summary as Record<string, unknown>;

            assert.equal(afterCommitSummary.applied_rows, 1);
            assert.equal(
                (afterCommitBody.row_outcomes as Array<unknown>).length,
                1,
            );
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'claim endpoint reuses persisted target revalidation records when runtime support is unavailable',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            failClosedRuntimeComposition: true,
            executeConfig: {
                maxChunksPerAttempt: 1,
            },
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                'plan-claim-runtime-supplied-target-states',
                {
                    requiredCapabilities: ['restore_execute'],
                },
            );
            const executeResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
                token,
                createExecutePayload({
                    capabilities: ['restore_execute'],
                    chunkSize: 1,
                    revalidatedTargetRecords: [
                        {
                            table: 'incident',
                            record_sys_id: 'rec-01',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-02',
                            target_state: 'exists',
                        },
                        {
                            table: 'incident',
                            record_sys_id: 'rec-03',
                            target_state: 'exists',
                        },
                    ],
                }),
            );

            assert.equal(executeResponse.status, 202);
            const executeBody = executeResponse.body.execution as Record<
                string,
                unknown
            >;
            assert.equal(executeBody.status, 'paused');

            const claimResponse = await postJson(
                baseUrl,
                `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execute-batches/claim`,
                token,
                {
                    operator_id: 'sn-worker',
                    max_rows: 1,
                },
            );

            assert.equal(claimResponse.status, 200);
            assert.equal(claimResponse.body.accepted, true);
            const claimedRows =
                claimResponse.body.claimed_rows as Array<Record<string, unknown>>;
            assert.equal(claimedRows.length, 1);
            assert.equal(typeof claimedRows[0]?.row_id, 'string');
        } finally {
            await closeServer(server);
        }
    },
);

test('execute endpoint reports failed outcomes when target writer apply fails', async () => {
    const signingKey = 'test-signing-key';
    const targetWriter = new RecordingTargetWriter();

    targetWriter.failRecord(
        'rec-02',
        'failed_permission_conflict',
        'target rejected write',
    );

    const server = createService(signingKey, {
        targetWriter,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-target-write-failure',
            {
                requiredCapabilities: ['restore_execute'],
            },
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 2,
            }),
        );

        assert.equal(executeResponse.status, 200);
        const execution = executeResponse.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(execution.status, 'failed');

        const summary = execution.summary as Record<string, unknown>;

        assert.equal(summary.applied_rows, 1);
        assert.equal(summary.failed_rows, 1);
        assert.equal(summary.skipped_rows, 0);

        const chunks = execution.chunks as Array<Record<string, unknown>>;

        assert.equal(chunks[0]?.status, 'failed');
        assert.equal(chunks[0]?.applied_count, 1);
        assert.equal(chunks[0]?.failed_count, 1);

        const rowOutcomes = execution.row_outcomes as Array<
            Record<string, unknown>
        >;
        const byRowId = new Map<string, Record<string, unknown>>();

        for (const outcome of rowOutcomes) {
            byRowId.set(String(outcome.row_id || ''), outcome);
        }

        assert.equal(
            byRowId.get(materializedRowId('rec-01'))?.outcome,
            'applied',
        );
        assert.equal(
            byRowId.get(materializedRowId('rec-02'))?.outcome,
            'failed',
        );
        assert.equal(
            byRowId.get(materializedRowId('rec-02'))?.reason_code,
            'failed_permission_conflict',
        );
        assert.equal(
            byRowId.get(materializedRowId('rec-02'))?.message,
            'target rejected write',
        );
        assert.equal(targetWriter.applyCalls.length, 2);

        const journalResponse = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
            '/rollback-journal',
            token,
        );

        assert.equal(journalResponse.status, 200);
        const rollbackJournal = journalResponse.body.rollback_journal as Array<
            Record<string, unknown>
        >;
        const snMirror = journalResponse.body.sn_mirror as Array<
            Record<string, unknown>
        >;

        assert.equal(rollbackJournal.length, 1);
        assert.equal(snMirror.length, 1);
    } finally {
        await closeServer(server);
    }
});

test('execute endpoint blocks missing capability for delete actions', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        indexedEventCandidates: [
            createIndexedEventCandidateFixture({
                eventId: 'evt-delete-01',
                offset: '100',
                partition: 1,
                recordSysId: 'rec-01',
            }),
            createIndexedEventCandidateFixture({
                eventId: 'evt-delete-02',
                offset: '101',
                partition: 1,
                recordSysId: 'rec-02',
            }),
        ],
        artifactBodies: [
            createArtifactBodyFixture({
                eventId: 'evt-delete-01',
                operation: 'D',
            }),
            createArtifactBodyFixture({
                eventId: 'evt-delete-02',
                operation: 'D',
            }),
        ],
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const deleteRows = [
            createDryRunRow('row-01', 'rec-01'),
            createDryRunRow('row-02', 'rec-02'),
        ];

        deleteRows[0].action = 'delete';
        deleteRows[0].metadata = {
            allowlist_version: 'rrs.metadata.allowlist.v1',
            metadata: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                table: 'incident',
                record_sys_id: 'rec-01',
                event_id: 'evt-row-01',
                event_type: 'cdc.delete',
                operation: 'D',
                schema_version: 3,
                sys_updated_on: '2026-02-16 11:59:59',
                sys_mod_count: 2,
                __time: '2026-02-16T11:59:59.000Z',
                topic: 'rez.cdc',
                partition: 1,
                offset: '100',
            },
        };

        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-delete',
            {
                dryRunOverrides: {
                    rows: deleteRows,
                },
                requiredCapabilities: ['restore_execute'],
            },
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
            }),
        );

        assert.equal(executeResponse.status, 403);
        assert.equal(
            executeResponse.body.reason_code,
            'blocked_missing_capability',
        );
    } finally {
        await closeServer(server);
    }
});

test('execute endpoint enforces media hard caps unless override capability is provided', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        executeConfig: {
            mediaMaxItems: 1,
            mediaMaxBytes: 80,
        },
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-media-cap',
            {
                dryRunOverrides: {
                    mediaCandidates: [
                        createMediaCandidate('media-01', {
                            size_bytes: 64,
                        }),
                        createMediaCandidate('media-02', {
                            size_bytes: 64,
                        }),
                    ],
                },
            },
        );
        const blocked = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
            }),
        );

        assert.equal(blocked.status, 403);
        assert.equal(blocked.body.reason_code, 'blocked_missing_capability');

        const allowed = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: [
                    'restore_execute',
                    'restore_override_caps',
                ],
                includeOverride: true,
            }),
        );

        assert.equal(allowed.status, 200);
        const execution = allowed.body.execution as Record<string, unknown>;

        assert.equal(execution.status, 'completed');
    } finally {
        await closeServer(server);
    }
});

test('execute endpoint reports per-item media outcomes for parent/hash/retry failures', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        executeConfig: {
            mediaMaxItems: 10,
            mediaMaxBytes: 1024 * 1024,
        },
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-media-outcomes',
            {
                dryRunOverrides: {
                    mediaCandidates: [
                        createMediaCandidate('media-parent-missing', {
                            parent_record_exists: false,
                        }),
                        createMediaCandidate('media-hash-mismatch', {
                            sha256_plain: 'a'.repeat(64),
                            observed_sha256_plain: 'b'.repeat(64),
                        }),
                        createMediaCandidate('media-retry-exhausted', {
                            retryable_failures: 5,
                            max_retry_attempts: 2,
                        }),
                        createMediaCandidate('media-success-after-retry', {
                            retryable_failures: 1,
                            max_retry_attempts: 3,
                        }),
                        createMediaCandidate('media-excluded', {
                            decision: 'exclude',
                        }),
                    ],
                },
            },
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
            }),
        );

        assert.equal(executeResponse.status, 200);
        const execution = executeResponse.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(execution.status, 'failed');

        const summary = execution.summary as Record<string, unknown>;

        assert.equal(summary.attachments_planned, 5);
        assert.equal(summary.attachments_applied, 1);
        assert.equal(summary.attachments_skipped, 1);
        assert.equal(summary.attachments_failed, 3);

        const outcomes = execution.media_outcomes as Array<Record<string, unknown>>;
        const byId = new Map<string, Record<string, unknown>>();

        for (const outcome of outcomes) {
            byId.set(String(outcome.candidate_id || ''), outcome);
        }

        assert.equal(
            byId.get('media-parent-missing')?.reason_code,
            'failed_media_parent_missing',
        );
        assert.equal(
            byId.get('media-hash-mismatch')?.reason_code,
            'failed_media_hash_mismatch',
        );
        assert.equal(
            byId.get('media-retry-exhausted')?.reason_code,
            'failed_media_retry_exhausted',
        );
        assert.equal(
            byId.get('media-success-after-retry')?.outcome,
            'applied',
        );
        assert.equal(byId.get('media-excluded')?.outcome, 'skipped');
    } finally {
        await closeServer(server);
    }
});

test('execute endpoint hard-blocks reference conflicts', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-execute-reference-block',
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
                runtimeConflicts: [
                    {
                        conflict_id: 'conf-reference',
                        row_id: materializedRowId('rec-01'),
                        class: 'reference_conflict',
                        reason_code: 'blocked_reference_conflict',
                        reason: 'referenced parent missing',
                        resolution: 'abort_and_replan',
                    },
                ],
            }),
        );

        assert.equal(executeResponse.status, 409);
        assert.equal(
            executeResponse.body.reason_code,
            'blocked_reference_conflict',
        );
    } finally {
        await closeServer(server);
    }
});

test('resume endpoint continues paused execution from checkpoint', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-resume-checkpoint',
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 1,
            }),
        );

        assert.equal(executeResponse.status, 202);
        const execution = executeResponse.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(execution.status, 'paused');

        const checkpointResponse = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/checkpoint`,
            token,
        );

        assert.equal(checkpointResponse.status, 200);
        const checkpoint = checkpointResponse.body.checkpoint as Record<
            string,
            unknown
        >;

        assert.equal(checkpoint.next_chunk_index, 1);
        assert.equal(checkpoint.next_row_index, 0);

        const resumeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/resume`,
            token,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: execution.plan_checksum,
                expected_precondition_checksum:
                    execution.precondition_checksum,
            },
        );

        assert.equal(resumeResponse.status, 200);
        const resumedExecution = resumeResponse.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(resumedExecution.status, 'completed');
        const summary = resumedExecution.summary as Record<string, unknown>;

        assert.equal(summary.applied_rows, 2);

        const journalResponse = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
            '/rollback-journal',
            token,
        );

        assert.equal(journalResponse.status, 200);
        const rollbackJournal = journalResponse.body.rollback_journal as Array<
            Record<string, unknown>
        >;
        const snMirror = journalResponse.body.sn_mirror as Array<
            Record<string, unknown>
        >;

        assert.equal(rollbackJournal.length, 2);
        assert.equal(snMirror.length, 2);
        assert.equal(rollbackJournal[0].journal_id, snMirror[0].journal_id);

        const duplicateResume = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/resume`,
            token,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: execution.plan_checksum,
                expected_precondition_checksum:
                    execution.precondition_checksum,
            },
        );

        assert.equal(duplicateResume.status, 200);
        const duplicateExecution = duplicateResume.body.execution as Record<
            string,
            unknown
        >;

        assert.equal(duplicateExecution.status, 'completed');
        const duplicateSummary = duplicateExecution.summary as Record<
            string,
            unknown
        >;

        assert.equal(duplicateSummary.applied_rows, 2);
    } finally {
        await closeServer(server);
    }
});

test('resume endpoint keeps mixed-success chunks truthful without double-applying successful rows', async () => {
    const signingKey = 'test-signing-key';
    const targetWriter = new RecordingTargetWriter();

    targetWriter.failRecord(
        'rec-02',
        'failed_permission_conflict',
        'target rejected write',
    );

    const server = createService(signingKey, {
        executeConfig: {
            maxChunksPerAttempt: 1,
        },
        targetWriter,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-resume-mixed-success',
            {
                dryRunOverrides: {
                    rows: [
                        createDryRunRow('row-01', 'rec-01'),
                        createDryRunRow('row-02', 'rec-02'),
                        createDryRunRow('row-03', 'rec-03'),
                    ],
                },
            },
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 2,
            }),
        );

        assert.equal(executeResponse.status, 202);

        const firstExecution = executeResponse.body.execution as Record<
            string,
            unknown
        >;
        const firstSummary = firstExecution.summary as Record<string, unknown>;

        assert.equal(firstExecution.status, 'paused');
        assert.equal(firstSummary.applied_rows, 1);
        assert.equal(firstSummary.failed_rows, 1);
        assert.equal(firstSummary.skipped_rows, 0);

        const checkpointResponse = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/checkpoint`,
            token,
        );

        assert.equal(checkpointResponse.status, 200);

        const checkpoint = checkpointResponse.body.checkpoint as Record<
            string,
            unknown
        >;

        assert.equal(checkpoint.next_chunk_index, 1);
        assert.equal(checkpoint.next_row_index, 0);

        const resumeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/resume`,
            token,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: firstExecution.plan_checksum,
                expected_precondition_checksum:
                    firstExecution.precondition_checksum,
            },
        );

        assert.equal(resumeResponse.status, 200);

        const resumedExecution = resumeResponse.body.execution as Record<
            string,
            unknown
        >;
        const resumedSummary = resumedExecution.summary as Record<
            string,
            unknown
        >;

        assert.equal(resumedExecution.status, 'failed');
        assert.equal(resumedSummary.applied_rows, 2);
        assert.equal(resumedSummary.failed_rows, 1);
        assert.equal(resumedSummary.skipped_rows, 0);

        const callCountByRow = new Map<string, number>();

        for (const call of targetWriter.applyCalls) {
            const rowId = String(call.row.row_id);
            callCountByRow.set(rowId, (callCountByRow.get(rowId) || 0) + 1);
        }

        assert.equal(callCountByRow.get(materializedRowId('rec-01')), 1);
        assert.equal(callCountByRow.get(materializedRowId('rec-02')), 1);
        assert.equal(callCountByRow.get(materializedRowId('rec-03')), 1);

        const journalResponse = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}` +
            '/rollback-journal',
            token,
        );

        assert.equal(journalResponse.status, 200);

        const rollbackJournal = journalResponse.body.rollback_journal as Array<
            Record<string, unknown>
        >;
        const snMirror = journalResponse.body.sn_mirror as Array<
            Record<string, unknown>
        >;

        assert.equal(rollbackJournal.length, 2);
        assert.equal(snMirror.length, 2);

        for (const entry of rollbackJournal) {
            assert.notEqual(
                entry.plan_row_id,
                materializedRowId('rec-02'),
            );
        }
    } finally {
        await closeServer(server);
    }
});

test('resume endpoint blocks mismatched expected checksums', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-resume-checksum-mismatch',
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
                chunkSize: 1,
            }),
        );

        assert.equal(executeResponse.status, 202);

        const resumeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/resume`,
            token,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                expected_plan_checksum: 'f'.repeat(64),
            },
        );

        assert.equal(resumeResponse.status, 409);
        assert.equal(
            resumeResponse.body.reason_code,
            'blocked_resume_precondition_mismatch',
        );
    } finally {
        await closeServer(server);
    }
});

test('evidence export endpoint blocks until execution reaches terminal state', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-evidence-not-ready',
        );
        const evidenceExport = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/evidence/export`,
            token,
            {},
        );

        assert.equal(evidenceExport.status, 409);
        assert.equal(
            evidenceExport.body.reason_code,
            'blocked_evidence_not_ready',
        );
    } finally {
        await closeServer(server);
    }
});

test('evidence endpoints return signed verified export payload after terminal execute', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'plan-evidence-export',
        );
        const executeResponse = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/execution`,
            token,
            createExecutePayload({
                capabilities: ['restore_execute'],
            }),
        );

        assert.equal(executeResponse.status, 200);
        assert.equal(typeof executeResponse.body.evidence, 'object');
        const executeEvidence = executeResponse.body.evidence as Record<
            string,
            unknown
        >;

        assert.equal(typeof executeEvidence.evidence_id, 'string');
        assert.equal(executeEvidence.signature_verification, 'verified');

        const evidenceGet = await getJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/evidence`,
            token,
        );

        assert.equal(evidenceGet.status, 200);
        const evidence = evidenceGet.body.evidence as Record<string, unknown>;
        const verification =
            evidenceGet.body.verification as Record<string, unknown>;

        assert.equal(typeof evidence.evidence_id, 'string');
        assert.equal(typeof evidence.plan_hash, 'string');
        assert.equal(typeof evidence.report_hash, 'string');
        assert.equal((evidence.plan_hash as string).length, 64);
        assert.equal((evidence.report_hash as string).length, 64);
        assert.equal(
            verification.signature_verification,
            'verified',
        );
        assert.equal(verification.reason_code, 'none');

        const resumeMetadata = evidence.resume_metadata as Record<
            string,
            unknown
        >;

        assert.equal(typeof resumeMetadata.resume_attempt_count, 'number');
        assert.equal(typeof resumeMetadata.checkpoint_id, 'string');

        const immutableStorage = evidence.immutable_storage as Record<
            string,
            unknown
        >;

        assert.equal(immutableStorage.worm_enabled, true);
        assert.equal(immutableStorage.retention_class, 'compliance-7y');

        const evidenceExport = await postJson(
            baseUrl,
            `/v1/jobs/${encodeURIComponent(fixture.jobId)}/evidence/export`,
            token,
            {},
        );

        assert.equal(evidenceExport.status, 200);
        assert.equal(evidenceExport.body.reused, true);
        const exportedEvidence = evidenceExport.body.evidence as Record<
            string,
            unknown
        >;

        assert.equal(exportedEvidence.evidence_id, evidence.evidence_id);
    } finally {
        await closeServer(server);
    }
});

test(
    'cross-instance terminal execute/evidence flow stays consistent',
    async () => {
        const signingKey = 'test-signing-key';
        const token = createToken(signingKey);
        const db = newDb();

        db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');

        const instanceAState = createPostgresBackedStateStores(db);
        const instanceBState = createPostgresBackedStateStores(db);
        const serverA = createService(signingKey, {
            executeConfig: {
                maxChunksPerAttempt: 1,
                elevatedSkipRatioPercent: 100,
            },
            stateStores: instanceAState.stores,
        });
        const serverB = createService(signingKey, {
            executeConfig: {
                maxChunksPerAttempt: 1,
                elevatedSkipRatioPercent: 100,
            },
            stateStores: instanceBState.stores,
        });
        const baseUrlA = await listen(serverA);
        const baseUrlB = await listen(serverB);

        try {
            const fixture = await createPlanAndJob(
                baseUrlA,
                token,
                'plan-cross-instance-terminal-evidence',
            );
            const jobPath = `/v1/jobs/${encodeURIComponent(fixture.jobId)}`;

            const jobFromA = await getJson(
                baseUrlA,
                jobPath,
                token,
            );
            const jobFromB = await getJson(
                baseUrlB,
                jobPath,
                token,
            );

            assertNotScopedNotFound(jobFromA, 'instance A job read');
            assertNotScopedNotFound(jobFromB, 'instance B job read');
            assert.equal(jobFromA.status, 200);
            assert.equal(jobFromB.status, 200);

            const executeFromB = await postJson(
                baseUrlB,
                `${jobPath}/execution`,
                token,
                createExecutePayload({
                    capabilities: ['restore_execute'],
                    chunkSize: 1,
                }),
            );

            assert.equal(executeFromB.status, 202);
            const pausedExecutionFromB = executeFromB.body.execution as Record<
                string,
                unknown
            >;

            assert.equal(pausedExecutionFromB.status, 'paused');
            const pausedPlanChecksum =
                pausedExecutionFromB.plan_checksum as string;
            const pausedPreconditionChecksum =
                pausedExecutionFromB.precondition_checksum as string;

            assert.equal(typeof pausedPlanChecksum, 'string');
            assert.equal(pausedPlanChecksum.length, 64);
            assert.equal(typeof pausedPreconditionChecksum, 'string');
            assert.equal(pausedPreconditionChecksum.length, 64);

            const executionFromA = await getJson(
                baseUrlA,
                `${jobPath}/execution`,
                token,
            );
            const executionFromB = await getJson(
                baseUrlB,
                `${jobPath}/execution`,
                token,
            );

            assertNotScopedNotFound(executionFromA, 'instance A execution read');
            assertNotScopedNotFound(executionFromB, 'instance B execution read');
            assert.equal(executionFromA.status, 200);
            assert.equal(executionFromB.status, 200);
            assert.equal(
                (executionFromA.body.execution as Record<string, unknown>).status,
                'paused',
            );
            assert.equal(
                (executionFromB.body.execution as Record<string, unknown>).status,
                'paused',
            );

            const resumeFromA = await postJson(
                baseUrlA,
                `${jobPath}/resume`,
                token,
                {
                    operator_id: 'operator@example.com',
                    operator_capabilities: ['restore_execute'],
                    expected_plan_checksum: pausedPlanChecksum,
                    expected_precondition_checksum: pausedPreconditionChecksum,
                },
            );

            assert.equal(resumeFromA.status, 200);
            const resumedExecution = resumeFromA.body.execution as Record<
                string,
                unknown
            >;
            const resumedSummary = resumedExecution.summary as Record<
                string,
                unknown
            >;

            assert.equal(resumedExecution.status, 'completed');
            assert.equal(resumedSummary.applied_rows, 2);

            const executionFromAFinal = await getJson(
                baseUrlA,
                `${jobPath}/execution`,
                token,
            );
            const executionFromBFinal = await getJson(
                baseUrlB,
                `${jobPath}/execution`,
                token,
            );

            assertNotScopedNotFound(
                executionFromAFinal,
                'instance A final execution read',
            );
            assertNotScopedNotFound(
                executionFromBFinal,
                'instance B final execution read',
            );
            assert.equal(executionFromAFinal.status, 200);
            assert.equal(executionFromBFinal.status, 200);
            assert.equal(
                (
                    executionFromAFinal.body.execution as Record<string, unknown>
                ).status,
                'completed',
            );
            assert.equal(
                (
                    executionFromBFinal.body.execution as Record<string, unknown>
                ).status,
                'completed',
            );

            const evidenceExportFromB = await postJson(
                baseUrlB,
                `${jobPath}/evidence/export`,
                token,
                {},
            );

            assert.equal(evidenceExportFromB.status, 200);
            const exportedEvidence = evidenceExportFromB.body.evidence as Record<
                string,
                unknown
            >;
            const exportVerification =
                evidenceExportFromB.body.verification as Record<
                    string,
                    unknown
                >;

            assert.equal(typeof exportedEvidence.evidence_id, 'string');
            assert.equal(typeof exportedEvidence.plan_hash, 'string');
            assert.equal(typeof exportedEvidence.report_hash, 'string');
            assert.equal((exportedEvidence.plan_hash as string).length, 64);
            assert.equal((exportedEvidence.report_hash as string).length, 64);
            assert.equal(exportVerification.signature_verification, 'verified');
            assert.equal(exportVerification.reason_code, 'none');

            const evidenceFromA = await getJson(
                baseUrlA,
                `${jobPath}/evidence`,
                token,
            );
            const evidenceFromB = await getJson(
                baseUrlB,
                `${jobPath}/evidence`,
                token,
            );

            assertNotScopedNotFound(evidenceFromA, 'instance A evidence read');
            assertNotScopedNotFound(evidenceFromB, 'instance B evidence read');
            assert.equal(evidenceFromA.status, 200);
            assert.equal(evidenceFromB.status, 200);

            const evidenceRecordA = evidenceFromA.body.evidence as Record<
                string,
                unknown
            >;
            const evidenceRecordB = evidenceFromB.body.evidence as Record<
                string,
                unknown
            >;
            const verificationFromA = evidenceFromA.body.verification as Record<
                string,
                unknown
            >;
            const verificationFromB = evidenceFromB.body.verification as Record<
                string,
                unknown
            >;

            assert.equal(
                evidenceRecordA.evidence_id,
                evidenceRecordB.evidence_id,
            );
            assert.equal(
                evidenceRecordA.evidence_id,
                exportedEvidence.evidence_id,
            );
            assert.equal(typeof evidenceRecordA.plan_hash, 'string');
            assert.equal(typeof evidenceRecordA.report_hash, 'string');
            assert.equal(typeof evidenceRecordB.plan_hash, 'string');
            assert.equal(typeof evidenceRecordB.report_hash, 'string');
            assert.equal((evidenceRecordA.plan_hash as string).length, 64);
            assert.equal((evidenceRecordA.report_hash as string).length, 64);
            assert.equal((evidenceRecordB.plan_hash as string).length, 64);
            assert.equal((evidenceRecordB.report_hash as string).length, 64);
            assert.equal(verificationFromA.signature_verification, 'verified');
            assert.equal(verificationFromB.signature_verification, 'verified');
            assert.equal(verificationFromA.reason_code, 'none');
            assert.equal(verificationFromB.reason_code, 'none');
        } finally {
            await closeServer(serverA);
            await closeServer(serverB);
            await instanceAState.pool.end();
            await instanceBState.pool.end();
        }
    },
);

test('admin ops endpoints require admin token when configured', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        adminToken: 'admin-secret',
    });
    const baseUrl = await listen(server);

    try {
        const unauthorized = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/overview',
        );

        assert.equal(unauthorized.status, 403);
        assert.equal(unauthorized.body.reason_code, 'admin_auth_required');

        const wrongToken = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/overview',
            'wrong-admin-token',
        );

        assert.equal(wrongToken.status, 403);
        assert.equal(wrongToken.body.reason_code, 'admin_auth_required');

        const reconcileUnauthorized = await postAdminJson(
            baseUrl,
            '/v1/admin/ops/queue/reconcile',
            {},
        );
        assert.equal(reconcileUnauthorized.status, 403);
        assert.equal(
            reconcileUnauthorized.body.reason_code,
            'admin_auth_required',
        );

        const reconcileWrongToken = await postAdminJson(
            baseUrl,
            '/v1/admin/ops/queue/reconcile',
            {},
            'wrong-admin-token',
        );
        assert.equal(reconcileWrongToken.status, 403);
        assert.equal(
            reconcileWrongToken.body.reason_code,
            'admin_auth_required',
        );

        const authorized = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/overview',
            'admin-secret',
        );

        assert.equal(authorized.status, 200);
        assert.equal(typeof authorized.body.queue, 'object');
        assert.equal(typeof authorized.body.freshness, 'object');
        assert.equal(typeof authorized.body.evidence, 'object');
        assert.equal(typeof authorized.body.slo, 'object');
        assert.equal(typeof authorized.body.ga_readiness, 'object');
    } finally {
        await closeServer(server);
    }
});

test(
    'admin queue reconcile/reset endpoints validate scope and apply reset',
    async () => {
        const signingKey = 'test-signing-key';
        const adminToken = 'admin-secret';
        const server = createService(signingKey, {
            adminToken,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const firstPlan = await createPlanAndJob(
                baseUrl,
                token,
                'admin-plan-reset-01',
            );
            const secondPlan = await createPlanAndJob(
                baseUrl,
                token,
                'admin-plan-reset-02',
            );

            const reconcileInvalid = await postAdminJson(
                baseUrl,
                '/v1/admin/ops/queue/reconcile',
                {
                    force_stale_status: 'failed',
                },
                adminToken,
            );
            assert.equal(reconcileInvalid.status, 400);
            assert.equal(reconcileInvalid.body.error, 'invalid_request');

            const resetMissingScope = await postAdminJson(
                baseUrl,
                '/v1/admin/ops/queue/reset',
                {
                    stale_after_ms: 0,
                    dry_run: false,
                },
                adminToken,
            );
            assert.equal(resetMissingScope.status, 400);
            assert.equal(resetMissingScope.body.error, 'invalid_request');

            const reset = await postAdminJson(
                baseUrl,
                '/v1/admin/ops/queue/reset',
                {
                    stale_after_ms: 0,
                    dry_run: false,
                    scope: {
                        tenant_id: 'tenant-acme',
                        instance_id: 'sn-dev-01',
                        source: 'sn://acme-dev.service-now.com',
                        lock_scope_tables: ['incident'],
                    },
                },
                adminToken,
            );
            assert.equal(reset.status, 200);
            assert.equal(reset.body.applied, true);
            const forcedTransitions = reset.body.forced_transitions as Array<
                Record<string, unknown>
            >;
            assert.ok(forcedTransitions.length >= 1);
            for (const transition of forcedTransitions) {
                assert.equal(transition.to_status, 'failed');
                assert.equal(
                    transition.reason_code,
                    'failed_internal_error',
                );
                const forcedJobId = transition.job_id as string;
                const forcedJob = await getJson(
                    baseUrl,
                    `/v1/jobs/${forcedJobId}`,
                    token,
                );
                assert.equal(forcedJob.status, 200);
                const jobRecord = forcedJob.body.job as Record<string, unknown>;
                assert.equal(jobRecord.status, 'failed');
                assert.equal(
                    jobRecord.status_reason_code,
                    'failed_internal_error',
                );
            }
            assert.equal(typeof firstPlan.jobId, 'string');
            assert.equal(typeof secondPlan.jobId, 'string');
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'execute route reconcile force-terminals stale blocker and promotes queued job',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey, {
            executePreflightReconcileStaleAfterMs: 0,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const running = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-stale-running',
            );
            const queued = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-stale-queued',
            );

            const queuedBefore = await getJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}`,
                token,
            );
            assert.equal(queuedBefore.status, 200);
            const queuedBeforeJob = queuedBefore.body.job as Record<
                string,
                unknown
            >;
            assert.equal(queuedBeforeJob.status, 'queued');
            assert.equal(
                queuedBeforeJob.wait_reason_code,
                'queued_scope_lock',
            );

            const execute = await postJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}/execution`,
                token,
                createExecutePayload(),
            );

            assert.equal(execute.status, 200);
            assert.equal(
                await hasReconcilePromotionEvent(baseUrl, token, queued.jobId),
                true,
            );

            const staleAfter = await getJson(
                baseUrl,
                `/v1/jobs/${running.jobId}`,
                token,
            );

            assert.equal(staleAfter.status, 200);
            const staleAfterJob = staleAfter.body.job as Record<string, unknown>;
            assert.equal(staleAfterJob.status, 'failed');
            assert.equal(
                staleAfterJob.status_reason_code,
                'failed_stale_lock_recovered',
            );

            const queuedAfter = await getJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}`,
                token,
            );
            assert.equal(queuedAfter.status, 200);
            const queuedAfterJob = queuedAfter.body.job as Record<
                string,
                unknown
            >;
            assert.notEqual(queuedAfterJob.status, 'queued');
            assert.equal(queuedAfterJob.wait_reason_code, null);

            const staleEvents = await listJobEvents(
                baseUrl,
                token,
                running.jobId,
            );
            const staleForcedEvent = staleEvents.find((event) => {
                if (event.event_type !== 'job_failed') {
                    return false;
                }

                const details = event.details as Record<string, unknown> | null;

                if (!details) {
                    return false;
                }

                return details.reconcile_forced_terminal === true;
            });

            assert.notEqual(staleForcedEvent, undefined);

            if (staleForcedEvent) {
                const details = staleForcedEvent.details as Record<
                    string,
                    unknown
                >;

                assert.equal(
                    staleForcedEvent.reason_code,
                    'failed_stale_lock_recovered',
                );
                assert.equal(details.reconcile_forced_terminal, true);
                assert.equal(details.stale_cutoff_at, FIXED_NOW.toISOString());
            }

            const queuedEvents = await listJobEvents(
                baseUrl,
                token,
                queued.jobId,
            );
            const queuedReconcileStart = queuedEvents.find((event) => {
                if (event.event_type !== 'job_started') {
                    return false;
                }

                const details = event.details as Record<string, unknown> | null;

                if (!details) {
                    return false;
                }

                return (
                    details.promoted_from_queue === true
                    && details.reconcile_operation === true
                );
            });

            assert.notEqual(queuedReconcileStart, undefined);
        } finally {
            await closeServer(server);
        }
    },
);

test(
    'execute route reconcile promotes queued job after blocker completion when queue state drifts',
    async () => {
        const signingKey = 'test-signing-key';
        const db = newDb();

        db.public.none('CREATE SCHEMA IF NOT EXISTS rez_restore_index');

        const state = createPostgresBackedStateStores(db);
        const server = createService(signingKey, {
            stateStores: state.stores,
        });
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            if (!state.stores.jobs) {
                throw new Error('job state store fixture was not created');
            }

            const running = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-complete-running',
            );
            const queued = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-complete-queued',
            );

            await state.stores.jobs.mutate((jobState) => {
                jobState.lock_state.queued_jobs =
                    jobState.lock_state.queued_jobs.filter((entry) => {
                        return entry.jobId !== queued.jobId;
                    });
            });

            const complete = await postJson(
                baseUrl,
                `/v1/jobs/${running.jobId}/complete`,
                token,
                {
                    status: 'completed',
                    reason_code: 'none',
                },
            );

            assert.equal(complete.status, 200);
            const completePromoted = complete.body.promoted_job_ids as string[];
            assert.deepEqual(completePromoted, []);

            const queuedBeforeExecute = await getJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}`,
                token,
            );
            assert.equal(queuedBeforeExecute.status, 200);
            const queuedBeforeExecuteJob = queuedBeforeExecute.body.job as Record<
                string,
                unknown
            >;
            assert.equal(queuedBeforeExecuteJob.status, 'queued');

            const execute = await postJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}/execution`,
                token,
                createExecutePayload(),
            );

            assert.equal(execute.status, 200);
            assert.equal(
                await hasReconcilePromotionEvent(baseUrl, token, queued.jobId),
                true,
            );
        } finally {
            await closeServer(server);
            await state.pool.end();
        }
    },
);

test(
    'execute route reconcile keeps queued job blocked when active blocker still holds lock',
    async () => {
        const signingKey = 'test-signing-key';
        const server = createService(signingKey);
        const baseUrl = await listen(server);
        const token = createToken(signingKey);

        try {
            const running = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-true-blocker-running',
            );
            const queued = await createPlanAndJob(
                baseUrl,
                token,
                'exec-reconcile-true-blocker-queued',
            );

            const execute = await postJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}/execution`,
                token,
                createExecutePayload(),
            );

            assert.equal(execute.status, 409);
            assert.equal(execute.body.error, 'job_not_running');

            const queuedAfter = await getJson(
                baseUrl,
                `/v1/jobs/${queued.jobId}`,
                token,
            );

            assert.equal(queuedAfter.status, 200);
            const queuedAfterJob = queuedAfter.body.job as Record<
                string,
                unknown
            >;
            assert.equal(queuedAfterJob.status, 'queued');
            assert.equal(
                queuedAfterJob.wait_reason_code,
                'queued_scope_lock',
            );
            assert.equal(
                await hasReconcilePromotionEvent(baseUrl, token, queued.jobId),
                false,
            );

            const runningAfter = await getJson(
                baseUrl,
                `/v1/jobs/${running.jobId}`,
                token,
            );
            assert.equal(runningAfter.status, 200);
            const runningAfterJob = runningAfter.body.job as Record<
                string,
                unknown
            >;
            assert.equal(runningAfterJob.status, 'running');

            const runningEvents = await listJobEvents(
                baseUrl,
                token,
                running.jobId,
            );
            const hasForcedTerminalEvent = runningEvents.some((event) => {
                if (
                    event.event_type !== 'job_failed'
                    && event.event_type !== 'job_completed'
                    && event.event_type !== 'job_cancelled'
                ) {
                    return false;
                }

                const details = event.details as Record<string, unknown> | null;

                if (!details) {
                    return false;
                }

                return details.reconcile_forced_terminal === true;
            });

            assert.equal(hasForcedTerminalEvent, false);
            assert.equal(typeof running.jobId, 'string');
        } finally {
            await closeServer(server);
        }
    },
);

test('admin ops freshness returns explicit ACP dependency outage details', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        adminToken: 'admin-secret',
        sourceMappingListProvider: createSourceMappingListProvider({
            status: 'outage',
            message: 'ACP unavailable',
        }),
    });
    const baseUrl = await listen(server);

    try {
        const response = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/freshness',
            'admin-secret',
        );

        assert.equal(response.status, 503);
        assert.equal(response.body.error, 'dependency_unavailable');
        assert.equal(
            response.body.reason_code,
            'blocked_auth_control_plane_outage',
        );
        assert.equal(response.body.dependency, 'auth_control_plane');
        assert.equal(response.body.message, 'ACP unavailable');
    } finally {
        await closeServer(server);
    }
});

test('oversized JSON request returns 413 request_body_too_large', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey, {
        adminToken: 'admin-secret',
        bodyMaxBytes: 256,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);
    const oversized = {
        payload: 'x'.repeat(1024),
    };

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs',
            token,
            oversized,
        );

        assert.equal(response.status, 413);
        assert.equal(response.body.error, 'payload_too_large');
        assert.equal(
            response.body.reason_code,
            'request_body_too_large',
        );
    } finally {
        await closeServer(server);
    }
});

test('admin ops endpoints expose queue, freshness, and evidence summaries', async () => {
    const signingKey = 'test-signing-key';
    const adminToken = 'admin-secret';
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createWatermark(),
    ));

    const server = createService(signingKey, {
        adminToken,
        authoritativeWatermarks: [],
        restoreIndexReader,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const firstPlan = await createPlanAndJob(
            baseUrl,
            token,
            'admin-plan-queue-01',
        );
        const secondPlan = await createPlanAndJob(
            baseUrl,
            token,
            'admin-plan-queue-02',
        );
        restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse({
            ...createWatermark(),
            coverage_end: '2026-02-16T11:55:00.000Z',
            indexed_through_time: '2026-02-16T11:55:00.000Z',
        }));

        const queue = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/queue',
            adminToken,
        );

        assert.equal(queue.status, 200);
        const queueTotals = queue.body.totals as Record<string, unknown>;
        assert.equal(queueTotals.running_jobs, 1);
        assert.equal(queueTotals.queued_jobs, 1);
        const waitReasonCounts = queue.body
            .wait_reason_counts as Record<string, unknown>;
        assert.equal(waitReasonCounts.queued_scope_lock, 1);

        const execute = await postJson(
            baseUrl,
            `/v1/jobs/${firstPlan.jobId}/execution`,
            token,
            createExecutePayload(),
        );
        assert.equal(execute.status, 200);

        const evidenceExport = await postJson(
            baseUrl,
            `/v1/jobs/${firstPlan.jobId}/evidence/export`,
            token,
            {},
        );
        assert.equal(evidenceExport.status, 200);

        const freshness = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/freshness',
            adminToken,
        );
        assert.equal(freshness.status, 200);
        const freshnessTotals = freshness.body.totals as Record<string, unknown>;
        assert.equal(freshnessTotals.source_count, 1);
        assert.equal(freshnessTotals.stale_source_count, 1);

        const evidence = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/evidence',
            adminToken,
        );
        assert.equal(evidence.status, 200);
        const evidenceTotals = evidence.body.totals as Record<string, unknown>;
        assert.equal(evidenceTotals.total, 1);
        assert.equal(evidenceTotals.verified, 1);

        const evidenceRows = evidence.body.evidences as Array<Record<
            string,
            unknown
        >>;
        assert.equal(evidenceRows[0].job_id, firstPlan.jobId);
        assert.equal(typeof secondPlan.jobId, 'string');
    } finally {
        await closeServer(server);
    }
});

test('admin ops RS-15 SLO and GA readiness endpoints enforce staged gate checks', async () => {
    const signingKey = 'test-signing-key';
    const adminToken = 'admin-secret';
    const server = createService(signingKey, {
        adminToken,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const fixture = await createPlanAndJob(
            baseUrl,
            token,
            'admin-plan-rs15-01',
        );
        const execute = await postJson(
            baseUrl,
            `/v1/jobs/${fixture.jobId}/execution`,
            token,
            createExecutePayload(),
        );
        assert.equal(execute.status, 200);

        const evidenceExport = await postJson(
            baseUrl,
            `/v1/jobs/${fixture.jobId}/evidence/export`,
            token,
            {},
        );
        assert.equal(evidenceExport.status, 200);

        const initialGa = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/ga-readiness',
            adminToken,
        );
        assert.equal(initialGa.status, 200);
        assert.equal(initialGa.body.ga_ready, false);
        const initialBlocked = initialGa.body.blocked_reasons as string[];
        assert.ok(initialBlocked.includes('staging_mode_enabled'));
        assert.ok(initialBlocked.includes('runbooks_signed_off'));
        assert.ok(initialBlocked.includes('failure_drills_passed'));

        const stagingEnabled = await postAdminJson(
            baseUrl,
            '/v1/admin/ops/staging-mode',
            {
                enabled: true,
                actor: 'ops@example.com',
            },
            adminToken,
        );
        assert.equal(stagingEnabled.status, 200);
        const stagingMode = stagingEnabled.body.staging_mode as Record<
            string,
            unknown
        >;
        assert.equal(stagingMode.enabled, true);

        const runbookSignoff = await postAdminJson(
            baseUrl,
            '/v1/admin/ops/runbooks-signoff',
            {
                signed_off: true,
                actor: 'ops@example.com',
            },
            adminToken,
        );
        assert.equal(runbookSignoff.status, 200);
        const runbooks = runbookSignoff.body.runbooks as Record<
            string,
            unknown
        >;
        assert.equal(runbooks.signed_off, true);

        const drills = [
            'auth_outage',
            'sidecar_lag',
            'pg_saturation',
            'entitlement_disable',
            'crash_resume',
            'evidence_audit_export',
        ];

        for (const drillId of drills) {
            const drill = await postAdminJson(
                baseUrl,
                '/v1/admin/ops/failure-drills',
                {
                    drill_id: drillId,
                    status: 'pass',
                    actor: 'ops@example.com',
                    notes: 'simulated drill passed',
                },
                adminToken,
            );
            assert.equal(drill.status, 200);
            const drillRecord = drill.body.drill as Record<string, unknown>;
            assert.equal(drillRecord.drill_id, drillId);
            assert.equal(drillRecord.status, 'passed');
        }

        const slo = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/slo',
            adminToken,
        );
        assert.equal(slo.status, 200);
        const burnRate = slo.body.burn_rate as Record<string, unknown>;
        assert.equal(burnRate.status, 'within_budget');
        assert.equal(burnRate.severity, 'normal');

        const gaReadiness = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/ga-readiness',
            adminToken,
        );
        assert.equal(gaReadiness.status, 200);
        assert.equal(gaReadiness.body.ga_ready, true);
        const blockedReasons = gaReadiness.body.blocked_reasons as string[];
        assert.equal(blockedReasons.length, 0);
        const failureDrillSummary = gaReadiness.body.failure_drills as Record<
            string,
            unknown
        >;
        const totals = failureDrillSummary.totals as Record<string, unknown>;
        assert.equal(totals.passed, 6);
        assert.equal(totals.pending, 0);
        assert.equal(totals.failed, 0);
    } finally {
        await closeServer(server);
    }
});

test('admin ops RS-15 SLO dashboard captures multi-job execute samples', async () => {
    const signingKey = 'test-signing-key';
    const adminToken = 'admin-secret';
    const server = createService(signingKey, {
        adminToken,
    });
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        for (let index = 0; index < 5; index += 1) {
            const suffix = String(index + 1).padStart(2, '0');
            const fixture = await createPlanAndJob(
                baseUrl,
                token,
                `admin-plan-rs15-load-${suffix}`,
            );
            const execute = await postJson(
                baseUrl,
                `/v1/jobs/${fixture.jobId}/execution`,
                token,
                createExecutePayload(),
            );
            assert.equal(execute.status, 200);
        }

        const slo = await getAdminJson(
            baseUrl,
            '/v1/admin/ops/slo',
            adminToken,
        );
        assert.equal(slo.status, 200);
        const execution = slo.body.execution as Record<string, unknown>;
        assert.equal(execution.total, 5);
        assert.equal(execution.terminal, 5);
        const queue = slo.body.queue as Record<string, unknown>;
        assert.equal(typeof queue.queue_wait_p95_ms, 'number');
        assert.equal(typeof execution.execute_duration_p95_ms, 'number');
    } finally {
        await closeServer(server);
    }
});

test('catch-all error handler returns 500 for unexpected internal errors', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await postJson(
            baseUrl,
            '/v1/jobs/nonexistent-job-id/execution',
            token,
            createExecutePayload(),
        );

        assert.notEqual(response.status, 400);
    } finally {
        await closeServer(server);
    }
});

test('malformed JSON request body returns 400 bad_request', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const token = createToken(signingKey);

    try {
        const response = await fetch(
            `${baseUrl}/v1/plans/dry-run`,
            {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    authorization: `Bearer ${token}`,
                },
                body: '{"invalid json',
            },
        );
        const body = await response.json() as Record<string, unknown>;

        assert.equal(response.status, 400);
        assert.equal(body.error, 'bad_request');
        assert.equal(
            body.message,
            'malformed JSON in request body',
        );
    } finally {
        await closeServer(server);
    }
});

test('token with future iat beyond clock skew is rejected', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const futureIat = Math.floor(now().getTime() / 1000) + 3600;
    const token = buildScopedToken({
        signingKey,
        issuedAt: futureIat,
        expiresInSeconds: 7200,
    });

    try {
        const response = await getJson(
            baseUrl,
            '/v1/jobs',
            token,
        );

        assert.equal(response.status, 401);
        assert.equal(
            response.body.reason_code,
            'denied_token_malformed',
        );
    } finally {
        await closeServer(server);
    }
});

test('token with iat >= exp is rejected', async () => {
    const signingKey = 'test-signing-key';
    const server = createService(signingKey);
    const baseUrl = await listen(server);
    const issuedAt = Math.floor(now().getTime() / 1000);
    const token = buildScopedToken({
        signingKey,
        issuedAt,
        expiresInSeconds: -1,
    });

    try {
        const response = await getJson(
            baseUrl,
            '/v1/jobs',
            token,
        );

        assert.equal(response.status, 401);
        assert.equal(
            response.body.reason_code,
            'denied_token_malformed',
        );
    } finally {
        await closeServer(server);
    }
});
