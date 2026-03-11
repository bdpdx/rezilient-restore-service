import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    computeRestorePlanHash,
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestorePlan as RestorePlanSchema,
    RestoreWatermark as RestoreWatermarkSchema,
} from '@rezilient/types';
import { RestoreJobService } from '../jobs/job-service';
import { RestoreLockManager } from '../locks/lock-manager';
import {
    buildApprovalPlaceholder,
    buildPlanHashInput,
    type CreateDryRunPlanRequest,
    type RestoreDryRunPlanRecord,
} from '../plans/models';
import { RestorePlanService } from '../plans/plan-service';
import { InMemoryRestorePlanStateStore } from '../plans/plan-state-store';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from '../plans/materialization-service';
import {
    InMemoryRestoreTargetStateLookup,
    NoopRestoreTargetStateLookup,
    type RestoreTargetStateLookup,
} from '../plans/target-reconciliation';
import { SourceRegistry } from '../registry/source-registry';
import { InMemoryRestoreIndexStateReader } from '../restore-index/state-reader';
import { RestoreExecutionService } from './execute-service';
import {
    InMemoryRestoreExecutionStateStore,
    RestoreExecutionState,
    RestoreExecutionStateStore,
} from './execute-state-store';
import type {
    RestoreReasonCode,
    RestoreTargetWriteRequest,
    RestoreTargetWriteResult,
    RestoreTargetWriter,
} from './models';
import { NoopRestoreTargetWriter } from './models';

const FIXED_NOW = new Date('2026-02-16T12:00:00.000Z');

function now(): Date {
    return new Date(FIXED_NOW.getTime());
}

function claims() {
    return {
        iss: 'rez-auth-control-plane',
        sub: 'client-1',
        aud: 'rezilient:rrs',
        jti: 'tok-1',
        iat: 100,
        exp: 200,
        service_scope: 'rrs' as const,
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
    };
}

class RecordingTargetWriter implements RestoreTargetWriter {
    readonly applyCalls: RestoreTargetWriteRequest[] = [];

    readonly appliedActionsByRecord = new Map<
        string,
        'delete' | 'insert' | 'skip' | 'update'
    >();

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

        this.appliedActionsByRecord.set(
            input.row.record_sys_id,
            input.row.action,
        );

        return {
            outcome: 'applied',
            reason_code: 'none',
        };
    }
}

class FailOnMutateExecutionStateStore implements RestoreExecutionStateStore {
    private readonly delegate = new InMemoryRestoreExecutionStateStore();

    private mutationCount = 0;

    private failed = false;

    constructor(private readonly failOnMutation: number) {
    }

    async read() {
        return this.delegate.read();
    }

    async mutate<T>(
        mutator: (
            state: RestoreExecutionState,
        ) => T | Promise<T>,
    ): Promise<T> {
        this.mutationCount += 1;

        if (
            !this.failed &&
            this.mutationCount === this.failOnMutation
        ) {
            this.failed = true;
            throw new Error('simulated execution state persist failure');
        }

        return this.delegate.mutate(mutator);
    }
}

function createRow(
    rowId: string,
    action: 'update' | 'insert' | 'delete' | 'skip' = 'update',
) {
    return {
        row_id: rowId,
        table: 'incident',
        record_sys_id: `rec-${rowId}`,
        action,
        precondition_hash: 'a'.repeat(64),
        metadata: {
            allowlist_version: 'rrs.metadata.allowlist.v1',
            metadata: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                table: 'incident',
                record_sys_id: `rec-${rowId}`,
                event_id: `evt-${rowId}`,
                event_type: 'cdc.write',
                operation: action === 'delete' ? 'D' : 'U',
                schema_version: 3,
                sys_updated_on: '2026-02-16 11:59:59',
                sys_mod_count: 2,
                __time: '2026-02-16T11:59:59.000Z',
                topic: 'rez.cdc',
                partition: 1,
                offset: '100',
            },
        },
        values: action === 'skip'
            ? undefined
            : {
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
        sha256_plain: string;
        size_bytes: number;
    }>,
) {
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

function createScopeDrivenDryRunPayload(
    planId: string,
    scopeOverrides?: Record<string, unknown>,
) {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-16T12:00:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version:
                'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
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
            tables: ['incident'],
            record_sys_ids: ['rec-scope-delete', 'rec-scope-update'],
            ...scopeOverrides,
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        conflicts: [],
        delete_candidates: [],
        media_candidates: [],
    };
}

function seedScopeMaterializationCandidate(input: {
    artifactReader: InMemoryRestoreArtifactBodyReader;
    eventId: string;
    indexReader: InMemoryRestoreIndexStateReader;
    operation: 'D' | 'U';
    recordSysId: string;
}): void {
    const artifactKey = `rez/restore/event=${input.eventId}.artifact.json`;
    const manifestKey = `rez/restore/event=${input.eventId}.manifest.json`;

    input.indexReader.upsertIndexedEventCandidate({
        artifactKey,
        eventId: input.eventId,
        eventTime: '2026-02-16T11:59:59.000Z',
        instanceId: 'sn-dev-01',
        manifestKey,
        offset: input.operation === 'D' ? '101' : '102',
        partition: 1,
        recordSysId: input.recordSysId,
        source: 'sn://acme-dev.service-now.com',
        sysModCount: input.operation === 'D' ? 3 : 2,
        sysUpdatedOn: '2026-02-16 11:59:59',
        table: 'incident',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    });
    input.artifactReader.setArtifactBody({
        artifactKey,
        body: {
            __op: input.operation,
            __schema_version: 3,
            __type: input.operation === 'D' ? 'cdc.delete' : 'cdc.write',
            row_enc: {
                alg: 'AES-256-CBC',
                module: 'x_rezrp_rezilient.encrypter',
                format: 'kmf',
                compression: 'none',
                ciphertext: `cipher-${input.recordSysId}`,
            },
        },
        manifestKey,
    });
}

function createAuthoritativeWatermark(): Record<string, unknown> {
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
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-16T12:00:00.000Z',
    };
}

function buildActionCounts(
    rows: ReturnType<typeof createRow>[],
    conflicts: Record<string, unknown>[],
    mediaCandidates: Record<string, unknown>[],
) {
    const counts = {
        update: 0,
        insert: 0,
        delete: 0,
        skip: 0,
        conflict: conflicts.length,
        attachment_apply: 0,
        attachment_skip: 0,
    };

    for (const row of rows) {
        counts[row.action] += 1;
    }

    for (const candidate of mediaCandidates) {
        if (candidate.decision === 'include') {
            counts.attachment_apply += 1;
        } else if (candidate.decision === 'exclude') {
            counts.attachment_skip += 1;
        }
    }

    return counts;
}

function buildFinalizedPlanRecord(
    planId: string,
    rows: ReturnType<typeof createRow>[],
    conflicts: Record<string, unknown>[] = [],
    mediaCandidates: Record<string, unknown>[] = [],
): RestoreDryRunPlanRecord {
    const request: CreateDryRunPlanRequest = {
        input_mode: 'scope_driven',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        requested_by: 'operator@example.com',
        pit: {
            restore_time: '2026-02-16T12:00:00.000Z',
            restore_timezone: 'UTC',
            pit_algorithm_version:
                'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
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
            tables: ['incident'],
            record_sys_ids: rows.map((row) => row.record_sys_id),
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        rows: rows as CreateDryRunPlanRequest['rows'],
        conflicts: conflicts as CreateDryRunPlanRequest['conflicts'],
        delete_candidates: [],
        media_candidates:
            mediaCandidates as CreateDryRunPlanRequest['media_candidates'],
        watermarks: [{
            topic: 'rez.cdc',
            partition: 1,
        }],
        pit_candidates: [],
        approval: buildApprovalPlaceholder(),
    };
    const actionCounts = buildActionCounts(rows, conflicts, mediaCandidates);
    const planHashInput = buildPlanHashInput(request, actionCounts);
    const planHashData = computeRestorePlanHash(planHashInput);
    const watermark = RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    );

    return {
        tenant_id: request.tenant_id,
        instance_id: request.instance_id,
        source: request.source,
        plan: RestorePlanSchema.parse({
            contract_version: RESTORE_CONTRACT_VERSION,
            plan_id: request.plan_id,
            plan_hash: planHashData.plan_hash,
            plan_hash_algorithm: PLAN_HASH_ALGORITHM,
            plan_hash_input_version: PLAN_HASH_INPUT_VERSION,
            generated_at: '2026-02-16T12:00:00.000Z',
            pit: request.pit,
            scope: request.scope,
            execution_options: request.execution_options,
            action_counts: actionCounts,
            conflicts: request.conflicts,
            approval: buildApprovalPlaceholder(),
            metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
        }),
        plan_hash_input: planHashInput,
        gate: {
            executability: 'executable',
            reason_code: 'none',
            unresolved_delete_candidates: 0,
            unresolved_media_candidates: 0,
            unresolved_hard_block_conflicts: 0,
            stale_partition_count: 0,
            unknown_partition_count: 0,
        },
        delete_candidates: [],
        media_candidates:
            JSON.parse(JSON.stringify(mediaCandidates)) as
            RestoreDryRunPlanRecord['media_candidates'],
        pit_resolutions: [],
        watermarks: [watermark],
    };
}

async function buildFixture(options?: {
    executeConfig?: {
        defaultChunkSize?: number;
        maxRows?: number;
        elevatedSkipRatioPercent?: number;
        maxChunksPerAttempt?: number;
        mediaChunkSize?: number;
        mediaMaxItems?: number;
        mediaMaxBytes?: number;
        mediaMaxRetryAttempts?: number;
    };
    rows?: ReturnType<typeof createRow>[];
    mediaCandidates?: Record<string, unknown>[];
    requiredCapabilities?: string[];
    targetWriter?: RestoreTargetWriter;
    stateStore?: RestoreExecutionStateStore;
    targetStateLookup?: RestoreTargetStateLookup;
}) {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const planState = new InMemoryRestorePlanStateStore();
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));

    const plans = new RestorePlanService(
        registry,
        now,
        planState,
        restoreIndexReader,
        undefined,
        undefined,
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        registry,
        now,
        undefined,
        undefined,
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
        options?.executeConfig,
        now,
        options?.stateStore,
        options?.targetWriter || new NoopRestoreTargetWriter(),
        options?.targetStateLookup || new NoopRestoreTargetStateLookup(),
    );
    const planRows = options?.rows || [
        createRow('row-01'),
        createRow('row-02'),
    ];
    const plan = buildFinalizedPlanRecord(
        'plan-1',
        planRows,
        [],
        options?.mediaCandidates || [],
    );
    await planState.mutate((state) => {
        state.plans_by_id[plan.plan.plan_id] = plan;
    });

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.plan.plan_id,
            plan_hash: plan.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: options?.requiredCapabilities || [
                'restore_execute',
            ],
            requested_by: 'operator@example.com',
        },
        claims(),
    );

    assert.equal(job.success, true);
    if (!job.success) {
        throw new Error('failed to create job fixture');
    }

    return {
        execute,
        jobId: job.job.job_id,
    };
}

async function buildScopeDrivenFixture(): Promise<{
    execute: RestoreExecutionService;
    jobId: string;
    materializedRows: Array<{
        action: 'delete' | 'insert' | 'skip' | 'update';
        record_sys_id: string;
    }>;
    targetWriter: RecordingTargetWriter;
}> {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();
    const artifactReader = new InMemoryRestoreArtifactBodyReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));
    seedScopeMaterializationCandidate({
        artifactReader,
        eventId: 'evt-scope-delete',
        indexReader: restoreIndexReader,
        operation: 'D',
        recordSysId: 'rec-scope-delete',
    });
    seedScopeMaterializationCandidate({
        artifactReader,
        eventId: 'evt-scope-update',
        indexReader: restoreIndexReader,
        operation: 'U',
        recordSysId: 'rec-scope-update',
    });

    const plans = new RestorePlanService(
        registry,
        now,
        undefined,
        restoreIndexReader,
        undefined,
        new RestoreRowMaterializationService(
            artifactReader,
            new NoopRestoreTargetStateLookup(),
        ),
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        registry,
        now,
        undefined,
        undefined,
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
    const targetWriter = new RecordingTargetWriter();
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        undefined,
        now,
        undefined,
        targetWriter,
        new NoopRestoreTargetStateLookup(),
    );
    const draft = await plans.createDryRunPlan(
        createScopeDrivenDryRunPayload('plan-scope-exec'),
        claims(),
    );

    assert.equal(draft.success, true);
    if (!draft.success) {
        throw new Error('failed to create scope-driven dry-run plan fixture');
    }
    assert.equal(draft.reconciliation_state, 'draft');

    const finalized = await plans.finalizeTargetReconciliation(
        'plan-scope-exec',
        {
            finalized_by: 'sn-worker',
            reconciled_records: [
                {
                    table: 'incident',
                    record_sys_id: 'rec-scope-delete',
                    target_state: 'exists',
                },
                {
                    table: 'incident',
                    record_sys_id: 'rec-scope-update',
                    target_state: 'exists',
                },
            ],
        },
        claims(),
    );

    assert.equal(finalized.success, true);
    if (!finalized.success) {
        throw new Error('failed to finalize scope-driven dry-run fixture');
    }

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: finalized.record.plan.plan_id,
            plan_hash: finalized.record.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_delete', 'restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );

    assert.equal(job.success, true);
    if (!job.success) {
        throw new Error('failed to create scope-driven job fixture');
    }

    return {
        execute,
        jobId: job.job.job_id,
        materializedRows: finalized.record.plan_hash_input.rows.map((row) => ({
            action: row.action,
            record_sys_id: row.record_sys_id,
        })),
        targetWriter,
    };
}

test(
    'scope-driven dry-run persists materialized rows that execute uses unchanged',
    async () => {
        const fixture = await buildScopeDrivenFixture();
        const result = await fixture.execute.executeJob(
            fixture.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_delete', 'restore_execute'],
            },
            claims(),
        );

        assert.equal(result.success, true);
        if (!result.success) {
            return;
        }

        assert.equal(result.statusCode, 200);
        assert.equal(result.record.status, 'completed');
        assert.equal(result.record.summary.applied_rows, 2);

        for (const row of fixture.materializedRows) {
            assert.equal(
                fixture.targetWriter.appliedActionsByRecord.get(
                    row.record_sys_id,
                ),
                row.action,
            );
        }
    },
);

test('unresolved media candidates block execution until decisions are set', async () => {
    const unresolved = createMediaCandidate('media-unresolved');

    delete unresolved.decision;

    const fixture = await buildFixture({
        mediaCandidates: [unresolved],
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, false);

    if (result.success) {
        return;
    }

    assert.equal(result.statusCode, 409);
    assert.equal(result.reasonCode, 'blocked_unresolved_media_candidates');
});

test('media hard-cap enforcement requires override capability and confirmation', async () => {
    const mediaCandidates = [
        createMediaCandidate('media-01', {
            size_bytes: 64,
        }),
        createMediaCandidate('media-02', {
            size_bytes: 64,
        }),
    ];
    const fixtureWithoutOverride = await buildFixture({
        mediaCandidates,
        executeConfig: {
            mediaMaxItems: 1,
            mediaMaxBytes: 80,
        },
    });
    const blocked = await fixtureWithoutOverride.execute.executeJob(
        fixtureWithoutOverride.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(blocked.success, false);

    if (blocked.success) {
        return;
    }

    assert.equal(blocked.statusCode, 403);
    assert.equal(blocked.reasonCode, 'blocked_missing_capability');
    assert.match(blocked.message, /elevated confirmation/i);

    const fixtureWithOverride = await buildFixture({
        mediaCandidates,
        executeConfig: {
            mediaMaxItems: 1,
            mediaMaxBytes: 80,
        },
    });
    const allowed = await fixtureWithOverride.execute.executeJob(
        fixtureWithOverride.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: [
                'restore_execute',
                'restore_override_caps',
            ],
            elevated_confirmation: {
                confirmed: true,
                confirmation: 'I UNDERSTAND',
                reason: 'approved media cap override',
            },
        },
        claims(),
    );

    assert.equal(allowed.success, true);
});

test('media outcomes record parent checks, hash verification, and retry results', async () => {
    const mediaCandidates = [
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
        createMediaCandidate('media-applied-after-retry', {
            retryable_failures: 1,
            max_retry_attempts: 3,
        }),
        createMediaCandidate('media-excluded', {
            decision: 'exclude',
        }),
    ];
    const fixture = await buildFixture({
        mediaCandidates,
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    const byId = new Map<string, Record<string, unknown>>();

    for (const outcome of result.record.media_outcomes) {
        byId.set(outcome.candidate_id, outcome as unknown as Record<
            string,
            unknown
        >);
    }

    assert.equal(result.record.status, 'failed');
    assert.equal(result.record.summary.attachments_planned, 5);
    assert.equal(result.record.summary.attachments_applied, 1);
    assert.equal(result.record.summary.attachments_skipped, 1);
    assert.equal(result.record.summary.attachments_failed, 3);
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
        byId.get('media-applied-after-retry')?.outcome,
        'applied',
    );
    assert.equal(byId.get('media-excluded')?.outcome, 'skipped');
});

test('conflict matrix allows skip for non-reference conflict classes', async () => {
    const classes = [
        'value_conflict',
        'missing_row_conflict',
        'unexpected_existing_conflict',
        'schema_conflict',
        'permission_conflict',
        'stale_conflict',
    ] as const;

    for (const conflictClass of classes) {
        const fixture = await buildFixture({
            executeConfig: {
                elevatedSkipRatioPercent: 100,
            },
        });
        const reasonCode = conflictClass === 'schema_conflict'
            ? 'failed_schema_conflict'
            : conflictClass === 'permission_conflict'
            ? 'failed_permission_conflict'
            : 'failed_internal_error';
        const result = await fixture.execute.executeJob(
            fixture.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                chunk_size: 2,
                runtime_conflicts: [
                    {
                        conflict_id: `conf-${conflictClass}`,
                        row_id: 'row-01',
                        class: conflictClass,
                        reason_code: reasonCode,
                        reason: 'runtime drift',
                        resolution: 'skip',
                    },
                ],
            },
            claims(),
        );

        assert.equal(result.success, true);

        if (!result.success) {
            continue;
        }

        const row = result.record.row_outcomes.find((entry) =>
            entry.row_id === 'row-01'
        );

        assert.equal(row?.outcome, 'skipped');
        assert.equal(row?.conflict_class, conflictClass);
        assert.equal(result.record.summary.fallback_chunk_count, 1);
    }
});

test('reference conflicts hard-block execution', async () => {
    const fixture = await buildFixture();
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            runtime_conflicts: [
                {
                    conflict_id: 'conf-reference',
                    row_id: 'row-01',
                    class: 'reference_conflict',
                    reason_code: 'blocked_reference_conflict',
                    reason: 'reference missing',
                    resolution: 'abort_and_replan',
                },
            ],
        },
        claims(),
    );

    assert.equal(result.success, false);

    if (result.success) {
        return;
    }

    assert.equal(result.statusCode, 409);
    assert.equal(result.reasonCode, 'blocked_reference_conflict');
});

test('missing delete capability blocks destructive actions', async () => {
    const fixture = await buildFixture({
        rows: [
            createRow('row-01', 'delete'),
            createRow('row-02', 'update'),
        ],
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, false);

    if (result.success) {
        return;
    }

    assert.equal(result.statusCode, 403);
    assert.equal(result.reasonCode, 'blocked_missing_capability');
    assert.match(result.message, /restore_delete/);
});

test('override capability and elevated confirmation are enforced', async () => {
    const fixture = await buildFixture({
        executeConfig: {
            elevatedSkipRatioPercent: 20,
        },
    });
    const withoutOverride = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            runtime_conflicts: [
                {
                    conflict_id: 'conf-override',
                    row_id: 'row-01',
                    class: 'value_conflict',
                    reason_code: 'failed_internal_error',
                    reason: 'drift detected',
                    resolution: 'skip',
                },
            ],
        },
        claims(),
    );

    assert.equal(withoutOverride.success, false);

    if (withoutOverride.success) {
        return;
    }

    assert.equal(withoutOverride.statusCode, 403);
    assert.equal(withoutOverride.reasonCode, 'blocked_missing_capability');
    assert.match(withoutOverride.message, /elevated confirmation/i);

    const fixtureWithOverride = await buildFixture({
        executeConfig: {
            elevatedSkipRatioPercent: 20,
        },
    });
    const withOverride = await fixtureWithOverride.execute.executeJob(
        fixtureWithOverride.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: [
                'restore_execute',
                'restore_override_caps',
            ],
            runtime_conflicts: [
                {
                    conflict_id: 'conf-override',
                    row_id: 'row-01',
                    class: 'value_conflict',
                    reason_code: 'failed_internal_error',
                    reason: 'drift detected',
                    resolution: 'skip',
                },
            ],
            elevated_confirmation: {
                confirmed: true,
                confirmation: 'I UNDERSTAND',
                reason: 'operator-approved high skip ratio',
            },
        },
        claims(),
    );

    assert.equal(withOverride.success, true);
});

test('execute routes update/insert/delete rows through target writer apply path', async () => {
    const targetWriter = new RecordingTargetWriter();
    const fixture = await buildFixture({
        rows: [
            createRow('row-update', 'update'),
            createRow('row-insert', 'insert'),
            createRow('row-delete', 'delete'),
            createRow('row-skip', 'skip'),
        ],
        executeConfig: {
            elevatedSkipRatioPercent: 100,
        },
        targetWriter,
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: [
                'restore_delete',
                'restore_execute',
            ],
            chunk_size: 4,
        },
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    const applyActions = targetWriter.applyCalls
        .map((call) => call.row.action)
        .sort((left, right) => left.localeCompare(right));

    assert.deepEqual(applyActions, [
        'delete',
        'insert',
        'update',
    ]);
    assert.equal(targetWriter.applyCalls.length, 3);
    assert.equal(targetWriter.appliedActionsByRecord.size, 3);
    assert.equal(result.record.summary.applied_rows, 3);
    assert.equal(result.record.summary.skipped_rows, 1);
    assert.equal(result.record.summary.failed_rows, 0);

    const skippedOutcome = result.record.row_outcomes.find((outcome) =>
        outcome.row_id === 'row-skip'
    );

    assert.equal(skippedOutcome?.outcome, 'skipped');
    const journal = await fixture.execute.getRollbackJournal(fixture.jobId);

    assert.notEqual(journal, null);
    assert.equal(journal?.journal_entries.length, 3);
    assert.equal(journal?.sn_mirror_entries.length, 3);
});

test('target writer failed apply yields failed row outcome and truthful summary', async () => {
    const targetWriter = new RecordingTargetWriter();

    targetWriter.failRecord(
        'rec-row-02',
        'failed_permission_conflict',
        'target rejected write',
    );

    const fixture = await buildFixture({
        rows: [
            createRow('row-01'),
            createRow('row-02'),
        ],
        targetWriter,
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 2,
        },
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    assert.equal(result.record.status, 'failed');
    assert.equal(result.record.summary.applied_rows, 1);
    assert.equal(result.record.summary.failed_rows, 1);
    assert.equal(result.record.summary.skipped_rows, 0);
    assert.equal(result.record.chunks[0].status, 'failed');
    assert.equal(result.record.chunks[0].applied_count, 1);
    assert.equal(result.record.chunks[0].failed_count, 1);

    const failedOutcome = result.record.row_outcomes.find((outcome) =>
        outcome.row_id === 'row-02'
    );

    assert.equal(failedOutcome?.outcome, 'failed');
    assert.equal(
        failedOutcome?.reason_code,
        'failed_permission_conflict',
    );
    assert.equal(failedOutcome?.message, 'target rejected write');
    const journal = await fixture.execute.getRollbackJournal(fixture.jobId);

    assert.notEqual(journal, null);
    assert.equal(journal?.journal_entries.length, 1);
    assert.equal(journal?.sn_mirror_entries.length, 1);
});

test('chunk failure falls back to row isolation and records outcomes', async () => {
    const fixture = await buildFixture({
        rows: [
            createRow('row-01'),
            createRow('row-02'),
            createRow('row-03'),
        ],
        executeConfig: {
            elevatedSkipRatioPercent: 100,
        },
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 3,
            runtime_conflicts: [
                {
                    conflict_id: 'conf-row-02',
                    row_id: 'row-02',
                    class: 'value_conflict',
                    reason_code: 'failed_internal_error',
                    reason: 'runtime mismatch',
                    resolution: 'skip',
                },
            ],
        },
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    assert.equal(result.record.chunks.length, 1);
    assert.equal(result.record.chunks[0].status, 'row_fallback');
    assert.equal(result.record.summary.applied_rows, 2);
    assert.equal(result.record.summary.skipped_rows, 1);
    assert.equal(result.record.summary.failed_rows, 0);
    assert.equal(result.record.summary.fallback_chunk_count, 1);
});

test('checkpoint does not advance to next chunk until chunk progress is durably persisted', async () => {
    const targetWriter = new RecordingTargetWriter();
    const fixture = await buildFixture({
        rows: [
            createRow('row-01'),
            createRow('row-02'),
        ],
        targetWriter,
        stateStore: new FailOnMutateExecutionStateStore(2),
    });

    await assert.rejects(
        async () => fixture.execute.executeJob(
            fixture.jobId,
            {
                operator_id: 'operator@example.com',
                operator_capabilities: ['restore_execute'],
                chunk_size: 2,
            },
            claims(),
        ),
        /simulated execution state persist failure/,
    );

    const checkpoint = await fixture.execute.getCheckpoint(fixture.jobId);

    assert.notEqual(checkpoint, null);
    assert.equal(checkpoint?.next_chunk_index, 0);
    assert.equal(checkpoint?.next_row_index, 1);

    const journal = await fixture.execute.getRollbackJournal(fixture.jobId);

    assert.notEqual(journal, null);
    assert.equal(journal?.journal_entries.length, 1);
    assert.equal(journal?.sn_mirror_entries.length, 1);
    assert.equal(targetWriter.applyCalls.length, 2);
});

test('resume continues from checkpoint when execution pauses by chunk budget', async () => {
    const fixture = await buildFixture({
        rows: [
            createRow('row-01'),
            createRow('row-02'),
            createRow('row-03'),
        ],
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
    });
    const first = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 1,
        },
        claims(),
    );

    assert.equal(first.success, true);

    if (!first.success) {
        return;
    }

    assert.equal(first.statusCode, 202);
    assert.equal(first.record.status, 'paused');
    assert.equal(first.record.checkpoint.next_chunk_index, 1);
    assert.equal(first.record.checkpoint.next_row_index, 0);
    assert.equal(first.record.summary.applied_rows, 1);

    const resumed = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(resumed.success, true);

    if (!resumed.success) {
        return;
    }

    assert.equal(resumed.statusCode, 202);
    assert.equal(resumed.record.status, 'paused');
    assert.equal(resumed.record.checkpoint.next_chunk_index, 2);
    assert.equal(resumed.record.checkpoint.next_row_index, 0);

    const resumedAgain = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(resumedAgain.success, true);

    if (!resumedAgain.success) {
        return;
    }

    assert.equal(resumedAgain.statusCode, 200);
    assert.equal(resumedAgain.record.status, 'completed');
    assert.equal(resumedAgain.record.summary.applied_rows, 3);
    assert.equal(resumedAgain.record.checkpoint.next_chunk_index, 3);
    assert.equal(resumedAgain.record.checkpoint.next_row_index, 0);
});

test('partial-failure chunk pause/resume remains truthful and avoids double-apply of successful rows', async () => {
    const targetWriter = new RecordingTargetWriter();

    targetWriter.failRecord(
        'rec-row-02',
        'failed_permission_conflict',
        'target rejected write',
    );

    const fixture = await buildFixture({
        rows: [
            createRow('row-01'),
            createRow('row-02'),
            createRow('row-03'),
        ],
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
        targetWriter,
    });

    const first = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 2,
        },
        claims(),
    );

    assert.equal(first.success, true);

    if (!first.success) {
        return;
    }

    assert.equal(first.statusCode, 202);
    assert.equal(first.record.status, 'paused');
    assert.equal(first.record.summary.applied_rows, 1);
    assert.equal(first.record.summary.failed_rows, 1);
    assert.equal(first.record.summary.skipped_rows, 0);
    assert.equal(first.record.checkpoint.next_chunk_index, 1);
    assert.equal(first.record.checkpoint.next_row_index, 0);

    const resumed = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(resumed.success, true);

    if (!resumed.success) {
        return;
    }

    assert.equal(resumed.statusCode, 200);
    assert.equal(resumed.record.status, 'failed');
    assert.equal(resumed.record.summary.applied_rows, 2);
    assert.equal(resumed.record.summary.failed_rows, 1);
    assert.equal(resumed.record.summary.skipped_rows, 0);

    const callCountByRow = new Map<string, number>();

    for (const call of targetWriter.applyCalls) {
        const rowId = call.row.row_id;
        callCountByRow.set(rowId, (callCountByRow.get(rowId) || 0) + 1);
    }

    assert.equal(callCountByRow.get('row-01'), 1);
    assert.equal(callCountByRow.get('row-02'), 1);
    assert.equal(callCountByRow.get('row-03'), 1);

    const journal = await fixture.execute.getRollbackJournal(fixture.jobId);

    assert.notEqual(journal, null);
    assert.equal(journal?.journal_entries.length, 2);
    assert.equal(journal?.sn_mirror_entries.length, 2);

    for (const entry of journal?.journal_entries || []) {
        assert.notEqual(entry.plan_row_id, 'row-02');
    }
});

test('duplicate resume attempts are idempotent after completion', async () => {
    const fixture = await buildFixture({
        executeConfig: {
            maxChunksPerAttempt: 1,
            elevatedSkipRatioPercent: 100,
        },
    });

    const first = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 1,
        },
        claims(),
    );

    assert.equal(first.success, true);

    if (!first.success) {
        return;
    }

    const second = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(second.success, true);

    if (!second.success) {
        return;
    }

    const third = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(third.success, true);

    if (!third.success) {
        return;
    }

    assert.equal(third.statusCode, 200);
    assert.equal(third.record.status, 'completed');
    assert.equal(third.record.summary.applied_rows, 2);
    assert.equal(third.record.resume_attempt_count, 2);
});

test('resume fails when persisted checkpoint_id does not match row cursor', async () => {
    const stateStore = new InMemoryRestoreExecutionStateStore();
    const fixture = await buildFixture({
        executeConfig: {
            maxChunksPerAttempt: 1,
        },
        stateStore,
    });
    const first = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 1,
        },
        claims(),
    );

    assert.equal(first.success, true);
    if (!first.success) {
        return;
    }

    await stateStore.mutate((state) => {
        const record = state.records_by_job_id[fixture.jobId];

        if (!record) {
            throw new Error('missing execution record in test fixture');
        }

        record.checkpoint.checkpoint_id = 'chk_tampered';
    });

    const resumed = await fixture.execute.resumeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            expected_plan_checksum: first.record.plan_checksum,
            expected_precondition_checksum: first.record.precondition_checksum,
        },
        claims(),
    );

    assert.equal(resumed.success, false);
    if (resumed.success) {
        return;
    }

    assert.equal(resumed.statusCode, 409);
    assert.equal(resumed.error, 'resume_checkpoint_mismatch');
    assert.equal(
        resumed.reasonCode,
        'blocked_resume_precondition_mismatch',
    );
    assert.match(resumed.message, /checkpoint_id/i);
});

test('execute blocks when execute-time target revalidation detects drift', async () => {
    const targetStateLookup = new InMemoryRestoreTargetStateLookup();

    targetStateLookup.setTargetRecordState({
        record_sys_id: 'rec-row-01',
        state: 'missing',
        table: 'incident',
    });

    const fixture = await buildFixture({
        rows: [createRow('row-01', 'update')],
        targetStateLookup,
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, false);
    if (result.success) {
        return;
    }

    assert.equal(result.statusCode, 409);
    assert.equal(result.error, 'target_revalidation_failed');
    assert.equal(result.reasonCode, 'blocked_reference_conflict');
    assert.match(result.message, /target revalidation/i);
});

test('rollback journal includes authoritative entries and SN mirror linkage', async () => {
    const fixture = await buildFixture({
        executeConfig: {
            elevatedSkipRatioPercent: 100,
        },
    });
    const result = await fixture.execute.executeJob(
        fixture.jobId,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
            chunk_size: 2,
        },
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    const bundle = await fixture.execute.getRollbackJournal(fixture.jobId);

    assert.notEqual(bundle, null);
    assert.equal(bundle?.journal_entries.length, 2);
    assert.equal(bundle?.sn_mirror_entries.length, 2);
    assert.equal(
        bundle?.journal_entries[0].journal_id,
        bundle?.sn_mirror_entries[0].journal_id,
    );
});

test('executeJob rejects when job not in running state', async () => {
    const fixture = await buildFixture();

    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const planState = new InMemoryRestorePlanStateStore();
    const jobService = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
        undefined,
        undefined,
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
    const restoreIndexReader =
        new InMemoryRestoreIndexStateReader();
    restoreIndexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(
            createAuthoritativeWatermark(),
        ),
    );
    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        planState,
        restoreIndexReader,
        undefined,
        undefined,
    );
    const plan = buildFinalizedPlanRecord('plan-paused', [
        createRow('row-01'),
    ]);
    await planState.mutate((state) => {
        state.plans_by_id[plan.plan.plan_id] = plan;
    });
    const job = await jobService.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.plan.plan_id,
            plan_hash: plan.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );
    assert.equal(job.success, true);
    if (!job.success) {
        return;
    }

    await jobService.pauseJob(
        job.job.job_id,
        'paused_token_refresh_grace_exhausted',
    );

    const execute = new RestoreExecutionService(
        jobService,
        plans,
        {},
        now,
    );
    const result = await execute.executeJob(
        job.job.job_id,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 409);
    }
});

test('executeJob rejects when plan not found', async () => {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const jobService = new RestoreJobService(
        new RestoreLockManager(),
        registry,
        now,
        undefined,
        undefined,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await planService.getPlan(planId);

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
    const planState = new InMemoryRestorePlanStateStore();
    const restoreIndexReader =
        new InMemoryRestoreIndexStateReader();
    restoreIndexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(
            createAuthoritativeWatermark(),
        ),
    );
    const planService = new RestorePlanService(
        registry,
        now,
        planState,
        restoreIndexReader,
        undefined,
        undefined,
    );
    const plan = buildFinalizedPlanRecord('plan-exec-missing', [
        createRow('row-01'),
    ]);
    await planState.mutate((state) => {
        state.plans_by_id[plan.plan.plan_id] = plan;
    });
    const job = await jobService.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.plan.plan_id,
            plan_hash: plan.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );
    assert.equal(job.success, true);
    if (!job.success) {
        return;
    }

    const emptyPlanService = new RestorePlanService(
        registry,
        now,
    );
    const execute = new RestoreExecutionService(
        jobService,
        emptyPlanService,
        {},
        now,
    );
    const result = await execute.executeJob(
        job.job.job_id,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 409);
        assert.match(result.message, /plan/i);
    }
});

test('getExecution returns null for unknown job', async () => {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const execute = new RestoreExecutionService(
        new RestoreJobService(
            new RestoreLockManager(),
            registry,
            now,
        ),
        new RestorePlanService(registry, now),
        {},
        now,
    );

    const result = await execute.getExecution(
        'nonexistent',
    );

    assert.equal(result, null);
});

test('getCheckpoint returns null for unknown job', async () => {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const execute = new RestoreExecutionService(
        new RestoreJobService(
            new RestoreLockManager(),
            registry,
            now,
        ),
        new RestorePlanService(registry, now),
        {},
        now,
    );

    const result = await execute.getCheckpoint(
        'nonexistent',
    );

    assert.equal(result, null);
});

test('claim blocks when execute-time target revalidation fails', async () => {
    const targetStateLookup = new InMemoryRestoreTargetStateLookup();

    targetStateLookup.setTargetRecordState({
        record_sys_id: 'rec-row-01',
        state: 'missing',
        table: 'incident',
    });

    const fixture = await buildFixture({
        rows: [createRow('row-01', 'update')],
        targetStateLookup,
    });
    const claim = await fixture.execute.claimBatch(
        fixture.jobId,
        {
            operator_id: 'sn-worker',
            max_rows: 1,
        },
        claims(),
    );

    assert.equal(claim.success, false);
    if (claim.success) {
        return;
    }

    assert.equal(claim.statusCode, 409);
    assert.equal(claim.error, 'target_revalidation_failed');
    assert.equal(claim.reasonCode, 'blocked_reference_conflict');
});

test(
    'claim/commit keeps summary totals tied to committed row outcomes',
    async () => {
        const fixture = await buildFixture();
        const claim = await fixture.execute.claimBatch(
            fixture.jobId,
            {
                operator_id: 'sn-worker',
                max_rows: 1,
            },
            claims(),
        );

        assert.equal(claim.success, true);
        if (!claim.success) {
            return;
        }

        assert.equal(claim.response.accepted, true);
        assert.equal(claim.response.claimed_rows.length, 1);
        assert.equal(claim.response.claimed_rows[0]?.row_id, 'row-01');

        const preCommitExecution = await fixture.execute.getExecution(
            fixture.jobId,
        );

        assert.ok(preCommitExecution);
        assert.equal(preCommitExecution?.summary.applied_rows, 0);
        assert.equal(preCommitExecution?.row_outcomes.length, 0);

        const commit1 = await fixture.execute.commitBatch(
            fixture.jobId,
            {
                claim_id: String(claim.response.claim_id || ''),
                committed_by: 'sn-worker',
                row_outcomes: [{
                    row_id: 'row-01',
                    outcome: 'applied',
                    reason_code: 'none',
                }],
            },
            claims(),
        );

        assert.equal(commit1.success, true);
        if (!commit1.success) {
            return;
        }

        assert.equal(commit1.response.accepted, true);
        assert.equal(commit1.response.execution_status, 'paused');
        assert.equal(commit1.response.summary?.applied_rows, 1);
        assert.equal(commit1.response.summary?.failed_rows, 0);

        const claim2 = await fixture.execute.claimBatch(
            fixture.jobId,
            {
                operator_id: 'sn-worker',
                max_rows: 1,
            },
            claims(),
        );

        assert.equal(claim2.success, true);
        if (!claim2.success) {
            return;
        }

        assert.equal(claim2.response.accepted, true);
        assert.equal(claim2.response.claimed_rows[0]?.row_id, 'row-02');

        const commit2 = await fixture.execute.commitBatch(
            fixture.jobId,
            {
                claim_id: String(claim2.response.claim_id || ''),
                committed_by: 'sn-worker',
                row_outcomes: [{
                    row_id: 'row-02',
                    outcome: 'failed',
                    reason_code: 'failed_permission_conflict',
                    message: 'target rejected write',
                }],
            },
            claims(),
        );

        assert.equal(commit2.success, true);
        if (!commit2.success) {
            return;
        }

        assert.equal(commit2.response.accepted, true);
        assert.equal(commit2.response.execution_status, 'failed');
        assert.equal(commit2.response.summary?.applied_rows, 1);
        assert.equal(commit2.response.summary?.failed_rows, 1);
        assert.equal(commit2.response.reason_code, 'failed_internal_error');

        const execution = await fixture.execute.getExecution(
            fixture.jobId,
        );
        const rollbackJournal = await fixture.execute.getRollbackJournal(
            fixture.jobId,
        );

        assert.ok(execution);
        assert.equal(execution?.status, 'failed');
        assert.equal(execution?.summary.applied_rows, 1);
        assert.equal(execution?.summary.failed_rows, 1);
        assert.equal(execution?.row_outcomes.length, 2);
        assert.equal(rollbackJournal?.journal_entries.length, 1);
    },
);

test(
    'claim/commit keeps rollback journal and mirror aligned to applied outcomes only',
    async () => {
        const fixture = await buildFixture();
        const claim = await fixture.execute.claimBatch(
            fixture.jobId,
            {
                operator_id: 'sn-worker',
                max_rows: 2,
            },
            claims(),
        );

        assert.equal(claim.success, true);
        if (!claim.success) {
            return;
        }

        assert.equal(claim.response.accepted, true);
        assert.equal(claim.response.claimed_rows.length, 2);
        assert.equal(claim.response.claimed_rows[0]?.row_id, 'row-01');
        assert.equal(claim.response.claimed_rows[1]?.row_id, 'row-02');

        const commit = await fixture.execute.commitBatch(
            fixture.jobId,
            {
                claim_id: String(claim.response.claim_id || ''),
                committed_by: 'sn-worker',
                row_outcomes: [{
                    row_id: 'row-01',
                    outcome: 'applied',
                    reason_code: 'none',
                }, {
                    row_id: 'row-02',
                    outcome: 'skipped',
                    reason_code: 'none',
                    message: 'target row is missing; delete was skipped',
                }],
            },
            claims(),
        );

        assert.equal(commit.success, true);
        if (!commit.success) {
            return;
        }

        assert.equal(commit.response.accepted, true);
        assert.equal(commit.response.execution_status, 'completed');
        assert.equal(commit.response.summary?.applied_rows, 1);
        assert.equal(commit.response.summary?.skipped_rows, 1);
        assert.equal(commit.response.summary?.failed_rows, 0);

        const execution = await fixture.execute.getExecution(
            fixture.jobId,
        );
        const rollbackJournal = await fixture.execute.getRollbackJournal(
            fixture.jobId,
        );

        assert.ok(execution);
        assert.equal(execution?.status, 'completed');
        assert.equal(execution?.summary.applied_rows, 1);
        assert.equal(execution?.summary.skipped_rows, 1);
        assert.equal(execution?.summary.failed_rows, 0);
        assert.equal(rollbackJournal?.journal_entries.length, 1);
        assert.equal(rollbackJournal?.sn_mirror_entries.length, 1);
        assert.equal(
            rollbackJournal?.journal_entries[0]?.plan_row_id,
            'row-01',
        );
        assert.equal(
            rollbackJournal?.sn_mirror_entries[0]?.plan_row_id,
            'row-01',
        );
    },
);

test('commit blocks when committed row set mismatches active claim', async () => {
    const fixture = await buildFixture();
    const claim = await fixture.execute.claimBatch(
        fixture.jobId,
        {
            operator_id: 'sn-worker',
            max_rows: 2,
        },
        claims(),
    );

    assert.equal(claim.success, true);
    if (!claim.success) {
        return;
    }

    assert.equal(claim.response.accepted, true);
    assert.equal(claim.response.claimed_rows.length, 2);

    const commit = await fixture.execute.commitBatch(
        fixture.jobId,
        {
            claim_id: String(claim.response.claim_id || ''),
            committed_by: 'sn-worker',
            row_outcomes: [{
                row_id: 'row-01',
                outcome: 'applied',
                reason_code: 'none',
            }],
        },
        claims(),
    );

    assert.equal(commit.success, false);
    if (commit.success) {
        return;
    }

    assert.equal(commit.statusCode, 409);
    assert.equal(commit.error, 'claim_commit_mismatch');
    assert.equal(
        commit.reasonCode,
        'blocked_resume_precondition_mismatch',
    );

    const execution = await fixture.execute.getExecution(
        fixture.jobId,
    );

    assert.ok(execution);
    assert.equal(execution?.summary.applied_rows, 0);
    assert.equal(execution?.summary.failed_rows, 0);
    assert.equal(execution?.row_outcomes.length, 0);
});

test('claim blocks overlapping active claims until commit is accepted', async () => {
    const fixture = await buildFixture();
    const claim1 = await fixture.execute.claimBatch(
        fixture.jobId,
        {
            operator_id: 'sn-worker',
            max_rows: 1,
        },
        claims(),
    );

    assert.equal(claim1.success, true);
    if (!claim1.success) {
        return;
    }

    assert.equal(claim1.response.accepted, true);

    const claim2 = await fixture.execute.claimBatch(
        fixture.jobId,
        {
            operator_id: 'sn-worker',
            max_rows: 1,
        },
        claims(),
    );

    assert.equal(claim2.success, false);
    if (claim2.success) {
        return;
    }

    assert.equal(claim2.statusCode, 409);
    assert.equal(claim2.error, 'claim_in_progress');

    const commit = await fixture.execute.commitBatch(
        fixture.jobId,
        {
            claim_id: String(claim1.response.claim_id || ''),
            committed_by: 'sn-worker',
            row_outcomes: [{
                row_id: 'row-01',
                outcome: 'applied',
                reason_code: 'none',
            }],
        },
        claims(),
    );

    assert.equal(commit.success, true);
    if (!commit.success) {
        return;
    }

    const claim3 = await fixture.execute.claimBatch(
        fixture.jobId,
        {
            operator_id: 'sn-worker',
            max_rows: 1,
        },
        claims(),
    );

    assert.equal(claim3.success, true);
    if (!claim3.success) {
        return;
    }

    assert.equal(claim3.response.accepted, true);
    assert.equal(claim3.response.claimed_rows[0]?.row_id, 'row-02');
});
