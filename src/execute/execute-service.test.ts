import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { RestoreJobService } from '../jobs/job-service';
import { RestoreLockManager } from '../locks/lock-manager';
import { RestorePlanService } from '../plans/plan-service';
import { SourceRegistry } from '../registry/source-registry';
import { InMemoryRestoreIndexStateReader } from '../restore-index/state-reader';
import { RestoreExecutionService } from './execute-service';

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

function createDryRunPayload(
    planId: string,
    rows: ReturnType<typeof createRow>[],
    conflicts?: Record<string, unknown>[],
    mediaCandidates?: Record<string, unknown>[],
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
            encoded_query: 'active=true',
        },
        execution_options: {
            missing_row_mode: 'existing_only',
            conflict_policy: 'review_required',
            schema_compatibility_mode: 'compatible_only',
            workflow_mode: 'suppressed_default',
        },
        rows,
        conflicts: conflicts || [],
        delete_candidates: [],
        media_candidates: mediaCandidates || [],
        watermarks: [
            {
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
            },
        ],
        pit_candidates: [],
    };
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
}) {
    const registry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));

    const plans = new RestorePlanService(
        registry,
        now,
        undefined,
        restoreIndexReader,
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        registry,
        now,
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        options?.executeConfig,
        now,
    );
    const planRows = options?.rows || [
        createRow('row-01'),
        createRow('row-02'),
    ];
    const plan = await plans.createDryRunPlan(
        createDryRunPayload(
            'plan-1',
            planRows,
            undefined,
            options?.mediaCandidates,
        ),
        claims(),
    );

    assert.equal(plan.success, true);
    if (!plan.success) {
        throw new Error('failed to create dry-run plan fixture');
    }

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.record.plan.plan_id,
            plan_hash: plan.record.plan.plan_hash,
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

    const jobService = new RestoreJobService(
        new RestoreLockManager(),
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
    );
    const restoreIndexReader =
        new InMemoryRestoreIndexStateReader();
    restoreIndexReader.upsertWatermark(
        RestoreWatermarkSchema.parse(
            createAuthoritativeWatermark(),
        ),
    );
    const plans = new RestorePlanService(
        new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        now,
        undefined,
        restoreIndexReader,
    );
    const plan = await plans.createDryRunPlan(
        createDryRunPayload('plan-paused', [
            createRow('row-01'),
        ]),
        claims(),
    );
    assert.equal(plan.success, true);
    if (!plan.success) {
        return;
    }
    const job = await jobService.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.record.plan.plan_id,
            plan_hash: plan.record.plan.plan_hash,
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
    );
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
        undefined,
        restoreIndexReader,
    );
    const plan = await planService.createDryRunPlan(
        createDryRunPayload('plan-exec-missing', [
            createRow('row-01'),
        ]),
        claims(),
    );
    assert.equal(plan.success, true);
    if (!plan.success) {
        return;
    }
    const job = await jobService.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: plan.record.plan.plan_id,
            plan_hash: plan.record.plan.plan_hash,
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
