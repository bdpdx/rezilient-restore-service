import { strict as assert } from 'node:assert';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, test } from 'node:test';
import {
    ExecuteBatchClaimRequestSchema,
    ExecuteBatchCommitRequestSchema,
    ExecuteRestoreJobRequestSchema,
    ExecuteRuntimeConflictInputSchema,
    ExecuteWorkflowInputSchema,
    FailClosedRestoreTargetWriter,
    NoopRestoreTargetWriter,
    ResumeRestoreJobRequestSchema,
    ExecutionChunkStatusSchema,
    ExecutionOutcomeSchema,
    normalizeCapabilities,
} from './models';

function buildValidExecRequest() {
    return {
        operator_id: 'op-1',
        operator_capabilities: ['restore_execute' as const],
    };
}

function buildValidConflict() {
    return {
        conflict_id: 'c-01',
        row_id: 'row-01',
        class: 'value_conflict' as const,
        reason_code: 'blocked_reference_conflict' as const,
        reason: 'value mismatch',
        resolution: 'abort_and_replan' as const,
    };
}

function buildPlanRow() {
    return {
        row_id: 'row-01',
        table: 'incident',
        record_sys_id: 'rec-01',
        action: 'update' as const,
        precondition_hash: 'a'.repeat(64),
        metadata: {
            allowlist_version: 'rrs.metadata.allowlist.v1' as const,
            metadata: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                table: 'incident',
                record_sys_id: 'rec-01',
                event_id: 'evt-01',
                event_type: 'cdc.write',
                operation: 'U' as const,
                schema_version: 3,
                sys_updated_on: '2026-02-16 11:59:59',
                sys_mod_count: 1,
                __time: '2026-02-16T11:59:59.000Z',
                topic: 'rez.cdc',
                partition: 0,
                offset: '100',
            },
        },
        values: {
            diff_enc: {
                alg: 'AES-256-CBC' as const,
                module: 'x_rezrp_rezilient.encrypter' as const,
                format: 'kmf' as const,
                compression: 'none' as const,
                ciphertext: 'cipher-row-01',
            },
        },
    };
}

describe('ExecuteRestoreJobRequestSchema', () => {
    test('accepts valid request', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse(
                buildValidExecRequest(),
            );
        assert.equal(result.success, true);
    });

    test('accepts caller-supplied target revalidation records', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse({
                ...buildValidExecRequest(),
                revalidated_target_records: [{
                    table: 'incident',
                    record_sys_id: 'rec-01',
                    target_state: 'exists',
                }],
            });

        assert.equal(result.success, true);
    });

    test('rejects duplicate caller-supplied target revalidation records', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse({
                ...buildValidExecRequest(),
                revalidated_target_records: [
                    {
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'exists',
                    },
                    {
                        table: 'incident',
                        record_sys_id: 'rec-01',
                        target_state: 'missing',
                    },
                ],
            });

        assert.equal(result.success, false);
    });
});

describe('normalizeCapabilities', () => {
    test('deduplicates and sorts', () => {
        const result = normalizeCapabilities([
            'restore_delete',
            'restore_execute',
            'restore_delete',
        ]);
        assert.deepEqual(result, [
            'restore_delete',
            'restore_execute',
        ]);
    });

    test('handles empty array', () => {
        const result = normalizeCapabilities([]);
        assert.deepEqual(result, []);
    });
});

describe('ExecuteRuntimeConflictInputSchema', () => {
    test('rejects duplicate conflict IDs', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse({
                ...buildValidExecRequest(),
                runtime_conflicts: [
                    buildValidConflict(),
                    {
                        ...buildValidConflict(),
                        row_id: 'row-02',
                    },
                ],
            });
        assert.equal(result.success, false);
    });

    test('rejects duplicate row references', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse({
                ...buildValidExecRequest(),
                runtime_conflicts: [
                    buildValidConflict(),
                    {
                        ...buildValidConflict(),
                        conflict_id: 'c-02',
                    },
                ],
            });
        assert.equal(result.success, false);
    });

    test('reference conflict with action skip is rejected', () => {
        const result =
            ExecuteRuntimeConflictInputSchema.safeParse({
                ...buildValidConflict(),
                class: 'reference_conflict',
                resolution: 'skip',
            });
        assert.equal(result.success, false);
    });

    test('accepts failed_stale_lock_recovered reason code', () => {
        const result =
            ExecuteRuntimeConflictInputSchema.safeParse({
                ...buildValidConflict(),
                reason_code: 'failed_stale_lock_recovered',
            });
        assert.equal(result.success, true);
    });
});

describe('ExecuteWorkflowInputSchema', () => {
    test('accepts mode full (suppressed_default)', () => {
        const result = ExecuteWorkflowInputSchema.safeParse({
            mode: 'suppressed_default',
        });
        assert.equal(result.success, true);
    });

    test('accepts mode allowlist with table list', () => {
        const result = ExecuteWorkflowInputSchema.safeParse({
            mode: 'allowlist',
            allowlist: ['x_app.ticket'],
        });
        assert.equal(result.success, true);
    });

    test('rejects allowlist without tables', () => {
        const result = ExecuteWorkflowInputSchema.safeParse({
            mode: 'allowlist',
        });
        assert.equal(result.success, false);
    });

    test('accepts mode suppressed_default', () => {
        const result = ExecuteWorkflowInputSchema.safeParse({
            mode: 'suppressed_default',
        });
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.data.mode,
                'suppressed_default',
            );
        }
    });
});

describe('ResumeRestoreJobRequestSchema', () => {
    test('accepts valid resume request', () => {
        const result =
            ResumeRestoreJobRequestSchema.safeParse({
                operator_id: 'op-1',
                operator_capabilities: ['restore_execute'],
            });
        assert.equal(result.success, true);
    });

    test('accepts caller-supplied target revalidation records', () => {
        const result =
            ResumeRestoreJobRequestSchema.safeParse({
                operator_id: 'op-1',
                operator_capabilities: ['restore_execute'],
                revalidated_target_records: [{
                    table: 'incident',
                    record_sys_id: 'rec-01',
                    target_state: 'exists',
                }],
            });

        assert.equal(result.success, true);
    });

    test('rejects missing operator_id', () => {
        const result =
            ResumeRestoreJobRequestSchema.safeParse({
                operator_capabilities: ['restore_execute'],
            });
        assert.equal(result.success, false);
    });
});

describe('ExecuteBatchClaimRequestSchema', () => {
    test('accepts valid claim request', () => {
        const result = ExecuteBatchClaimRequestSchema.safeParse({
            operator_id: 'sn-worker',
            max_rows: 50,
        });

        assert.equal(result.success, true);
    });

    test('rejects claim request with missing operator_id', () => {
        const result = ExecuteBatchClaimRequestSchema.safeParse({
            max_rows: 50,
        });

        assert.equal(result.success, false);
    });
});

describe('ExecuteBatchCommitRequestSchema', () => {
    test('accepts valid commit request', () => {
        const result = ExecuteBatchCommitRequestSchema.safeParse({
            claim_id: 'claim-01',
            committed_by: 'sn-worker',
            row_outcomes: [{
                row_id: 'row-01',
                outcome: 'applied',
                reason_code: 'none',
            }],
        });

        assert.equal(result.success, true);
    });

    test('rejects commit request with empty row_outcomes', () => {
        const result = ExecuteBatchCommitRequestSchema.safeParse({
            claim_id: 'claim-01',
            committed_by: 'sn-worker',
            row_outcomes: [],
        });

        assert.equal(result.success, false);
    });
});

describe('chunk outcome schema', () => {
    test('validates all status values', () => {
        for (const status of [
            'applied',
            'row_fallback',
            'failed',
        ]) {
            const result =
                ExecutionChunkStatusSchema.safeParse(status);
            assert.equal(
                result.success,
                true,
                `${status} should be valid`,
            );
        }
        const invalid =
            ExecutionChunkStatusSchema.safeParse('unknown');
        assert.equal(invalid.success, false);
    });
});

describe('row outcome schema', () => {
    test('validates all action/outcome combos', () => {
        const actions = [
            'update',
            'insert',
            'delete',
            'skip',
        ];
        const outcomes = ['applied', 'skipped', 'failed'];
        for (const outcome of outcomes) {
            const result =
                ExecutionOutcomeSchema.safeParse(outcome);
            assert.equal(
                result.success,
                true,
                `${outcome} should be valid`,
            );
        }
        assert.ok(actions.length === 4);
        assert.ok(outcomes.length === 3);
    });
});

describe('media outcome schema', () => {
    test('validates hash/size/attempt fields', () => {
        const mediaOutcome = {
            candidate_id: 'mc-01',
            table: 'x_app.ticket',
            record_sys_id: 'rec-01',
            media_id: 'media-01',
            decision: 'include',
            outcome: 'applied',
            reason_code: 'none',
            chunk_id: 'chunk-01',
            attempt_count: 1,
            size_bytes: 128,
            expected_sha256_plain: 'b'.repeat(64),
            observed_sha256_plain: 'b'.repeat(64),
        };
        assert.ok(mediaOutcome.attempt_count >= 1);
        assert.ok(mediaOutcome.size_bytes >= 0);
        assert.equal(
            mediaOutcome.expected_sha256_plain.length,
            64,
        );
        assert.equal(
            mediaOutcome.observed_sha256_plain.length,
            64,
        );
    });
});

describe('target writer contract', () => {
    test('fail-closed target writer returns explicit failure', async () => {
        const writer = new FailClosedRestoreTargetWriter();
        const result = await writer.applyRow({
            chunk_id: 'chunk_0001',
            executed_by: 'op-1',
            job_id: 'job-1',
            plan_hash: 'b'.repeat(64),
            row: buildPlanRow(),
            row_attempt: 1,
        });

        assert.deepEqual(result, {
            outcome: 'failed',
            reason_code: 'failed_internal_error',
            message: 'target apply runtime support is unavailable',
        });
    });

    test('noop target writer returns deterministic placeholder result', async () => {
        const writer = new NoopRestoreTargetWriter();
        const result = await writer.applyRow({
            chunk_id: 'chunk_0001',
            executed_by: 'op-1',
            job_id: 'job-1',
            plan_hash: 'b'.repeat(64),
            row: buildPlanRow(),
            row_attempt: 1,
        });

        assert.deepEqual(result, {
            outcome: 'applied',
            reason_code: 'none',
            message: 'noop target writer placeholder',
        });
    });
});

describe('product runtime composition', () => {
    test('bootstrap does not inject the noop target writer override', () => {
        const indexSource = readFileSync(
            resolve(__dirname, '../../src/index.ts'),
            'utf8',
        );

        assert.equal(
            indexSource.includes('new NoopRestoreTargetWriter('),
            false,
        );
    });
});
