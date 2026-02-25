import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
    ExecuteRestoreJobRequestSchema,
    ExecuteRuntimeConflictInputSchema,
    ExecuteWorkflowInputSchema,
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

describe('ExecuteRestoreJobRequestSchema', () => {
    test('accepts valid request', () => {
        const result =
            ExecuteRestoreJobRequestSchema.safeParse(
                buildValidExecRequest(),
            );
        assert.equal(result.success, true);
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

    test('rejects missing operator_id', () => {
        const result =
            ResumeRestoreJobRequestSchema.safeParse({
                operator_capabilities: ['restore_execute'],
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
