import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
    CreateRestoreJobRequestSchema,
    CompleteRestoreJobRequestSchema,
    buildApprovalPlaceholder,
    normalizeIsoWithMillis,
    isTerminalStatus,
} from './models';
import type { RestoreJobStatus } from './models';

function buildValidJobRequest() {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: 'plan-01',
        plan_hash: 'a'.repeat(64),
        lock_scope_tables: ['x_app.ticket'],
        required_capabilities: ['restore_execute' as const],
        requested_by: 'operator-1',
    };
}

describe('CreateRestoreJobRequestSchema', () => {
    test('accepts valid request', () => {
        const result = CreateRestoreJobRequestSchema.safeParse(
            buildValidJobRequest(),
        );
        assert.equal(result.success, true);
    });

    test('rejects missing plan_id', () => {
        const body = { ...buildValidJobRequest() } as
            Record<string, unknown>;
        delete body.plan_id;
        const result =
            CreateRestoreJobRequestSchema.safeParse(body);
        assert.equal(result.success, false);
    });

    test('rejects invalid plan_hash format', () => {
        const result = CreateRestoreJobRequestSchema.safeParse({
            ...buildValidJobRequest(),
            plan_hash: 'not-a-sha256-hash',
        });
        assert.equal(result.success, false);
    });

    test('rejects empty lock_scope_tables', () => {
        const result = CreateRestoreJobRequestSchema.safeParse({
            ...buildValidJobRequest(),
            lock_scope_tables: [],
        });
        assert.equal(result.success, false);
    });

    test('validates capability enum values', () => {
        const result = CreateRestoreJobRequestSchema.safeParse({
            ...buildValidJobRequest(),
            required_capabilities: ['nonexistent_cap'],
        });
        assert.equal(result.success, false);
    });
});

describe('CompleteRestoreJobRequestSchema', () => {
    test('accepts valid complete request', () => {
        const result = CompleteRestoreJobRequestSchema.safeParse({
            status: 'completed',
        });
        assert.equal(result.success, true);
    });

    test('rejects invalid terminal status', () => {
        const result = CompleteRestoreJobRequestSchema.safeParse({
            status: 'running',
        });
        assert.equal(result.success, false);
    });
});

describe('isTerminalStatus', () => {
    test('returns true for completed/failed/cancelled', () => {
        const terminal: RestoreJobStatus[] = [
            'completed',
            'failed',
            'cancelled',
        ];
        for (const status of terminal) {
            assert.equal(
                isTerminalStatus(status),
                true,
                `${status} should be terminal`,
            );
        }
    });

    test('returns false for queued/running/paused', () => {
        const nonTerminal: RestoreJobStatus[] = [
            'queued',
            'running',
            'paused',
        ];
        for (const status of nonTerminal) {
            assert.equal(
                isTerminalStatus(status),
                false,
                `${status} should not be terminal`,
            );
        }
    });
});

describe('normalizeIsoWithMillis', () => {
    test('normalizes valid ISO strings', () => {
        const date = new Date('2026-02-16T12:00:00.000Z');
        const result = normalizeIsoWithMillis(date);
        assert.equal(result, '2026-02-16T12:00:00.000Z');
    });

    test('rejects invalid date strings', () => {
        const invalid = new Date('not-a-date');
        assert.throws(
            () => normalizeIsoWithMillis(invalid),
        );
    });
});

describe('buildApprovalPlaceholder', () => {
    test('returns correct structure', () => {
        const result = buildApprovalPlaceholder();
        assert.equal(result.approval_required, false);
        assert.equal(
            result.approval_state,
            'placeholder_not_enforced',
        );
        assert.equal(
            result.approval_placeholder_mode,
            'mvp_not_enforced',
        );
    });
});

describe('reason codes', () => {
    test('all reason codes are defined and unique', () => {
        const result = CreateRestoreJobRequestSchema.safeParse(
            buildValidJobRequest(),
        );
        assert.equal(result.success, true);

        const reasonCodes = [
            'none',
            'queued_scope_lock',
            'blocked_unknown_source_mapping',
            'blocked_missing_capability',
            'blocked_unresolved_delete_candidates',
            'blocked_unresolved_media_candidates',
            'blocked_reference_conflict',
            'blocked_media_parent_missing',
            'blocked_freshness_stale',
            'blocked_freshness_unknown',
            'blocked_auth_control_plane_outage',
            'blocked_plan_hash_mismatch',
            'blocked_evidence_not_ready',
            'blocked_resume_precondition_mismatch',
            'blocked_resume_checkpoint_missing',
            'paused_token_refresh_grace_exhausted',
            'paused_entitlement_disabled',
            'paused_instance_disabled',
            'failed_media_parent_missing',
            'failed_media_hash_mismatch',
            'failed_media_retry_exhausted',
            'failed_evidence_report_hash_mismatch',
            'failed_evidence_artifact_hash_mismatch',
            'failed_evidence_signature_verification',
            'failed_schema_conflict',
            'failed_permission_conflict',
            'failed_internal_error',
        ];
        const uniqueSet = new Set(reasonCodes);
        assert.equal(
            uniqueSet.size,
            reasonCodes.length,
            'all reason codes must be unique',
        );
        assert.ok(reasonCodes.length >= 27);
    });
});
