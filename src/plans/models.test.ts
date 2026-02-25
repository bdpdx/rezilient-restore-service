import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
    parseCreateDryRunPlanRequest,
    buildApprovalPlaceholder,
    buildPlanHashInput,
} from './models';
import type { CreateDryRunPlanRequest } from './models';

const PIT_ALGORITHM_VERSION =
    'pit.v1.sys_updated_on-sys_mod_count-__time-event_id';

function buildValidRow(overrides: Record<string, unknown> = {}) {
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

function buildValidWatermark(
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
        indexed_through_time: '2026-02-16T12:00:00.000Z',
        coverage_start: '2026-02-16T00:00:00.000Z',
        coverage_end: '2026-02-16T12:00:00.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-16T12:12:00.000Z',
        ...overrides,
    };
}

function buildValidDryRunRequest(
    overrides: Record<string, unknown> = {},
) {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: 'plan-01',
        requested_by: 'operator-1',
        pit: {
            restore_time: '2026-02-16T12:00:00.123Z',
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
        rows: [buildValidRow()],
        watermarks: [buildValidWatermark()],
        ...overrides,
    };
}

describe('parseCreateDryRunPlanRequest', () => {
    test('accepts valid minimal request', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest(),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(result.data.plan_id, 'plan-01');
        }
    });

    test('accepts full request with all optional fields', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                conflicts: [{
                    conflict_id: 'c-01',
                    class: 'value_conflict',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    reason_code: 'blocked_reference_conflict',
                    reason: 'value mismatch',
                    observed_at: '2026-02-16T12:15:00.000Z',
                }],
                delete_candidates: [{
                    candidate_id: 'dc-01',
                    row_id: 'row-01',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    decision: 'skip_deletion',
                }],
                media_candidates: [{
                    candidate_id: 'mc-01',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    media_id: 'media-01',
                    size_bytes: 128,
                    sha256_plain: 'b'.repeat(64),
                    decision: 'include',
                }],
                pit_candidates: [{
                    row_id: 'row-01',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    versions: [{
                        sys_updated_on: '2026-02-16 12:00:00',
                        __time: '2026-02-16T12:00:00.123Z',
                        event_id: 'evt-01',
                    }],
                }],
                approval: {
                    approval_required: false,
                    approval_state: 'placeholder_not_enforced',
                    approval_placeholder_mode:
                        'mvp_not_enforced',
                },
            }),
        );
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(result.data.conflicts.length, 1);
            assert.equal(
                result.data.delete_candidates.length,
                1,
            );
            assert.equal(
                result.data.media_candidates.length,
                1,
            );
            assert.equal(
                result.data.pit_candidates.length,
                1,
            );
            assert.ok(result.data.approval);
        }
    });

    test('rejects missing pit', () => {
        const body = buildValidDryRunRequest() as
            Record<string, unknown>;
        delete body.pit;
        const result = parseCreateDryRunPlanRequest(body);
        assert.equal(result.success, false);
    });

    test('rejects missing scope', () => {
        const body = buildValidDryRunRequest() as
            Record<string, unknown>;
        delete body.scope;
        const result = parseCreateDryRunPlanRequest(body);
        assert.equal(result.success, false);
    });

    test('rejects invalid row action', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                rows: [buildValidRow({ action: 'explode' })],
            }),
        );
        assert.equal(result.success, false);
    });

    test('rejects invalid conflict class', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                conflicts: [{
                    conflict_id: 'c-01',
                    class: 'unknown_class',
                    table: 'x_app.ticket',
                    record_sys_id: 'rec-01',
                    reason_code: 'blocked_reference_conflict',
                    reason: 'test',
                    observed_at: '2026-02-16T12:15:00.000Z',
                }],
            }),
        );
        assert.equal(result.success, false);
    });

    test('validates delete candidate structure', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                delete_candidates: [
                    { row_id: 'row-01' },
                ],
            }),
        );
        assert.equal(result.success, false);
    });

    test('validates media candidate structure', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                media_candidates: [
                    { candidate_id: 'mc-01' },
                ],
            }),
        );
        assert.equal(result.success, false);
    });

    test('validates watermark structure', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                watermarks: [{ topic: 'rez.cdc' }],
            }),
        );
        assert.equal(result.success, false);
    });

    test('validates pit candidate structure', () => {
        const result = parseCreateDryRunPlanRequest(
            buildValidDryRunRequest({
                pit_candidates: [
                    { row_id: 'row-01' },
                ],
            }),
        );
        assert.equal(result.success, false);
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

    test('returns provided approval when present', () => {
        const input = {
            approval_required: true,
            approval_state: 'approved' as const,
            approval_placeholder_mode:
                'mvp_not_enforced' as const,
        };
        const result = buildApprovalPlaceholder(input);
        assert.equal(result.approval_required, true);
        assert.equal(result.approval_state, 'approved');
    });
});

describe('buildPlanHashInput', () => {
    function buildParsedRequest(): CreateDryRunPlanRequest {
        const raw = buildValidDryRunRequest();
        const result = parseCreateDryRunPlanRequest(raw);
        assert.equal(result.success, true);
        if (!result.success) {
            throw new Error('unreachable');
        }
        return result.data;
    }

    test('produces deterministic output', () => {
        const request = buildParsedRequest();
        const counts = {
            update: 0,
            insert: 0,
            delete: 0,
            skip: 1,
            conflict: 0,
            attachment_apply: 0,
            attachment_skip: 0,
        };
        const hash1 = buildPlanHashInput(request, counts);
        const hash2 = buildPlanHashInput(request, counts);
        assert.deepEqual(hash1, hash2);
    });

    test('changes when rows change', () => {
        const request1 = buildParsedRequest();
        const counts = {
            update: 0,
            insert: 0,
            delete: 0,
            skip: 1,
            conflict: 0,
            attachment_apply: 0,
            attachment_skip: 0,
        };
        const hash1 = JSON.stringify(
            buildPlanHashInput(request1, counts),
        );

        const raw2 = buildValidDryRunRequest({
            rows: [
                buildValidRow({ row_id: 'row-02' }),
            ],
        });
        const result2 = parseCreateDryRunPlanRequest(raw2);
        assert.equal(result2.success, true);
        if (!result2.success) {
            throw new Error('unreachable');
        }
        const hash2 = JSON.stringify(
            buildPlanHashInput(result2.data, counts),
        );

        assert.notEqual(hash1, hash2);
    });
});
