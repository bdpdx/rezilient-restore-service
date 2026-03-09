import { z } from 'zod';
import {
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestoreApprovalMetadata as RestoreApprovalMetadataSchema,
    RestoreConflict as RestoreConflictSchema,
    RestoreDeleteCandidate as RestoreDeleteCandidateSchema,
    RestoreDryRunRequest as RestoreDryRunRequestSchema,
    RestoreDryRunWatermarkHint as RestoreDryRunWatermarkHintSchema,
    RestoreExecutionOptions as RestoreExecutionOptionsSchema,
    RestoreMediaCandidate as RestoreMediaCandidateSchema,
    RestorePitContract as RestorePitContractSchema,
    RestorePitCandidate as RestorePitCandidateSchema,
    RestorePlanHashInput as RestorePlanHashInputSchema,
    RestorePlanHashRowInput as RestorePlanHashRowInputSchema,
    RestoreScope as RestoreScopeSchema,
    RestoreWatermark as RestoreWatermarkSchema,
} from '@rezilient/types';
import type {
    RestoreApprovalMetadata,
    RestoreConflict,
    RestoreDeleteCandidate as SharedRestoreDeleteCandidate,
    RestoreDryRunRequest as SharedRestoreDryRunRequest,
    RestoreExecutionOptions,
    RestoreMediaCandidate,
    RestorePitContract,
    RestorePitCandidate as SharedRestorePitCandidate,
    RestorePlan,
    RestorePlanHashInput,
    RestorePlanHashRowInput,
    RestoreReasonCode,
    RestoreScope,
    RestoreDryRunWatermarkHint,
    RestoreWatermark,
} from '@rezilient/types';

export type DryRunExecutability = 'executable' | 'preview_only' | 'blocked';
export type DryRunInputMode = 'legacy_rows' | 'scope_driven';

const RestoreDryRunRequestWatermarkSchema = z.union([
    RestoreWatermarkSchema,
    RestoreDryRunWatermarkHintSchema,
]);

type RestoreDryRunRequestWatermark =
    | RestoreWatermark
    | RestoreDryRunWatermarkHint;

// Temporary staged adapter kept only for explicit test compatibility coverage.
const RestoreDryRunLegacyRequestSchema = z
    .object({
        tenant_id: z.string().min(1),
        instance_id: z.string().min(1),
        source: z.string().min(1),
        plan_id: z.string().min(1),
        requested_by: z.string().min(1),
        pit: RestorePitContractSchema,
        scope: RestoreScopeSchema,
        execution_options: RestoreExecutionOptionsSchema,
        rows: z.array(RestorePlanHashRowInputSchema).min(1),
        conflicts: z.array(RestoreConflictSchema).optional().default([]),
        delete_candidates: z
            .array(RestoreDeleteCandidateSchema)
            .optional()
            .default([]),
        media_candidates: z
            .array(RestoreMediaCandidateSchema)
            .optional()
            .default([]),
        watermarks: z.array(RestoreDryRunRequestWatermarkSchema).min(1),
        pit_candidates: z
            .array(RestorePitCandidateSchema)
            .optional()
            .default([]),
        approval: RestoreApprovalMetadataSchema.optional(),
    })
    .strict();

type RestoreDryRunLegacyRequest = z.infer<
    typeof RestoreDryRunLegacyRequestSchema
>;

export type RestoreDeleteCandidate = SharedRestoreDeleteCandidate;
export type RestorePitCandidate = SharedRestorePitCandidate;

export interface CreateDryRunPlanRequest {
    input_mode: DryRunInputMode;
    tenant_id: string;
    instance_id: string;
    source: string;
    plan_id: string;
    requested_by: string;
    pit: RestorePitContract;
    scope: RestoreScope;
    execution_options: RestoreExecutionOptions;
    rows: RestorePlanHashRowInput[];
    conflicts: RestoreConflict[];
    delete_candidates: RestoreDeleteCandidate[];
    media_candidates: RestoreMediaCandidate[];
    watermarks: RestoreDryRunWatermarkHint[];
    pit_candidates: RestorePitCandidate[];
    approval?: RestoreApprovalMetadata;
}

export interface ParseCreateDryRunPlanRequestSuccess {
    success: true;
    data: CreateDryRunPlanRequest;
}

export interface ParseCreateDryRunPlanRequestFailure {
    success: false;
    message: string;
}

export type ParseCreateDryRunPlanRequestResult =
    | ParseCreateDryRunPlanRequestSuccess
    | ParseCreateDryRunPlanRequestFailure;

export interface ParseCreateDryRunPlanRequestOptions {
    allowLegacyRowsCompat?: boolean;
}

export interface RestorePitResolutionRecord {
    row_id: string;
    table: string;
    record_sys_id: string;
    winning_event_id: string;
    winning_sys_updated_on: string;
    winning_sys_mod_count?: number;
    winning_event_time: string;
}

export type FreshnessUnknownDetail =
    | 'no_indexed_coverage'
    | 'partition_not_indexed'
    | 'restore_index_unavailable'
    | 'invalid_authoritative_timestamp';

export interface RestoreDryRunGate {
    executability: DryRunExecutability;
    reason_code: RestoreReasonCode;
    freshness_unknown_detail?: FreshnessUnknownDetail;
    unresolved_delete_candidates: number;
    unresolved_media_candidates: number;
    unresolved_hard_block_conflicts: number;
    stale_partition_count: number;
    unknown_partition_count: number;
}

export interface RestoreDryRunPlanRecord {
    tenant_id: string;
    instance_id: string;
    source: string;
    plan: RestorePlan;
    plan_hash_input: RestorePlanHashInput;
    gate: RestoreDryRunGate;
    delete_candidates: RestoreDeleteCandidate[];
    media_candidates: RestoreMediaCandidate[];
    pit_resolutions: RestorePitResolutionRecord[];
    watermarks: RestoreWatermark[];
}

export interface CreateDryRunPlanSuccess {
    success: true;
    statusCode: number;
    record: RestoreDryRunPlanRecord;
}

export interface CreateDryRunPlanFailure {
    success: false;
    statusCode: number;
    error: string;
    reasonCode?: RestoreReasonCode;
    freshnessUnknownDetail?: FreshnessUnknownDetail;
    message: string;
}

export type CreateDryRunPlanResult =
    | CreateDryRunPlanSuccess
    | CreateDryRunPlanFailure;

export interface RestoreActionCountsRecord {
    update: number;
    insert: number;
    delete: number;
    skip: number;
    conflict: number;
    attachment_apply: number;
    attachment_skip: number;
}

function normalizeWatermarkHints(
    watermarks: RestoreDryRunRequestWatermark[],
): RestoreDryRunWatermarkHint[] {
    const normalizedWatermarks: RestoreDryRunWatermarkHint[] = [];

    for (const watermark of watermarks) {
        const normalizedWatermark: RestoreDryRunWatermarkHint = {
            topic: watermark.topic,
        };

        if (typeof watermark.partition === 'number') {
            normalizedWatermark.partition = watermark.partition;
        }

        normalizedWatermarks.push(normalizedWatermark);
    }

    return normalizedWatermarks;
}

function buildParsedRequest(
    input: {
        input_mode: DryRunInputMode;
        tenant_id: string;
        instance_id: string;
        source: string;
        plan_id: string;
        requested_by: string;
        pit: RestorePitContract;
        scope: RestoreScope;
        execution_options: RestoreExecutionOptions;
        rows?: RestorePlanHashRowInput[];
        conflicts: RestoreConflict[];
        delete_candidates: RestoreDeleteCandidate[];
        media_candidates: RestoreMediaCandidate[];
        watermarks?: RestoreDryRunRequestWatermark[];
        pit_candidates?: RestorePitCandidate[];
        approval?: RestoreApprovalMetadata;
    },
): ParseCreateDryRunPlanRequestResult {
    return {
        success: true,
        data: {
            input_mode: input.input_mode,
            tenant_id: input.tenant_id,
            instance_id: input.instance_id,
            source: input.source,
            plan_id: input.plan_id,
            requested_by: input.requested_by,
            pit: input.pit,
            scope: input.scope,
            execution_options: input.execution_options,
            rows: input.rows || [],
            conflicts: input.conflicts,
            delete_candidates: input.delete_candidates,
            media_candidates: input.media_candidates,
            watermarks: normalizeWatermarkHints(input.watermarks || []),
            pit_candidates: input.pit_candidates || [],
            approval: input.approval,
        },
    };
}

function parseScopeDrivenDryRunRequest(
    request: SharedRestoreDryRunRequest,
): ParseCreateDryRunPlanRequestResult {
    const compatibilityAdapter = request.compatibility_adapter;

    return buildParsedRequest({
        input_mode: 'scope_driven',
        tenant_id: request.tenant_id,
        instance_id: request.instance_id,
        source: request.source,
        plan_id: request.plan_id,
        requested_by: request.requested_by,
        pit: request.pit,
        scope: request.scope,
        execution_options: request.execution_options,
        conflicts: request.conflicts,
        delete_candidates: request.delete_candidates,
        media_candidates: request.media_candidates,
        watermarks: compatibilityAdapter?.watermarks,
        pit_candidates: compatibilityAdapter?.pit_candidates,
        approval: request.approval,
    });
}

function parseLegacyDryRunRequest(
    request: RestoreDryRunLegacyRequest,
): ParseCreateDryRunPlanRequestResult {
    return buildParsedRequest({
        input_mode: 'legacy_rows',
        tenant_id: request.tenant_id,
        instance_id: request.instance_id,
        source: request.source,
        plan_id: request.plan_id,
        requested_by: request.requested_by,
        pit: request.pit,
        scope: request.scope,
        execution_options: request.execution_options,
        rows: request.rows,
        conflicts: request.conflicts,
        delete_candidates: request.delete_candidates,
        media_candidates: request.media_candidates,
        watermarks: request.watermarks,
        pit_candidates: request.pit_candidates,
        approval: request.approval,
    });
}

export function parseCreateDryRunPlanRequest(
    requestBody: unknown,
    options?: ParseCreateDryRunPlanRequestOptions,
): ParseCreateDryRunPlanRequestResult {
    const allowLegacyRowsCompat = options?.allowLegacyRowsCompat === true;
    const scopeDrivenParsed = RestoreDryRunRequestSchema.safeParse(requestBody);

    if (scopeDrivenParsed.success) {
        return parseScopeDrivenDryRunRequest(scopeDrivenParsed.data);
    }

    if (!allowLegacyRowsCompat) {
        return {
            success: false,
            message:
                scopeDrivenParsed.error.issues[0]?.message || 'Invalid request',
        };
    }

    const legacyParsed = RestoreDryRunLegacyRequestSchema.safeParse(requestBody);

    if (legacyParsed.success) {
        return parseLegacyDryRunRequest(legacyParsed.data);
    }

    const scopeDrivenIssue = scopeDrivenParsed.error.issues[0]?.message;
    const legacyIssue = legacyParsed.error.issues[0]?.message;
    const bodyAsRecord = (
        requestBody &&
        typeof requestBody === 'object' &&
        !Array.isArray(requestBody)
    )
        ? requestBody as Record<string, unknown>
        : null;
    const hasLegacyOnlyFields = bodyAsRecord !== null && (
        Object.prototype.hasOwnProperty.call(bodyAsRecord, 'rows') ||
        Object.prototype.hasOwnProperty.call(bodyAsRecord, 'watermarks') ||
        Object.prototype.hasOwnProperty.call(bodyAsRecord, 'pit_candidates')
    );
    const preferredMessage = hasLegacyOnlyFields
        ? legacyIssue || scopeDrivenIssue
        : scopeDrivenIssue || legacyIssue;

    return {
        success: false,
        message: preferredMessage || 'Invalid request',
    };
}

export function buildApprovalPlaceholder(
    approval?: RestoreApprovalMetadata,
): RestoreApprovalMetadata {
    if (approval) {
        return approval;
    }

    return {
        approval_required: false,
        approval_state: 'placeholder_not_enforced',
        approval_placeholder_mode: 'mvp_not_enforced',
    };
}

export function buildPlanHashInput(
    request: CreateDryRunPlanRequest,
    actionCounts: RestoreActionCountsRecord,
): RestorePlanHashInput {
    const sortedRows = [...request.rows]
        .sort((left, right) => left.row_id.localeCompare(right.row_id));
    const sortedMediaCandidates = [...request.media_candidates].sort(
        (left, right) => left.candidate_id.localeCompare(right.candidate_id),
    );
    const parsed = RestorePlanHashInputSchema.safeParse({
        contract_version: RESTORE_CONTRACT_VERSION,
        plan_hash_input_version: PLAN_HASH_INPUT_VERSION,
        plan_hash_algorithm: PLAN_HASH_ALGORITHM,
        pit: request.pit,
        scope: request.scope,
        execution_options: request.execution_options,
        action_counts: actionCounts,
        rows: sortedRows,
        media_candidates: sortedMediaCandidates,
        metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
    });

    if (!parsed.success) {
        throw new Error(
            parsed.error.issues[0]?.message ||
            'invalid dry-run plan-hash input payload',
        );
    }

    return parsed.data;
}
