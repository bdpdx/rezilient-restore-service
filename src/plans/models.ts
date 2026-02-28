import { z } from 'zod';
import {
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestoreApprovalMetadata as RestoreApprovalMetadataSchema,
    RestoreConflict as RestoreConflictSchema,
    RestoreDryRunWatermarkHint as RestoreDryRunWatermarkHintSchema,
    RestoreExecutionOptions as RestoreExecutionOptionsSchema,
    RestoreMediaCandidate as RestoreMediaCandidateSchema,
    RestorePitContract as RestorePitContractSchema,
    RestorePitRowTuple as RestorePitRowTupleSchema,
    RestorePlanHashInput as RestorePlanHashInputSchema,
    RestorePlanHashRowInput as RestorePlanHashRowInputSchema,
    RestoreScope as RestoreScopeSchema,
    RestoreWatermark as RestoreWatermarkSchema,
} from '@rezilient/types';
import type {
    RestoreApprovalMetadata,
    RestoreConflict,
    RestoreExecutionOptions,
    RestoreMediaCandidate,
    RestorePitContract,
    RestorePitRowTuple,
    RestorePlan,
    RestorePlanHashInput,
    RestorePlanHashRowInput,
    RestoreReasonCode,
    RestoreScope,
    RestoreDryRunWatermarkHint,
    RestoreWatermark,
} from '@rezilient/types';

export type DryRunExecutability = 'executable' | 'preview_only' | 'blocked';

const BaseDryRunRequestSchema = z
    .object({
        tenant_id: z.string().min(1),
        instance_id: z.string().min(1),
        source: z.string().min(1),
        plan_id: z.string().min(1),
        requested_by: z.string().min(1),
        pit: z.unknown(),
        scope: z.unknown(),
        execution_options: z.unknown(),
        rows: z.array(z.unknown()).min(1),
        conflicts: z.array(z.unknown()).optional().default([]),
        delete_candidates: z.array(z.unknown()).optional().default([]),
        media_candidates: z.array(z.unknown()).optional().default([]),
        watermarks: z.array(z.unknown()).min(1),
        pit_candidates: z.array(z.unknown()).optional().default([]),
        approval: z.unknown().optional(),
    })
    .strict();

const RestoreDeleteCandidateSchema = z
    .object({
        candidate_id: z.string().min(1),
        row_id: z.string().min(1),
        table: z.string().min(1),
        record_sys_id: z.string().min(1),
        decision: z.enum(['allow_deletion', 'skip_deletion']).optional(),
    })
    .strict();

const RestorePitCandidateSchema = z
    .object({
        row_id: z.string().min(1),
        table: z.string().min(1),
        record_sys_id: z.string().min(1),
        versions: z.array(z.unknown()).min(1),
    })
    .strict();

const RestoreDryRunRequestWatermarkSchema = z.union([
    RestoreWatermarkSchema,
    RestoreDryRunWatermarkHintSchema,
]);

type RestoreDryRunRequestWatermark =
    | RestoreWatermark
    | RestoreDryRunWatermarkHint;

export interface RestoreDeleteCandidate {
    candidate_id: string;
    row_id: string;
    table: string;
    record_sys_id: string;
    decision?: 'allow_deletion' | 'skip_deletion';
}

export interface RestorePitCandidate {
    row_id: string;
    table: string;
    record_sys_id: string;
    versions: RestorePitRowTuple[];
}

export interface CreateDryRunPlanRequest {
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

function parseNested<T>(
    schema: {
        safeParse: (value: unknown) => {
            success: boolean;
            data?: T;
            error?: {
                issues?: Array<{
                    message: string;
                }>;
            };
        };
    },
    value: unknown,
    fieldName: string,
): {
    success: boolean;
    data?: T;
    message?: string;
} {
    const parsed = schema.safeParse(value);

    if (!parsed.success) {
        const issueMessage = parsed.error?.issues?.[0]?.message ||
            'invalid value';

        return {
            success: false,
            message: `${fieldName}: ${issueMessage}`,
        };
    }

    return {
        success: true,
        data: parsed.data as T,
    };
}

function parseArrayWithSchema<T>(
    schema: {
        safeParse: (value: unknown) => {
            success: boolean;
            data?: T;
            error?: {
                issues?: Array<{
                    message: string;
                }>;
            };
        };
    },
    values: unknown[],
    fieldName: string,
): {
    success: boolean;
    data?: T[];
    message?: string;
} {
    const out: T[] = [];

    for (let index = 0; index < values.length; index += 1) {
        const parsed = parseNested<T>(
            schema,
            values[index],
            `${fieldName}[${index}]`,
        );

        if (!parsed.success) {
            return {
                success: false,
                message: parsed.message,
            };
        }

        out.push(parsed.data as T);
    }

    return {
        success: true,
        data: out,
    };
}

export function parseCreateDryRunPlanRequest(
    requestBody: unknown,
): ParseCreateDryRunPlanRequestResult {
    const baseParsed = BaseDryRunRequestSchema.safeParse(requestBody);

    if (!baseParsed.success) {
        return {
            success: false,
            message: baseParsed.error.issues[0]?.message || 'Invalid request',
        };
    }

    const base = baseParsed.data;

    const pitParsed = parseNested<RestorePitContract>(
        RestorePitContractSchema,
        base.pit,
        'pit',
    );

    if (!pitParsed.success) {
        return {
            success: false,
            message: pitParsed.message as string,
        };
    }

    const scopeParsed = parseNested<RestoreScope>(
        RestoreScopeSchema,
        base.scope,
        'scope',
    );

    if (!scopeParsed.success) {
        return {
            success: false,
            message: scopeParsed.message as string,
        };
    }

    const executionOptionsParsed = parseNested<RestoreExecutionOptions>(
        RestoreExecutionOptionsSchema,
        base.execution_options,
        'execution_options',
    );

    if (!executionOptionsParsed.success) {
        return {
            success: false,
            message: executionOptionsParsed.message as string,
        };
    }

    const rowsParsed = parseArrayWithSchema<RestorePlanHashRowInput>(
        RestorePlanHashRowInputSchema,
        base.rows,
        'rows',
    );

    if (!rowsParsed.success) {
        return {
            success: false,
            message: rowsParsed.message as string,
        };
    }

    const conflictsParsed = parseArrayWithSchema<RestoreConflict>(
        RestoreConflictSchema,
        base.conflicts,
        'conflicts',
    );

    if (!conflictsParsed.success) {
        return {
            success: false,
            message: conflictsParsed.message as string,
        };
    }

    const deleteCandidates: RestoreDeleteCandidate[] = [];
    for (let index = 0; index < base.delete_candidates.length; index += 1) {
        const parsed = RestoreDeleteCandidateSchema.safeParse(
            base.delete_candidates[index],
        );

        if (!parsed.success) {
            return {
                success: false,
                message: `delete_candidates[${index}]: ` +
                    (parsed.error.issues[0]?.message || 'invalid value'),
            };
        }

        deleteCandidates.push(parsed.data);
    }

    const mediaCandidatesParsed = parseArrayWithSchema<RestoreMediaCandidate>(
        RestoreMediaCandidateSchema,
        base.media_candidates,
        'media_candidates',
    );

    if (!mediaCandidatesParsed.success) {
        return {
            success: false,
            message: mediaCandidatesParsed.message as string,
        };
    }

    const watermarksParsed = parseArrayWithSchema<
        RestoreDryRunRequestWatermark
    >(
        RestoreDryRunRequestWatermarkSchema,
        base.watermarks,
        'watermarks',
    );

    if (!watermarksParsed.success) {
        return {
            success: false,
            message: watermarksParsed.message as string,
        };
    }

    const parsedWatermarks =
        watermarksParsed.data as RestoreDryRunRequestWatermark[];
    const normalizedWatermarks: RestoreDryRunWatermarkHint[] = [];

    for (let index = 0; index < parsedWatermarks.length; index += 1) {
        const watermark = parsedWatermarks[index];
        const normalizedWatermark: RestoreDryRunWatermarkHint = {
            topic: watermark.topic,
        };

        if (typeof watermark.partition === 'number') {
            normalizedWatermark.partition = watermark.partition;
        }

        normalizedWatermarks.push(normalizedWatermark);
    }

    const pitCandidates: RestorePitCandidate[] = [];
    for (let index = 0; index < base.pit_candidates.length; index += 1) {
        const baseCandidateParsed = RestorePitCandidateSchema.safeParse(
            base.pit_candidates[index],
        );

        if (!baseCandidateParsed.success) {
            return {
                success: false,
                message: `pit_candidates[${index}]: ` +
                    (baseCandidateParsed.error.issues[0]?.message ||
                    'invalid value'),
            };
        }

        const versionsParsed = parseArrayWithSchema<RestorePitRowTuple>(
            RestorePitRowTupleSchema,
            baseCandidateParsed.data.versions,
            `pit_candidates[${index}].versions`,
        );

        if (!versionsParsed.success) {
            return {
                success: false,
                message: versionsParsed.message as string,
            };
        }

        pitCandidates.push({
            row_id: baseCandidateParsed.data.row_id,
            table: baseCandidateParsed.data.table,
            record_sys_id: baseCandidateParsed.data.record_sys_id,
            versions: versionsParsed.data as RestorePitRowTuple[],
        });
    }

    let approval: RestoreApprovalMetadata | undefined;

    if (base.approval !== undefined) {
        const approvalParsed = parseNested<RestoreApprovalMetadata>(
            RestoreApprovalMetadataSchema,
            base.approval,
            'approval',
        );

        if (!approvalParsed.success) {
            return {
                success: false,
                message: approvalParsed.message as string,
            };
        }

        approval = approvalParsed.data;
    }

    return {
        success: true,
        data: {
            tenant_id: base.tenant_id,
            instance_id: base.instance_id,
            source: base.source,
            plan_id: base.plan_id,
            requested_by: base.requested_by,
            pit: pitParsed.data as RestorePitContract,
            scope: scopeParsed.data as RestoreScope,
            execution_options: executionOptionsParsed.data as RestoreExecutionOptions,
            rows: rowsParsed.data as RestorePlanHashRowInput[],
            conflicts: conflictsParsed.data as RestoreConflict[],
            delete_candidates: deleteCandidates,
            media_candidates:
                mediaCandidatesParsed.data as RestoreMediaCandidate[],
            watermarks: normalizedWatermarks,
            pit_candidates: pitCandidates,
            approval,
        },
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
