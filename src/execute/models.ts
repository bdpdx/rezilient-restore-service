import { z } from 'zod';
import { RESTORE_CONTRACT_VERSION } from '@rezilient/types';
import type { RestoreJournalEntry } from '@rezilient/types';

const RESTORE_CAPABILITIES = [
    'restore_execute',
    'restore_delete',
    'restore_override_caps',
    'restore_schema_override',
] as const;

const RESTORE_REASON_CODES = [
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
] as const;

const RESTORE_CONFLICT_CLASSES = [
    'value_conflict',
    'missing_row_conflict',
    'unexpected_existing_conflict',
    'reference_conflict',
    'schema_conflict',
    'permission_conflict',
    'stale_conflict',
] as const;

const RESTORE_CONFLICT_RESOLUTIONS = [
    'skip',
    'abort_and_replan',
] as const;

const RESTORE_PLAN_ACTIONS = [
    'update',
    'insert',
    'delete',
    'skip',
] as const;

const RestoreCapabilitySchema = z.enum(RESTORE_CAPABILITIES);
const RestoreReasonCodeSchema = z.enum(RESTORE_REASON_CODES);
const RestoreConflictClassSchema = z.enum(RESTORE_CONFLICT_CLASSES);
const RestoreConflictResolutionSchema = z.enum(
    RESTORE_CONFLICT_RESOLUTIONS,
);
const RestorePlanActionSchema = z.enum(RESTORE_PLAN_ACTIONS);

export type RestoreCapability = z.infer<typeof RestoreCapabilitySchema>;
export type RestoreReasonCode = z.infer<typeof RestoreReasonCodeSchema>;
export type RestoreConflictClass = z.infer<typeof RestoreConflictClassSchema>;
export type RestoreConflictResolution = z.infer<
    typeof RestoreConflictResolutionSchema
>;
export type RestorePlanAction = z.infer<typeof RestorePlanActionSchema>;

export const ExecutionWorkflowModeSchema = z.enum([
    'suppressed_default',
    'allowlist',
]);

export type ExecutionWorkflowMode = z.infer<
    typeof ExecutionWorkflowModeSchema
>;

export const ExecutionOutcomeSchema = z.enum([
    'applied',
    'skipped',
    'failed',
]);

export type ExecutionOutcome = z.infer<typeof ExecutionOutcomeSchema>;

export const ExecutionChunkStatusSchema = z.enum([
    'applied',
    'row_fallback',
    'failed',
]);

export type ExecutionChunkStatus = z.infer<typeof ExecutionChunkStatusSchema>;

export const ExecutionStatusSchema = z.enum([
    'paused',
    'completed',
    'failed',
]);

export type ExecutionStatus = z.infer<typeof ExecutionStatusSchema>;

export const ElevatedConfirmationSchema = z
    .object({
        confirmed: z.literal(true),
        confirmation: z.string().min(1),
        reason: z.string().min(1),
    })
    .strict();

export type ElevatedConfirmation = z.infer<typeof ElevatedConfirmationSchema>;

export const ExecuteRuntimeConflictInputSchema = z
    .object({
        conflict_id: z.string().min(1),
        row_id: z.string().min(1),
        class: RestoreConflictClassSchema,
        reason_code: RestoreReasonCodeSchema,
        reason: z.string().min(1),
        resolution: RestoreConflictResolutionSchema.optional(),
    })
    .strict()
    .superRefine((conflict, ctx) => {
        if (conflict.resolution === undefined) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'runtime conflicts require explicit resolution',
                path: ['resolution'],
            });
            return;
        }

        if (
            conflict.class === 'reference_conflict' &&
            conflict.resolution === 'skip'
        ) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message:
                    'reference_conflict cannot resolve to skip in P0',
                path: ['resolution'],
            });
        }
    });

export type ExecuteRuntimeConflictInput = z.infer<
    typeof ExecuteRuntimeConflictInputSchema
>;

export const ExecuteWorkflowInputSchema = z
    .object({
        mode: ExecutionWorkflowModeSchema,
        allowlist: z.array(z.string().min(1)).default([]),
    })
    .strict()
    .superRefine((workflow, ctx) => {
        if (
            workflow.mode === 'suppressed_default' &&
            workflow.allowlist.length > 0
        ) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'suppressed_default mode cannot include allowlist',
                path: ['allowlist'],
            });
        }

        if (
            workflow.mode === 'allowlist' &&
            workflow.allowlist.length === 0
        ) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'allowlist mode requires at least one entry',
                path: ['allowlist'],
            });
        }
    });

export type ExecuteWorkflowInput = z.infer<typeof ExecuteWorkflowInputSchema>;

export const ExecuteRestoreJobRequestSchema = z
    .object({
        operator_id: z.string().min(1),
        operator_capabilities: z.array(RestoreCapabilitySchema).min(1),
        chunk_size: z.number().int().positive().max(1000).optional(),
        workflow: ExecuteWorkflowInputSchema.optional(),
        runtime_conflicts: z
            .array(ExecuteRuntimeConflictInputSchema)
            .default([]),
        elevated_confirmation: ElevatedConfirmationSchema.optional(),
    })
    .strict()
    .superRefine((request, ctx) => {
        const conflictIds = new Set<string>();
        const rowIds = new Set<string>();

        for (const conflict of request.runtime_conflicts) {
            if (conflictIds.has(conflict.conflict_id)) {
                ctx.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: 'runtime conflict ids must be unique',
                    path: ['runtime_conflicts'],
                });
            }

            if (rowIds.has(conflict.row_id)) {
                ctx.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: 'runtime conflicts must be unique by row_id',
                    path: ['runtime_conflicts'],
                });
            }

            conflictIds.add(conflict.conflict_id);
            rowIds.add(conflict.row_id);
        }
    });

export type ExecuteRestoreJobRequest = z.infer<
    typeof ExecuteRestoreJobRequestSchema
>;

export const ResumeRestoreJobRequestSchema = z
    .object({
        operator_id: z.string().min(1),
        operator_capabilities: z.array(RestoreCapabilitySchema).min(1),
        runtime_conflicts: z
            .array(ExecuteRuntimeConflictInputSchema)
            .default([]),
        expected_plan_checksum: z
            .string()
            .regex(/^[a-f0-9]{64}$/i)
            .optional(),
        expected_precondition_checksum: z
            .string()
            .regex(/^[a-f0-9]{64}$/i)
            .optional(),
    })
    .strict();

export type ResumeRestoreJobRequest = z.infer<
    typeof ResumeRestoreJobRequestSchema
>;

export interface ExecutionResumeCheckpoint {
    checkpoint_id: string;
    next_chunk_index: number;
    total_chunks: number;
    last_chunk_id: string | null;
    row_attempt_by_row: Record<string, number>;
    updated_at: string;
}

export interface RestoreJournalMirrorRecord {
    mirror_id: string;
    journal_id: string;
    job_id: string;
    plan_hash: string;
    plan_row_id: string;
    table: string;
    record_sys_id: string;
    action: RestorePlanAction;
    outcome: ExecutionOutcome;
    reason_code: RestoreReasonCode;
    chunk_id: string;
    row_attempt: number;
    linked_at: string;
}

export interface RestoreRollbackJournalBundle {
    journal_entries: RestoreJournalEntry[];
    sn_mirror_entries: RestoreJournalMirrorRecord[];
}

export interface ExecuteRowOutcome {
    row_id: string;
    table: string;
    record_sys_id: string;
    action: RestorePlanAction;
    outcome: ExecutionOutcome;
    reason_code: RestoreReasonCode;
    chunk_id: string;
    used_row_fallback: boolean;
    conflict_id?: string;
    conflict_class?: RestoreConflictClass;
    conflict_resolution?: RestoreConflictResolution;
    message?: string;
}

export interface ExecuteChunkOutcome {
    chunk_id: string;
    row_count: number;
    status: ExecutionChunkStatus;
    started_at: string;
    completed_at: string;
    applied_count: number;
    skipped_count: number;
    failed_count: number;
    fallback_trigger_conflict_ids: string[];
}

export interface ExecuteSummary {
    planned_rows: number;
    applied_rows: number;
    skipped_rows: number;
    failed_rows: number;
    attachments_planned: number;
    attachments_applied: number;
    attachments_skipped: number;
    attachments_failed: number;
    chunk_count: number;
    fallback_chunk_count: number;
    runtime_conflict_count: number;
}

export interface ExecuteMediaOutcome {
    candidate_id: string;
    table: string;
    record_sys_id: string;
    attachment_sys_id?: string;
    media_id?: string;
    decision: 'include' | 'exclude';
    outcome: ExecutionOutcome;
    reason_code: RestoreReasonCode;
    chunk_id: string;
    attempt_count: number;
    size_bytes: number;
    expected_sha256_plain: string;
    observed_sha256_plain?: string;
    message?: string;
}

export interface RestoreExecutionRecord {
    contract_version: typeof RESTORE_CONTRACT_VERSION;
    job_id: string;
    plan_id: string;
    plan_hash: string;
    plan_checksum: string;
    precondition_checksum: string;
    status: ExecutionStatus;
    reason_code: RestoreReasonCode;
    started_at: string;
    completed_at: string | null;
    executed_by: string;
    chunk_size: number;
    workflow_mode: ExecutionWorkflowMode;
    workflow_allowlist: string[];
    elevated_confirmation_used: boolean;
    capabilities_used: RestoreCapability[];
    resume_attempt_count: number;
    checkpoint: ExecutionResumeCheckpoint;
    summary: ExecuteSummary;
    chunks: ExecuteChunkOutcome[];
    row_outcomes: ExecuteRowOutcome[];
    media_outcomes: ExecuteMediaOutcome[];
}

export interface ExecuteRestoreJobSuccess {
    success: true;
    statusCode: number;
    record: RestoreExecutionRecord;
    promoted_job_ids: string[];
}

export interface ExecuteRestoreJobFailure {
    success: false;
    statusCode: number;
    error: string;
    reasonCode?: RestoreReasonCode;
    message: string;
}

export type ExecuteRestoreJobResult =
    | ExecuteRestoreJobSuccess
    | ExecuteRestoreJobFailure;

export interface ExecuteServiceConfig {
    defaultChunkSize: number;
    maxRows: number;
    elevatedSkipRatioPercent: number;
    maxChunksPerAttempt: number;
    mediaChunkSize: number;
    mediaMaxItems: number;
    mediaMaxBytes: number;
    mediaMaxRetryAttempts: number;
}

export function normalizeCapabilities(
    capabilities: RestoreCapability[],
): RestoreCapability[] {
    const seen = new Set<RestoreCapability>();

    for (const capability of capabilities) {
        seen.add(capability);
    }

    return Array.from(seen).sort((left, right) =>
        left.localeCompare(right),
    );
}
