import { z } from 'zod';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from '../constants';

const SHA256_HEX = /^[a-f0-9]{64}$/i;
const ISO_WITH_MILLIS =
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

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

const RESTORE_JOB_STATUS = [
    'queued',
    'running',
    'paused',
    'completed',
    'failed',
    'cancelled',
] as const;

const RestoreCapabilitySchema = z.enum(RESTORE_CAPABILITIES);
const RestoreReasonCodeSchema = z.enum(RESTORE_REASON_CODES);
const RestoreJobStatusSchema = z.enum(RESTORE_JOB_STATUS);

const ApprovalMetadataSchema = z
    .object({
        approval_required: z.boolean(),
        approval_state: z.enum([
            'placeholder_not_enforced',
            'approval_not_required',
            'requested',
            'approved',
            'rejected',
            'expired',
        ]),
        approval_policy_id: z.string().min(1).optional(),
        approval_requested_at: z.string().regex(ISO_WITH_MILLIS).optional(),
        approval_requested_by: z.string().min(1).optional(),
        approval_decided_at: z.string().regex(ISO_WITH_MILLIS).optional(),
        approval_decided_by: z.string().min(1).optional(),
        approval_decision: z
            .enum(['approve', 'reject', 'placeholder'])
            .optional(),
        approval_decision_reason: z.string().min(1).optional(),
        approval_external_ref: z.string().min(1).optional(),
        approval_snapshot_hash: z.string().regex(SHA256_HEX).optional(),
        approval_valid_until: z.string().regex(ISO_WITH_MILLIS).optional(),
        approval_revalidated_at: z.string().regex(ISO_WITH_MILLIS).optional(),
        approval_revalidation_result: z
            .enum(['not_applicable', 'valid', 'expired', 'rejected'])
            .optional(),
        approval_placeholder_mode: z.literal('mvp_not_enforced'),
    })
    .strict();

export type RestoreCapability = z.infer<typeof RestoreCapabilitySchema>;
export type RestoreReasonCode = z.infer<typeof RestoreReasonCodeSchema>;
export type RestoreJobStatus = z.infer<typeof RestoreJobStatusSchema>;
export type RestoreApprovalMetadata = z.infer<typeof ApprovalMetadataSchema>;

export const CreateRestoreJobRequestSchema = z
    .object({
        tenant_id: z.string().min(1),
        instance_id: z.string().min(1),
        source: z.string().min(1),
        plan_id: z.string().min(1),
        plan_hash: z.string().regex(SHA256_HEX),
        lock_scope_tables: z.array(z.string().min(1)).min(1),
        required_capabilities: z.array(RestoreCapabilitySchema).min(1),
        requested_by: z.string().min(1),
        approval: ApprovalMetadataSchema.optional(),
    })
    .strict();

export type CreateRestoreJobRequest = z.infer<
    typeof CreateRestoreJobRequestSchema
>;

export interface RestorePlanMetadataRecord {
    contract_version: typeof RESTORE_CONTRACT_VERSION;
    plan_id: string;
    plan_hash: string;
    tenant_id: string;
    instance_id: string;
    source: string;
    lock_scope_tables: string[];
    requested_by: string;
    requested_at: string;
    approval: RestoreApprovalMetadata;
    metadata_allowlist_version: typeof RESTORE_METADATA_ALLOWLIST_VERSION;
}

export interface RestoreJobRecord {
    contract_version: typeof RESTORE_CONTRACT_VERSION;
    job_id: string;
    tenant_id: string;
    instance_id: string;
    source: string;
    plan_id: string;
    plan_hash: string;
    status: RestoreJobStatus;
    status_reason_code: RestoreReasonCode;
    lock_scope_tables: string[];
    required_capabilities: RestoreCapability[];
    requested_by: string;
    requested_at: string;
    approval: RestoreApprovalMetadata;
    metadata_allowlist_version: typeof RESTORE_METADATA_ALLOWLIST_VERSION;
    queue_position: number | null;
    wait_reason_code: RestoreReasonCode | null;
    wait_tables: string[];
    started_at: string | null;
    completed_at: string | null;
    updated_at: string;
}

export interface RestoreJobAuditEvent {
    event_id: string;
    event_type:
        | 'job_created'
        | 'job_queued'
        | 'job_started'
        | 'job_paused'
        | 'job_completed'
        | 'job_failed'
        | 'job_cancelled';
    job_id: string;
    reason_code: RestoreReasonCode;
    created_at: string;
    details: Record<string, unknown>;
}

export interface CreateRestoreJobSuccess {
    success: true;
    job: RestoreJobRecord;
}

export interface CreateRestoreJobFailure {
    success: false;
    statusCode: number;
    error: string;
    reasonCode?: RestoreReasonCode;
    message: string;
}

export type CreateRestoreJobResult =
    | CreateRestoreJobSuccess
    | CreateRestoreJobFailure;

export interface CompleteRestoreJobSuccess {
    success: true;
    job: RestoreJobRecord;
    promoted_job_ids: string[];
}

export interface CompleteRestoreJobFailure {
    success: false;
    statusCode: number;
    error: string;
    message: string;
}

export type CompleteRestoreJobResult =
    | CompleteRestoreJobSuccess
    | CompleteRestoreJobFailure;

export interface PauseRestoreJobSuccess {
    success: true;
    job: RestoreJobRecord;
}

export interface PauseRestoreJobFailure {
    success: false;
    statusCode: number;
    error: string;
    message: string;
}

export type PauseRestoreJobResult =
    | PauseRestoreJobSuccess
    | PauseRestoreJobFailure;

export interface ResumeRestoreJobSuccess {
    success: true;
    job: RestoreJobRecord;
}

export interface ResumeRestoreJobFailure {
    success: false;
    statusCode: number;
    error: string;
    message: string;
}

export type ResumeRestoreJobResult =
    | ResumeRestoreJobSuccess
    | ResumeRestoreJobFailure;

export const CompleteRestoreJobRequestSchema = z
    .object({
        status: z.enum(['completed', 'failed', 'cancelled']),
        reason_code: RestoreReasonCodeSchema.optional(),
    })
    .strict();

export type CompleteRestoreJobRequest = z.infer<
    typeof CompleteRestoreJobRequestSchema
>;

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

export function normalizeIsoWithMillis(date: Date): string {
    const value = date.toISOString();

    if (!ISO_WITH_MILLIS.test(value)) {
        throw new Error('timestamp must be ISO with milliseconds');
    }

    return value;
}

export function isTerminalStatus(status: RestoreJobStatus): boolean {
    return (
        status === 'completed' ||
        status === 'failed' ||
        status === 'cancelled'
    );
}
