import { createHash } from 'node:crypto';
import {
    canonicalJsonStringify,
    RESTORE_CONTRACT_VERSION,
} from '@rezilient/types';
import type {
    RestoreConflict,
    RestoreJournalEntry,
    RestorePlanHashRowInput,
} from '@rezilient/types';
import { AuthTokenClaims } from '../auth/claims';
import { normalizeIsoWithMillis, RestoreJobRecord } from '../jobs/models';
import { RestoreJobService } from '../jobs/job-service';
import {
    type FinalizeTargetReconciliationRecord,
    RestoreDryRunPlanRecord,
} from '../plans/models';
import { RestorePlanService } from '../plans/plan-service';
import {
    buildTargetStateLookupKey,
    FailClosedRestoreTargetStateLookup,
    reconcileSourceOperationWithTargetState,
    type RestoreSourceOperation,
    type RestoreTargetRecordState,
    type RestoreTargetStateLookup,
} from '../plans/target-reconciliation';
import {
    InMemoryRestoreExecutionStateStore,
    RestoreExecutionStateStore,
} from './execute-state-store';
import {
    ExecuteBatchClaimRequest,
    ExecuteBatchClaimResponse,
    ExecuteBatchClaimResult,
    ExecuteBatchClaimedRow,
    ExecuteBatchCommitRequest,
    ExecuteBatchCommitResponse,
    ExecuteBatchCommitResult,
    ExecuteChunkOutcome,
    ExecuteRestoreJobRequest,
    ExecuteRestoreJobRequestSchema,
    ExecuteRestoreJobResult,
    ExecuteMediaOutcome,
    ExecuteRowOutcome,
    ExecuteRuntimeConflictInput,
    ExecuteServiceConfig,
    FailClosedRestoreTargetWriter,
    ExecutionResumeCheckpoint,
    PersistedExecuteBatchClaim,
    PersistedExecuteBatchClaimRow,
    RestoreExecutionRecord,
    RestoreJournalMirrorRecord,
    RestoreRollbackJournalBundle,
    RestoreReasonCode,
    RestoreTargetWriter,
    ResumeRestoreJobRequest,
    ResumeRestoreJobRequestSchema,
    normalizeCapabilities,
} from './models';

const DEFAULT_EXECUTE_CONFIG: ExecuteServiceConfig = {
    defaultChunkSize: 100,
    maxRows: 10000,
    elevatedSkipRatioPercent: 20,
    maxChunksPerAttempt: 0,
    mediaChunkSize: 25,
    mediaMaxItems: 1000,
    mediaMaxBytes: 100 * 1024 * 1024,
    mediaMaxRetryAttempts: 3,
    executionProgressMode: 'commit_driven',
};

function reasonForAbort(
    conflictClass: RestoreConflict['class'],
): RestoreReasonCode {
    if (conflictClass === 'schema_conflict') {
        return 'failed_schema_conflict';
    }

    if (conflictClass === 'permission_conflict') {
        return 'failed_permission_conflict';
    }

    if (conflictClass === 'reference_conflict') {
        return 'blocked_reference_conflict';
    }

    return 'failed_internal_error';
}

function chunkRows(
    rows: RestorePlanHashRowInput[],
    chunkSize: number,
): RestorePlanHashRowInput[][] {
    const out: RestorePlanHashRowInput[][] = [];

    for (let index = 0; index < rows.length; index += chunkSize) {
        out.push(rows.slice(index, index + chunkSize));
    }

    return out;
}

function asChunkId(index: number): string {
    const serial = String(index + 1).padStart(4, '0');
    return `chunk_${serial}`;
}

function asMediaChunkId(index: number): string {
    const serial = String(index + 1).padStart(4, '0');
    return `media_chunk_${serial}`;
}

function toSha256Hex(payload: unknown): string {
    return createHash('sha256')
        .update(canonicalJsonStringify(payload), 'utf8')
        .digest('hex');
}

function buildCheckpointId(
    jobId: string,
    planHash: string,
    nextChunkIndex: number,
    nextRowIndex: number,
): string {
    const checksum = createHash('sha256')
        .update(
            `${jobId}:${planHash}:${nextChunkIndex}:${nextRowIndex}`,
            'utf8',
        )
        .digest('hex');

    return `chk_${checksum.slice(0, 24)}`;
}

function buildClaimId(
    jobId: string,
    planHash: string,
    nextChunkIndex: number,
    nextRowIndex: number,
    claimedAt: string,
): string {
    const checksum = createHash('sha256')
        .update(
            `${jobId}:${planHash}:${nextChunkIndex}:${nextRowIndex}:` +
                `${claimedAt}`,
            'utf8',
        )
        .digest('hex');

    return `claim_${checksum.slice(0, 24)}`;
}

function buildJournalId(
    jobId: string,
    planHash: string,
    rowId: string,
    rowAttempt: number,
): string {
    const checksum = createHash('sha256')
        .update(
            `${jobId}:${planHash}:${rowId}:${rowAttempt}`,
            'utf8',
        )
        .digest('hex');

    return `journal_${checksum.slice(0, 24)}`;
}

function buildMirrorId(journalId: string): string {
    const checksum = createHash('sha256')
        .update(journalId, 'utf8')
        .digest('hex');

    return `sn_mirror_${checksum.slice(0, 24)}`;
}

function buildFailure(
    statusCode: number,
    error: string,
    message: string,
    reasonCode?: RestoreReasonCode,
): ExecuteRestoreJobResult {
    return {
        success: false,
        statusCode,
        error,
        reasonCode,
        message,
    };
}

function buildClaimFailure(
    statusCode: number,
    error: string,
    message: string,
    reasonCode?: RestoreReasonCode,
): ExecuteBatchClaimResult {
    return {
        success: false,
        statusCode,
        error,
        reasonCode,
        message,
    };
}

function buildCommitFailure(
    statusCode: number,
    error: string,
    message: string,
    reasonCode?: RestoreReasonCode,
): ExecuteBatchCommitResult {
    return {
        success: false,
        statusCode,
        error,
        reasonCode,
        message,
    };
}

function readSourceOperationFromRow(
    row: RestorePlanHashRowInput,
): RestoreSourceOperation | null {
    const metadataContainer = (
        row.metadata &&
        typeof row.metadata === 'object'
    )
        ? row.metadata as {
            metadata?: Record<string, unknown>;
        }
        : undefined;
    const metadata = metadataContainer?.metadata;
    const operation = metadata?.operation;

    if (operation === 'D' || operation === 'I' || operation === 'U') {
        return operation;
    }

    if (row.action === 'delete') {
        return 'D';
    }

    if (row.action === 'insert') {
        return 'I';
    }

    if (row.action === 'update') {
        return 'U';
    }

    return null;
}

function isCheckpointIdValid(
    record: RestoreExecutionRecord,
): boolean {
    const expectedCheckpointId = buildCheckpointId(
        record.job_id,
        record.plan_hash,
        record.checkpoint.next_chunk_index,
        record.checkpoint.next_row_index || 0,
    );

    return expectedCheckpointId === record.checkpoint.checkpoint_id;
}

function ensureScopeMatch(
    job: RestoreJobRecord,
    claims: AuthTokenClaims,
): {
    ok: boolean;
    message?: string;
} {
    if (job.tenant_id !== claims.tenant_id) {
        return {
            ok: false,
            message: 'tenant_id does not match token scope',
        };
    }

    if (job.instance_id !== claims.instance_id) {
        return {
            ok: false,
            message: 'instance_id does not match token scope',
        };
    }

    if (job.source !== claims.source) {
        return {
            ok: false,
            message: 'source does not match token scope',
        };
    }

    return {
        ok: true,
    };
}

function requiresDeleteCapability(plan: RestoreDryRunPlanRecord): boolean {
    for (const row of plan.plan_hash_input.rows) {
        if (row.action === 'delete') {
            return true;
        }
    }

    for (const candidate of plan.delete_candidates) {
        if (candidate.decision === 'allow_deletion') {
            return true;
        }
    }

    return false;
}

function hasUnresolvedMediaCandidates(
    plan: RestoreDryRunPlanRecord,
): boolean {
    for (const candidate of plan.media_candidates) {
        if (!candidate.decision) {
            return true;
        }
    }

    return false;
}

function countPlannedSkips(plan: RestoreDryRunPlanRecord): number {
    let count = 0;

    for (const row of plan.plan_hash_input.rows) {
        if (row.action === 'skip') {
            count += 1;
        }
    }

    return count;
}

function countPlannedMediaBytes(plan: RestoreDryRunPlanRecord): number {
    let total = 0;

    for (const candidate of plan.media_candidates) {
        if (candidate.decision === 'include') {
            total += candidate.size_bytes;
        }
    }

    return total;
}

function countPlannedMediaItems(plan: RestoreDryRunPlanRecord): number {
    let total = 0;

    for (const candidate of plan.media_candidates) {
        if (candidate.decision === 'include') {
            total += 1;
        }
    }

    return total;
}

function ensureConflictsResolvable(
    conflicts: Array<{
        class: RestoreConflict['class'];
        conflict_id: string;
        resolution?: 'skip' | 'abort_and_replan';
    }>,
): {
    ok: boolean;
    message?: string;
    reasonCode?: RestoreReasonCode;
} {
    for (const conflict of conflicts) {
        if (conflict.class === 'reference_conflict') {
            return {
                ok: false,
                reasonCode: 'blocked_reference_conflict',
                message:
                    `reference_conflict ${conflict.conflict_id} blocks ` +
                    'execution and requires replan',
            };
        }

        if (!conflict.resolution) {
            return {
                ok: false,
                reasonCode: 'failed_internal_error',
                message:
                    `conflict ${conflict.conflict_id} is unresolved; ` +
                    'execution requires explicit resolution',
            };
        }

        if (conflict.resolution === 'abort_and_replan') {
            return {
                ok: false,
                reasonCode: reasonForAbort(conflict.class),
                message:
                    `conflict ${conflict.conflict_id} requested ` +
                    'abort_and_replan',
            };
        }
    }

    return {
        ok: true,
    };
}

function toConflictMap(
    conflicts: ExecuteRuntimeConflictInput[],
): Map<string, ExecuteRuntimeConflictInput> {
    const out = new Map<string, ExecuteRuntimeConflictInput>();

    for (const conflict of conflicts) {
        out.set(conflict.row_id, conflict);
    }

    return out;
}

function computePlanChecksum(plan: RestoreDryRunPlanRecord): string {
    return toSha256Hex(plan.plan_hash_input);
}

function computePreconditionChecksum(plan: RestoreDryRunPlanRecord): string {
    const normalized = {
        gate: plan.gate,
        delete_candidates: [...plan.delete_candidates].sort((left, right) => {
            return left.candidate_id.localeCompare(right.candidate_id);
        }),
        conflicts: [...plan.plan.conflicts].sort((left, right) => {
            return left.conflict_id.localeCompare(right.conflict_id);
        }),
        watermarks: [...plan.watermarks].sort((left, right) => {
            if (left.topic !== right.topic) {
                return left.topic.localeCompare(right.topic);
            }

            return left.partition - right.partition;
        }),
    };

    return toSha256Hex(normalized);
}

function buildInitialCheckpoint(
    jobId: string,
    planHash: string,
    totalChunks: number,
    nowIso: string,
): ExecutionResumeCheckpoint {
    return {
        checkpoint_id: buildCheckpointId(jobId, planHash, 0, 0),
        next_chunk_index: 0,
        next_row_index: 0,
        total_chunks: totalChunks,
        last_chunk_id: null,
        row_attempt_by_row: {},
        updated_at: nowIso,
    };
}

function latestRowOutcomesByRowId(
    rowOutcomes: ExecuteRowOutcome[],
): Map<string, ExecuteRowOutcome> {
    const latest = new Map<string, ExecuteRowOutcome>();

    for (const row of rowOutcomes) {
        latest.set(row.row_id, row);
    }

    return latest;
}

function latestMediaOutcomesByCandidateId(
    mediaOutcomes: ExecuteMediaOutcome[],
): Map<string, ExecuteMediaOutcome> {
    const latest = new Map<string, ExecuteMediaOutcome>();

    for (const media of mediaOutcomes) {
        latest.set(media.candidate_id, media);
    }

    return latest;
}

function latestChunksByChunkId(
    chunks: ExecuteChunkOutcome[],
): Map<string, ExecuteChunkOutcome> {
    const latest = new Map<string, ExecuteChunkOutcome>();

    for (const chunk of chunks) {
        latest.set(chunk.chunk_id, chunk);
    }

    return latest;
}

function countChunkOutcomesForChunk(
    rowOutcomes: ExecuteRowOutcome[],
    chunkId: string,
): {
    appliedCount: number;
    skippedCount: number;
    failedCount: number;
} {
    const effectiveRows = new Map<string, ExecuteRowOutcome>();

    for (const outcome of rowOutcomes) {
        if (outcome.chunk_id !== chunkId) {
            continue;
        }

        effectiveRows.set(outcome.row_id, outcome);
    }

    let appliedCount = 0;
    let skippedCount = 0;
    let failedCount = 0;

    for (const outcome of effectiveRows.values()) {
        if (outcome.outcome === 'applied') {
            appliedCount += 1;
            continue;
        }

        if (outcome.outcome === 'skipped') {
            skippedCount += 1;
            continue;
        }

        failedCount += 1;
    }

    return {
        appliedCount,
        skippedCount,
        failedCount,
    };
}

function selectClaimRows(
    chunks: RestorePlanHashRowInput[][],
    checkpoint: ExecutionResumeCheckpoint,
    maxRows: number,
): {
    rows: PersistedExecuteBatchClaimRow[];
    next_chunk_index: number;
    next_row_index: number;
} {
    const selected: PersistedExecuteBatchClaimRow[] = [];
    let chunkIndex = checkpoint.next_chunk_index;
    let rowIndex = checkpoint.next_row_index;

    if (chunkIndex >= chunks.length) {
        return {
            rows: selected,
            next_chunk_index: chunkIndex,
            next_row_index: rowIndex,
        };
    }

    while (chunkIndex < chunks.length && selected.length < maxRows) {
        const rows = chunks[chunkIndex];

        while (rowIndex < rows.length && selected.length < maxRows) {
            const row = rows[rowIndex];
            const rowAttempt =
                (checkpoint.row_attempt_by_row[row.row_id] || 0) + 1;

            selected.push({
                row_id: row.row_id,
                chunk_id: asChunkId(chunkIndex),
                chunk_index: chunkIndex,
                row_index: rowIndex,
                row_attempt: rowAttempt,
            });

            rowIndex += 1;
        }

        if (rowIndex >= rows.length) {
            chunkIndex += 1;
            rowIndex = 0;
        }
    }

    return {
        rows: selected,
        next_chunk_index: chunkIndex,
        next_row_index: rowIndex,
    };
}

function summarizeExecution(
    plannedRows: number,
    plannedAttachments: number,
    chunks: ExecuteChunkOutcome[],
    rowOutcomes: ExecuteRowOutcome[],
    mediaOutcomes: ExecuteMediaOutcome[],
): RestoreExecutionRecord['summary'] {
    let appliedRows = 0;
    let skippedRows = 0;
    let failedRows = 0;
    let attachmentsApplied = 0;
    let attachmentsSkipped = 0;
    let attachmentsFailed = 0;
    const runtimeConflictIds = new Set<string>();

    const effectiveRows = latestRowOutcomesByRowId(rowOutcomes);

    for (const row of effectiveRows.values()) {
        if (row.outcome === 'applied') {
            appliedRows += 1;
        } else if (row.outcome === 'skipped') {
            skippedRows += 1;
        } else {
            failedRows += 1;
        }

        if (row.conflict_id) {
            runtimeConflictIds.add(row.conflict_id);
        }
    }

    let fallbackChunkCount = 0;
    const effectiveChunks = latestChunksByChunkId(chunks);

    for (const chunk of effectiveChunks.values()) {
        if (chunk.status === 'row_fallback') {
            fallbackChunkCount += 1;
        }
    }

    const effectiveMedia = latestMediaOutcomesByCandidateId(mediaOutcomes);

    for (const media of effectiveMedia.values()) {
        if (media.outcome === 'applied') {
            attachmentsApplied += 1;
            continue;
        }

        if (media.outcome === 'skipped') {
            attachmentsSkipped += 1;
            continue;
        }

        attachmentsFailed += 1;
    }

    return {
        planned_rows: plannedRows,
        applied_rows: appliedRows,
        skipped_rows: skippedRows,
        failed_rows: failedRows,
        attachments_planned: plannedAttachments,
        attachments_applied: attachmentsApplied,
        attachments_skipped: attachmentsSkipped,
        attachments_failed: attachmentsFailed,
        chunk_count: effectiveChunks.size,
        fallback_chunk_count: fallbackChunkCount,
        runtime_conflict_count: runtimeConflictIds.size,
    };
}

function cloneExecutionRecord(
    record: RestoreExecutionRecord,
): RestoreExecutionRecord {
    const cloned = JSON.parse(
        JSON.stringify(record),
    ) as RestoreExecutionRecord;

    if (!Array.isArray(cloned.revalidated_target_records)) {
        cloned.revalidated_target_records = [];
    }

    return cloned;
}

function cloneJournalEntries(
    entries: RestoreJournalEntry[],
): RestoreJournalEntry[] {
    return JSON.parse(
        JSON.stringify(entries),
    ) as RestoreJournalEntry[];
}

function cloneMirrorEntries(
    entries: RestoreJournalMirrorRecord[],
): RestoreJournalMirrorRecord[] {
    return JSON.parse(
        JSON.stringify(entries),
    ) as RestoreJournalMirrorRecord[];
}

function cloneTargetRevalidationRecords(
    records: FinalizeTargetReconciliationRecord[],
): FinalizeTargetReconciliationRecord[] {
    return JSON.parse(
        JSON.stringify(records),
    ) as FinalizeTargetReconciliationRecord[];
}

export class RestoreExecutionService {
    private readonly config: ExecuteServiceConfig;

    constructor(
        private readonly jobs: RestoreJobService,
        private readonly plans: RestorePlanService,
        config?: Partial<ExecuteServiceConfig>,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestoreExecutionStateStore =
            new InMemoryRestoreExecutionStateStore(),
        private readonly targetWriter: RestoreTargetWriter =
            new FailClosedRestoreTargetWriter(),
        private readonly targetStateLookup: RestoreTargetStateLookup =
            new FailClosedRestoreTargetStateLookup(),
    ) {
        this.config = {
            ...DEFAULT_EXECUTE_CONFIG,
            ...(config || {}),
        };
    }

    async executeJob(
        jobId: string,
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): Promise<ExecuteRestoreJobResult> {
        const existing = await this.getExecutionRecord(jobId);

        if (existing) {
            if (existing.status === 'paused') {
                return buildFailure(
                    409,
                    'job_paused',
                    'job is paused; use resume endpoint',
                    'paused_token_refresh_grace_exhausted',
                );
            }

            return {
                success: true,
                statusCode: 200,
                record: cloneExecutionRecord(existing),
                promoted_job_ids: [],
            };
        }

        const job = await this.jobs.getJob(jobId);

        if (!job) {
            return buildFailure(404, 'not_found', 'job not found');
        }

        const scopeCheck = ensureScopeMatch(job, claims);

        if (!scopeCheck.ok) {
            return buildFailure(
                403,
                'scope_blocked',
                scopeCheck.message || 'scope mismatch',
                'blocked_unknown_source_mapping',
            );
        }

        if (job.status !== 'running') {
            return buildFailure(
                409,
                'job_not_running',
                'job must be running before execute',
            );
        }

        const parsed = ExecuteRestoreJobRequestSchema.safeParse(requestBody);

        if (!parsed.success) {
            return buildFailure(
                400,
                'invalid_request',
                parsed.error.issues[0]?.message || 'Invalid request',
            );
        }

        const request = parsed.data;
        const planLookup = await this.lookupAndValidateExecutablePlan(job);

        if (!planLookup.ok) {
            return planLookup.failure;
        }

        const plan = planLookup.plan;
        const rowValidation = this.validateRuntimeConflictRows(
            plan,
            request.runtime_conflicts,
        );

        if (!rowValidation.ok) {
            return rowValidation.failure;
        }

        const runtimeConflictCheck = ensureConflictsResolvable(
            request.runtime_conflicts.map((conflict) => {
                return {
                    class: conflict.class,
                    conflict_id: conflict.conflict_id,
                    resolution: conflict.resolution,
                };
            }),
        );

        if (!runtimeConflictCheck.ok) {
            return buildFailure(
                409,
                'runtime_conflict_blocked',
                runtimeConflictCheck.message ||
                    'runtime conflict blocked execute',
                runtimeConflictCheck.reasonCode,
            );
        }

        const capabilityCheck = this.validateCapabilities(request, job, plan);

        if (!capabilityCheck.ok) {
            return buildFailure(
                403,
                'missing_capability',
                capabilityCheck.message || 'missing required capabilities',
                'blocked_missing_capability',
            );
        }

        const workflow = this.resolveWorkflow(request, plan);

        if (!workflow.ok) {
            return buildFailure(
                409,
                'workflow_mismatch',
                workflow.message || 'workflow mismatch',
                'failed_internal_error',
            );
        }

        const executeRevalidation = await this.revalidateExecuteTimeTargetStates({
            job,
            revalidatedTargetRecords: request.revalidated_target_records,
            rows: plan.plan_hash_input.rows,
        });

        if (!executeRevalidation.ok) {
            return buildFailure(
                executeRevalidation.statusCode || 409,
                executeRevalidation.error || 'target_revalidation_failed',
                executeRevalidation.message ||
                    'execute-time target revalidation failed',
                executeRevalidation.reasonCode,
            );
        }

        const chunkSize = request.chunk_size || this.config.defaultChunkSize;
        const chunks = chunkRows(plan.plan_hash_input.rows, chunkSize);
        const startedAt = normalizeIsoWithMillis(this.now());

        const record: RestoreExecutionRecord = {
            contract_version: RESTORE_CONTRACT_VERSION,
            job_id: job.job_id,
            plan_id: plan.plan.plan_id,
            plan_hash: plan.plan.plan_hash,
            plan_checksum: computePlanChecksum(plan),
            precondition_checksum: computePreconditionChecksum(plan),
            status: 'paused',
            reason_code: 'none',
            started_at: startedAt,
            completed_at: null,
            executed_by: request.operator_id,
            chunk_size: chunkSize,
            workflow_mode: workflow.workflowMode,
            workflow_allowlist: workflow.allowlist,
            elevated_confirmation_used: request.elevated_confirmation !==
                undefined,
            capabilities_used: normalizeCapabilities(
                request.operator_capabilities,
            ),
            revalidated_target_records: cloneTargetRevalidationRecords(
                request.revalidated_target_records,
            ),
            resume_attempt_count: 0,
            checkpoint: buildInitialCheckpoint(
                job.job_id,
                plan.plan.plan_hash,
                chunks.length,
                startedAt,
            ),
            summary: {
                planned_rows: plan.plan_hash_input.rows.length,
                applied_rows: 0,
                skipped_rows: 0,
                failed_rows: 0,
                attachments_planned: plan.media_candidates.length,
                attachments_applied: 0,
                attachments_skipped: 0,
                attachments_failed: 0,
                chunk_count: 0,
                fallback_chunk_count: 0,
                runtime_conflict_count: 0,
            },
            chunks: [],
            row_outcomes: [],
            media_outcomes: [],
        };

        if (this.config.executionProgressMode === 'commit_driven') {
            return this.startCommitDrivenExecution(
                job,
                plan,
                record,
                chunks,
            );
        }

        return this.processAttempt(
            job,
            plan,
            record,
            toConflictMap(request.runtime_conflicts),
        );
    }

    async resumeJob(
        jobId: string,
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): Promise<ExecuteRestoreJobResult> {
        const existing = await this.getExecutionRecord(jobId);

        if (!existing) {
            return buildFailure(
                409,
                'resume_checkpoint_missing',
                'resume checkpoint is unavailable for this job',
                'blocked_resume_checkpoint_missing',
            );
        }

        if (existing.status !== 'paused') {
            if (existing.status === 'completed' || existing.status === 'failed') {
                return {
                    success: true,
                    statusCode: 200,
                    record: cloneExecutionRecord(existing),
                    promoted_job_ids: [],
                };
            }

            return buildFailure(
                409,
                'job_not_paused',
                'job must be paused before resume',
                'blocked_resume_checkpoint_missing',
            );
        }

        const checkpointIntegrity = this.validateCheckpointIntegrity(existing);

        if (!checkpointIntegrity.ok) {
            return buildFailure(
                checkpointIntegrity.failure?.statusCode || 409,
                checkpointIntegrity.failure?.error ||
                    'resume_checkpoint_mismatch',
                checkpointIntegrity.failure?.message ||
                    'checkpoint validation failed',
                checkpointIntegrity.failure?.reasonCode,
            );
        }

        const job = await this.jobs.getJob(jobId);

        if (!job) {
            return buildFailure(404, 'not_found', 'job not found');
        }

        const scopeCheck = ensureScopeMatch(job, claims);

        if (!scopeCheck.ok) {
            return buildFailure(
                403,
                'scope_blocked',
                scopeCheck.message || 'scope mismatch',
                'blocked_unknown_source_mapping',
            );
        }

        const parsed = ResumeRestoreJobRequestSchema.safeParse(requestBody);

        if (!parsed.success) {
            return buildFailure(
                400,
                'invalid_request',
                parsed.error.issues[0]?.message || 'Invalid request',
            );
        }

        const request = parsed.data;
        const resumeCapabilityCheck = this.validateResumeCapabilities(
            request,
            existing,
        );

        if (!resumeCapabilityCheck.ok) {
            return buildFailure(
                403,
                'missing_capability',
                resumeCapabilityCheck.message ||
                    'missing required capabilities for resume',
                'blocked_missing_capability',
            );
        }

        const planLookup = await this.lookupAndValidateExecutablePlan(job);

        if (!planLookup.ok) {
            return planLookup.failure;
        }

        const plan = planLookup.plan;
        const rowValidation = this.validateRuntimeConflictRows(
            plan,
            request.runtime_conflicts,
        );

        if (!rowValidation.ok) {
            return rowValidation.failure;
        }

        const runtimeConflictCheck = ensureConflictsResolvable(
            request.runtime_conflicts.map((conflict) => {
                return {
                    class: conflict.class,
                    conflict_id: conflict.conflict_id,
                    resolution: conflict.resolution,
                };
            }),
        );

        if (!runtimeConflictCheck.ok) {
            return buildFailure(
                409,
                'runtime_conflict_blocked',
                runtimeConflictCheck.message ||
                    'runtime conflict blocked resume',
                runtimeConflictCheck.reasonCode,
            );
        }

        const expectedPlanChecksum = request.expected_plan_checksum;

        if (expectedPlanChecksum && expectedPlanChecksum !== existing.plan_checksum) {
            return buildFailure(
                409,
                'resume_plan_checksum_mismatch',
                'resume plan checksum does not match checkpoint',
                'blocked_resume_precondition_mismatch',
            );
        }

        const expectedPreconditionChecksum =
            request.expected_precondition_checksum;

        if (
            expectedPreconditionChecksum &&
            expectedPreconditionChecksum !== existing.precondition_checksum
        ) {
            return buildFailure(
                409,
                'resume_precondition_checksum_mismatch',
                'resume precondition checksum does not match checkpoint',
                'blocked_resume_precondition_mismatch',
            );
        }

        const currentPlanChecksum = computePlanChecksum(plan);
        const currentPreconditionChecksum = computePreconditionChecksum(plan);
        const revalidatedTargetRecords =
            request.revalidated_target_records.length > 0
                ? request.revalidated_target_records
                : existing.revalidated_target_records || [];

        if (currentPlanChecksum !== existing.plan_checksum) {
            return buildFailure(
                409,
                'resume_plan_checksum_mismatch',
                'current plan checksum does not match checkpoint',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (currentPreconditionChecksum !== existing.precondition_checksum) {
            return buildFailure(
                409,
                'resume_precondition_checksum_mismatch',
                'resume preconditions no longer match checkpoint',
                'blocked_resume_precondition_mismatch',
            );
        }

        const resumeRevalidation = await this.revalidateExecuteTimeTargetStates({
            job,
            revalidatedTargetRecords,
            rows: plan.plan_hash_input.rows,
        });

        if (!resumeRevalidation.ok) {
            return buildFailure(
                resumeRevalidation.statusCode || 409,
                resumeRevalidation.error || 'target_revalidation_failed',
                resumeRevalidation.message ||
                    'execute-time target revalidation failed',
                resumeRevalidation.reasonCode,
            );
        }

        const resume = await this.jobs.resumePausedJob(jobId);

        if (!resume.success) {
            return buildFailure(
                resume.statusCode,
                resume.error,
                resume.message,
                'blocked_resume_checkpoint_missing',
            );
        }

        const resumedRecord = cloneExecutionRecord(existing);

        resumedRecord.revalidated_target_records =
            cloneTargetRevalidationRecords(revalidatedTargetRecords);

        if (this.config.executionProgressMode === 'commit_driven') {
            return this.resumeCommitDrivenExecution(
                resume.job,
                plan,
                resumedRecord,
            );
        }

        return this.processAttempt(
            resume.job,
            plan,
            resumedRecord,
            toConflictMap(request.runtime_conflicts),
        );
    }

    async claimBatch(
        jobId: string,
        request: ExecuteBatchClaimRequest,
        claims: AuthTokenClaims,
    ): Promise<ExecuteBatchClaimResult> {
        const job = await this.jobs.getJob(jobId);

        if (!job) {
            return buildClaimFailure(404, 'not_found', 'job not found');
        }

        const scopeCheck = ensureScopeMatch(job, claims);

        if (!scopeCheck.ok) {
            return buildClaimFailure(
                403,
                'scope_blocked',
                scopeCheck.message || 'scope mismatch',
                'blocked_unknown_source_mapping',
            );
        }

        if (job.status !== 'running' && job.status !== 'paused') {
            return buildClaimFailure(
                409,
                'job_not_claimable',
                'job must be running or paused before claiming a batch',
                'blocked_resume_checkpoint_missing',
            );
        }

        const planLookup = await this.lookupAndValidateExecutablePlan(job);

        if (!planLookup.ok) {
            return buildClaimFailure(
                planLookup.failure.statusCode,
                planLookup.failure.success
                    ? 'failed_internal_error'
                    : planLookup.failure.error,
                planLookup.failure.success
                    ? 'unexpected execute plan lookup success payload'
                    : planLookup.failure.message,
                planLookup.failure.success
                    ? 'failed_internal_error'
                    : planLookup.failure.reasonCode,
            );
        }

        const plan = planLookup.plan;
        const requestedRevalidatedTargetRecords =
            cloneTargetRevalidationRecords(
                request.revalidated_target_records || [],
            );
        let record = await this.getExecutionRecord(jobId);

        if (!record) {
            const startedAt = normalizeIsoWithMillis(this.now());
            const chunkSize = this.config.defaultChunkSize;
            const chunks = chunkRows(plan.plan_hash_input.rows, chunkSize);

            record = {
                contract_version: RESTORE_CONTRACT_VERSION,
                job_id: job.job_id,
                plan_id: plan.plan.plan_id,
                plan_hash: plan.plan.plan_hash,
                plan_checksum: computePlanChecksum(plan),
                precondition_checksum: computePreconditionChecksum(plan),
                status: 'paused',
                reason_code: 'none',
                started_at: startedAt,
                completed_at: null,
                executed_by: request.operator_id,
                chunk_size: chunkSize,
                workflow_mode: plan.plan.execution_options.workflow_mode,
                workflow_allowlist: [],
                elevated_confirmation_used: false,
                capabilities_used: normalizeCapabilities(
                    job.required_capabilities,
                ),
                revalidated_target_records:
                    requestedRevalidatedTargetRecords,
                resume_attempt_count: 0,
                checkpoint: buildInitialCheckpoint(
                    job.job_id,
                    plan.plan.plan_hash,
                    chunks.length,
                    startedAt,
                ),
                summary: {
                    planned_rows: plan.plan_hash_input.rows.length,
                    applied_rows: 0,
                    skipped_rows: 0,
                    failed_rows: 0,
                    attachments_planned: plan.media_candidates.length,
                    attachments_applied: 0,
                    attachments_skipped: 0,
                    attachments_failed: 0,
                    chunk_count: 0,
                    fallback_chunk_count: 0,
                    runtime_conflict_count: 0,
                },
                chunks: [],
                row_outcomes: [],
                media_outcomes: [],
            };

            await this.persistExecutionDelta(job.job_id, record, [], []);
        } else if (
            (record.revalidated_target_records || []).length === 0 &&
            requestedRevalidatedTargetRecords.length > 0
        ) {
            record.revalidated_target_records =
                requestedRevalidatedTargetRecords;
            const rollbackState = await this.loadRollbackJournalState(
                job.job_id,
            );

            await this.persistExecutionDelta(
                job.job_id,
                record,
                rollbackState.journalEntries,
                rollbackState.mirrorEntries,
            );
        }

        const checkpointIntegrity = this.validateCheckpointIntegrity(record);

        if (!checkpointIntegrity.ok) {
            return buildClaimFailure(
                checkpointIntegrity.failure?.statusCode || 409,
                checkpointIntegrity.failure?.error ||
                    'resume_checkpoint_mismatch',
                checkpointIntegrity.failure?.message ||
                    'checkpoint validation failed',
                checkpointIntegrity.failure?.reasonCode,
            );
        }

        if (record.status === 'completed' || record.status === 'failed') {
            const response: ExecuteBatchClaimResponse = {
                accepted: false,
                job_id: job.job_id,
                claim_id: null,
                claimed_at: null,
                claimed_by: null,
                claimed_rows: [],
                has_more_rows: false,
                message: 'execution is already terminal; no claimable rows remain',
                reason_code: record.reason_code,
                requested_max_rows: request.max_rows || null,
            };

            return {
                success: true,
                statusCode: 200,
                response,
            };
        }

        const chunks = chunkRows(plan.plan_hash_input.rows, record.chunk_size);
        const checkpointChunkIndex = record.checkpoint.next_chunk_index;
        const checkpointRowIndex = record.checkpoint.next_row_index || 0;

        if (record.checkpoint.total_chunks !== chunks.length) {
            return buildClaimFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint chunk cardinality mismatches current plan',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (checkpointChunkIndex > chunks.length) {
            return buildClaimFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint chunk index exceeds current plan chunk bounds',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (
            checkpointChunkIndex === chunks.length &&
            checkpointRowIndex !== 0
        ) {
            return buildClaimFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint row cursor must reset at terminal chunk index',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (
            checkpointChunkIndex < chunks.length &&
            checkpointRowIndex > chunks[checkpointChunkIndex].length
        ) {
            return buildClaimFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint row cursor exceeds chunk row bounds',
                'blocked_resume_precondition_mismatch',
            );
        }

        const state = await this.stateStore.read();
        const existingClaimId = state.active_claim_id_by_job_id[job.job_id];

        if (existingClaimId) {
            const existingClaim = state.claims_by_id[existingClaimId];

            if (existingClaim && existingClaim.status === 'claimed') {
                return buildClaimFailure(
                    409,
                    'claim_in_progress',
                    'an uncommitted execute batch claim already exists',
                    'blocked_resume_precondition_mismatch',
                );
            }
        }

        const requestedMaxRows = request.max_rows || null;
        const maxRows = request.max_rows || record.chunk_size;
        const selection = selectClaimRows(
            chunks,
            record.checkpoint,
            maxRows,
        );

        if (selection.rows.length === 0) {
            const response: ExecuteBatchClaimResponse = {
                accepted: false,
                job_id: job.job_id,
                claim_id: null,
                claimed_at: null,
                claimed_by: null,
                claimed_rows: [],
                has_more_rows: false,
                message: 'no claimable rows remain for this execution',
                reason_code: 'none',
                requested_max_rows: requestedMaxRows,
            };

            return {
                success: true,
                statusCode: 200,
                response,
            };
        }

        const rowsById = new Map(
            plan.plan_hash_input.rows.map((row) => [row.row_id, row]),
        );
        const selectedPlanRows: RestorePlanHashRowInput[] = [];

        for (const claimRow of selection.rows) {
            const row = rowsById.get(claimRow.row_id);

            if (!row) {
                return buildClaimFailure(
                    500,
                    'claim_plan_mismatch',
                    'claimed row is missing from persisted plan',
                    'failed_internal_error',
                );
            }

            selectedPlanRows.push(row);
        }

        const claimRevalidation = await this.revalidateExecuteTimeTargetStates({
            job,
            revalidatedTargetRecords:
                record.revalidated_target_records || [],
            rows: selectedPlanRows,
        });

        if (!claimRevalidation.ok) {
            return buildClaimFailure(
                claimRevalidation.statusCode || 409,
                claimRevalidation.error || 'target_revalidation_failed',
                claimRevalidation.message ||
                    'execute-time target revalidation failed',
                claimRevalidation.reasonCode,
            );
        }

        const claimedAt = normalizeIsoWithMillis(this.now());
        const claimId = buildClaimId(
            job.job_id,
            record.plan_hash,
            record.checkpoint.next_chunk_index,
            record.checkpoint.next_row_index,
            claimedAt,
        );
        const claimRecord: PersistedExecuteBatchClaim = {
            claim_id: claimId,
            job_id: job.job_id,
            plan_id: plan.plan.plan_id,
            plan_hash: plan.plan.plan_hash,
            claimed_at: claimedAt,
            claimed_by: request.operator_id,
            next_chunk_index: selection.next_chunk_index,
            next_row_index: selection.next_row_index,
            rows: selection.rows,
            status: 'claimed',
            committed_at: null,
            committed_by: null,
        };
        const persisted = await this.stateStore.mutate((mutableState) => {
            const currentActiveClaimId =
                mutableState.active_claim_id_by_job_id[job.job_id];

            if (currentActiveClaimId) {
                const currentActiveClaim =
                    mutableState.claims_by_id[currentActiveClaimId];

                if (currentActiveClaim && currentActiveClaim.status === 'claimed') {
                    return false;
                }
            }

            mutableState.claims_by_id[claimId] = claimRecord;
            mutableState.active_claim_id_by_job_id[job.job_id] = claimId;

            return true;
        });

        if (!persisted) {
            return buildClaimFailure(
                409,
                'claim_in_progress',
                'an uncommitted execute batch claim already exists',
                'blocked_resume_precondition_mismatch',
            );
        }

        const claimedRows: ExecuteBatchClaimedRow[] = [];

        for (const claimRow of selection.rows) {
            const row = rowsById.get(claimRow.row_id);

            if (!row) {
                return buildClaimFailure(
                    500,
                    'claim_plan_mismatch',
                    'claimed row is missing from persisted plan',
                    'failed_internal_error',
                );
            }

            claimedRows.push({
                row_id: row.row_id,
                table: row.table,
                record_sys_id: row.record_sys_id,
                action: row.action,
                chunk_id: claimRow.chunk_id,
                row_attempt: claimRow.row_attempt,
                values: row.values,
            });
        }

        const response: ExecuteBatchClaimResponse = {
            accepted: true,
            job_id: job.job_id,
            claim_id: claimId,
            claimed_at: claimedAt,
            claimed_by: request.operator_id,
            claimed_rows: claimedRows,
            has_more_rows: selection.next_chunk_index < chunks.length,
            message: 'execute batch claim accepted',
            reason_code: 'none',
            requested_max_rows: requestedMaxRows,
        };

        return {
            success: true,
            statusCode: 200,
            response,
        };
    }

    async commitBatch(
        jobId: string,
        request: ExecuteBatchCommitRequest,
        claims: AuthTokenClaims,
    ): Promise<ExecuteBatchCommitResult> {
        const job = await this.jobs.getJob(jobId);

        if (!job) {
            return buildCommitFailure(404, 'not_found', 'job not found');
        }

        const scopeCheck = ensureScopeMatch(job, claims);

        if (!scopeCheck.ok) {
            return buildCommitFailure(
                403,
                'scope_blocked',
                scopeCheck.message || 'scope mismatch',
                'blocked_unknown_source_mapping',
            );
        }

        if (job.status !== 'running' && job.status !== 'paused') {
            return buildCommitFailure(
                409,
                'job_not_committable',
                'job must be running or paused before committing a batch',
                'blocked_resume_checkpoint_missing',
            );
        }

        const planLookup = await this.lookupAndValidateExecutablePlan(job);

        if (!planLookup.ok) {
            return buildCommitFailure(
                planLookup.failure.statusCode,
                planLookup.failure.success
                    ? 'failed_internal_error'
                    : planLookup.failure.error,
                planLookup.failure.success
                    ? 'unexpected execute plan lookup success payload'
                    : planLookup.failure.message,
                planLookup.failure.success
                    ? 'failed_internal_error'
                    : planLookup.failure.reasonCode,
            );
        }

        const plan = planLookup.plan;
        const state = await this.stateStore.read();
        const record = state.records_by_job_id[job.job_id];

        if (!record) {
            return buildCommitFailure(
                409,
                'execution_missing',
                'execution record is unavailable for this job',
                'blocked_resume_checkpoint_missing',
            );
        }

        const claim = state.claims_by_id[request.claim_id];

        if (!claim || claim.job_id !== job.job_id) {
            return buildCommitFailure(
                409,
                'claim_not_found',
                'execute batch claim is missing or out of scope',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (claim.status !== 'claimed') {
            return buildCommitFailure(
                409,
                'claim_not_claimed',
                'execute batch claim is no longer claimable',
                'blocked_resume_precondition_mismatch',
            );
        }

        const activeClaimId = state.active_claim_id_by_job_id[job.job_id];

        if (activeClaimId !== request.claim_id) {
            return buildCommitFailure(
                409,
                'claim_not_active',
                'execute batch claim is not the active claim for this job',
                'blocked_resume_precondition_mismatch',
            );
        }

        const chunks = chunkRows(plan.plan_hash_input.rows, record.chunk_size);
        const planRowsById = new Map(
            plan.plan_hash_input.rows.map((row) => [row.row_id, row]),
        );
        const outcomeByRowId = new Map<string, typeof request.row_outcomes[0]>();

        for (const rowOutcome of request.row_outcomes) {
            if (outcomeByRowId.has(rowOutcome.row_id)) {
                return buildCommitFailure(
                    409,
                    'claim_commit_mismatch',
                    'row outcomes must be unique by row_id',
                    'blocked_resume_precondition_mismatch',
                );
            }

            outcomeByRowId.set(rowOutcome.row_id, rowOutcome);
        }

        if (outcomeByRowId.size !== claim.rows.length) {
            return buildCommitFailure(
                409,
                'claim_commit_mismatch',
                'committed row outcomes must match the claimed row count',
                'blocked_resume_precondition_mismatch',
            );
        }

        const updatedRecord = cloneExecutionRecord(record);
        const journalEntries = cloneJournalEntries(
            state.rollback_journal_by_job_id[job.job_id] || [],
        );
        const mirrorEntries = cloneMirrorEntries(
            state.sn_mirror_by_job_id[job.job_id] || [],
        );
        const touchedChunks = new Map<string, number>();

        for (const claimRow of claim.rows) {
            const row = planRowsById.get(claimRow.row_id);
            const incomingOutcome = outcomeByRowId.get(claimRow.row_id);

            if (!row || !incomingOutcome) {
                return buildCommitFailure(
                    409,
                    'claim_commit_mismatch',
                    'commit payload does not match claimed rows',
                    'blocked_resume_precondition_mismatch',
                );
            }

            touchedChunks.set(claimRow.chunk_id, claimRow.chunk_index);
            updatedRecord.checkpoint.row_attempt_by_row[row.row_id] =
                claimRow.row_attempt;
            updatedRecord.row_outcomes.push({
                row_id: row.row_id,
                table: row.table,
                record_sys_id: row.record_sys_id,
                action: row.action,
                outcome: incomingOutcome.outcome,
                reason_code: incomingOutcome.reason_code,
                chunk_id: claimRow.chunk_id,
                used_row_fallback: false,
                message: incomingOutcome.message,
            });

            if (incomingOutcome.outcome === 'applied') {
                this.appendRollbackJournal(
                    updatedRecord,
                    row,
                    claimRow.chunk_id,
                    claimRow.row_attempt,
                    journalEntries,
                    mirrorEntries,
                );
            }
        }

        const committedAt = normalizeIsoWithMillis(this.now());
        updatedRecord.checkpoint.next_chunk_index = claim.next_chunk_index;
        updatedRecord.checkpoint.next_row_index = claim.next_row_index;
        updatedRecord.checkpoint.last_chunk_id = claim.rows.length > 0
            ? claim.rows[claim.rows.length - 1].chunk_id
            : updatedRecord.checkpoint.last_chunk_id;
        updatedRecord.checkpoint.updated_at = committedAt;
        updatedRecord.checkpoint.checkpoint_id = buildCheckpointId(
            updatedRecord.job_id,
            updatedRecord.plan_hash,
            claim.next_chunk_index,
            claim.next_row_index,
        );

        for (const [chunkId, chunkIndex] of touchedChunks.entries()) {
            const counts = countChunkOutcomesForChunk(
                updatedRecord.row_outcomes,
                chunkId,
            );
            const fallbackConflictIds = new Set<string>();

            for (const rowOutcome of updatedRecord.row_outcomes) {
                if (rowOutcome.chunk_id !== chunkId || !rowOutcome.conflict_id) {
                    continue;
                }

                fallbackConflictIds.add(rowOutcome.conflict_id);
            }

            const chunkStatus = counts.failedCount > 0
                ? 'failed'
                : fallbackConflictIds.size > 0
                ? 'row_fallback'
                : 'applied';
            const fallbackTriggerConflictIds = Array
                .from(fallbackConflictIds)
                .sort((left, right) => left.localeCompare(right));
            const existingChunkIndex = updatedRecord.chunks.findIndex((chunk) => {
                return chunk.chunk_id === chunkId;
            });
            const existingChunk = existingChunkIndex >= 0
                ? updatedRecord.chunks[existingChunkIndex]
                : undefined;
            const chunkOutcome: ExecuteChunkOutcome = {
                chunk_id: chunkId,
                row_count: chunks[chunkIndex]?.length || 0,
                status: chunkStatus,
                started_at: existingChunk?.started_at || claim.claimed_at,
                completed_at: committedAt,
                applied_count: counts.appliedCount,
                skipped_count: counts.skippedCount,
                failed_count: counts.failedCount,
                fallback_trigger_conflict_ids: fallbackTriggerConflictIds,
            };

            if (existingChunkIndex >= 0) {
                updatedRecord.chunks[existingChunkIndex] = chunkOutcome;
            } else {
                updatedRecord.chunks.push(chunkOutcome);
            }
        }

        updatedRecord.summary = summarizeExecution(
            plan.plan_hash_input.rows.length,
            plan.media_candidates.length,
            updatedRecord.chunks,
            updatedRecord.row_outcomes,
            updatedRecord.media_outcomes,
        );

        if (
            updatedRecord.checkpoint.next_chunk_index >= chunks.length &&
            updatedRecord.media_outcomes.length === 0 &&
            plan.media_candidates.length > 0
        ) {
            this.processMediaCandidates(plan, updatedRecord);
            updatedRecord.summary = summarizeExecution(
                plan.plan_hash_input.rows.length,
                plan.media_candidates.length,
                updatedRecord.chunks,
                updatedRecord.row_outcomes,
                updatedRecord.media_outcomes,
            );
        }

        const atEnd = updatedRecord.checkpoint.next_chunk_index >= chunks.length;

        if (atEnd) {
            const hasFailures = updatedRecord.summary.failed_rows > 0 ||
                updatedRecord.summary.attachments_failed > 0;
            const terminalStatus = hasFailures ? 'failed' : 'completed';
            const terminalReason: RestoreReasonCode = hasFailures
                ? 'failed_internal_error'
                : 'none';
            const completion = await this.jobs.completeJob(job.job_id, {
                status: terminalStatus,
                reason_code: terminalReason,
            });

            if (!completion.success) {
                return buildCommitFailure(
                    500,
                    'job_completion_failed',
                    completion.message,
                    'failed_internal_error',
                );
            }

            updatedRecord.status = terminalStatus;
            updatedRecord.reason_code = terminalReason;
            updatedRecord.completed_at = normalizeIsoWithMillis(this.now());
        }

        const updatedClaim: PersistedExecuteBatchClaim = {
            ...claim,
            status: 'committed',
            committed_at: committedAt,
            committed_by: request.committed_by,
        };
        const persisted = await this.stateStore.mutate((mutableState) => {
            const currentActiveClaimId =
                mutableState.active_claim_id_by_job_id[job.job_id];
            const currentClaim = mutableState.claims_by_id[request.claim_id];

            if (
                currentActiveClaimId !== request.claim_id ||
                !currentClaim ||
                currentClaim.status !== 'claimed'
            ) {
                return false;
            }

            mutableState.claims_by_id[request.claim_id] = updatedClaim;
            delete mutableState.active_claim_id_by_job_id[job.job_id];
            mutableState.records_by_job_id[job.job_id] =
                cloneExecutionRecord(updatedRecord);

            if (journalEntries.length > 0) {
                mutableState.rollback_journal_by_job_id[job.job_id] =
                    cloneJournalEntries(journalEntries);
            } else {
                delete mutableState.rollback_journal_by_job_id[job.job_id];
            }

            if (mirrorEntries.length > 0) {
                mutableState.sn_mirror_by_job_id[job.job_id] =
                    cloneMirrorEntries(mirrorEntries);
            } else {
                delete mutableState.sn_mirror_by_job_id[job.job_id];
            }

            return true;
        });

        if (!persisted) {
            return buildCommitFailure(
                409,
                'claim_not_active',
                'execute batch claim is no longer active',
                'blocked_resume_precondition_mismatch',
            );
        }

        const response: ExecuteBatchCommitResponse = {
            accepted: true,
            claim_id: request.claim_id,
            committed_rows: request.row_outcomes.length,
            job_id: job.job_id,
            execution_status: updatedRecord.status,
            checkpoint: {
                ...updatedRecord.checkpoint,
                row_attempt_by_row: {
                    ...updatedRecord.checkpoint.row_attempt_by_row,
                },
            },
            summary: {
                ...updatedRecord.summary,
            },
            message: 'execute batch commit accepted',
            reason_code: updatedRecord.reason_code,
        };

        return {
            success: true,
            statusCode: 200,
            response,
        };
    }

    async getExecution(jobId: string): Promise<RestoreExecutionRecord | null> {
        return this.getExecutionRecord(jobId);
    }

    async listExecutions(): Promise<RestoreExecutionRecord[]> {
        const state = await this.stateStore.read();

        return Object.values(state.records_by_job_id)
            .map((record) => cloneExecutionRecord(record))
            .sort((left, right) => {
                return left.started_at.localeCompare(right.started_at);
            });
    }

    async getCheckpoint(jobId: string): Promise<ExecutionResumeCheckpoint | null> {
        const record = await this.getExecutionRecord(jobId);

        if (!record) {
            return null;
        }

        return {
            ...record.checkpoint,
            row_attempt_by_row: {
                ...record.checkpoint.row_attempt_by_row,
            },
        };
    }

    async getRollbackJournal(
        jobId: string,
    ): Promise<RestoreRollbackJournalBundle | null> {
        const state = await this.stateStore.read();
        const journalEntries = state.rollback_journal_by_job_id[jobId];
        const mirrorEntries = state.sn_mirror_by_job_id[jobId];

        if (!journalEntries && !mirrorEntries) {
            return null;
        }

        return {
            journal_entries: cloneJournalEntries(journalEntries || []),
            sn_mirror_entries: cloneMirrorEntries(mirrorEntries || []),
        };
    }

    private validateCheckpointIntegrity(
        record: RestoreExecutionRecord,
    ): {
        ok: boolean;
        failure?: {
            statusCode: number;
            error: string;
            message: string;
            reasonCode?: RestoreReasonCode;
        };
    } {
        if (isCheckpointIdValid(record)) {
            return {
                ok: true,
            };
        }

        return {
            ok: false,
            failure: {
                statusCode: 409,
                error: 'resume_checkpoint_mismatch',
                message:
                    'checkpoint_id does not match deterministic resume state',
                reasonCode: 'blocked_resume_precondition_mismatch',
            },
        };
    }

    private async revalidateExecuteTimeTargetStates(input: {
        job: RestoreJobRecord;
        revalidatedTargetRecords?: FinalizeTargetReconciliationRecord[];
        rows: RestorePlanHashRowInput[];
    }): Promise<{
        ok: boolean;
        statusCode?: number;
        error?: string;
        reasonCode?: RestoreReasonCode;
        message?: string;
    }> {
        const rowsToRevalidate = input.rows.filter((row) => {
            return row.action !== 'skip';
        });

        if (rowsToRevalidate.length === 0) {
            return {
                ok: true,
            };
        }

        const uniqueRecords = new Map<
            string,
            {
                record_sys_id: string;
                table: string;
            }
        >();

        for (const row of rowsToRevalidate) {
            const key = buildTargetStateLookupKey({
                record_sys_id: row.record_sys_id,
                table: row.table,
            });

            if (uniqueRecords.has(key)) {
                continue;
            }

            uniqueRecords.set(key, {
                record_sys_id: row.record_sys_id,
                table: row.table,
            });
        }

        let targetStatesByKey: Map<string, RestoreTargetRecordState> | null =
            null;

        if (
            Array.isArray(input.revalidatedTargetRecords) &&
            input.revalidatedTargetRecords.length > 0
        ) {
            targetStatesByKey = new Map<string, RestoreTargetRecordState>();

            for (const record of input.revalidatedTargetRecords) {
                const key = buildTargetStateLookupKey({
                    record_sys_id: record.record_sys_id,
                    table: record.table,
                });

                if (targetStatesByKey.has(key)) {
                    return {
                        ok: false,
                        statusCode: 409,
                        error: 'target_revalidation_incomplete',
                        reasonCode: 'failed_internal_error',
                        message:
                            'execute-time target revalidation records must '
                            + 'be unique by table + record_sys_id',
                    };
                }

                targetStatesByKey.set(key, record.target_state);
            }

            for (const key of uniqueRecords.keys()) {
                if (!targetStatesByKey.has(key)) {
                    return {
                        ok: false,
                        statusCode: 409,
                        error: 'target_revalidation_incomplete',
                        reasonCode: 'failed_internal_error',
                        message:
                            'execute-time target revalidation records are '
                            + 'incomplete for the requested plan rows',
                    };
                }
            }
        } else {
            try {
                targetStatesByKey = await this.targetStateLookup.lookupTargetState({
                    instance_id: input.job.instance_id,
                    records: Array.from(uniqueRecords.values()),
                    source: input.job.source,
                    tenant_id: input.job.tenant_id,
                });
            } catch (error: unknown) {
                return {
                    ok: false,
                    statusCode: 503,
                    error: 'target_revalidation_unavailable',
                    reasonCode: 'failed_internal_error',
                    message:
                        'execute-time target revalidation is unavailable: '
                        + String((error as Error)?.message || error),
                };
            }
        }

        if (!targetStatesByKey) {
            return {
                ok: false,
                statusCode: 503,
                error: 'target_revalidation_unavailable',
                reasonCode: 'failed_internal_error',
                message:
                    'execute-time target revalidation is unavailable: '
                    + 'target state lookup result is unavailable',
            };
        }

        for (const row of rowsToRevalidate) {
            const key = buildTargetStateLookupKey({
                record_sys_id: row.record_sys_id,
                table: row.table,
            });
            const targetState = targetStatesByKey.get(key);

            if (!targetState) {
                continue;
            }

            const sourceOperation = readSourceOperationFromRow(row);

            if (!sourceOperation) {
                continue;
            }

            const reconciliation = reconcileSourceOperationWithTargetState({
                source_operation: sourceOperation,
                target_state: targetState,
            });

            if (reconciliation.decision === 'block') {
                return {
                    ok: false,
                    statusCode: 409,
                    error: 'target_revalidation_failed',
                    reasonCode: 'blocked_reference_conflict',
                    message:
                        'execute-time target revalidation blocked row '
                        + `${row.row_id}: ${reconciliation.blocking_reason}`,
                };
            }

            if (reconciliation.plan_action !== row.action) {
                return {
                    ok: false,
                    statusCode: 409,
                    error: 'target_revalidation_failed',
                    reasonCode: 'blocked_reference_conflict',
                    message:
                        'execute-time target revalidation changed row '
                        + `${row.row_id} action from ${row.action} to `
                        + `${reconciliation.plan_action}`,
                };
            }
        }

        return {
            ok: true,
        };
    }

    private async getExecutionRecord(
        jobId: string,
    ): Promise<RestoreExecutionRecord | null> {
        const state = await this.stateStore.read();
        const record = state.records_by_job_id[jobId];

        if (!record) {
            return null;
        }

        return cloneExecutionRecord(record);
    }

    private async loadRollbackJournalState(
        jobId: string,
    ): Promise<{
        journalEntries: RestoreJournalEntry[];
        mirrorEntries: RestoreJournalMirrorRecord[];
    }> {
        const state = await this.stateStore.read();

        return {
            journalEntries: cloneJournalEntries(
                state.rollback_journal_by_job_id[jobId] || [],
            ),
            mirrorEntries: cloneMirrorEntries(
                state.sn_mirror_by_job_id[jobId] || [],
            ),
        };
    }

    private async persistExecutionDelta(
        jobId: string,
        record: RestoreExecutionRecord,
        journalEntries: RestoreJournalEntry[],
        mirrorEntries: RestoreJournalMirrorRecord[],
    ): Promise<void> {
        await this.stateStore.mutate((state) => {
            state.records_by_job_id[jobId] = cloneExecutionRecord(record);

            if (journalEntries.length > 0) {
                state.rollback_journal_by_job_id[jobId] =
                    cloneJournalEntries(journalEntries);
            } else {
                delete state.rollback_journal_by_job_id[jobId];
            }

            if (mirrorEntries.length > 0) {
                state.sn_mirror_by_job_id[jobId] =
                    cloneMirrorEntries(mirrorEntries);
            } else {
                delete state.sn_mirror_by_job_id[jobId];
            }
        });
    }

    private async startCommitDrivenExecution(
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
        chunks: RestorePlanHashRowInput[][],
    ): Promise<ExecuteRestoreJobResult> {
        if (chunks.length === 0) {
            return this.finalizeCommitDrivenExecution(
                job,
                plan,
                record,
                [],
                [],
            );
        }

        await this.persistExecutionDelta(
            job.job_id,
            record,
            [],
            [],
        );

        return {
            success: true,
            statusCode: 202,
            record,
            promoted_job_ids: [],
        };
    }

    private async resumeCommitDrivenExecution(
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
    ): Promise<ExecuteRestoreJobResult> {
        const chunks = chunkRows(plan.plan_hash_input.rows, record.chunk_size);
        const rollbackState = await this.loadRollbackJournalState(job.job_id);

        record.resume_attempt_count += 1;
        record.status = 'paused';
        record.reason_code = 'none';
        record.completed_at = null;

        if (record.checkpoint.next_chunk_index >= chunks.length) {
            return this.finalizeCommitDrivenExecution(
                job,
                plan,
                record,
                rollbackState.journalEntries,
                rollbackState.mirrorEntries,
            );
        }

        await this.persistExecutionDelta(
            job.job_id,
            record,
            rollbackState.journalEntries,
            rollbackState.mirrorEntries,
        );

        return {
            success: true,
            statusCode: 202,
            record,
            promoted_job_ids: [],
        };
    }

    private async finalizeCommitDrivenExecution(
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
        journalEntries: RestoreJournalEntry[],
        mirrorEntries: RestoreJournalMirrorRecord[],
    ): Promise<ExecuteRestoreJobResult> {
        if (
            record.media_outcomes.length === 0 &&
            plan.media_candidates.length > 0
        ) {
            this.processMediaCandidates(plan, record);
        }

        record.summary = summarizeExecution(
            plan.plan_hash_input.rows.length,
            plan.media_candidates.length,
            record.chunks,
            record.row_outcomes,
            record.media_outcomes,
        );

        const hasFailures = record.summary.failed_rows > 0 ||
            record.summary.attachments_failed > 0;
        const terminalStatus = hasFailures ? 'failed' : 'completed';
        const terminalReason: RestoreReasonCode = hasFailures
            ? 'failed_internal_error'
            : 'none';
        const completion = await this.jobs.completeJob(job.job_id, {
            status: terminalStatus,
            reason_code: terminalReason,
        });

        if (!completion.success) {
            return buildFailure(
                500,
                'job_completion_failed',
                completion.message,
                'failed_internal_error',
            );
        }

        record.status = terminalStatus;
        record.reason_code = terminalReason;
        record.completed_at = normalizeIsoWithMillis(this.now());

        await this.persistExecutionDelta(
            job.job_id,
            record,
            journalEntries,
            mirrorEntries,
        );

        return {
            success: true,
            statusCode: 200,
            record,
            promoted_job_ids: completion.promoted_job_ids,
        };
    }

    private async processAttempt(
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
        runtimeConflictByRow: Map<string, ExecuteRuntimeConflictInput>,
    ): Promise<ExecuteRestoreJobResult> {
        const chunks = chunkRows(plan.plan_hash_input.rows, record.chunk_size);
        const rollbackState = await this.loadRollbackJournalState(job.job_id);
        const journalEntries = rollbackState.journalEntries;
        const mirrorEntries = rollbackState.mirrorEntries;

        if (record.checkpoint.total_chunks !== chunks.length) {
            return buildFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint chunk cardinality mismatches current plan',
                'blocked_resume_precondition_mismatch',
            );
        }

        const checkpointNextChunkIndex = record.checkpoint.next_chunk_index;
        const checkpointNextRowIndex = record.checkpoint.next_row_index ?? 0;
        record.checkpoint.next_row_index = checkpointNextRowIndex;

        if (checkpointNextChunkIndex > chunks.length) {
            return buildFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint chunk index exceeds current plan chunk bounds',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (
            checkpointNextChunkIndex === chunks.length &&
            checkpointNextRowIndex !== 0
        ) {
            return buildFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint row cursor must reset at terminal chunk index',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (
            checkpointNextChunkIndex < chunks.length &&
            checkpointNextRowIndex > chunks[checkpointNextChunkIndex].length
        ) {
            return buildFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint row cursor exceeds chunk row bounds',
                'blocked_resume_precondition_mismatch',
            );
        }

        if (record.checkpoint.next_chunk_index >= chunks.length) {
            const completion = await this.jobs.completeJob(job.job_id, {
                status: 'completed',
                reason_code: 'none',
            });

            if (!completion.success) {
                return buildFailure(
                    500,
                    'job_completion_failed',
                    completion.message,
                );
            }

            record.status = 'completed';
            record.reason_code = 'none';
            record.completed_at = normalizeIsoWithMillis(this.now());
            record.summary = summarizeExecution(
                plan.plan_hash_input.rows.length,
                plan.media_candidates.length,
                record.chunks,
                record.row_outcomes,
                record.media_outcomes,
            );

            await this.persistExecutionDelta(
                job.job_id,
                record,
                journalEntries,
                mirrorEntries,
            );

            return {
                success: true,
                statusCode: 200,
                record,
                promoted_job_ids: completion.promoted_job_ids,
            };
        }

        record.resume_attempt_count += 1;

        const maxChunksPerAttempt = this.config.maxChunksPerAttempt > 0
            ? this.config.maxChunksPerAttempt
            : chunks.length;
        let chunkIndex = checkpointNextChunkIndex;
        let rowIndex = checkpointNextRowIndex;
        let completedChunkCount = 0;

        while (
            chunkIndex < chunks.length &&
            completedChunkCount < maxChunksPerAttempt
        ) {
            const rows = chunks[chunkIndex];
            const chunkId = asChunkId(chunkIndex);
            const chunkStartedAt = normalizeIsoWithMillis(this.now());
            const triggerConflictIds: string[] = [];

            for (const row of rows) {
                const conflict = runtimeConflictByRow.get(row.row_id);

                if (conflict) {
                    triggerConflictIds.push(conflict.conflict_id);
                }
            }

            const useFallback = triggerConflictIds.length > 0;
            const existingChunkCounts = countChunkOutcomesForChunk(
                record.row_outcomes,
                chunkId,
            );
            let appliedCount = existingChunkCounts.appliedCount;
            let skippedCount = existingChunkCounts.skippedCount;
            let failedCount = existingChunkCounts.failedCount;

            if (rowIndex < 0 || rowIndex > rows.length) {
                return buildFailure(
                    409,
                    'resume_checkpoint_mismatch',
                    'checkpoint row cursor exceeds chunk row bounds',
                    'blocked_resume_precondition_mismatch',
                );
            }

            for (
                let currentRowIndex = rowIndex;
                currentRowIndex < rows.length;
                currentRowIndex += 1
            ) {
                const row = rows[currentRowIndex];
                const attempt =
                    (record.checkpoint.row_attempt_by_row[row.row_id] || 0) + 1;
                record.checkpoint.row_attempt_by_row[row.row_id] = attempt;

                const conflict = runtimeConflictByRow.get(row.row_id);

                if (useFallback && conflict) {
                    record.row_outcomes.push({
                        row_id: row.row_id,
                        table: row.table,
                        record_sys_id: row.record_sys_id,
                        action: row.action,
                        outcome: 'skipped',
                        reason_code: conflict.reason_code,
                        chunk_id: chunkId,
                        used_row_fallback: true,
                        conflict_id: conflict.conflict_id,
                        conflict_class: conflict.class,
                        conflict_resolution: conflict.resolution,
                        message: conflict.reason,
                    });
                    skippedCount += 1;
                } else if (row.action === 'skip') {
                    record.row_outcomes.push({
                        row_id: row.row_id,
                        table: row.table,
                        record_sys_id: row.record_sys_id,
                        action: row.action,
                        outcome: 'skipped',
                        reason_code: 'none',
                        chunk_id: chunkId,
                        used_row_fallback: useFallback,
                    });
                    skippedCount += 1;
                } else {
                    let writeResult;

                    try {
                        writeResult = await this.targetWriter.applyRow({
                            chunk_id: chunkId,
                            executed_by: record.executed_by,
                            job_id: record.job_id,
                            plan_hash: record.plan_hash,
                            row,
                            row_attempt: attempt,
                        });
                    } catch (error) {
                        const message = error instanceof Error
                            ? error.message
                            : 'target writer apply threw non-error value';

                        record.row_outcomes.push({
                            row_id: row.row_id,
                            table: row.table,
                            record_sys_id: row.record_sys_id,
                            action: row.action,
                            outcome: 'failed',
                            reason_code: 'failed_internal_error',
                            chunk_id: chunkId,
                            used_row_fallback: useFallback,
                            message,
                        });
                        failedCount += 1;
                        writeResult = null;
                    }

                    if (writeResult) {
                        record.row_outcomes.push({
                            row_id: row.row_id,
                            table: row.table,
                            record_sys_id: row.record_sys_id,
                            action: row.action,
                            outcome: writeResult.outcome,
                            reason_code: writeResult.reason_code,
                            chunk_id: chunkId,
                            used_row_fallback: useFallback,
                            message: writeResult.message,
                        });

                        if (writeResult.outcome === 'applied') {
                            this.appendRollbackJournal(
                                record,
                                row,
                                chunkId,
                                attempt,
                                journalEntries,
                                mirrorEntries,
                            );
                            appliedCount += 1;
                        } else if (writeResult.outcome === 'skipped') {
                            skippedCount += 1;
                        } else {
                            failedCount += 1;
                        }
                    }
                }

                const rowCompletedAt = normalizeIsoWithMillis(this.now());
                record.checkpoint.next_chunk_index = chunkIndex;
                record.checkpoint.next_row_index = currentRowIndex + 1;
                record.checkpoint.last_chunk_id = chunkId;
                record.checkpoint.updated_at = rowCompletedAt;
                record.checkpoint.checkpoint_id = buildCheckpointId(
                    record.job_id,
                    record.plan_hash,
                    chunkIndex,
                    record.checkpoint.next_row_index,
                );

                await this.persistExecutionDelta(
                    job.job_id,
                    record,
                    journalEntries,
                    mirrorEntries,
                );
            }

            const chunkCompletedAt = normalizeIsoWithMillis(this.now());
            const chunkStatus = failedCount > 0
                ? 'failed'
                : useFallback
                ? 'row_fallback'
                : 'applied';
            const chunkOutcome: ExecuteChunkOutcome = {
                chunk_id: chunkId,
                row_count: rows.length,
                status: chunkStatus,
                started_at: chunkStartedAt,
                completed_at: chunkCompletedAt,
                applied_count: appliedCount,
                skipped_count: skippedCount,
                failed_count: failedCount,
                fallback_trigger_conflict_ids: triggerConflictIds,
            };
            const existingChunkIndex = record.chunks.findIndex((chunk) => {
                return chunk.chunk_id === chunkId;
            });

            if (existingChunkIndex >= 0) {
                record.chunks[existingChunkIndex] = chunkOutcome;
            } else {
                record.chunks.push(chunkOutcome);
            }

            record.checkpoint.next_chunk_index = chunkIndex + 1;
            record.checkpoint.next_row_index = 0;
            record.checkpoint.last_chunk_id = chunkId;
            record.checkpoint.updated_at = chunkCompletedAt;
            record.checkpoint.checkpoint_id = buildCheckpointId(
                record.job_id,
                record.plan_hash,
                record.checkpoint.next_chunk_index,
                0,
            );

            await this.persistExecutionDelta(
                job.job_id,
                record,
                journalEntries,
                mirrorEntries,
            );

            chunkIndex += 1;
            rowIndex = 0;
            completedChunkCount += 1;
        }

        record.summary = summarizeExecution(
            plan.plan_hash_input.rows.length,
            plan.media_candidates.length,
            record.chunks,
            record.row_outcomes,
            record.media_outcomes,
        );

        if (record.checkpoint.next_chunk_index < chunks.length) {
            const pause = await this.jobs.pauseJob(
                job.job_id,
                'paused_token_refresh_grace_exhausted',
            );

            if (!pause.success) {
                return buildFailure(
                    500,
                    'job_pause_failed',
                    pause.message,
                    'failed_internal_error',
                );
            }

            record.status = 'paused';
            record.reason_code = 'paused_token_refresh_grace_exhausted';
            record.completed_at = null;

            await this.persistExecutionDelta(
                job.job_id,
                record,
                journalEntries,
                mirrorEntries,
            );

            return {
                success: true,
                statusCode: 202,
                record,
                promoted_job_ids: [],
            };
        }

        if (
            record.media_outcomes.length === 0 &&
            plan.media_candidates.length > 0
        ) {
            this.processMediaCandidates(plan, record);
            record.summary = summarizeExecution(
                plan.plan_hash_input.rows.length,
                plan.media_candidates.length,
                record.chunks,
                record.row_outcomes,
                record.media_outcomes,
            );
        }

        const hasFailures = record.summary.failed_rows > 0 ||
            record.summary.attachments_failed > 0;
        const terminalStatus = hasFailures ? 'failed' : 'completed';
        const terminalReason: RestoreReasonCode = hasFailures
            ? 'failed_internal_error'
            : 'none';
        const completion = await this.jobs.completeJob(job.job_id, {
            status: terminalStatus,
            reason_code: terminalReason,
        });

        if (!completion.success) {
            return buildFailure(
                500,
                'job_completion_failed',
                completion.message,
            );
        }

        record.status = terminalStatus;
        record.reason_code = terminalReason;
        record.completed_at = normalizeIsoWithMillis(this.now());

        await this.persistExecutionDelta(
            job.job_id,
            record,
            journalEntries,
            mirrorEntries,
        );

        return {
            success: true,
            statusCode: 200,
            record,
            promoted_job_ids: completion.promoted_job_ids,
        };
    }

    private appendRollbackJournal(
        record: RestoreExecutionRecord,
        row: RestorePlanHashRowInput,
        chunkId: string,
        rowAttempt: number,
        journalEntries: RestoreJournalEntry[],
        mirrorEntries: RestoreJournalMirrorRecord[],
    ): void {
        const journalId = buildJournalId(
            record.job_id,
            record.plan_hash,
            row.row_id,
            rowAttempt,
        );

        for (const existing of journalEntries) {
            if (existing.journal_id === journalId) {
                return;
            }
        }

        const beforeImage = row.values?.before_image_enc ||
            row.values?.diff_enc ||
            row.values?.after_image_enc;

        if (!beforeImage) {
            return;
        }

        const nowIso = normalizeIsoWithMillis(this.now());
        const entry: RestoreJournalEntry = {
            contract_version: RESTORE_CONTRACT_VERSION,
            journal_id: journalId,
            job_id: record.job_id,
            plan_hash: record.plan_hash,
            plan_row_id: row.row_id,
            table: row.table,
            record_sys_id: row.record_sys_id,
            action: row.action,
            touched_fields: ['__all_touched_fields__'],
            before_image_enc: beforeImage,
            chunk_id: chunkId,
            row_attempt: rowAttempt,
            executed_by: record.executed_by,
            executed_at: nowIso,
            outcome: 'applied',
        };

        journalEntries.push(entry);
        mirrorEntries.push({
            mirror_id: buildMirrorId(journalId),
            journal_id: journalId,
            job_id: record.job_id,
            plan_hash: record.plan_hash,
            plan_row_id: row.row_id,
            table: row.table,
            record_sys_id: row.record_sys_id,
            action: row.action,
            outcome: 'applied',
            reason_code: 'none',
            chunk_id: chunkId,
            row_attempt: rowAttempt,
            linked_at: nowIso,
        });
    }

    private processMediaCandidates(
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
    ): void {
        const candidates = plan.media_candidates;

        if (candidates.length === 0) {
            return;
        }

        const chunkSize = Math.max(1, this.config.mediaChunkSize);
        const defaultMaxRetryAttempts = Math.max(
            1,
            this.config.mediaMaxRetryAttempts,
        );
        let mediaChunkIndex = 0;

        for (
            let index = 0;
            index < candidates.length;
            index += chunkSize
        ) {
            const chunk = candidates.slice(index, index + chunkSize);
            const chunkId = asMediaChunkId(mediaChunkIndex);
            mediaChunkIndex += 1;

            for (const candidate of chunk) {
                const decision = candidate.decision || 'exclude';

                if (decision === 'exclude') {
                    record.media_outcomes.push({
                        candidate_id: candidate.candidate_id,
                        table: candidate.table,
                        record_sys_id: candidate.record_sys_id,
                        attachment_sys_id: candidate.attachment_sys_id,
                        media_id: candidate.media_id,
                        decision,
                        outcome: 'skipped',
                        reason_code: 'none',
                        chunk_id: chunkId,
                        attempt_count: 0,
                        size_bytes: candidate.size_bytes,
                        expected_sha256_plain: candidate.sha256_plain,
                        observed_sha256_plain: candidate.observed_sha256_plain,
                        message: 'operator excluded candidate',
                    });
                    continue;
                }

                if (candidate.parent_record_exists === false) {
                    record.media_outcomes.push({
                        candidate_id: candidate.candidate_id,
                        table: candidate.table,
                        record_sys_id: candidate.record_sys_id,
                        attachment_sys_id: candidate.attachment_sys_id,
                        media_id: candidate.media_id,
                        decision,
                        outcome: 'failed',
                        reason_code: 'failed_media_parent_missing',
                        chunk_id: chunkId,
                        attempt_count: 0,
                        size_bytes: candidate.size_bytes,
                        expected_sha256_plain: candidate.sha256_plain,
                        observed_sha256_plain: candidate.observed_sha256_plain,
                        message: 'parent record is missing',
                    });
                    continue;
                }

                const expectedHash = String(candidate.sha256_plain || '')
                    .toLowerCase();
                const observedHash = String(
                    candidate.observed_sha256_plain || candidate.sha256_plain || '',
                ).toLowerCase();

                if (!observedHash || observedHash !== expectedHash) {
                    record.media_outcomes.push({
                        candidate_id: candidate.candidate_id,
                        table: candidate.table,
                        record_sys_id: candidate.record_sys_id,
                        attachment_sys_id: candidate.attachment_sys_id,
                        media_id: candidate.media_id,
                        decision,
                        outcome: 'failed',
                        reason_code: 'failed_media_hash_mismatch',
                        chunk_id: chunkId,
                        attempt_count: 0,
                        size_bytes: candidate.size_bytes,
                        expected_sha256_plain: candidate.sha256_plain,
                        observed_sha256_plain:
                            candidate.observed_sha256_plain,
                        message:
                            'candidate hash verification failed before apply',
                    });
                    continue;
                }

                const configuredMaxAttempts =
                    candidate.max_retry_attempts &&
                        candidate.max_retry_attempts > 0
                    ? candidate.max_retry_attempts
                    : defaultMaxRetryAttempts;
                let remainingRetryableFailures =
                    candidate.retryable_failures || 0;
                let attemptCount = 0;
                let applied = false;

                while (attemptCount < configuredMaxAttempts) {
                    attemptCount += 1;

                    if (remainingRetryableFailures > 0) {
                        remainingRetryableFailures -= 1;
                        continue;
                    }

                    applied = true;
                    break;
                }

                if (!applied) {
                    record.media_outcomes.push({
                        candidate_id: candidate.candidate_id,
                        table: candidate.table,
                        record_sys_id: candidate.record_sys_id,
                        attachment_sys_id: candidate.attachment_sys_id,
                        media_id: candidate.media_id,
                        decision,
                        outcome: 'failed',
                        reason_code: 'failed_media_retry_exhausted',
                        chunk_id: chunkId,
                        attempt_count: attemptCount,
                        size_bytes: candidate.size_bytes,
                        expected_sha256_plain: candidate.sha256_plain,
                        observed_sha256_plain:
                            candidate.observed_sha256_plain,
                        message:
                            'retry budget exhausted for media transfer',
                    });
                    continue;
                }

                record.media_outcomes.push({
                    candidate_id: candidate.candidate_id,
                    table: candidate.table,
                    record_sys_id: candidate.record_sys_id,
                    attachment_sys_id: candidate.attachment_sys_id,
                    media_id: candidate.media_id,
                    decision,
                    outcome: 'applied',
                    reason_code: 'none',
                    chunk_id: chunkId,
                    attempt_count: attemptCount,
                    size_bytes: candidate.size_bytes,
                    expected_sha256_plain: candidate.sha256_plain,
                    observed_sha256_plain: candidate.sha256_plain,
                });
            }
        }
    }

    private async lookupAndValidateExecutablePlan(
        job: RestoreJobRecord,
    ): Promise<{
        ok: true;
        plan: RestoreDryRunPlanRecord;
    } | {
        ok: false;
        failure: ExecuteRestoreJobResult;
    }> {
        const plan = await this.plans.getPlan(job.plan_id);

        if (!plan) {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'plan_missing',
                    'job plan is unavailable in plan store',
                ),
            };
        }

        if (plan.plan.plan_hash !== job.plan_hash) {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'plan_hash_mismatch',
                    'job plan_hash does not match immutable plan record',
                    'blocked_plan_hash_mismatch',
                ),
            };
        }

        if (plan.gate.executability !== 'executable') {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'plan_not_executable',
                    'dry-run plan gate is not executable',
                    plan.gate.reason_code,
                ),
            };
        }

        const unresolvedDeletes = plan.delete_candidates.some((candidate) =>
            !candidate.decision
        );

        if (unresolvedDeletes) {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'delete_candidates_unresolved',
                    'all delete candidates require explicit decisions',
                    'blocked_unresolved_delete_candidates',
                ),
            };
        }

        if (hasUnresolvedMediaCandidates(plan)) {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'media_candidates_unresolved',
                    'all media candidates require explicit include/exclude ' +
                        'decisions',
                    'blocked_unresolved_media_candidates',
                ),
            };
        }

        const planConflictCheck = ensureConflictsResolvable(
            plan.plan.conflicts.map((conflict) => {
                return {
                    class: conflict.class,
                    conflict_id: conflict.conflict_id,
                    resolution: conflict.resolution,
                };
            }),
        );

        if (!planConflictCheck.ok) {
            return {
                ok: false,
                failure: buildFailure(
                    409,
                    'plan_conflict_blocked',
                    planConflictCheck.message ||
                        'plan conflict blocked execute',
                    planConflictCheck.reasonCode,
                ),
            };
        }

        return {
            ok: true,
            plan,
        };
    }

    private validateRuntimeConflictRows(
        plan: RestoreDryRunPlanRecord,
        runtimeConflicts: ExecuteRuntimeConflictInput[],
    ): {
        ok: true;
    } | {
        ok: false;
        failure: ExecuteRestoreJobResult;
    } {
        const rowSet = new Set(
            plan.plan_hash_input.rows.map((row) => row.row_id),
        );

        for (const conflict of runtimeConflicts) {
            if (!rowSet.has(conflict.row_id)) {
                return {
                    ok: false,
                    failure: buildFailure(
                        400,
                        'invalid_request',
                        `runtime conflict row_id ${conflict.row_id} ` +
                        'is not part of the approved plan',
                    ),
                };
            }
        }

        return {
            ok: true,
        };
    }

    private validateCapabilities(
        request: ExecuteRestoreJobRequest,
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
    ): {
        ok: boolean;
        message?: string;
    } {
        const required = new Set<
            | 'restore_execute'
            | 'restore_delete'
            | 'restore_override_caps'
            | 'restore_schema_override'
        >();

        required.add('restore_execute');

        for (const capability of job.required_capabilities) {
            required.add(capability);
        }

        if (requiresDeleteCapability(plan)) {
            required.add('restore_delete');
        }

        if (
            plan.plan.execution_options.schema_compatibility_mode ===
            'manual_override'
        ) {
            required.add('restore_schema_override');
        }

        const predictedSkipCount = countPlannedSkips(plan) +
            request.runtime_conflicts.length;
        const plannedRowCount = plan.plan_hash_input.rows.length;
        const skipRatioPercent = plannedRowCount === 0
            ? 0
            : (predictedSkipCount / plannedRowCount) * 100;
        const plannedMediaItems = countPlannedMediaItems(plan);
        const plannedMediaBytes = countPlannedMediaBytes(plan);
        const rowOverrideRequired = plannedRowCount > this.config.maxRows ||
            skipRatioPercent > this.config.elevatedSkipRatioPercent;
        const mediaOverrideRequired =
            plannedMediaItems > this.config.mediaMaxItems ||
            plannedMediaBytes > this.config.mediaMaxBytes;
        const overrideRequired = rowOverrideRequired || mediaOverrideRequired;

        if (overrideRequired) {
            required.add('restore_override_caps');

            if (!request.elevated_confirmation) {
                const overrideReasons: string[] = [];

                if (plannedRowCount > this.config.maxRows) {
                    overrideReasons.push(
                        'planned rows exceed configured hard cap',
                    );
                }

                if (skipRatioPercent > this.config.elevatedSkipRatioPercent) {
                    overrideReasons.push(
                        'predicted skip ratio exceeds elevated threshold',
                    );
                }

                if (plannedMediaItems > this.config.mediaMaxItems) {
                    overrideReasons.push(
                        'attachment/media item count exceeds cap',
                    );
                }

                if (plannedMediaBytes > this.config.mediaMaxBytes) {
                    overrideReasons.push(
                        'attachment/media byte total exceeds cap',
                    );
                }

                return {
                    ok: false,
                    message:
                        'elevated confirmation is required for this ' +
                        'execution scope: ' + overrideReasons.join('; '),
                };
            }
        }

        const provided = new Set(request.operator_capabilities);
        const missing: string[] = [];

        for (const capability of required) {
            if (!provided.has(capability)) {
                missing.push(capability);
            }
        }

        if (missing.length > 0) {
            return {
                ok: false,
                message:
                    'missing required capabilities: ' +
                    missing.sort((left, right) => {
                        return left.localeCompare(right);
                    }).join(', '),
            };
        }

        return {
            ok: true,
        };
    }

    private validateResumeCapabilities(
        request: ResumeRestoreJobRequest,
        record: RestoreExecutionRecord,
    ): {
        ok: boolean;
        message?: string;
    } {
        const provided = new Set(request.operator_capabilities);
        const missing: string[] = [];

        for (const capability of record.capabilities_used) {
            if (!provided.has(capability)) {
                missing.push(capability);
            }
        }

        if (missing.length > 0) {
            return {
                ok: false,
                message:
                    'resume requires prior execute capabilities: ' +
                    missing.sort((left, right) => {
                        return left.localeCompare(right);
                    }).join(', '),
            };
        }

        return {
            ok: true,
        };
    }

    private resolveWorkflow(
        request: ExecuteRestoreJobRequest,
        plan: RestoreDryRunPlanRecord,
    ): {
        ok: boolean;
        message?: string;
        workflowMode: 'suppressed_default' | 'allowlist';
        allowlist: string[];
    } {
        const planWorkflowMode = plan.plan.execution_options.workflow_mode;
        const requestedWorkflow = request.workflow;

        if (!requestedWorkflow) {
            return {
                ok: true,
                workflowMode: planWorkflowMode,
                allowlist: [],
            };
        }

        if (requestedWorkflow.mode !== planWorkflowMode) {
            return {
                ok: false,
                message:
                    'workflow mode mismatch with approved plan execution ' +
                    'options',
                workflowMode: planWorkflowMode,
                allowlist: [],
            };
        }

        if (requestedWorkflow.mode === 'suppressed_default') {
            return {
                ok: true,
                workflowMode: requestedWorkflow.mode,
                allowlist: [],
            };
        }

        const allowlist = [...requestedWorkflow.allowlist]
            .sort((left, right) => {
                return left.localeCompare(right);
            });

        return {
            ok: true,
            workflowMode: requestedWorkflow.mode,
            allowlist,
        };
    }
}
