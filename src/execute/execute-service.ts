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
import { RestoreDryRunPlanRecord } from '../plans/models';
import { RestorePlanService } from '../plans/plan-service';
import {
    InMemoryRestoreExecutionStateStore,
    RestoreExecutionState,
    RestoreExecutionStateStore,
} from './execute-state-store';
import {
    ExecuteChunkOutcome,
    ExecuteRestoreJobRequest,
    ExecuteRestoreJobRequestSchema,
    ExecuteRestoreJobResult,
    ExecuteMediaOutcome,
    ExecuteRowOutcome,
    ExecuteRuntimeConflictInput,
    ExecuteServiceConfig,
    ExecutionResumeCheckpoint,
    RestoreExecutionRecord,
    RestoreJournalMirrorRecord,
    RestoreRollbackJournalBundle,
    RestoreReasonCode,
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
): string {
    const checksum = createHash('sha256')
        .update(`${jobId}:${planHash}:${nextChunkIndex}`, 'utf8')
        .digest('hex');

    return `chk_${checksum.slice(0, 24)}`;
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
        checkpoint_id: buildCheckpointId(jobId, planHash, 0),
        next_chunk_index: 0,
        total_chunks: totalChunks,
        last_chunk_id: null,
        row_attempt_by_row: {},
        updated_at: nowIso,
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

    for (const row of rowOutcomes) {
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

    for (const chunk of chunks) {
        if (chunk.status === 'row_fallback') {
            fallbackChunkCount += 1;
        }
    }

    for (const media of mediaOutcomes) {
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
        chunk_count: chunks.length,
        fallback_chunk_count: fallbackChunkCount,
        runtime_conflict_count: runtimeConflictIds.size,
    };
}

export class RestoreExecutionService {
    private readonly records = new Map<string, RestoreExecutionRecord>();

    private readonly rollbackJournal = new Map<string, RestoreJournalEntry[]>();

    private readonly snMirrors = new Map<string, RestoreJournalMirrorRecord[]>();

    private readonly config: ExecuteServiceConfig;

    private initialized = false;

    private initializationPromise: Promise<void> | null = null;

    constructor(
        private readonly jobs: RestoreJobService,
        private readonly plans: RestorePlanService,
        config?: Partial<ExecuteServiceConfig>,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestoreExecutionStateStore =
            new InMemoryRestoreExecutionStateStore(),
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
        await this.ensureInitialized();

        const existing = this.records.get(jobId);

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
                record: existing,
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
        await this.ensureInitialized();

        const existing = this.records.get(jobId);

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
                    record: existing,
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

        const resume = await this.jobs.resumePausedJob(jobId);

        if (!resume.success) {
            return buildFailure(
                resume.statusCode,
                resume.error,
                resume.message,
                'blocked_resume_checkpoint_missing',
            );
        }

        return this.processAttempt(
            resume.job,
            plan,
            existing,
            toConflictMap(request.runtime_conflicts),
        );
    }

    async getExecution(jobId: string): Promise<RestoreExecutionRecord | null> {
        await this.ensureInitialized();

        const record = this.records.get(jobId);

        if (!record) {
            return null;
        }

        return JSON.parse(JSON.stringify(record)) as RestoreExecutionRecord;
    }

    async listExecutions(): Promise<RestoreExecutionRecord[]> {
        await this.ensureInitialized();

        return Array.from(this.records.values())
            .map((record) =>
                JSON.parse(JSON.stringify(record)) as RestoreExecutionRecord
            )
            .sort((left, right) => {
                return left.started_at.localeCompare(right.started_at);
            });
    }

    async getCheckpoint(jobId: string): Promise<ExecutionResumeCheckpoint | null> {
        await this.ensureInitialized();

        const record = this.records.get(jobId);

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
        await this.ensureInitialized();

        const journalEntries = this.rollbackJournal.get(jobId);
        const mirrorEntries = this.snMirrors.get(jobId);

        if (!journalEntries && !mirrorEntries) {
            return null;
        }

        return {
            journal_entries: [...(journalEntries || [])],
            sn_mirror_entries: [...(mirrorEntries || [])],
        };
    }

    private async setExecutionRecord(
        jobId: string,
        record: RestoreExecutionRecord,
    ): Promise<void> {
        this.records.set(jobId, record);
        await this.persistState();
    }

    private async persistState(): Promise<void> {
        const snapshot = this.snapshotState();

        await this.stateStore.mutate((state) => {
            state.records_by_job_id = snapshot.records_by_job_id;
            state.rollback_journal_by_job_id =
                snapshot.rollback_journal_by_job_id;
            state.sn_mirror_by_job_id = snapshot.sn_mirror_by_job_id;
        });
    }

    private snapshotState(): RestoreExecutionState {
        const recordsByJobId: Record<string, RestoreExecutionRecord> = {};

        for (const [jobId, record] of this.records.entries()) {
            recordsByJobId[jobId] = record;
        }

        const rollbackJournalByJobId: Record<string, RestoreJournalEntry[]> = {};

        for (const [jobId, entries] of this.rollbackJournal.entries()) {
            rollbackJournalByJobId[jobId] = entries;
        }

        const snMirrorByJobId: Record<string, RestoreJournalMirrorRecord[]> = {};

        for (const [jobId, entries] of this.snMirrors.entries()) {
            snMirrorByJobId[jobId] = entries;
        }

        return JSON.parse(
            JSON.stringify({
                records_by_job_id: recordsByJobId,
                rollback_journal_by_job_id: rollbackJournalByJobId,
                sn_mirror_by_job_id: snMirrorByJobId,
            }),
        ) as RestoreExecutionState;
    }

    private async processAttempt(
        job: RestoreJobRecord,
        plan: RestoreDryRunPlanRecord,
        record: RestoreExecutionRecord,
        runtimeConflictByRow: Map<string, ExecuteRuntimeConflictInput>,
    ): Promise<ExecuteRestoreJobResult> {
        const chunks = chunkRows(plan.plan_hash_input.rows, record.chunk_size);

        if (record.checkpoint.total_chunks !== chunks.length) {
            return buildFailure(
                409,
                'resume_checkpoint_mismatch',
                'checkpoint chunk cardinality mismatches current plan',
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

            await this.setExecutionRecord(job.job_id, record);

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
        const startChunkIndex = record.checkpoint.next_chunk_index;
        const stopExclusive = Math.min(
            chunks.length,
            startChunkIndex + maxChunksPerAttempt,
        );

        for (
            let chunkIndex = startChunkIndex;
            chunkIndex < stopExclusive;
            chunkIndex += 1
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
            let appliedCount = 0;
            let skippedCount = 0;
            let failedCount = 0;

            for (const row of rows) {
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
                    continue;
                }

                if (row.action === 'skip') {
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
                    continue;
                }

                record.row_outcomes.push({
                    row_id: row.row_id,
                    table: row.table,
                    record_sys_id: row.record_sys_id,
                    action: row.action,
                    outcome: 'applied',
                    reason_code: 'none',
                    chunk_id: chunkId,
                    used_row_fallback: useFallback,
                });
                this.appendRollbackJournal(record, row, chunkId, attempt);
                appliedCount += 1;
            }

            const chunkCompletedAt = normalizeIsoWithMillis(this.now());
            record.chunks.push({
                chunk_id: chunkId,
                row_count: rows.length,
                status: useFallback ? 'row_fallback' : 'applied',
                started_at: chunkStartedAt,
                completed_at: chunkCompletedAt,
                applied_count: appliedCount,
                skipped_count: skippedCount,
                failed_count: failedCount,
                fallback_trigger_conflict_ids: triggerConflictIds,
            });

            record.checkpoint.next_chunk_index = chunkIndex + 1;
            record.checkpoint.last_chunk_id = chunkId;
            record.checkpoint.updated_at = chunkCompletedAt;
            record.checkpoint.checkpoint_id = buildCheckpointId(
                record.job_id,
                record.plan_hash,
                record.checkpoint.next_chunk_index,
            );
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

            await this.setExecutionRecord(job.job_id, record);

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

        await this.setExecutionRecord(job.job_id, record);

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
    ): void {
        const journalEntries = this.rollbackJournal.get(record.job_id) || [];
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
        this.rollbackJournal.set(record.job_id, journalEntries);

        const mirrors = this.snMirrors.get(record.job_id) || [];
        mirrors.push({
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
        this.snMirrors.set(record.job_id, mirrors);
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

    private async ensureInitialized(): Promise<void> {
        if (this.initialized) {
            return;
        }

        if (!this.initializationPromise) {
            this.initializationPromise = this.initialize();
        }

        await this.initializationPromise;
    }

    private async initialize(): Promise<void> {
        const state = await this.stateStore.read();

        for (const [jobId, record] of Object.entries(state.records_by_job_id)) {
            this.records.set(jobId, record);
        }

        for (
            const [jobId, entries] of Object.entries(
                state.rollback_journal_by_job_id,
            )
        ) {
            this.rollbackJournal.set(jobId, entries);
        }

        for (const [jobId, entries] of Object.entries(state.sn_mirror_by_job_id)) {
            this.snMirrors.set(jobId, entries);
        }

        this.initialized = true;
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
