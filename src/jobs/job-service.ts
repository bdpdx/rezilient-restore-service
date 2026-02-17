import { randomUUID } from 'node:crypto';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from '../constants';
import { AuthTokenClaims } from '../auth/claims';
import { RestoreLockManager } from '../locks/lock-manager';
import { SourceRegistry } from '../registry/source-registry';
import {
    buildApprovalPlaceholder,
    CompleteRestoreJobRequest,
    CompleteRestoreJobResult,
    CompleteRestoreJobRequestSchema,
    CreateRestoreJobRequest,
    CreateRestoreJobRequestSchema,
    CreateRestoreJobResult,
    isTerminalStatus,
    normalizeIsoWithMillis,
    PauseRestoreJobResult,
    ResumeRestoreJobResult,
    RestoreJobAuditEvent,
    RestoreJobStatus,
    RestoreJobRecord,
    RestorePlanMetadataRecord,
    RestoreReasonCode,
} from './models';

function normalizeTables(tables: string[]): string[] {
    const tableSet = new Set<string>();

    for (const table of tables) {
        tableSet.add(table.trim());
    }

    return Array.from(tableSet)
        .filter((table) => table.length > 0)
        .sort((left, right) => left.localeCompare(right));
}

function assertStatus(
    status: RestoreJobStatus,
): 'completed' | 'failed' | 'cancelled' {
    if (
        status !== 'completed' &&
        status !== 'failed' &&
        status !== 'cancelled'
    ) {
        throw new Error('status must be completed, failed, or cancelled');
    }

    return status;
}

export class RestoreJobService {
    private readonly plans = new Map<string, RestorePlanMetadataRecord>();

    private readonly jobs = new Map<string, RestoreJobRecord>();

    private readonly events = new Map<string, RestoreJobAuditEvent[]>();

    constructor(
        private readonly lockManager: RestoreLockManager,
        private readonly sourceRegistry: SourceRegistry,
        private readonly now: () => Date = () => new Date(),
    ) {}

    createJob(
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): CreateRestoreJobResult {
        const parsed = CreateRestoreJobRequestSchema.safeParse(requestBody);

        if (!parsed.success) {
            return {
                success: false,
                statusCode: 400,
                error: 'invalid_request',
                message: parsed.error.issues[0]?.message || 'Invalid request',
            };
        }

        const request = parsed.data;

        const mappingCheck = this.validateScopeRequest(request, claims);

        if (!mappingCheck.allowed) {
            return {
                success: false,
                statusCode: 403,
                error: 'scope_blocked',
                reasonCode: mappingCheck.reasonCode,
                message: mappingCheck.message,
            };
        }

        const existingPlan = this.plans.get(request.plan_id);

        if (existingPlan && existingPlan.plan_hash !== request.plan_hash) {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_hash_mismatch',
                reasonCode: 'blocked_plan_hash_mismatch',
                message: 'plan_id already exists with a different plan_hash',
            };
        }

        const nowIso = normalizeIsoWithMillis(this.now());

        if (!existingPlan) {
            const plan: RestorePlanMetadataRecord = {
                contract_version: RESTORE_CONTRACT_VERSION,
                plan_id: request.plan_id,
                plan_hash: request.plan_hash,
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                source: request.source,
                lock_scope_tables: normalizeTables(request.lock_scope_tables),
                requested_by: request.requested_by,
                requested_at: nowIso,
                approval: buildApprovalPlaceholder(request.approval),
                metadata_allowlist_version:
                    RESTORE_METADATA_ALLOWLIST_VERSION,
            };

            this.plans.set(plan.plan_id, plan);
        }

        const jobId = `job_${randomUUID()}`;
        const lockDecision = this.lockManager.acquire({
            jobId,
            tenantId: request.tenant_id,
            instanceId: request.instance_id,
            tables: request.lock_scope_tables,
        });
        const jobStatus: RestoreJobStatus = lockDecision.state === 'running'
            ? 'running'
            : 'queued';
        const job: RestoreJobRecord = {
            contract_version: RESTORE_CONTRACT_VERSION,
            job_id: jobId,
            tenant_id: request.tenant_id,
            instance_id: request.instance_id,
            source: request.source,
            plan_id: request.plan_id,
            plan_hash: request.plan_hash,
            status: jobStatus,
            status_reason_code: lockDecision.reasonCode,
            lock_scope_tables: normalizeTables(request.lock_scope_tables),
            required_capabilities: request.required_capabilities,
            requested_by: request.requested_by,
            requested_at: nowIso,
            approval: buildApprovalPlaceholder(request.approval),
            metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
            queue_position: lockDecision.queuePosition || null,
            wait_reason_code: lockDecision.state === 'queued'
                ? lockDecision.reasonCode
                : null,
            wait_tables: lockDecision.blockedTables,
            started_at: lockDecision.state === 'running' ? nowIso : null,
            completed_at: null,
            updated_at: nowIso,
        };

        this.jobs.set(job.job_id, job);

        this.appendEvent({
            event_id: `evt_${randomUUID()}`,
            event_type: 'job_created',
            job_id: job.job_id,
            reason_code: 'none',
            created_at: nowIso,
            details: {
                plan_id: job.plan_id,
            },
        });

        if (job.status === 'queued') {
            this.appendEvent({
                event_id: `evt_${randomUUID()}`,
                event_type: 'job_queued',
                job_id: job.job_id,
                reason_code: 'queued_scope_lock',
                created_at: nowIso,
                details: {
                    blocked_tables: lockDecision.blockedTables,
                    queue_position: lockDecision.queuePosition || 1,
                },
            });
        } else {
            this.appendEvent({
                event_id: `evt_${randomUUID()}`,
                event_type: 'job_started',
                job_id: job.job_id,
                reason_code: 'none',
                created_at: nowIso,
                details: {
                    lock_scope_tables: job.lock_scope_tables,
                },
            });
        }

        return {
            success: true,
            job,
        };
    }

    completeJob(
        jobId: string,
        requestBody: unknown,
    ): CompleteRestoreJobResult {
        const parsed = CompleteRestoreJobRequestSchema.safeParse(requestBody);

        if (!parsed.success) {
            return {
                success: false,
                statusCode: 400,
                error: 'invalid_request',
                message: parsed.error.issues[0]?.message || 'Invalid request',
            };
        }

        const request = parsed.data;

        return this.completeJobInternal(jobId, request);
    }

    getJob(jobId: string): RestoreJobRecord | null {
        return this.jobs.get(jobId) || null;
    }

    pauseJob(
        jobId: string,
        reasonCode: RestoreReasonCode = 'paused_token_refresh_grace_exhausted',
    ): PauseRestoreJobResult {
        const job = this.jobs.get(jobId);

        if (!job) {
            return {
                success: false,
                statusCode: 404,
                error: 'not_found',
                message: 'job not found',
            };
        }

        if (isTerminalStatus(job.status)) {
            return {
                success: false,
                statusCode: 409,
                error: 'already_terminal',
                message: 'job already completed/failed/cancelled',
            };
        }

        if (job.status !== 'running') {
            return {
                success: false,
                statusCode: 409,
                error: 'job_not_running',
                message: 'job must be running before pause',
            };
        }

        const nowIso = normalizeIsoWithMillis(this.now());
        const paused: RestoreJobRecord = {
            ...job,
            status: 'paused',
            status_reason_code: reasonCode,
            updated_at: nowIso,
        };

        this.jobs.set(jobId, paused);
        this.appendEvent({
            event_id: `evt_${randomUUID()}`,
            event_type: 'job_paused',
            job_id: jobId,
            reason_code: reasonCode,
            created_at: nowIso,
            details: {},
        });

        return {
            success: true,
            job: paused,
        };
    }

    resumePausedJob(jobId: string): ResumeRestoreJobResult {
        const job = this.jobs.get(jobId);

        if (!job) {
            return {
                success: false,
                statusCode: 404,
                error: 'not_found',
                message: 'job not found',
            };
        }

        if (isTerminalStatus(job.status)) {
            return {
                success: false,
                statusCode: 409,
                error: 'already_terminal',
                message: 'job already completed/failed/cancelled',
            };
        }

        if (job.status !== 'paused') {
            return {
                success: false,
                statusCode: 409,
                error: 'job_not_paused',
                message: 'job must be paused before resume',
            };
        }

        const nowIso = normalizeIsoWithMillis(this.now());
        const resumed: RestoreJobRecord = {
            ...job,
            status: 'running',
            status_reason_code: 'none',
            updated_at: nowIso,
        };

        this.jobs.set(jobId, resumed);
        this.appendEvent({
            event_id: `evt_${randomUUID()}`,
            event_type: 'job_started',
            job_id: jobId,
            reason_code: 'none',
            created_at: nowIso,
            details: {
                resumed_from_pause: true,
            },
        });

        return {
            success: true,
            job: resumed,
        };
    }

    listJobEvents(jobId: string): RestoreJobAuditEvent[] {
        return [...(this.events.get(jobId) || [])];
    }

    listJobs(): RestoreJobRecord[] {
        return Array.from(this.jobs.values())
            .map((job) => ({
                ...job,
                lock_scope_tables: [...job.lock_scope_tables],
                required_capabilities: [...job.required_capabilities],
                wait_tables: [...job.wait_tables],
                approval: {
                    ...job.approval,
                },
            }))
            .sort((left, right) => {
                return left.requested_at.localeCompare(right.requested_at);
            });
    }

    getLockSnapshot(): {
        running: Array<{ jobId: string; tables: string[] }>;
        queued: Array<{ jobId: string; tables: string[] }>;
    } {
        return this.lockManager.snapshot();
    }

    listPlans(): RestorePlanMetadataRecord[] {
        return Array.from(this.plans.values());
    }

    private completeJobInternal(
        jobId: string,
        request: CompleteRestoreJobRequest,
    ): CompleteRestoreJobResult {
        const job = this.jobs.get(jobId);

        if (!job) {
            return {
                success: false,
                statusCode: 404,
                error: 'not_found',
                message: 'job not found',
            };
        }

        if (isTerminalStatus(job.status)) {
            return {
                success: false,
                statusCode: 409,
                error: 'already_terminal',
                message: 'job already completed/failed/cancelled',
            };
        }

        const status = assertStatus(request.status);
        const nowIso = normalizeIsoWithMillis(this.now());

        if (job.status === 'queued') {
            this.lockManager.dequeue(job.job_id);
        }

        const release = this.lockManager.release(job.job_id);
        const reasonCode = request.reason_code || 'none';
        const updated: RestoreJobRecord = {
            ...job,
            status,
            status_reason_code: reasonCode,
            queue_position: null,
            wait_reason_code: null,
            wait_tables: [],
            completed_at: nowIso,
            updated_at: nowIso,
        };

        this.jobs.set(job.job_id, updated);

        this.appendEvent({
            event_id: `evt_${randomUUID()}`,
            event_type: status === 'completed'
                ? 'job_completed'
                : status === 'failed'
                ? 'job_failed'
                : 'job_cancelled',
            job_id: job.job_id,
            reason_code: reasonCode,
            created_at: nowIso,
            details: {
                released_locks: release.released,
            },
        });

        const promotedJobIds: string[] = [];

        for (const promoted of release.promoted) {
            const queued = this.jobs.get(promoted.jobId);

            if (!queued) {
                continue;
            }

            const promotedRecord: RestoreJobRecord = {
                ...queued,
                status: 'running',
                status_reason_code: promoted.reasonCode,
                queue_position: null,
                wait_reason_code: null,
                wait_tables: [],
                started_at: nowIso,
                updated_at: nowIso,
            };

            this.jobs.set(promoted.jobId, promotedRecord);
            promotedJobIds.push(promoted.jobId);
            this.appendEvent({
                event_id: `evt_${randomUUID()}`,
                event_type: 'job_started',
                job_id: promoted.jobId,
                reason_code: promoted.reasonCode,
                created_at: nowIso,
                details: {
                    promoted_from_queue: true,
                },
            });
        }

        return {
            success: true,
            job: updated,
            promoted_job_ids: promotedJobIds,
        };
    }

    private appendEvent(event: RestoreJobAuditEvent): void {
        const events = this.events.get(event.job_id);

        if (events) {
            events.push(event);
            return;
        }

        this.events.set(event.job_id, [event]);
    }

    private validateScopeRequest(
        request: CreateRestoreJobRequest,
        claims: AuthTokenClaims,
    ): {
        allowed: boolean;
        reasonCode: RestoreReasonCode;
        message: string;
    } {
        if (claims.tenant_id !== request.tenant_id) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'tenant_id does not match token scope',
            };
        }

        if (claims.instance_id !== request.instance_id) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'instance_id does not match token scope',
            };
        }

        if (claims.source !== request.source) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'source does not match token scope',
            };
        }

        return this.sourceRegistry.validateScope({
            tenantId: request.tenant_id,
            instanceId: request.instance_id,
            source: request.source,
        });
    }
}
