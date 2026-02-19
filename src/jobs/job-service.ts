import { randomUUID } from 'node:crypto';
import {
    compareCrossServiceAuditEventsForReplay,
    CrossServiceAuditEvent,
    fromLegacyRestoreJobAuditEvent,
} from '@rezilient/types';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from '../constants';
import { AuthTokenClaims } from '../auth/claims';
import { RestoreLockManager } from '../locks/lock-manager';
import { SourceRegistry } from '../registry/source-registry';
import {
    InMemoryRestoreJobStateStore,
    RestoreJobState,
    RestoreJobStateStore,
} from './job-state-store';
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

function cloneValue<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

export class RestoreJobService {
    constructor(
        private readonly lockManager: RestoreLockManager,
        private readonly sourceRegistry: SourceRegistry,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestoreJobStateStore =
            new InMemoryRestoreJobStateStore(),
    ) {
        this.lockManager.loadState(this.stateStore.read().lock_state);
    }

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

        return this.mutateState((state) => {
            const existingPlan = state.plans_by_id[request.plan_id];

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
            const normalizedTables = normalizeTables(request.lock_scope_tables);

            if (!existingPlan) {
                const plan: RestorePlanMetadataRecord = {
                    contract_version: RESTORE_CONTRACT_VERSION,
                    plan_id: request.plan_id,
                    plan_hash: request.plan_hash,
                    tenant_id: request.tenant_id,
                    instance_id: request.instance_id,
                    source: request.source,
                    lock_scope_tables: normalizedTables,
                    requested_by: request.requested_by,
                    requested_at: nowIso,
                    approval: buildApprovalPlaceholder(request.approval),
                    metadata_allowlist_version:
                        RESTORE_METADATA_ALLOWLIST_VERSION,
                };

                state.plans_by_id[plan.plan_id] = plan;
            }

            const jobId = `job_${randomUUID()}`;
            const lockDecision = this.lockManager.acquire({
                jobId,
                tenantId: request.tenant_id,
                instanceId: request.instance_id,
                tables: normalizedTables,
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
                lock_scope_tables: normalizedTables,
                required_capabilities: [...request.required_capabilities],
                requested_by: request.requested_by,
                requested_at: nowIso,
                approval: buildApprovalPlaceholder(request.approval),
                metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
                queue_position: lockDecision.queuePosition || null,
                wait_reason_code: lockDecision.state === 'queued'
                    ? lockDecision.reasonCode
                    : null,
                wait_tables: [...lockDecision.blockedTables],
                started_at: lockDecision.state === 'running' ? nowIso : null,
                completed_at: null,
                updated_at: nowIso,
            };

            state.jobs_by_id[job.job_id] = job;

            this.appendEvent(state, {
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
                this.appendEvent(state, {
                    event_id: `evt_${randomUUID()}`,
                    event_type: 'job_queued',
                    job_id: job.job_id,
                    reason_code: 'queued_scope_lock',
                    created_at: nowIso,
                    details: {
                        blocked_tables: [...lockDecision.blockedTables],
                        queue_position: lockDecision.queuePosition || 1,
                    },
                });
            } else {
                this.appendEvent(state, {
                    event_id: `evt_${randomUUID()}`,
                    event_type: 'job_started',
                    job_id: job.job_id,
                    reason_code: 'none',
                    created_at: nowIso,
                    details: {
                        lock_scope_tables: [...job.lock_scope_tables],
                    },
                });
            }

            return {
                success: true,
                job: cloneValue(job),
            };
        });
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
        const job = this.stateStore.read().jobs_by_id[jobId];

        if (!job) {
            return null;
        }

        return cloneValue(job);
    }

    pauseJob(
        jobId: string,
        reasonCode: RestoreReasonCode = 'paused_token_refresh_grace_exhausted',
    ): PauseRestoreJobResult {
        return this.mutateState((state) => {
            const job = state.jobs_by_id[jobId];

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

            state.jobs_by_id[jobId] = paused;
            this.appendEvent(state, {
                event_id: `evt_${randomUUID()}`,
                event_type: 'job_paused',
                job_id: jobId,
                reason_code: reasonCode,
                created_at: nowIso,
                details: {},
            });

            return {
                success: true,
                job: cloneValue(paused),
            };
        });
    }

    resumePausedJob(jobId: string): ResumeRestoreJobResult {
        return this.mutateState((state) => {
            const job = state.jobs_by_id[jobId];

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

            state.jobs_by_id[jobId] = resumed;
            this.appendEvent(state, {
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
                job: cloneValue(resumed),
            };
        });
    }

    listJobEvents(jobId: string): RestoreJobAuditEvent[] {
        const events = this.stateStore.read().events_by_job_id[jobId] || [];

        return cloneValue(events);
    }

    listCrossServiceJobEvents(jobId: string): CrossServiceAuditEvent[] {
        const events =
            this.stateStore.read().cross_service_events_by_job_id[jobId] || [];
        const ordered = cloneValue(events);

        ordered.sort((left, right) =>
            compareCrossServiceAuditEventsForReplay(left, right)
        );

        return ordered;
    }

    listJobs(): RestoreJobRecord[] {
        return Object.values(this.stateStore.read().jobs_by_id)
            .map((job) => cloneValue(job))
            .sort((left, right) => {
                return left.requested_at.localeCompare(right.requested_at);
            });
    }

    getLockSnapshot(): {
        running: Array<{ jobId: string; tables: string[] }>;
        queued: Array<{ jobId: string; tables: string[] }>;
    } {
        const state = this.stateStore.read();

        this.lockManager.loadState(state.lock_state);

        return this.lockManager.snapshot();
    }

    listPlans(): RestorePlanMetadataRecord[] {
        return Object.values(this.stateStore.read().plans_by_id)
            .map((plan) => cloneValue(plan))
            .sort((left, right) => left.requested_at.localeCompare(right.requested_at));
    }

    private completeJobInternal(
        jobId: string,
        request: CompleteRestoreJobRequest,
    ): CompleteRestoreJobResult {
        return this.mutateState((state) => {
            const job = state.jobs_by_id[jobId];

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

            state.jobs_by_id[job.job_id] = updated;

            this.appendEvent(state, {
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
                const queued = state.jobs_by_id[promoted.jobId];

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

                state.jobs_by_id[promoted.jobId] = promotedRecord;
                promotedJobIds.push(promoted.jobId);
                this.appendEvent(state, {
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
                job: cloneValue(updated),
                promoted_job_ids: [...promotedJobIds],
            };
        });
    }

    private mutateState<T>(mutator: (state: RestoreJobState) => T): T {
        return this.stateStore.mutate((state) => {
            this.lockManager.loadState(state.lock_state);
            const result = mutator(state);

            state.lock_state = this.lockManager.exportState();

            return result;
        });
    }

    private appendEvent(
        state: RestoreJobState,
        event: RestoreJobAuditEvent,
    ): void {
        const events = state.events_by_job_id[event.job_id];

        if (events) {
            events.push(event);
        } else {
            state.events_by_job_id[event.job_id] = [event];
        }

        const job = state.jobs_by_id[event.job_id];

        if (!job) {
            return;
        }

        const normalizedEvent = fromLegacyRestoreJobAuditEvent(event, {
            instance_id: job.instance_id,
            plan_hash: job.plan_hash,
            plan_id: job.plan_id,
            source: job.source,
            tenant_id: job.tenant_id,
        });
        const crossServiceEvents =
            state.cross_service_events_by_job_id[event.job_id];

        if (crossServiceEvents) {
            crossServiceEvents.push(normalizedEvent);
        } else {
            state.cross_service_events_by_job_id[event.job_id] = [normalizedEvent];
        }
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
