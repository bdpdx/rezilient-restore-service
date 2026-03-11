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
import {
    LockDecision,
    RestoreLockManager,
    RestoreLockManagerState,
} from '../locks/lock-manager';
import {
    AcpResolveSourceMappingResult,
} from '../registry/acp-source-mapping-client';
import { SourceRegistry } from '../registry/source-registry';
import {
    createSourceRegistryBackedResolver,
    SourceMappingResolver,
} from '../registry/source-mapping-resolver';
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

function arraysEqual(left: string[], right: string[]): boolean {
    if (left.length !== right.length) {
        return false;
    }

    for (let index = 0; index < left.length; index += 1) {
        if (left[index] !== right[index]) {
            return false;
        }
    }

    return true;
}

function asEpochMillis(value: string | null | undefined): number | null {
    if (!value) {
        return null;
    }

    const parsed = Date.parse(value);

    if (Number.isNaN(parsed)) {
        return null;
    }

    return parsed;
}

function jobStaleTimestamp(job: RestoreJobRecord): number | null {
    const updatedAt = asEpochMillis(job.updated_at);

    if (updatedAt !== null) {
        return updatedAt;
    }

    const startedAt = asEpochMillis(job.started_at);

    if (startedAt !== null) {
        return startedAt;
    }

    return asEpochMillis(job.requested_at);
}

function isLockHoldingStatus(status: RestoreJobStatus): boolean {
    return status === 'running' || status === 'paused';
}

function compareByRequestedAt(left: RestoreJobRecord, right: RestoreJobRecord): number {
    const byRequestedAt = left.requested_at.localeCompare(right.requested_at);

    if (byRequestedAt !== 0) {
        return byRequestedAt;
    }

    return left.job_id.localeCompare(right.job_id);
}

function compareActiveLockPriority(
    left: RestoreJobRecord,
    right: RestoreJobRecord,
): number {
    const leftStartedAt = asEpochMillis(left.started_at)
        ?? asEpochMillis(left.requested_at)
        ?? asEpochMillis(left.updated_at)
        ?? 0;
    const rightStartedAt = asEpochMillis(right.started_at)
        ?? asEpochMillis(right.requested_at)
        ?? asEpochMillis(right.updated_at)
        ?? 0;

    if (leftStartedAt !== rightStartedAt) {
        return leftStartedAt - rightStartedAt;
    }

    return left.job_id.localeCompare(right.job_id);
}

function compareQueuedLockPriority(
    left: RestoreJobRecord,
    right: RestoreJobRecord,
): number {
    const leftQueuePosition = left.queue_position ?? Number.MAX_SAFE_INTEGER;
    const rightQueuePosition = right.queue_position ?? Number.MAX_SAFE_INTEGER;

    if (leftQueuePosition !== rightQueuePosition) {
        return leftQueuePosition - rightQueuePosition;
    }

    return compareByRequestedAt(left, right);
}

function terminalEventType(
    status: 'completed' | 'failed' | 'cancelled',
): 'job_completed' | 'job_failed' | 'job_cancelled' {
    if (status === 'completed') {
        return 'job_completed';
    }

    if (status === 'failed') {
        return 'job_failed';
    }

    return 'job_cancelled';
}

export interface QueueReconcileScope {
    tenant_id?: string;
    instance_id?: string;
    source?: string;
    lock_scope_tables?: string[];
}

export interface QueueReconcileRequest {
    dry_run?: boolean;
    scope?: QueueReconcileScope;
    stale_after_ms?: number;
    force_stale_status?: 'completed' | 'failed' | 'cancelled';
    force_reason_code?: RestoreReasonCode;
    preserve_stale_job_ids?: string[];
}

export interface QueueReconcileAnomaly {
    code: string;
    job_id: string | null;
    status: RestoreJobStatus | null;
    details: Record<string, unknown>;
}

export interface QueueReconcileForcedTransition {
    job_id: string;
    from_status: RestoreJobStatus;
    to_status: 'completed' | 'failed' | 'cancelled';
    reason_code: RestoreReasonCode;
}

export interface QueueReconcilePromotion {
    job_id: string;
    from_status: RestoreJobStatus;
    to_status: 'running';
    reason_code: RestoreReasonCode;
}

export interface QueueReconcileResult {
    dry_run: boolean;
    applied: boolean;
    scope: QueueReconcileScope;
    stale_cutoff_at: string | null;
    anomalies: QueueReconcileAnomaly[];
    forced_transitions: QueueReconcileForcedTransition[];
    promoted_jobs: QueueReconcilePromotion[];
    promoted_job_ids: string[];
    lock_state_before: {
        running_jobs: number;
        queued_jobs: number;
    };
    lock_state_after: {
        running_jobs: number;
        queued_jobs: number;
    };
    totals: {
        jobs_scanned: number;
        jobs_in_scope: number;
        non_terminal_in_scope: number;
        stale_jobs_in_scope: number;
    };
}

export interface FinalizedRestorePlanRecord {
    plan_id: string;
    plan_hash: string;
    gate: {
        executability: 'executable' | 'preview_only' | 'blocked';
        reason_code: RestoreReasonCode;
    };
}

export interface RestoreFinalizedPlanReader {
    getFinalizedPlan(
        planId: string,
    ): Promise<FinalizedRestorePlanRecord | null>;
}

const EMPTY_FINALIZED_PLAN_READER: RestoreFinalizedPlanReader = {
    async getFinalizedPlan(): Promise<FinalizedRestorePlanRecord | null> {
        return null;
    },
};

interface QueueRebuildResult {
    lockState: RestoreLockManagerState;
    decisionsByJobId: Map<string, LockDecision>;
}

function normalizeQueueReconcileScope(
    scope: QueueReconcileScope | undefined,
): QueueReconcileScope {
    if (!scope) {
        return {};
    }

    const normalized: QueueReconcileScope = {};

    if (scope.tenant_id && scope.tenant_id.trim().length > 0) {
        normalized.tenant_id = scope.tenant_id.trim();
    }

    if (scope.instance_id && scope.instance_id.trim().length > 0) {
        normalized.instance_id = scope.instance_id.trim();
    }

    if (scope.source && scope.source.trim().length > 0) {
        normalized.source = scope.source.trim();
    }

    if (Array.isArray(scope.lock_scope_tables)) {
        const normalizedTables = normalizeTables(scope.lock_scope_tables);

        if (normalizedTables.length > 0) {
            normalized.lock_scope_tables = normalizedTables;
        }
    }

    return normalized;
}

function matchesScope(
    job: RestoreJobRecord,
    scope: QueueReconcileScope,
): boolean {
    if (scope.tenant_id && job.tenant_id !== scope.tenant_id) {
        return false;
    }

    if (scope.instance_id && job.instance_id !== scope.instance_id) {
        return false;
    }

    if (scope.source && job.source !== scope.source) {
        return false;
    }

    const tables = scope.lock_scope_tables;

    if (!tables || tables.length === 0) {
        return true;
    }

    for (const table of job.lock_scope_tables) {
        if (tables.includes(table)) {
            return true;
        }
    }

    return false;
}

function normalizeLockEntrySource(
    source: string | undefined,
    fallbackSource: string | undefined,
): string {
    const primary = source?.trim() || '';

    if (primary.length > 0) {
        return primary;
    }

    return fallbackSource?.trim() || '';
}

function hydrateLockEntrySource(
    entry: RestoreLockManagerState['running_jobs'][number],
    jobsById: Record<string, RestoreJobRecord>,
): RestoreLockManagerState['running_jobs'][number] {
    const fallbackSource = jobsById[entry.jobId]?.source;
    const source = normalizeLockEntrySource(entry.source, fallbackSource);
    const hydrated = {
        ...entry,
        tables: [...entry.tables],
    };

    if (source.length > 0) {
        hydrated.source = source;
    } else {
        delete hydrated.source;
    }

    return hydrated;
}

function hydrateLockStateSources(
    lockState: RestoreLockManagerState,
    jobsById: Record<string, RestoreJobRecord>,
): RestoreLockManagerState {
    return {
        running_jobs: lockState.running_jobs.map((entry) =>
            hydrateLockEntrySource(entry, jobsById)
        ),
        queued_jobs: lockState.queued_jobs.map((entry) =>
            hydrateLockEntrySource(entry, jobsById)
        ),
    };
}

export class RestoreJobService {
    private initialized = false;

    private initializationPromise: Promise<void> | null = null;

    private readonly sourceMappingResolver: SourceMappingResolver;

    constructor(
        private readonly lockManager: RestoreLockManager,
        sourceRegistry?: SourceRegistry,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestoreJobStateStore =
            new InMemoryRestoreJobStateStore(),
        sourceMappingResolver?: SourceMappingResolver,
        private readonly finalizedPlanReader: RestoreFinalizedPlanReader =
            EMPTY_FINALIZED_PLAN_READER,
    ) {
        if (sourceMappingResolver) {
            this.sourceMappingResolver = sourceMappingResolver;
        } else {
            if (!sourceRegistry) {
                throw new Error(
                    'sourceMappingResolver is required when sourceRegistry '
                    + 'is not provided',
                );
            }

            this.sourceMappingResolver = createSourceRegistryBackedResolver(
                sourceRegistry,
            );
        }
    }

    async createJob(
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): Promise<CreateRestoreJobResult> {
        await this.ensureInitialized();

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
        const mappingCheck = await this.validateScopeRequest(
            request,
            claims,
        );

        if (!mappingCheck.allowed) {
            return {
                success: false,
                statusCode: mappingCheck.statusCode,
                error: 'scope_blocked',
                reasonCode: mappingCheck.reasonCode,
                message: mappingCheck.message,
            };
        }

        let finalizedPlan: FinalizedRestorePlanRecord | null;

        try {
            finalizedPlan = await this.finalizedPlanReader.getFinalizedPlan(
                request.plan_id,
            );
        } catch (error: unknown) {
            return {
                success: false,
                statusCode: 503,
                error: 'plan_store_unavailable',
                message:
                    'finalized plan lookup failed: '
                    + String((error as Error)?.message || error),
            };
        }

        if (!finalizedPlan) {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_missing',
                reasonCode: 'blocked_plan_unavailable',
                message: 'job plan is unavailable in plan store',
            };
        }

        if (finalizedPlan.plan_hash !== request.plan_hash) {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_hash_mismatch',
                reasonCode: 'blocked_plan_hash_mismatch',
                message: 'plan_id already exists with a different plan_hash',
            };
        }

        if (finalizedPlan.gate.executability !== 'executable') {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_not_executable',
                reasonCode: finalizedPlan.gate.reason_code,
                message: 'dry-run plan gate is not executable',
            };
        }

        return this.mutateState((state) => {
            const existingPlan = state.plans_by_id[request.plan_id];

            if (
                existingPlan
                && existingPlan.plan_hash !== finalizedPlan.plan_hash
            ) {
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
                    plan_hash: finalizedPlan.plan_hash,
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
                source: request.source,
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
                plan_hash: finalizedPlan.plan_hash,
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

    async completeJob(
        jobId: string,
        requestBody: unknown,
    ): Promise<CompleteRestoreJobResult> {
        await this.ensureInitialized();

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

    async getJob(jobId: string): Promise<RestoreJobRecord | null> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();
        const job = state.jobs_by_id[jobId];

        if (!job) {
            return null;
        }

        return cloneValue(job);
    }

    async pauseJob(
        jobId: string,
        reasonCode: RestoreReasonCode = 'paused_token_refresh_grace_exhausted',
    ): Promise<PauseRestoreJobResult> {
        await this.ensureInitialized();

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

    async resumePausedJob(jobId: string): Promise<ResumeRestoreJobResult> {
        await this.ensureInitialized();

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

    async listJobEvents(jobId: string): Promise<RestoreJobAuditEvent[]> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();
        const events = state.events_by_job_id[jobId] || [];

        return cloneValue(events);
    }

    async listCrossServiceJobEvents(
        jobId: string,
    ): Promise<CrossServiceAuditEvent[]> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();
        const events = state.cross_service_events_by_job_id[jobId] || [];
        const ordered = cloneValue(events);

        ordered.sort((left, right) =>
            compareCrossServiceAuditEventsForReplay(left, right)
        );

        return ordered;
    }

    async listJobs(): Promise<RestoreJobRecord[]> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();

        return Object.values(state.jobs_by_id)
            .map((job) => cloneValue(job))
            .sort((left, right) => {
                return left.requested_at.localeCompare(right.requested_at);
            });
    }

    async getLockSnapshot(): Promise<{
        running: Array<{ jobId: string; tables: string[] }>;
        queued: Array<{ jobId: string; tables: string[] }>;
    }> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();
        const tempLockManager = new RestoreLockManager(
            state.lock_state,
        );

        return tempLockManager.snapshot();
    }

    async listPlans(): Promise<RestorePlanMetadataRecord[]> {
        await this.ensureInitialized();

        const state = await this.stateStore.read();

        return Object.values(state.plans_by_id)
            .map((plan) => cloneValue(plan))
            .sort((left, right) => left.requested_at.localeCompare(right.requested_at));
    }

    async reconcileQueueLocks(
        request: QueueReconcileRequest = {},
    ): Promise<QueueReconcileResult> {
        await this.ensureInitialized();

        const dryRun = request.dry_run !== false;
        const normalizedScope = normalizeQueueReconcileScope(request.scope);
        const staleAfterMs = request.stale_after_ms;

        if (staleAfterMs !== undefined && staleAfterMs < 0) {
            throw new Error('stale_after_ms must be greater than or equal to 0');
        }

        if (dryRun) {
            const state = await this.stateStore.read();

            return this.reconcileQueueLocksInternal(
                cloneValue(state),
                normalizedScope,
                request,
                true,
            );
        }

        return this.mutateState((state) => {
            return this.reconcileQueueLocksInternal(
                state,
                normalizedScope,
                request,
                false,
            );
        });
    }

    private reconcileQueueLocksInternal(
        state: RestoreJobState,
        scope: QueueReconcileScope,
        request: QueueReconcileRequest,
        dryRun: boolean,
    ): QueueReconcileResult {
        const nowDate = this.now();
        const nowIso = normalizeIsoWithMillis(nowDate);
        const staleAfterMs = request.stale_after_ms;
        const staleCutoffMs = staleAfterMs === undefined
            ? null
            : nowDate.getTime() - staleAfterMs;
        const staleCutoffAt = staleCutoffMs === null
            ? null
            : normalizeIsoWithMillis(new Date(staleCutoffMs));
        const lockStateBefore = {
            running_jobs: state.lock_state.running_jobs.length,
            queued_jobs: state.lock_state.queued_jobs.length,
        };
        const allJobs = Object.values(state.jobs_by_id)
            .sort(compareByRequestedAt);
        const jobsInScope = allJobs.filter((job) => matchesScope(job, scope));
        const nonTerminalInScope = jobsInScope.filter((job) =>
            !isTerminalStatus(job.status)
        );
        const persistedRunning = new Set(
            state.lock_state.running_jobs.map((entry) => entry.jobId),
        );
        const persistedQueued = new Set(
            state.lock_state.queued_jobs.map((entry) => entry.jobId),
        );
        const activeBlockingJobIds = new Set(
            allJobs
                .filter((job) => isLockHoldingStatus(job.status))
                .map((job) => job.job_id),
        );
        const persistedLockManager = new RestoreLockManager(state.lock_state);
        const anomalies: QueueReconcileAnomaly[] = [];
        const staleJobIds = new Set<string>();
        const staleForceCandidateJobIds = new Set<string>();
        const preservedStaleJobIds = new Set(
            (request.preserve_stale_job_ids || [])
                .map((jobId) => String(jobId || '').trim())
                .filter((jobId) => jobId.length > 0),
        );

        for (const job of nonTerminalInScope) {
            const inRunning = persistedRunning.has(job.job_id);
            const inQueued = persistedQueued.has(job.job_id);

            if (job.status === 'queued') {
                if (!inQueued || inRunning) {
                    anomalies.push({
                        code: 'lock_membership_mismatch',
                        job_id: job.job_id,
                        status: job.status,
                        details: {
                            expected_lock_state: 'queued',
                            in_running: inRunning,
                            in_queued: inQueued,
                        },
                    });
                }

                if (inQueued && !inRunning) {
                    const blockers = persistedLockManager.getBlockingLocks({
                        tenantId: job.tenant_id,
                        instanceId: job.instance_id,
                        source: job.source,
                        tables: job.lock_scope_tables,
                    });
                    const activeBlockerIds = blockers.blockerJobIds.filter(
                        (jobId) => activeBlockingJobIds.has(jobId),
                    );

                    if (
                        blockers.blockedTables.length === 0 ||
                        activeBlockerIds.length === 0
                    ) {
                        anomalies.push({
                            code: 'queued_missing_blocker',
                            job_id: job.job_id,
                            status: job.status,
                            details: {
                                blocked_tables: [...blockers.blockedTables],
                                blocker_job_ids: [...blockers.blockerJobIds],
                                orphaned_blocker_job_ids:
                                    blockers.blockerJobIds.filter((jobId) =>
                                        !activeBlockingJobIds.has(jobId),
                                    ),
                                queue_position: job.queue_position,
                                wait_reason_code: job.wait_reason_code,
                                wait_tables: [...job.wait_tables],
                            },
                        });
                    }
                }
            } else if (job.status === 'running' || job.status === 'paused') {
                if (!inRunning || inQueued) {
                    anomalies.push({
                        code: 'lock_membership_mismatch',
                        job_id: job.job_id,
                        status: job.status,
                        details: {
                            expected_lock_state: 'running',
                            in_running: inRunning,
                            in_queued: inQueued,
                        },
                    });
                }
            }

            if (staleCutoffMs === null) {
                continue;
            }

            const staleTimestamp = jobStaleTimestamp(job);

            if (staleTimestamp === null || staleTimestamp > staleCutoffMs) {
                continue;
            }

            staleJobIds.add(job.job_id);

            if (
                request.force_stale_status
                && !preservedStaleJobIds.has(job.job_id)
            ) {
                staleForceCandidateJobIds.add(job.job_id);
            }

            anomalies.push({
                code: 'stale_non_terminal_job',
                job_id: job.job_id,
                status: job.status,
                details: {
                    stale_cutoff_at: staleCutoffAt,
                    updated_at: job.updated_at,
                    started_at: job.started_at,
                    requested_at: job.requested_at,
                },
            });
        }

        const forcedTransitions: QueueReconcileForcedTransition[] = [];
        const forceStatus = request.force_stale_status;
        const forceReason = request.force_reason_code
            || 'failed_stale_lock_recovered';

        if (forceStatus) {
            for (const job of nonTerminalInScope) {
                if (!staleForceCandidateJobIds.has(job.job_id)) {
                    continue;
                }

                const current = state.jobs_by_id[job.job_id];

                if (!current || isTerminalStatus(current.status)) {
                    continue;
                }

                forcedTransitions.push({
                    job_id: current.job_id,
                    from_status: current.status,
                    to_status: forceStatus,
                    reason_code: forceReason,
                });

                state.jobs_by_id[current.job_id] = {
                    ...current,
                    status: forceStatus,
                    status_reason_code: forceReason,
                    queue_position: null,
                    wait_reason_code: null,
                    wait_tables: [],
                    completed_at: nowIso,
                    updated_at: nowIso,
                };

                if (dryRun) {
                    continue;
                }

                this.appendEvent(state, {
                    event_id: `evt_${randomUUID()}`,
                    event_type: terminalEventType(forceStatus),
                    job_id: current.job_id,
                    reason_code: forceReason,
                    created_at: nowIso,
                    details: {
                        reconcile_forced_terminal: true,
                        stale_cutoff_at: staleCutoffAt,
                    },
                });
            }
        }

        const rebuilt = this.buildReconciledLockState(state.jobs_by_id);
        const promotedJobs: QueueReconcilePromotion[] = [];
        const nonTerminalJobs = Object.values(state.jobs_by_id)
            .filter((job) => !isTerminalStatus(job.status))
            .sort(compareByRequestedAt);

        for (const job of nonTerminalJobs) {
            const decision = rebuilt.decisionsByJobId.get(job.job_id);

            if (!decision) {
                continue;
            }

            const inScope = matchesScope(job, scope);

            if (inScope) {
                if (
                    (job.status === 'running' || job.status === 'paused')
                    && decision.state === 'queued'
                ) {
                    anomalies.push({
                        code: 'active_job_conflicts_requeued',
                        job_id: job.job_id,
                        status: job.status,
                        details: {
                            blocked_tables: [...decision.blockedTables],
                            queue_position: decision.queuePosition || 1,
                        },
                    });
                }

                if (job.status === 'queued' && decision.state === 'running') {
                    anomalies.push({
                        code: 'queued_job_promotable',
                        job_id: job.job_id,
                        status: job.status,
                        details: {
                            previous_queue_position: job.queue_position,
                        },
                    });
                }
            }

            if (decision.state === 'running') {
                if (job.status === 'queued') {
                    const promoted: RestoreJobRecord = {
                        ...job,
                        status: 'running',
                        status_reason_code: decision.reasonCode,
                        queue_position: null,
                        wait_reason_code: null,
                        wait_tables: [],
                        started_at: job.started_at || nowIso,
                        updated_at: nowIso,
                    };

                    state.jobs_by_id[job.job_id] = promoted;
                    promotedJobs.push({
                        job_id: job.job_id,
                        from_status: 'queued',
                        to_status: 'running',
                        reason_code: decision.reasonCode,
                    });

                    if (dryRun) {
                        continue;
                    }

                    this.appendEvent(state, {
                        event_id: `evt_${randomUUID()}`,
                        event_type: 'job_started',
                        job_id: job.job_id,
                        reason_code: decision.reasonCode,
                        created_at: nowIso,
                        details: {
                            promoted_from_queue: true,
                            reconcile_operation: true,
                        },
                    });

                    continue;
                }

                if (
                    job.status === 'running'
                    && (
                        job.status_reason_code !== 'none'
                        || job.queue_position !== null
                        || job.wait_reason_code !== null
                        || job.wait_tables.length > 0
                    )
                ) {
                    state.jobs_by_id[job.job_id] = {
                        ...job,
                        status_reason_code: 'none',
                        queue_position: null,
                        wait_reason_code: null,
                        wait_tables: [],
                        updated_at: nowIso,
                    };
                    continue;
                }

                if (
                    job.status === 'paused'
                    && (
                        job.queue_position !== null
                        || job.wait_reason_code !== null
                        || job.wait_tables.length > 0
                    )
                ) {
                    state.jobs_by_id[job.job_id] = {
                        ...job,
                        queue_position: null,
                        wait_reason_code: null,
                        wait_tables: [],
                        updated_at: nowIso,
                    };
                }

                continue;
            }

            const queuePosition = decision.queuePosition || 1;
            const shouldUpdateQueued = (
                job.status !== 'queued'
                || job.status_reason_code !== 'queued_scope_lock'
                || job.queue_position !== queuePosition
                || job.wait_reason_code !== 'queued_scope_lock'
                || !arraysEqual(job.wait_tables, decision.blockedTables)
                || job.started_at !== null
            );

            if (!shouldUpdateQueued) {
                continue;
            }

            const requeued: RestoreJobRecord = {
                ...job,
                status: 'queued',
                status_reason_code: 'queued_scope_lock',
                queue_position: queuePosition,
                wait_reason_code: 'queued_scope_lock',
                wait_tables: [...decision.blockedTables],
                started_at: null,
                updated_at: nowIso,
            };

            state.jobs_by_id[job.job_id] = requeued;

            if (dryRun || job.status === 'queued') {
                continue;
            }

            this.appendEvent(state, {
                event_id: `evt_${randomUUID()}`,
                event_type: 'job_queued',
                job_id: job.job_id,
                reason_code: 'queued_scope_lock',
                created_at: nowIso,
                details: {
                    blocked_tables: [...decision.blockedTables],
                    queue_position: queuePosition,
                    reconcile_operation: true,
                },
            });
        }

        state.lock_state = cloneValue(rebuilt.lockState);

        if (!dryRun) {
            this.lockManager.loadState(rebuilt.lockState);
        }

        return {
            dry_run: dryRun,
            applied: !dryRun,
            scope: cloneValue(scope),
            stale_cutoff_at: staleCutoffAt,
            anomalies: cloneValue(anomalies),
            forced_transitions: cloneValue(forcedTransitions),
            promoted_jobs: cloneValue(promotedJobs),
            promoted_job_ids: promotedJobs.map((entry) => entry.job_id),
            lock_state_before: lockStateBefore,
            lock_state_after: {
                running_jobs: rebuilt.lockState.running_jobs.length,
                queued_jobs: rebuilt.lockState.queued_jobs.length,
            },
            totals: {
                jobs_scanned: allJobs.length,
                jobs_in_scope: jobsInScope.length,
                non_terminal_in_scope: nonTerminalInScope.length,
                stale_jobs_in_scope: staleJobIds.size,
            },
        };
    }

    private buildReconciledLockState(
        jobsById: Record<string, RestoreJobRecord>,
    ): QueueRebuildResult {
        const rebuildLockManager = new RestoreLockManager();
        const decisionsByJobId = new Map<string, LockDecision>();
        const nonTerminalJobs = Object.values(jobsById)
            .filter((job) => !isTerminalStatus(job.status));
        const activeJobs = nonTerminalJobs
            .filter((job) => job.status === 'running' || job.status === 'paused')
            .sort(compareActiveLockPriority);
        const queuedJobs = nonTerminalJobs
            .filter((job) => job.status === 'queued')
            .sort(compareQueuedLockPriority);

        for (const job of [...activeJobs, ...queuedJobs]) {
            const decision = rebuildLockManager.acquire({
                jobId: job.job_id,
                tenantId: job.tenant_id,
                instanceId: job.instance_id,
                source: job.source,
                tables: normalizeTables(job.lock_scope_tables),
            });

            decisionsByJobId.set(job.job_id, decision);
        }

        return {
            lockState: rebuildLockManager.exportState(),
            decisionsByJobId,
        };
    }

    private async completeJobInternal(
        jobId: string,
        request: CompleteRestoreJobRequest,
    ): Promise<CompleteRestoreJobResult> {
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

    private async mutateState<T>(
        mutator: (state: RestoreJobState) => T | Promise<T>,
    ): Promise<T> {
        return this.stateStore.mutate(async (state) => {
            const hydratedLockState = hydrateLockStateSources(
                state.lock_state,
                state.jobs_by_id,
            );

            state.lock_state = hydratedLockState;
            this.lockManager.loadState(hydratedLockState);
            const result = await mutator(state);

            state.lock_state = this.lockManager.exportState();

            return result;
        });
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
        const hydratedLockState = hydrateLockStateSources(
            state.lock_state,
            state.jobs_by_id,
        );

        this.lockManager.loadState(hydratedLockState);
        this.initialized = true;
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

    private async validateScopeRequest(
        request: CreateRestoreJobRequest,
        claims: AuthTokenClaims,
    ): Promise<{
        allowed: boolean;
        statusCode: number;
        reasonCode: RestoreReasonCode;
        message: string;
    }> {
        if (claims.tenant_id !== request.tenant_id) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'tenant_id does not match token scope',
            };
        }

        if (claims.instance_id !== request.instance_id) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'instance_id does not match token scope',
            };
        }

        if (claims.source !== request.source) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'source does not match token scope',
            };
        }

        let mappingResolution: AcpResolveSourceMappingResult;

        try {
            mappingResolution =
                await this.sourceMappingResolver.resolveSourceMapping({
                    tenantId: request.tenant_id,
                    instanceId: request.instance_id,
                    serviceScope: 'rrs',
                });
        } catch (error: unknown) {
            return {
                allowed: false,
                statusCode: 503,
                reasonCode: 'blocked_auth_control_plane_outage',
                message:
                    'auth control plane source mapping resolve failed: '
                    + String((error as Error)?.message || error),
            };
        }

        if (mappingResolution.status === 'outage') {
            return {
                allowed: false,
                statusCode: 503,
                reasonCode: 'blocked_auth_control_plane_outage',
                message: mappingResolution.message,
            };
        }

        if (mappingResolution.status === 'not_found') {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'tenant/instance mapping not found in ACP',
            };
        }

        const mapping = mappingResolution.mapping;

        if (
            mapping.tenantId !== request.tenant_id ||
            mapping.instanceId !== request.instance_id
        ) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'ACP returned mismatched tenant/instance mapping',
            };
        }

        if (mapping.source !== request.source) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'source does not match canonical ACP mapping',
            };
        }

        if (!mapping.serviceAllowed) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'service scope is not allowed by ACP mapping',
            };
        }

        if (
            mapping.tenantState !== 'active' ||
            mapping.entitlementState !== 'active' ||
            mapping.instanceState !== 'active'
        ) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message:
                    'ACP mapping is not active for tenant/entitlement/instance',
            };
        }

        return {
            allowed: true,
            statusCode: 200,
            reasonCode: 'none',
            message: 'scope validated',
        };
    }
}
