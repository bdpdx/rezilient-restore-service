import {
    createServer,
    IncomingMessage,
    Server,
    ServerResponse,
} from 'node:http';
import { URL } from 'node:url';
import { AuthTokenClaims } from './auth/claims';
import { RequestAuthenticator } from './auth/authenticator';
import {
    OpsAdminDependencyOutageError,
    RestoreOpsAdminService,
} from './admin/ops-admin-service';
import { RestoreEvidenceService } from './evidence/evidence-service';
import { RestoreExecutionService } from './execute/execute-service';
import {
    ExecuteBatchClaimRequestSchema,
    ExecuteBatchCommitRequestSchema,
} from './execute/models';
import {
    RestoreJobRecord,
    RestoreReasonCode,
} from './jobs/models';
import {
    QueueReconcileScope,
    RestoreJobService,
} from './jobs/job-service';
import {
    FinalizeTargetReconciliationRequestSchema,
    RestoreDryRunPlanRecord,
} from './plans/models';
import { RestorePlanService } from './plans/plan-service';

export interface RestoreServiceDependencies {
    admin: RestoreOpsAdminService;
    authenticator: RequestAuthenticator;
    evidence: RestoreEvidenceService;
    execute: RestoreExecutionService;
    jobs: RestoreJobService;
    plans: RestorePlanService;
}

export interface RestoreServiceServerOptions {
    adminToken?: string;
    maxJsonBodyBytes?: number;
    executePreflightReconcileStaleAfterMs?: number;
}

const DEFAULT_MAX_JSON_BODY_BYTES = 1_048_576;
const DEFAULT_EXECUTE_PREFLIGHT_RECONCILE_STALE_AFTER_MS =
    900_000;
const EXECUTE_PREFLIGHT_FORCE_STALE_STATUS: 'failed' = 'failed';
const EXECUTE_PREFLIGHT_FORCE_REASON_CODE: RestoreReasonCode =
    'failed_stale_lock_recovered';

class RequestBodyTooLargeError extends Error {
    constructor(public readonly maxBytes: number) {
        super(`request body exceeded max of ${maxBytes} bytes`);
    }
}

async function readJsonBody(
    request: IncomingMessage,
    maxJsonBodyBytes: number,
): Promise<Record<string, unknown>> {
    const chunks: Buffer[] = [];
    let totalBytes = 0;

    for await (const chunk of request) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        totalBytes += buffer.length;

        if (totalBytes > maxJsonBodyBytes) {
            throw new RequestBodyTooLargeError(maxJsonBodyBytes);
        }

        chunks.push(buffer);
    }

    if (chunks.length === 0) {
        return {};
    }

    const bodyText = Buffer.concat(chunks).toString('utf8');

    if (!bodyText.trim()) {
        return {};
    }

    const parsed = JSON.parse(bodyText) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('request body must be a JSON object');
    }

    return parsed as Record<string, unknown>;
}

function sendJson(
    response: ServerResponse,
    statusCode: number,
    payload: Record<string, unknown>,
): void {
    response.statusCode = statusCode;
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(payload));
}

function asJobId(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobEventsPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/events$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobCrossServiceEventsPath(pathname: string): string | null {
    const match = pathname.match(
        /^\/v1\/jobs\/([^/]+)\/events\/cross-service$/,
    );

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobCompletePath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/complete$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobExecutionPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/execution$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobResumePath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/resume$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobCheckpointPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/checkpoint$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobRollbackJournalPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/rollback-journal$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobEvidencePath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/evidence$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobEvidenceExportPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/evidence\/export$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asPlanId(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/plans\/([^/]+)$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asPlanTargetReconciliationFinalizePath(pathname: string): string | null {
    const match = pathname.match(
        /^\/v1\/plans\/([^/]+)\/target-reconciliation\/finalize$/,
    );

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobExecuteBatchClaimPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/execute-batches\/claim$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function asJobExecuteBatchCommitPath(pathname: string): string | null {
    const match = pathname.match(/^\/v1\/jobs\/([^/]+)\/execute-batches\/commit$/);

    if (!match) {
        return null;
    }

    return decodeURIComponent(match[1]);
}

function isAdminPath(pathname: string): boolean {
    return pathname.startsWith('/v1/admin/');
}

function isAdminAuthorized(
    request: IncomingMessage,
    options?: RestoreServiceServerOptions,
): boolean {
    if (!options?.adminToken) {
        return false;
    }

    return request.headers['x-rezilient-admin-token'] === options.adminToken;
}

interface ScopedOwnershipRecord {
    tenant_id: string;
    instance_id: string;
    source: string;
}

const SCOPED_NOT_FOUND_RESPONSE = {
    error: 'not_found',
    reason_code: 'blocked_unknown_source_mapping',
    message: 'resource not found within token scope',
} as const;

function isScopeMatch(
    record: ScopedOwnershipRecord,
    claims: AuthTokenClaims,
): boolean {
    return (
        record.tenant_id === claims.tenant_id &&
        record.instance_id === claims.instance_id &&
        record.source === claims.source
    );
}

function getScopedRecord<T extends ScopedOwnershipRecord>(
    record: T | null,
    claims: AuthTokenClaims,
): T | null {
    if (!record) {
        return null;
    }

    if (!isScopeMatch(record, claims)) {
        return null;
    }

    return record;
}

function sendScopedNotFound(response: ServerResponse): void {
    sendJson(response, 404, SCOPED_NOT_FOUND_RESPONSE);
}

function parseQueueScope(
    value: unknown,
): {
    success: true;
    scope: QueueReconcileScope | undefined;
} | {
    success: false;
    message: string;
} {
    if (value === undefined) {
        return {
            success: true,
            scope: undefined,
        };
    }

    if (
        !value ||
        typeof value !== 'object' ||
        Array.isArray(value)
    ) {
        return {
            success: false,
            message: 'scope must be an object when provided',
        };
    }

    const raw = value as Record<string, unknown>;
    const scope: QueueReconcileScope = {};
    const tenantId = raw.tenant_id;

    if (tenantId !== undefined) {
        if (typeof tenantId !== 'string' || tenantId.trim() === '') {
            return {
                success: false,
                message: 'scope.tenant_id must be a non-empty string',
            };
        }

        scope.tenant_id = tenantId.trim();
    }

    const instanceId = raw.instance_id;

    if (instanceId !== undefined) {
        if (typeof instanceId !== 'string' || instanceId.trim() === '') {
            return {
                success: false,
                message: 'scope.instance_id must be a non-empty string',
            };
        }

        scope.instance_id = instanceId.trim();
    }

    const source = raw.source;

    if (source !== undefined) {
        if (typeof source !== 'string' || source.trim() === '') {
            return {
                success: false,
                message: 'scope.source must be a non-empty string',
            };
        }

        scope.source = source.trim();
    }

    const lockScopeTables = raw.lock_scope_tables;

    if (lockScopeTables !== undefined) {
        if (!Array.isArray(lockScopeTables)) {
            return {
                success: false,
                message: 'scope.lock_scope_tables must be an array of strings',
            };
        }

        const tableSet = new Set<string>();

        for (const value of lockScopeTables) {
            if (typeof value !== 'string' || value.trim() === '') {
                return {
                    success: false,
                    message:
                        'scope.lock_scope_tables must contain non-empty strings',
                };
            }

            tableSet.add(value.trim());
        }

        if (tableSet.size === 0) {
            return {
                success: false,
                message: 'scope.lock_scope_tables must contain at least one table',
            };
        }

        scope.lock_scope_tables = Array.from(tableSet)
            .sort((left, right) => left.localeCompare(right));
    }

    return {
        success: true,
        scope,
    };
}

function hasRequiredQueueScopeFilters(
    scope: QueueReconcileScope | undefined,
): scope is QueueReconcileScope & {
    tenant_id: string;
    instance_id: string;
    source: string;
} {
    if (!scope) {
        return false;
    }

    return Boolean(
        scope.tenant_id &&
        scope.instance_id &&
        scope.source,
    );
}

function queueScopeFromJob(job: RestoreJobRecord): QueueReconcileScope {
    return {
        tenant_id: job.tenant_id,
        instance_id: job.instance_id,
        source: job.source,
        lock_scope_tables: [...job.lock_scope_tables],
    };
}

export function createRestoreServiceServer(
    deps: RestoreServiceDependencies,
    options?: RestoreServiceServerOptions,
): Server {
    const maxJsonBodyBytes = options?.maxJsonBodyBytes ||
        DEFAULT_MAX_JSON_BODY_BYTES;
    const executePreflightReconcileStaleAfterMs =
        options?.executePreflightReconcileStaleAfterMs
        ?? DEFAULT_EXECUTE_PREFLIGHT_RECONCILE_STALE_AFTER_MS;

    return createServer(async (request, response) => {
        try {
            const method = request.method || 'GET';
            const parsedUrl = new URL(request.url || '/', 'http://localhost');
            const pathname = parsedUrl.pathname;

            if (method === 'GET' && pathname === '/v1/health') {
                sendJson(response, 200, {
                    ok: true,
                });

                return;
            }

            if (isAdminPath(pathname)) {
                if (!isAdminAuthorized(request, options)) {
                    sendJson(response, 403, {
                        error: 'forbidden',
                        reason_code: 'admin_auth_required',
                    });

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/queue') {
                    sendJson(response, 200, await deps.admin.getQueueDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/freshness') {
                    sendJson(response, 200, await deps.admin.getFreshnessDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/evidence') {
                    sendJson(response, 200, await deps.admin.getEvidenceDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/slo') {
                    sendJson(response, 200, await deps.admin.getSloDashboard());

                    return;
                }

                if (
                    method === 'GET' &&
                    pathname === '/v1/admin/ops/ga-readiness'
                ) {
                    sendJson(response, 200, await deps.admin.getGaReadinessDashboard());

                    return;
                }

                if (method === 'POST' && pathname === '/v1/admin/ops/staging-mode') {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const enabled = body.enabled;
                    const actor = body.actor;

                    if (typeof enabled !== 'boolean') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'enabled must be a boolean',
                        });

                        return;
                    }

                    if (typeof actor !== 'string' || actor.trim() === '') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'actor must be a non-empty string',
                        });

                        return;
                    }

                    sendJson(
                        response,
                        200,
                        deps.admin.setStagingMode(enabled, actor.trim()),
                    );

                    return;
                }

                if (
                    method === 'POST' &&
                    pathname === '/v1/admin/ops/runbooks-signoff'
                ) {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const signedOff = body.signed_off;
                    const actor = body.actor;

                    if (typeof signedOff !== 'boolean') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'signed_off must be a boolean',
                        });

                        return;
                    }

                    if (typeof actor !== 'string' || actor.trim() === '') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'actor must be a non-empty string',
                        });

                        return;
                    }

                    sendJson(
                        response,
                        200,
                        deps.admin.setRunbookSignoff(signedOff, actor.trim()),
                    );

                    return;
                }

                if (method === 'POST' && pathname === '/v1/admin/ops/failure-drills') {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const drillId = body.drill_id;
                    const status = body.status;
                    const actor = body.actor;
                    const notes = body.notes;

                    if (typeof drillId !== 'string' || drillId.trim() === '') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'drill_id must be a non-empty string',
                        });

                        return;
                    }

                    if (typeof status !== 'string' || status.trim() === '') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'status must be a non-empty string',
                        });

                        return;
                    }

                    if (typeof actor !== 'string' || actor.trim() === '') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'actor must be a non-empty string',
                        });

                        return;
                    }

                    if (notes !== undefined && typeof notes !== 'string') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'notes must be a string when provided',
                        });

                        return;
                    }

                    const result = deps.admin.recordFailureDrillResult(
                        drillId.trim(),
                        status.trim(),
                        actor.trim(),
                        typeof notes === 'string' ? notes : undefined,
                    );

                    if (!result.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: result.message,
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        drill: result.record,
                    });

                    return;
                }

                if (
                    method === 'POST' &&
                    pathname === '/v1/admin/ops/queue/reconcile'
                ) {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const dryRun = body.dry_run;
                    const staleAfterMs = body.stale_after_ms;

                    if (dryRun !== undefined && typeof dryRun !== 'boolean') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'dry_run must be a boolean when provided',
                        });

                        return;
                    }

                    if (
                        staleAfterMs !== undefined &&
                        (
                            typeof staleAfterMs !== 'number' ||
                            Number.isNaN(staleAfterMs) ||
                            !Number.isFinite(staleAfterMs) ||
                            staleAfterMs < 0
                        )
                    ) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'stale_after_ms must be a non-negative number',
                        });

                        return;
                    }

                    if (body.force_stale_status !== undefined) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'force_stale_status is only supported on queue/reset',
                        });

                        return;
                    }

                    if (body.force_reason_code !== undefined) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'force_reason_code is only supported on queue/reset',
                        });

                        return;
                    }

                    const parsedScope = parseQueueScope(body.scope);

                    if (!parsedScope.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: parsedScope.message,
                        });

                        return;
                    }

                    if (
                        dryRun === false &&
                        !hasRequiredQueueScopeFilters(parsedScope.scope)
                    ) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'scope.tenant_id, scope.instance_id, and scope.source are required when dry_run is false',
                        });

                        return;
                    }

                    const reconcile = await deps.admin.reconcileQueue({
                        dry_run: dryRun as boolean | undefined,
                        scope: parsedScope.scope,
                        stale_after_ms: staleAfterMs as number | undefined,
                    });

                    sendJson(response, 200, {
                        ...reconcile,
                    });

                    return;
                }

                if (
                    method === 'POST' &&
                    pathname === '/v1/admin/ops/queue/reset'
                ) {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const dryRun = body.dry_run;
                    const staleAfterMs = body.stale_after_ms;
                    const forceStatus = body.force_status;
                    const forceReasonCode = body.force_reason_code;

                    if (dryRun !== undefined && typeof dryRun !== 'boolean') {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: 'dry_run must be a boolean when provided',
                        });

                        return;
                    }

                    if (
                        typeof staleAfterMs !== 'number' ||
                        Number.isNaN(staleAfterMs) ||
                        !Number.isFinite(staleAfterMs) ||
                        staleAfterMs < 0
                    ) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'stale_after_ms must be a non-negative number',
                        });

                        return;
                    }

                    const parsedScope = parseQueueScope(body.scope);

                    if (!parsedScope.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message: parsedScope.message,
                        });

                        return;
                    }

                    if (!hasRequiredQueueScopeFilters(parsedScope.scope)) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'scope.tenant_id, scope.instance_id, and scope.source are required',
                        });

                        return;
                    }

                    if (
                        forceStatus !== undefined &&
                        forceStatus !== 'completed' &&
                        forceStatus !== 'failed' &&
                        forceStatus !== 'cancelled'
                    ) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'force_status must be completed, failed, or cancelled when provided',
                        });

                        return;
                    }

                    if (
                        forceReasonCode !== undefined &&
                        (
                            typeof forceReasonCode !== 'string' ||
                            forceReasonCode.trim() === ''
                        )
                    ) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                'force_reason_code must be a non-empty string when provided',
                        });

                        return;
                    }

                    const reset = await deps.admin.resetQueue({
                        dry_run: dryRun as boolean | undefined,
                        scope: parsedScope.scope,
                        stale_after_ms: staleAfterMs,
                        force_status:
                            forceStatus as
                                | 'completed'
                                | 'failed'
                                | 'cancelled'
                                | undefined,
                        force_reason_code:
                            typeof forceReasonCode === 'string'
                                ? forceReasonCode.trim() as RestoreReasonCode
                                : undefined,
                    });

                    sendJson(response, 200, {
                        ...reset,
                    });

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/overview') {
                    const [
                        queue,
                        freshness,
                        evidence,
                        slo,
                        gaReadiness,
                    ] = await Promise.all([
                        deps.admin.getQueueDashboard(),
                        deps.admin.getFreshnessDashboard(),
                        deps.admin.getEvidenceDashboard(),
                        deps.admin.getSloDashboard(),
                        deps.admin.getGaReadinessDashboard(),
                    ]);

                    sendJson(response, 200, {
                        queue,
                        freshness,
                        evidence,
                        slo,
                        ga_readiness: gaReadiness,
                    });

                    return;
                }

                sendJson(response, 404, {
                    error: 'not_found',
                });

                return;
            }

            const authResult = deps.authenticator.authenticate(
                request.headers.authorization,
            );

            if (!authResult.success) {
                sendJson(response, 401, {
                    error: 'unauthorized',
                    reason_code: authResult.reasonCode,
                });

                return;
            }

            const claims = authResult.auth.claims;
            const getScopedJob = async (
                jobId: string,
            ): Promise<RestoreJobRecord | null> => {
                return getScopedRecord(await deps.jobs.getJob(jobId), claims);
            };
            const getScopedPlan = async (
                planId: string,
            ): Promise<RestoreDryRunPlanRecord | null> => {
                return getScopedRecord(await deps.plans.getPlan(planId), claims);
            };

            if (method === 'POST' && pathname === '/v1/jobs') {
                const body = await readJsonBody(request, maxJsonBodyBytes);
                const result = await deps.jobs.createJob(
                    body,
                    claims,
                );

                if (!result.success) {
                    sendJson(response, result.statusCode, {
                        error: result.error,
                        reason_code: result.reasonCode,
                        message: result.message,
                    });

                    return;
                }

                sendJson(response, 201, {
                    job: result.job,
                });

                return;
            }

            if (method === 'POST' && pathname === '/v1/plans/dry-run') {
                const body = await readJsonBody(request, maxJsonBodyBytes);
                const result = await deps.plans.createDryRunPlan(
                    body,
                    claims,
                );

                if (!result.success) {
                    sendJson(response, result.statusCode, {
                        error: result.error,
                        reason_code: result.reasonCode,
                        freshness_unknown_detail:
                            result.freshnessUnknownDetail,
                        message: result.message,
                    });

                    return;
                }

                sendJson(response, result.statusCode, {
                    plan: result.record.plan,
                    plan_hash_input: result.record.plan_hash_input,
                    gate: result.record.gate,
                    delete_candidates: result.record.delete_candidates,
                    media_candidates: result.record.media_candidates,
                    pit_resolutions: result.record.pit_resolutions,
                    watermarks: result.record.watermarks,
                    reconciliation_state: result.reconciliation_state,
                    target_reconciliation_records:
                        result.target_reconciliation_records,
                });

                return;
            }

            if (method === 'GET') {
                const planId = asPlanId(pathname);

                if (planId) {
                    const planRecord = await getScopedPlan(planId);

                    if (!planRecord) {
                        sendScopedNotFound(response);

                        return;
                    }

                    sendJson(response, 200, {
                        plan: planRecord.plan,
                        plan_hash_input: planRecord.plan_hash_input,
                        gate: planRecord.gate,
                        delete_candidates: planRecord.delete_candidates,
                        media_candidates: planRecord.media_candidates,
                        pit_resolutions: planRecord.pit_resolutions,
                        watermarks: planRecord.watermarks,
                    });

                    return;
                }

                const jobId = asJobId(pathname);

                if (jobId) {
                    const job = await getScopedJob(jobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    sendJson(response, 200, {
                        job,
                    });

                    return;
                }

                const eventsJobId = asJobEventsPath(pathname);

                if (eventsJobId) {
                    const job = await getScopedJob(eventsJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    sendJson(response, 200, {
                        events: await deps.jobs.listJobEvents(job.job_id),
                    });

                    return;
                }

                const crossServiceEventsJobId =
                    asJobCrossServiceEventsPath(pathname);

                if (crossServiceEventsJobId) {
                    const job = await getScopedJob(crossServiceEventsJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    sendJson(response, 200, {
                        events: await deps.jobs.listCrossServiceJobEvents(
                            job.job_id,
                        ),
                    });

                    return;
                }

                const executionJobId = asJobExecutionPath(pathname);

                if (executionJobId) {
                    const job = await getScopedJob(executionJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const execution = await deps.execute.getExecution(job.job_id);

                    if (!execution) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        execution,
                    });

                    return;
                }

                const checkpointJobId = asJobCheckpointPath(pathname);

                if (checkpointJobId) {
                    const job = await getScopedJob(checkpointJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const checkpoint = await deps.execute.getCheckpoint(
                        job.job_id,
                    );

                    if (!checkpoint) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        checkpoint,
                    });

                    return;
                }

                const rollbackJournalJobId = asJobRollbackJournalPath(pathname);

                if (rollbackJournalJobId) {
                    const job = await getScopedJob(rollbackJournalJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const journal = await deps.execute.getRollbackJournal(
                        job.job_id,
                    );

                    if (!journal) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        rollback_journal: journal.journal_entries,
                        sn_mirror: journal.sn_mirror_entries,
                    });

                    return;
                }

                const evidenceJobId = asJobEvidencePath(pathname);

                if (evidenceJobId) {
                    const job = await getScopedJob(evidenceJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const evidence = await deps.evidence.getEvidence(job.job_id);

                    if (!evidence) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        evidence: evidence.evidence,
                        verification: evidence.verification,
                        manifest_sha256: evidence.manifest_sha256,
                        generated_at: evidence.generated_at,
                    });

                    return;
                }
            }

            if (method === 'POST') {
                const finalizePlanId =
                    asPlanTargetReconciliationFinalizePath(pathname);

                if (finalizePlanId) {
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const parsed = FinalizeTargetReconciliationRequestSchema
                        .safeParse(body);

                    if (!parsed.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                parsed.error.issues[0]?.message ||
                                'invalid finalize target reconciliation request',
                        });

                        return;
                    }

                    const result = await deps.plans.finalizeTargetReconciliation(
                        finalizePlanId,
                        parsed.data,
                        claims,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                            requested_record_count:
                                result.requested_record_count,
                        });

                        return;
                    }

                    sendJson(response, result.statusCode, {
                        accepted: true,
                        reused_existing_plan: result.reused_existing_plan,
                        requested_record_count: result.requested_record_count,
                        finalized_record_count: result.finalized_record_count,
                        reconciliation_state: 'finalized',
                        plan: result.record.plan,
                        plan_hash_input: result.record.plan_hash_input,
                        gate: result.record.gate,
                        delete_candidates: result.record.delete_candidates,
                        media_candidates: result.record.media_candidates,
                        pit_resolutions: result.record.pit_resolutions,
                        watermarks: result.record.watermarks,
                    });

                    return;
                }

                const evidenceExportJobId = asJobEvidenceExportPath(pathname);

                if (evidenceExportJobId) {
                    const job = await getScopedJob(evidenceExportJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const result = await deps.evidence.exportEvidence(
                        job.job_id,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                        });

                        return;
                    }

                    sendJson(response, result.statusCode, {
                        evidence: result.record.evidence,
                        verification: result.record.verification,
                        manifest_sha256: result.record.manifest_sha256,
                        generated_at: result.record.generated_at,
                        reused: result.reused,
                    });

                    return;
                }

                const executeBatchClaimJobId = asJobExecuteBatchClaimPath(
                    pathname,
                );

                if (executeBatchClaimJobId) {
                    const job = await getScopedJob(executeBatchClaimJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const parsed = ExecuteBatchClaimRequestSchema.safeParse(body);

                    if (!parsed.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                parsed.error.issues[0]?.message ||
                                'invalid execute batch claim request',
                        });

                        return;
                    }

                    const result = await deps.execute.claimBatch(
                        job.job_id,
                        parsed.data,
                        claims,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                        });

                        return;
                    }

                    sendJson(response, result.statusCode, {
                        ...result.response,
                    });

                    return;
                }

                const executeBatchCommitJobId = asJobExecuteBatchCommitPath(
                    pathname,
                );

                if (executeBatchCommitJobId) {
                    const job = await getScopedJob(executeBatchCommitJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const parsed = ExecuteBatchCommitRequestSchema.safeParse(body);

                    if (!parsed.success) {
                        sendJson(response, 400, {
                            error: 'invalid_request',
                            message:
                                parsed.error.issues[0]?.message ||
                                'invalid execute batch commit request',
                        });

                        return;
                    }

                    const result = await deps.execute.commitBatch(
                        job.job_id,
                        parsed.data,
                        claims,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                        });

                        return;
                    }

                    const responsePayload: Record<string, unknown> = {
                        ...result.response,
                    };

                    if (
                        result.response.execution_status === 'completed' ||
                        result.response.execution_status === 'failed'
                    ) {
                        const evidenceResult = await deps.evidence.ensureEvidence(
                            result.response.job_id,
                        );

                        if (evidenceResult.success) {
                            responsePayload.evidence = {
                                evidence_id:
                                    evidenceResult.record.evidence.evidence_id,
                                signature_verification:
                                    evidenceResult.record.verification
                                        .signature_verification,
                                reused: evidenceResult.reused,
                            };
                        }
                    }

                    sendJson(response, result.statusCode, responsePayload);

                    return;
                }

                const resumeJobId = asJobResumePath(pathname);

                if (resumeJobId) {
                    const job = await getScopedJob(resumeJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = await deps.execute.resumeJob(
                        job.job_id,
                        body,
                        claims,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                        });

                        return;
                    }

                    const responsePayload: Record<string, unknown> = {
                        execution: result.record,
                        promoted_job_ids: result.promoted_job_ids,
                    };

                    if (
                        result.record.status === 'completed' ||
                        result.record.status === 'failed'
                    ) {
                        const evidenceResult = await deps.evidence.ensureEvidence(
                            result.record.job_id,
                        );

                        if (evidenceResult.success) {
                            responsePayload.evidence = {
                                evidence_id:
                                    evidenceResult.record.evidence.evidence_id,
                                signature_verification:
                                    evidenceResult.record.verification
                                        .signature_verification,
                                reused: evidenceResult.reused,
                            };
                        }
                    }

                    sendJson(response, result.statusCode, {
                        ...responsePayload,
                    });

                    return;
                }

                const executeJobId = asJobExecutionPath(pathname);

                if (executeJobId) {
                    const job = await getScopedJob(executeJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    await deps.jobs.reconcileQueueLocks({
                        dry_run: false,
                        scope: queueScopeFromJob(job),
                        stale_after_ms: executePreflightReconcileStaleAfterMs,
                        force_stale_status:
                            EXECUTE_PREFLIGHT_FORCE_STALE_STATUS,
                        force_reason_code:
                            EXECUTE_PREFLIGHT_FORCE_REASON_CODE,
                        preserve_stale_job_ids: [job.job_id],
                    });

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = await deps.execute.executeJob(
                        job.job_id,
                        body,
                        claims,
                    );

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            reason_code: result.reasonCode,
                            message: result.message,
                        });

                        return;
                    }

                    const responsePayload: Record<string, unknown> = {
                        execution: result.record,
                        promoted_job_ids: result.promoted_job_ids,
                    };

                    if (
                        result.record.status === 'completed' ||
                        result.record.status === 'failed'
                    ) {
                        const evidenceResult = await deps.evidence.ensureEvidence(
                            result.record.job_id,
                        );

                        if (evidenceResult.success) {
                            responsePayload.evidence = {
                                evidence_id:
                                    evidenceResult.record.evidence.evidence_id,
                                signature_verification:
                                    evidenceResult.record.verification
                                        .signature_verification,
                                reused: evidenceResult.reused,
                            };
                        }
                    }

                    sendJson(response, result.statusCode, {
                        ...responsePayload,
                    });

                    return;
                }

                const jobId = asJobCompletePath(pathname);

                if (jobId) {
                    const job = await getScopedJob(jobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = await deps.jobs.completeJob(job.job_id, body);

                    if (!result.success) {
                        sendJson(response, result.statusCode, {
                            error: result.error,
                            message: result.message,
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        job: result.job,
                        promoted_job_ids: result.promoted_job_ids,
                    });

                    return;
                }
            }

            sendJson(response, 404, {
                error: 'not_found',
            });
        } catch (error: unknown) {
            if (error instanceof OpsAdminDependencyOutageError) {
                sendJson(response, error.statusCode, {
                    error: 'dependency_unavailable',
                    reason_code: error.reasonCode,
                    dependency: error.dependency,
                    message: error.message,
                });

                return;
            }

            if (error instanceof RequestBodyTooLargeError) {
                sendJson(response, 413, {
                    error: 'payload_too_large',
                    reason_code: 'request_body_too_large',
                    message: error.message,
                });

                return;
            }

            if (error instanceof SyntaxError) {
                sendJson(response, 400, {
                    error: 'bad_request',
                    message: 'malformed JSON in request body',
                });

                return;
            }

            sendJson(response, 500, {
                error: 'internal_error',
                message: 'an unexpected error occurred',
            });
        }
    });
}
