import {
    createServer,
    IncomingMessage,
    Server,
    ServerResponse,
} from 'node:http';
import { URL } from 'node:url';
import { AuthTokenClaims } from './auth/claims';
import { RequestAuthenticator } from './auth/authenticator';
import { RestoreOpsAdminService } from './admin/ops-admin-service';
import { RestoreEvidenceService } from './evidence/evidence-service';
import { RestoreExecutionService } from './execute/execute-service';
import { RestoreJobRecord } from './jobs/models';
import { RestoreJobService } from './jobs/job-service';
import { RestoreDryRunPlanRecord } from './plans/models';
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
}

const DEFAULT_MAX_JSON_BODY_BYTES = 1_048_576;

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

export function createRestoreServiceServer(
    deps: RestoreServiceDependencies,
    options?: RestoreServiceServerOptions,
): Server {
    const maxJsonBodyBytes = options?.maxJsonBodyBytes ||
        DEFAULT_MAX_JSON_BODY_BYTES;

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
                    sendJson(response, 200, deps.admin.getQueueDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/freshness') {
                    sendJson(response, 200, deps.admin.getFreshnessDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/evidence') {
                    sendJson(response, 200, deps.admin.getEvidenceDashboard());

                    return;
                }

                if (method === 'GET' && pathname === '/v1/admin/ops/slo') {
                    sendJson(response, 200, deps.admin.getSloDashboard());

                    return;
                }

                if (
                    method === 'GET' &&
                    pathname === '/v1/admin/ops/ga-readiness'
                ) {
                    sendJson(response, 200, deps.admin.getGaReadinessDashboard());

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

                if (method === 'GET' && pathname === '/v1/admin/ops/overview') {
                    sendJson(response, 200, {
                        queue: deps.admin.getQueueDashboard(),
                        freshness: deps.admin.getFreshnessDashboard(),
                        evidence: deps.admin.getEvidenceDashboard(),
                        slo: deps.admin.getSloDashboard(),
                        ga_readiness: deps.admin.getGaReadinessDashboard(),
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
            const getScopedJob = (jobId: string): RestoreJobRecord | null => {
                return getScopedRecord(deps.jobs.getJob(jobId), claims);
            };
            const getScopedPlan = (
                planId: string,
            ): RestoreDryRunPlanRecord | null => {
                return getScopedRecord(deps.plans.getPlan(planId), claims);
            };

            if (method === 'POST' && pathname === '/v1/jobs') {
                const body = await readJsonBody(request, maxJsonBodyBytes);
                const result = deps.jobs.createJob(
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
                const result = deps.plans.createDryRunPlan(
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

                sendJson(response, result.statusCode, {
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

            if (method === 'GET') {
                const planId = asPlanId(pathname);

                if (planId) {
                    const planRecord = getScopedPlan(planId);

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
                    const job = getScopedJob(jobId);

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
                    const job = getScopedJob(eventsJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    sendJson(response, 200, {
                        events: deps.jobs.listJobEvents(job.job_id),
                    });

                    return;
                }

                const executionJobId = asJobExecutionPath(pathname);

                if (executionJobId) {
                    const job = getScopedJob(executionJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const execution = deps.execute.getExecution(job.job_id);

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
                    const job = getScopedJob(checkpointJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const checkpoint = deps.execute.getCheckpoint(
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
                    const job = getScopedJob(rollbackJournalJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const journal = deps.execute.getRollbackJournal(
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
                    const job = getScopedJob(evidenceJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const evidence = deps.evidence.getEvidence(job.job_id);

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
                const evidenceExportJobId = asJobEvidenceExportPath(pathname);

                if (evidenceExportJobId) {
                    const job = getScopedJob(evidenceExportJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const result = deps.evidence.exportEvidence(
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

                const resumeJobId = asJobResumePath(pathname);

                if (resumeJobId) {
                    const job = getScopedJob(resumeJobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = deps.execute.resumeJob(
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
                        const evidenceResult = deps.evidence.ensureEvidence(
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
                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = deps.execute.executeJob(
                        executeJobId,
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
                        const evidenceResult = deps.evidence.ensureEvidence(
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
                    const job = getScopedJob(jobId);

                    if (!job) {
                        sendScopedNotFound(response);

                        return;
                    }

                    const body = await readJsonBody(request, maxJsonBodyBytes);
                    const result = deps.jobs.completeJob(job.job_id, body);

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
            if (error instanceof RequestBodyTooLargeError) {
                sendJson(response, 413, {
                    error: 'payload_too_large',
                    reason_code: 'request_body_too_large',
                    message: error.message,
                });

                return;
            }

            const message = error instanceof Error
                ? error.message
                : 'unknown_error';

            sendJson(response, 400, {
                error: 'bad_request',
                message,
            });
        }
    });
}
