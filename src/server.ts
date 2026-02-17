import {
    createServer,
    IncomingMessage,
    Server,
    ServerResponse,
} from 'node:http';
import { URL } from 'node:url';
import { RequestAuthenticator } from './auth/authenticator';
import { RestoreEvidenceService } from './evidence/evidence-service';
import { RestoreExecutionService } from './execute/execute-service';
import { RestoreJobService } from './jobs/job-service';
import { RestorePlanService } from './plans/plan-service';

export interface RestoreServiceDependencies {
    authenticator: RequestAuthenticator;
    evidence: RestoreEvidenceService;
    execute: RestoreExecutionService;
    jobs: RestoreJobService;
    plans: RestorePlanService;
}

async function readJsonBody(
    request: IncomingMessage,
): Promise<Record<string, unknown>> {
    const chunks: Buffer[] = [];

    for await (const chunk of request) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
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

export function createRestoreServiceServer(
    deps: RestoreServiceDependencies,
): Server {
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

            if (method === 'POST' && pathname === '/v1/jobs') {
                const body = await readJsonBody(request);
                const result = deps.jobs.createJob(
                    body,
                    authResult.auth.claims,
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
                const body = await readJsonBody(request);
                const result = deps.plans.createDryRunPlan(
                    body,
                    authResult.auth.claims,
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
                    const planRecord = deps.plans.getPlan(planId);

                    if (!planRecord) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

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
                    const job = deps.jobs.getJob(jobId);

                    if (!job) {
                        sendJson(response, 404, {
                            error: 'not_found',
                        });

                        return;
                    }

                    sendJson(response, 200, {
                        job,
                    });

                    return;
                }

                const eventsJobId = asJobEventsPath(pathname);

                if (eventsJobId) {
                    sendJson(response, 200, {
                        events: deps.jobs.listJobEvents(eventsJobId),
                    });

                    return;
                }

                const executionJobId = asJobExecutionPath(pathname);

                if (executionJobId) {
                    const execution = deps.execute.getExecution(executionJobId);

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
                    const checkpoint = deps.execute.getCheckpoint(
                        checkpointJobId,
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
                    const journal = deps.execute.getRollbackJournal(
                        rollbackJournalJobId,
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
                    const evidence = deps.evidence.getEvidence(evidenceJobId);

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
                    const result = deps.evidence.exportEvidence(
                        evidenceExportJobId,
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
                    const body = await readJsonBody(request);
                    const result = deps.execute.resumeJob(
                        resumeJobId,
                        body,
                        authResult.auth.claims,
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
                    const body = await readJsonBody(request);
                    const result = deps.execute.executeJob(
                        executeJobId,
                        body,
                        authResult.auth.claims,
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
                    const body = await readJsonBody(request);
                    const result = deps.jobs.completeJob(jobId, body);

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
