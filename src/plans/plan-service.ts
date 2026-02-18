import {
    computeRestorePlanHash,
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestorePlan,
    RestoreReasonCode,
    selectLatestPitRowTuple,
} from '@rezilient/types';
import { AuthTokenClaims } from '../auth/claims';
import { normalizeIsoWithMillis } from '../jobs/models';
import { SourceRegistry } from '../registry/source-registry';
import {
    buildApprovalPlaceholder,
    buildPlanHashInput,
    CreateDryRunPlanRequest,
    CreateDryRunPlanResult,
    parseCreateDryRunPlanRequest,
    RestoreActionCountsRecord,
    RestoreDryRunGate,
    RestoreDryRunPlanRecord,
    RestorePitResolutionRecord,
} from './models';

function buildActionCounts(
    request: CreateDryRunPlanRequest,
): RestoreActionCountsRecord {
    const counts = {
        update: 0,
        insert: 0,
        delete: 0,
        skip: 0,
        conflict: request.conflicts.length,
        attachment_apply: 0,
        attachment_skip: 0,
    };

    for (const row of request.rows) {
        if (row.action === 'update') {
            counts.update += 1;
            continue;
        }

        if (row.action === 'insert') {
            counts.insert += 1;
            continue;
        }

        if (row.action === 'delete') {
            counts.delete += 1;
            continue;
        }

        counts.skip += 1;
    }

    for (const candidate of request.media_candidates) {
        if (candidate.decision === 'include') {
            counts.attachment_apply += 1;
            continue;
        }

        if (candidate.decision === 'exclude') {
            counts.attachment_skip += 1;
        }
    }

    return counts;
}

function countUnresolvedDeleteCandidates(
    request: CreateDryRunPlanRequest,
): number {
    let count = 0;

    for (const candidate of request.delete_candidates) {
        if (!candidate.decision) {
            count += 1;
        }
    }

    return count;
}

function countUnresolvedHardBlockConflicts(
    request: CreateDryRunPlanRequest,
): number {
    let count = 0;

    for (const conflict of request.conflicts) {
        if (conflict.class !== 'reference_conflict') {
            continue;
        }

        if (!conflict.resolution) {
            count += 1;
        }
    }

    return count;
}

function countUnresolvedMediaCandidates(
    request: CreateDryRunPlanRequest,
): number {
    let count = 0;

    for (const candidate of request.media_candidates) {
        if (!candidate.decision) {
            count += 1;
        }
    }

    return count;
}

function summarizeFreshness(request: CreateDryRunPlanRequest): {
    stale: number;
    unknown: number;
} {
    let stale = 0;
    let unknown = 0;

    for (const watermark of request.watermarks) {
        if (
            watermark.freshness === 'unknown' ||
            watermark.reason_code === 'blocked_freshness_unknown'
        ) {
            unknown += 1;
            continue;
        }

        if (
            watermark.freshness === 'stale' ||
            watermark.executability !== 'executable' ||
            watermark.reason_code === 'blocked_freshness_stale'
        ) {
            stale += 1;
        }
    }

    return {
        stale,
        unknown,
    };
}

function evaluateGate(
    request: CreateDryRunPlanRequest,
): RestoreDryRunGate {
    const unresolvedDeleteCandidates = countUnresolvedDeleteCandidates(request);
    const unresolvedHardBlockConflicts = countUnresolvedHardBlockConflicts(
        request,
    );
    const unresolvedMediaCandidates = countUnresolvedMediaCandidates(
        request,
    );
    const freshness = summarizeFreshness(request);

    let executability: RestoreDryRunGate['executability'] = 'executable';
    let reasonCode: RestoreReasonCode = 'none';

    if (unresolvedDeleteCandidates > 0) {
        executability = 'blocked';
        reasonCode = 'blocked_unresolved_delete_candidates';
    } else if (unresolvedHardBlockConflicts > 0) {
        executability = 'blocked';
        reasonCode = 'blocked_reference_conflict';
    } else if (unresolvedMediaCandidates > 0) {
        executability = 'blocked';
        reasonCode = 'blocked_unresolved_media_candidates';
    } else if (freshness.unknown > 0) {
        executability = 'blocked';
        reasonCode = 'blocked_freshness_unknown';
    } else if (freshness.stale > 0) {
        executability = 'preview_only';
        reasonCode = 'blocked_freshness_stale';
    }

    return {
        executability,
        reason_code: reasonCode,
        unresolved_delete_candidates: unresolvedDeleteCandidates,
        unresolved_media_candidates: unresolvedMediaCandidates,
        unresolved_hard_block_conflicts: unresolvedHardBlockConflicts,
        stale_partition_count: freshness.stale,
        unknown_partition_count: freshness.unknown,
    };
}

function buildPitResolutions(
    request: CreateDryRunPlanRequest,
): RestorePitResolutionRecord[] {
    const out: RestorePitResolutionRecord[] = [];

    for (const candidate of request.pit_candidates) {
        const winner = selectLatestPitRowTuple(candidate.versions);

        out.push({
            row_id: candidate.row_id,
            table: candidate.table,
            record_sys_id: candidate.record_sys_id,
            winning_event_id: winner.event_id,
            winning_sys_updated_on: winner.sys_updated_on,
            winning_sys_mod_count: winner.sys_mod_count,
            winning_event_time: winner.__time,
        });
    }

    return out;
}

export class RestorePlanService {
    private readonly plans = new Map<string, RestoreDryRunPlanRecord>();

    constructor(
        private readonly sourceRegistry: SourceRegistry,
        private readonly now: () => Date = () => new Date(),
    ) {}

    createDryRunPlan(
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): CreateDryRunPlanResult {
        const parsed = parseCreateDryRunPlanRequest(requestBody);

        if (!parsed.success) {
            return {
                success: false,
                statusCode: 400,
                error: 'invalid_request',
                message: parsed.message || 'Invalid request',
            };
        }

        const request = parsed.data;
        const scopeCheck = this.validateScopeRequest(request, claims);

        if (!scopeCheck.allowed) {
            return {
                success: false,
                statusCode: 403,
                error: 'scope_blocked',
                reasonCode: scopeCheck.reasonCode,
                message: scopeCheck.message,
            };
        }

        const actionCounts = buildActionCounts(request);
        const planHashInput = buildPlanHashInput(request, actionCounts);
        const planHashData = computeRestorePlanHash(planHashInput);
        const existing = this.plans.get(request.plan_id);

        if (existing && existing.plan.plan_hash !== planHashData.plan_hash) {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_hash_mismatch',
                reasonCode: 'blocked_plan_hash_mismatch',
                message: 'plan_id already exists with a different plan_hash',
            };
        }

        if (existing) {
            return {
                success: true,
                statusCode: 200,
                record: existing,
            };
        }

        const nowIso = normalizeIsoWithMillis(this.now());
        const gate = evaluateGate(request);
        const plan = RestorePlan.parse({
            contract_version: RESTORE_CONTRACT_VERSION,
            plan_id: request.plan_id,
            plan_hash: planHashData.plan_hash,
            plan_hash_algorithm: PLAN_HASH_ALGORITHM,
            plan_hash_input_version: PLAN_HASH_INPUT_VERSION,
            generated_at: nowIso,
            pit: request.pit,
            scope: request.scope,
            execution_options: request.execution_options,
            action_counts: actionCounts,
            conflicts: request.conflicts,
            approval: buildApprovalPlaceholder(request.approval),
            metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
        });
        const record: RestoreDryRunPlanRecord = {
            tenant_id: request.tenant_id,
            instance_id: request.instance_id,
            source: request.source,
            plan,
            plan_hash_input: planHashInput,
            gate,
            delete_candidates: [...request.delete_candidates],
            media_candidates: [...request.media_candidates],
            pit_resolutions: buildPitResolutions(request),
            watermarks: [...request.watermarks],
        };

        this.plans.set(request.plan_id, record);

        return {
            success: true,
            statusCode: 201,
            record,
        };
    }

    getPlan(planId: string): RestoreDryRunPlanRecord | null {
        return this.plans.get(planId) || null;
    }

    listPlans(): RestoreDryRunPlanRecord[] {
        return Array.from(this.plans.values())
            .map((record) =>
                JSON.parse(JSON.stringify(record)) as RestoreDryRunPlanRecord
            )
            .sort((left, right) => {
                return left.plan.generated_at.localeCompare(
                    right.plan.generated_at,
                );
            });
    }

    private validateScopeRequest(
        request: CreateDryRunPlanRequest,
        claims: AuthTokenClaims,
    ): {
        allowed: boolean;
        reasonCode: RestoreReasonCode;
        message: string;
    } {
        if (
            claims.tenant_id !== request.tenant_id ||
            claims.instance_id !== request.instance_id ||
            claims.source !== request.source
        ) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message:
                    'token scope does not match tenant/instance/source request',
            };
        }

        const mappingValidation = this.sourceRegistry.validateScope({
            tenantId: request.tenant_id,
            instanceId: request.instance_id,
            source: request.source,
        });

        if (!mappingValidation.allowed) {
            return {
                allowed: false,
                reasonCode: mappingValidation.reasonCode,
                message: mappingValidation.message,
            };
        }

        return {
            allowed: true,
            reasonCode: 'none',
            message: 'scope validated',
        };
    }
}
