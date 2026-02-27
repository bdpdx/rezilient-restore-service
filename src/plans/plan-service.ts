import {
    computeRestorePlanHash,
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestorePlan,
    RestoreReasonCode,
    RestoreWatermark,
    selectLatestPitRowTuple,
} from '@rezilient/types';
import { AuthTokenClaims } from '../auth/claims';
import { normalizeIsoWithMillis } from '../jobs/models';
import {
    AcpResolveSourceMappingResult,
} from '../registry/acp-source-mapping-client';
import { SourceRegistry } from '../registry/source-registry';
import {
    createSourceRegistryBackedResolver,
    SourceMappingResolver,
} from '../registry/source-mapping-resolver';
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
import {
    InMemoryRestorePlanStateStore,
    RestorePlanStateStore,
} from './plan-state-store';
import {
    InMemoryRestoreIndexStateReader,
    RestoreIndexStateReader,
} from '../restore-index/state-reader';

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

function summarizeFreshness(watermarks: RestoreWatermark[]): {
    stale: number;
    unknown: number;
} {
    let stale = 0;
    let unknown = 0;

    for (const watermark of watermarks) {
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
    input: {
        request: CreateDryRunPlanRequest;
        watermarks: RestoreWatermark[];
    },
): RestoreDryRunGate {
    const unresolvedDeleteCandidates = countUnresolvedDeleteCandidates(
        input.request,
    );
    const unresolvedHardBlockConflicts = countUnresolvedHardBlockConflicts(
        input.request,
    );
    const unresolvedMediaCandidates = countUnresolvedMediaCandidates(
        input.request,
    );
    const freshness = summarizeFreshness(input.watermarks);

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

function sortRequestedPartitions(
    partitions: Map<string, {
        partition: number;
        topic: string;
    }>,
): Array<{
    partition: number;
    topic: string;
}> {
    return Array.from(partitions.values())
        .sort((left, right) => {
            if (left.topic === right.topic) {
                return left.partition - right.partition;
            }

            return left.topic.localeCompare(right.topic);
        });
}

function extractRequestedPartitionsFromRows(
    request: CreateDryRunPlanRequest,
): Array<{
    partition: number;
    topic: string;
}> {
    const partitions = new Map<string, {
        partition: number;
        topic: string;
    }>();

    for (const row of request.rows) {
        const topic = row.metadata.metadata.topic;
        const partition = row.metadata.metadata.partition;

        const normalizedPartition =
            typeof partition === 'number'
            && Number.isInteger(partition)
            && partition >= 0
                ? partition
                : null;

        if (typeof topic !== 'string' || normalizedPartition === null) {
            continue;
        }

        const normalizedTopic = topic.trim();

        if (!normalizedTopic) {
            continue;
        }

        const key = `${normalizedTopic}|${normalizedPartition}`;

        if (!partitions.has(key)) {
            partitions.set(key, {
                partition: normalizedPartition,
                topic: normalizedTopic,
            });
        }
    }

    return sortRequestedPartitions(partitions);
}

function extractRequestedPartitionsFromWatermarks(
    request: CreateDryRunPlanRequest,
): Array<{
    partition: number;
    topic: string;
}> {
    const partitions = new Map<string, {
        partition: number;
        topic: string;
    }>();

    for (const watermark of request.watermarks) {
        const key = `${watermark.topic}|${watermark.partition}`;

        if (!partitions.has(key)) {
            partitions.set(key, {
                partition: watermark.partition,
                topic: watermark.topic,
            });
        }
    }

    return sortRequestedPartitions(partitions);
}

function extractRequestedTopicsFromRows(
    request: CreateDryRunPlanRequest,
): Set<string> {
    const topics = new Set<string>();

    for (const row of request.rows) {
        const topic = row.metadata.metadata.topic;

        if (typeof topic !== 'string') {
            continue;
        }

        const normalizedTopic = topic.trim();

        if (!normalizedTopic) {
            continue;
        }

        topics.add(normalizedTopic);
    }

    return topics;
}

function extractRequestedTopicsFromWatermarks(
    request: CreateDryRunPlanRequest,
): Set<string> {
    const topics = new Set<string>();

    for (const watermark of request.watermarks) {
        const normalizedTopic = watermark.topic.trim();

        if (!normalizedTopic) {
            continue;
        }

        topics.add(normalizedTopic);
    }

    return topics;
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
    private readonly sourceMappingResolver: SourceMappingResolver;

    constructor(
        sourceRegistry?: SourceRegistry,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestorePlanStateStore =
            new InMemoryRestorePlanStateStore(),
        private readonly restoreIndexStateReader: RestoreIndexStateReader =
            new InMemoryRestoreIndexStateReader(),
        sourceMappingResolver?: SourceMappingResolver,
    ) {
        if (sourceMappingResolver) {
            this.sourceMappingResolver = sourceMappingResolver;
            return;
        }

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

    async createDryRunPlan(
        requestBody: unknown,
        claims: AuthTokenClaims,
    ): Promise<CreateDryRunPlanResult> {
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
        const scopeCheck = await this.validateScopeRequest(request, claims);

        if (!scopeCheck.allowed) {
            return {
                success: false,
                statusCode: scopeCheck.statusCode,
                error: 'scope_blocked',
                reasonCode: scopeCheck.reasonCode,
                message: scopeCheck.message,
            };
        }

        const measuredAt = normalizeIsoWithMillis(this.now());
        const requestedRowPartitions =
            extractRequestedPartitionsFromRows(request);
        const fallbackPartitions =
            extractRequestedPartitionsFromWatermarks(request);
        let authoritativeWatermarks: RestoreWatermark[];

        try {
            if (requestedRowPartitions.length > 0) {
                authoritativeWatermarks =
                    await this.restoreIndexStateReader
                        .readWatermarksForPartitions({
                            instanceId: request.instance_id,
                            measuredAt,
                            partitions: requestedRowPartitions,
                            source: request.source,
                            tenantId: request.tenant_id,
                        });
            } else {
                const sourceWatermarks =
                    await this.restoreIndexStateReader
                        .listWatermarksForSource({
                            instanceId: request.instance_id,
                            measuredAt,
                            source: request.source,
                            tenantId: request.tenant_id,
                        });
                const requestedRowTopics = extractRequestedTopicsFromRows(
                    request,
                );
                const requestedTopics = requestedRowTopics.size > 0
                    ? requestedRowTopics
                    : extractRequestedTopicsFromWatermarks(request);

                if (sourceWatermarks.length === 0) {
                    authoritativeWatermarks =
                        await this.restoreIndexStateReader
                            .readWatermarksForPartitions({
                                instanceId: request.instance_id,
                                measuredAt,
                                partitions: fallbackPartitions,
                                source: request.source,
                                tenantId: request.tenant_id,
                            });
                } else if (requestedTopics.size === 0) {
                    authoritativeWatermarks = sourceWatermarks;
                } else {
                    authoritativeWatermarks = sourceWatermarks.filter(
                        (watermark) => requestedTopics.has(watermark.topic),
                    );

                    if (authoritativeWatermarks.length === 0) {
                        authoritativeWatermarks =
                            await this.restoreIndexStateReader
                                .readWatermarksForPartitions({
                                    instanceId: request.instance_id,
                                    measuredAt,
                                    partitions: fallbackPartitions,
                                    source: request.source,
                                    tenantId: request.tenant_id,
                                });
                    }
                }
            }
        } catch (error: unknown) {
            return {
                success: false,
                statusCode: 503,
                error: 'restore_index_unavailable',
                reasonCode: 'blocked_freshness_unknown',
                message: `authoritative restore index read failed: ${
                    String((error as Error)?.message || error)
                }`,
            };
        }

        const actionCounts = buildActionCounts(request);
        const planHashInput = buildPlanHashInput(request, actionCounts);
        const planHashData = computeRestorePlanHash(planHashInput);
        return this.stateStore.mutate((state) => {
            const existing = state.plans_by_id[request.plan_id];

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
            const gate = evaluateGate({
                request,
                watermarks: authoritativeWatermarks,
            });
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
                watermarks: [...authoritativeWatermarks],
            };

            state.plans_by_id[request.plan_id] = record;

            return {
                success: true,
                statusCode: 201,
                record,
            };
        });
    }

    async getPlan(planId: string): Promise<RestoreDryRunPlanRecord | null> {
        const state = await this.stateStore.read();
        const record = state.plans_by_id[planId];

        if (!record) {
            return null;
        }

        return JSON.parse(JSON.stringify(record)) as RestoreDryRunPlanRecord;
    }

    async listPlans(): Promise<RestoreDryRunPlanRecord[]> {
        const state = await this.stateStore.read();

        return Object.values(state.plans_by_id)
            .map((record) =>
                JSON.parse(JSON.stringify(record)) as RestoreDryRunPlanRecord
            )
            .sort((left, right) => {
                return left.plan.generated_at.localeCompare(
                    right.plan.generated_at,
                );
            });
    }

    private async validateScopeRequest(
        request: CreateDryRunPlanRequest,
        claims: AuthTokenClaims,
    ): Promise<{
        allowed: boolean;
        statusCode: number;
        reasonCode: RestoreReasonCode;
        message: string;
    }> {
        if (
            claims.tenant_id !== request.tenant_id ||
            claims.instance_id !== request.instance_id ||
            claims.source !== request.source
        ) {
            return {
                allowed: false,
                statusCode: 403,
                reasonCode: 'blocked_unknown_source_mapping',
                message:
                    'token scope does not match tenant/instance/source request',
            };
        }

        let mappingResolution: AcpResolveSourceMappingResult;

        try {
            mappingResolution =
                await this.sourceMappingResolver.resolveSourceMapping({
                    instanceId: request.instance_id,
                    tenantId: request.tenant_id,
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
