import {
    computeRestorePlanHash,
    PLAN_HASH_ALGORITHM,
    PLAN_HASH_INPUT_VERSION,
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestorePlan,
    RestoreReasonCode,
    RestoreWatermark,
    type RestorePlanHashRowInput,
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
    CreateDryRunPlanFailure,
    CreateDryRunPlanRequest,
    CreateDryRunPlanResult,
    FinalizeTargetReconciliationRequest,
    FinalizeTargetReconciliationResult,
    FreshnessUnknownDetail,
    parseCreateDryRunPlanRequest,
    RestoreActionCountsRecord,
    RestoreDryRunPlanDraftRecord,
    RestoreDryRunGate,
    RestoreDryRunPlanRecord,
    RestorePitResolutionRecord,
    RestoreTargetReconciliationDraftRowRecord,
    RestoreTargetReconciliationRequestRecord,
    TargetReconciliationRecordState,
} from './models';
import {
    InMemoryRestorePlanStateStore,
    RestorePlanStateStore,
} from './plan-state-store';
import {
    InMemoryRestoreIndexStateReader,
    RestoreIndexAuthoritativeReader,
    RestoreIndexStateReader,
} from '../restore-index/state-reader';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationError,
    RestoreRowMaterializationService,
} from './materialization-service';
import type { MaterializationScopeRecord } from './materialization-service';
import { buildTargetStateLookupKey } from './target-reconciliation';

function buildActionCounts(
    request: Pick<CreateDryRunPlanRequest, 'conflicts' | 'media_candidates'>,
    rows: RestorePlanHashRowInput[],
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

    for (const row of rows) {
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

function cloneUnknown<T>(
    value: T,
): T {
    return JSON.parse(JSON.stringify(value)) as T;
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
    if (watermarks.length === 0) {
        return {
            stale: 0,
            unknown: 1,
        };
    }

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

function isUnknownFreshnessWatermark(
    watermark: RestoreWatermark,
): boolean {
    return (
        watermark.freshness === 'unknown' ||
        watermark.reason_code === 'blocked_freshness_unknown'
    );
}

function hasInvalidAuthoritativeTimestamp(
    watermark: RestoreWatermark,
): boolean {
    return (
        !Number.isFinite(Date.parse(watermark.indexed_through_time)) ||
        !Number.isFinite(Date.parse(watermark.measured_at))
    );
}

function deriveFreshnessUnknownDetail(input: {
    request: CreateDryRunPlanRequest;
    watermarks: RestoreWatermark[];
}): FreshnessUnknownDetail {
    if (input.watermarks.length === 0) {
        return 'no_indexed_coverage';
    }

    const unknownWatermarks = input.watermarks.filter(
        isUnknownFreshnessWatermark,
    );

    if (unknownWatermarks.length === 0) {
        return 'no_indexed_coverage';
    }

    if (unknownWatermarks.some(hasInvalidAuthoritativeTimestamp)) {
        return 'invalid_authoritative_timestamp';
    }

    const requestedPartitions = [
        ...extractRequestedPartitionsFromRows(input.request),
        ...extractRequestedPartitionsFromWatermarks(input.request),
    ];

    if (requestedPartitions.length > 0) {
        const requestedKeys = new Set(
            requestedPartitions.map((partition) => {
                return `${partition.topic}|${partition.partition}`;
            }),
        );

        for (const watermark of unknownWatermarks) {
            const topic = watermark.topic.trim();

            if (!topic) {
                continue;
            }

            if (!Number.isInteger(watermark.partition) || watermark.partition < 0) {
                continue;
            }

            const key = `${topic}|${watermark.partition}`;

            if (requestedKeys.has(key)) {
                return 'partition_not_indexed';
            }
        }
    }

    return 'no_indexed_coverage';
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
    let freshnessUnknownDetail: FreshnessUnknownDetail | undefined;

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
        freshnessUnknownDetail = deriveFreshnessUnknownDetail(input);
    } else if (freshness.stale > 0) {
        executability = 'preview_only';
        reasonCode = 'blocked_freshness_stale';
    }

    return {
        executability,
        reason_code: reasonCode,
        freshness_unknown_detail: freshnessUnknownDetail,
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
        if (typeof watermark.partition !== 'number') {
            continue;
        }

        const normalizedTopic = watermark.topic.trim();

        if (!normalizedTopic) {
            continue;
        }

        const key = `${normalizedTopic}|${watermark.partition}`;

        if (!partitions.has(key)) {
            partitions.set(key, {
                partition: watermark.partition,
                topic: normalizedTopic,
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

function buildTargetReconciliationRequestRecords(
    draftRows: RestoreTargetReconciliationDraftRowRecord[],
): RestoreTargetReconciliationRequestRecord[] {
    return [...draftRows]
        .sort((left, right) => {
            return left.row.row_id.localeCompare(right.row.row_id);
        })
        .map((draftRow) => {
            if (draftRow.row.action === 'skip') {
                throw new Error(
                    'scope-driven draft rows cannot contain skip actions',
                );
            }

            return {
                row_id: draftRow.row.row_id,
                table: draftRow.row.table,
                record_sys_id: draftRow.row.record_sys_id,
                source_operation: draftRow.source_operation,
                source_action: draftRow.row.action,
            };
        });
}

type ResolvePlanRowsResult = {
    draftRows?: RestoreTargetReconciliationDraftRowRecord[];
    reconciliationState: 'draft' | 'finalized';
    pitResolutions: RestorePitResolutionRecord[];
    rows: RestorePlanHashRowInput[];
} | {
    failure: CreateDryRunPlanFailure;
};

export interface RestorePlanServiceOptions {
    allowLegacyRowsCompat?: boolean;
}

function hasIndexedEventLookup(
    reader: RestoreIndexStateReader,
): reader is RestoreIndexAuthoritativeReader {
    return typeof (reader as Partial<RestoreIndexAuthoritativeReader>)
        .lookupIndexedEventCandidates === 'function';
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
        private readonly rowMaterializationService:
            RestoreRowMaterializationService =
            new RestoreRowMaterializationService(
                new InMemoryRestoreArtifactBodyReader(),
            ),
        private readonly options: RestorePlanServiceOptions = {},
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
        const parsed = parseCreateDryRunPlanRequest(requestBody, {
            allowLegacyRowsCompat:
                this.options.allowLegacyRowsCompat === true,
        });

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

        const resolvedRows = await this.resolvePlanRows(request);

        if ('failure' in resolvedRows) {
            return resolvedRows.failure;
        }

        const requestForPlanning: CreateDryRunPlanRequest = {
            ...request,
            rows: resolvedRows.rows,
        };
        const measuredAt = normalizeIsoWithMillis(this.now());
        const requestedRowPartitions =
            extractRequestedPartitionsFromRows(requestForPlanning);
        const fallbackHintPartitions =
            extractRequestedPartitionsFromWatermarks(requestForPlanning);
        let authoritativeWatermarks: RestoreWatermark[];

        try {
            const readFallbackHintPartitions = async (): Promise<
                RestoreWatermark[]
            > => {
                if (fallbackHintPartitions.length === 0) {
                    return [];
                }

                return this.restoreIndexStateReader
                    .readWatermarksForPartitions({
                        instanceId: requestForPlanning.instance_id,
                        measuredAt,
                        partitions: fallbackHintPartitions,
                        source: requestForPlanning.source,
                        tenantId: requestForPlanning.tenant_id,
                    });
            };

            if (requestedRowPartitions.length > 0) {
                authoritativeWatermarks =
                    await this.restoreIndexStateReader
                        .readWatermarksForPartitions({
                            instanceId: requestForPlanning.instance_id,
                            measuredAt,
                            partitions: requestedRowPartitions,
                            source: requestForPlanning.source,
                            tenantId: requestForPlanning.tenant_id,
                        });
            } else {
                const sourceWatermarks =
                    await this.restoreIndexStateReader
                        .listWatermarksForSource({
                            instanceId: requestForPlanning.instance_id,
                            measuredAt,
                            source: requestForPlanning.source,
                            tenantId: requestForPlanning.tenant_id,
                        });
                const requestedRowTopics = extractRequestedTopicsFromRows(
                    requestForPlanning,
                );
                const requestedTopics = requestedRowTopics.size > 0
                    ? requestedRowTopics
                    : extractRequestedTopicsFromWatermarks(requestForPlanning);

                if (sourceWatermarks.length === 0) {
                    authoritativeWatermarks =
                        await readFallbackHintPartitions();
                } else if (requestedTopics.size === 0) {
                    authoritativeWatermarks = sourceWatermarks;
                } else {
                    authoritativeWatermarks = sourceWatermarks.filter(
                        (watermark) => requestedTopics.has(watermark.topic),
                    );

                    if (authoritativeWatermarks.length === 0) {
                        authoritativeWatermarks =
                            await readFallbackHintPartitions();
                    }
                }
            }
        } catch (error: unknown) {
            return {
                success: false,
                statusCode: 503,
                error: 'restore_index_unavailable',
                reasonCode: 'blocked_freshness_unknown',
                freshnessUnknownDetail: 'restore_index_unavailable',
                message: `authoritative restore index read failed: ${
                    String((error as Error)?.message || error)
                }`,
            };
        }

        const planRecord = this.buildPlanRecord({
            authoritativeWatermarks,
            pitResolutions: resolvedRows.pitResolutions,
            request: requestForPlanning,
        });

        return this.stateStore.mutate((state) => {
            const existingFinalized = state.plans_by_id[request.plan_id];

            if (existingFinalized) {
                if (
                    resolvedRows.reconciliationState === 'finalized' &&
                    existingFinalized.plan.plan_hash !== planRecord.plan.plan_hash
                ) {
                    return {
                        success: false,
                        statusCode: 409,
                        error: 'plan_hash_mismatch',
                        reasonCode: 'blocked_plan_hash_mismatch',
                        message:
                            'plan_id already exists with a different '
                            + 'plan_hash',
                    };
                }

                return {
                    success: true,
                    statusCode: 200,
                    record: existingFinalized,
                    reconciliation_state: 'finalized',
                    target_reconciliation_records: [],
                };
            }

            if (resolvedRows.reconciliationState === 'draft') {
                const existingDraft = state.drafts_by_id[request.plan_id];

                if (existingDraft) {
                    return this.buildDraftCreateResult(existingDraft, 200);
                }

                const draftRows = resolvedRows.draftRows || [];
                const draftRecord: RestoreDryRunPlanDraftRecord = {
                    tenant_id: request.tenant_id,
                    instance_id: request.instance_id,
                    source: request.source,
                    plan_id: request.plan_id,
                    requested_by: request.requested_by,
                    pit: cloneUnknown(request.pit),
                    scope: cloneUnknown(request.scope),
                    execution_options: cloneUnknown(request.execution_options),
                    conflicts: cloneUnknown(request.conflicts),
                    delete_candidates: cloneUnknown(request.delete_candidates),
                    media_candidates: cloneUnknown(request.media_candidates),
                    watermark_hints: cloneUnknown(request.watermarks),
                    approval: buildApprovalPlaceholder(request.approval),
                    pit_resolutions: cloneUnknown(resolvedRows.pitResolutions),
                    draft_rows: cloneUnknown(draftRows),
                    target_reconciliation_records:
                        buildTargetReconciliationRequestRecords(
                            draftRows,
                        ),
                    authoritative_watermarks:
                        cloneUnknown(authoritativeWatermarks),
                    created_at: normalizeIsoWithMillis(this.now()),
                };

                state.drafts_by_id[request.plan_id] = draftRecord;

                return this.buildDraftCreateResult(draftRecord, 202);
            }

            if (state.drafts_by_id[request.plan_id]) {
                return {
                    success: false,
                    statusCode: 409,
                    error: 'plan_id_conflict',
                    reasonCode: 'blocked_plan_hash_mismatch',
                    message:
                        'plan_id already exists with a pending target '
                        + 'reconciliation draft',
                };
            }

            state.plans_by_id[request.plan_id] = planRecord;

            return {
                success: true,
                statusCode: 201,
                record: planRecord,
                reconciliation_state: 'finalized',
                target_reconciliation_records: [],
            };
        });
    }

    async finalizeTargetReconciliation(
        planId: string,
        request: FinalizeTargetReconciliationRequest,
        claims: AuthTokenClaims,
    ): Promise<FinalizeTargetReconciliationResult> {
        const requestedRecordCount = request.reconciled_records.length;

        return this.stateStore.mutate((state) => {
            const existingFinalized = state.plans_by_id[planId];

            if (existingFinalized) {
                if (
                    !this.isScopeMatch({
                        tenantId: existingFinalized.tenant_id,
                        instanceId: existingFinalized.instance_id,
                        source: existingFinalized.source,
                        claims,
                    })
                ) {
                    return {
                        success: false,
                        statusCode: 404,
                        error: 'not_found',
                        reasonCode: 'blocked_unknown_source_mapping',
                        message:
                            'plan is not visible in the caller token scope',
                        requested_record_count: requestedRecordCount,
                    };
                }

                return {
                    success: true,
                    statusCode: 200,
                    record: existingFinalized,
                    reused_existing_plan: true,
                    requested_record_count: requestedRecordCount,
                    finalized_record_count:
                        existingFinalized.plan_hash_input.rows.length,
                };
            }

            const draft = state.drafts_by_id[planId];

            if (
                !draft ||
                !this.isScopeMatch({
                    tenantId: draft.tenant_id,
                    instanceId: draft.instance_id,
                    source: draft.source,
                    claims,
                })
            ) {
                return {
                    success: false,
                    statusCode: 404,
                    error: 'not_found',
                    reasonCode: 'blocked_unknown_source_mapping',
                    message:
                        'plan is not visible in the caller token scope',
                    requested_record_count: requestedRecordCount,
                };
            }

            const targetStateByKey = new Map<
                string,
                TargetReconciliationRecordState
            >();

            for (const record of request.reconciled_records) {
                const key = buildTargetStateLookupKey({
                    record_sys_id: record.record_sys_id,
                    table: record.table,
                });

                if (targetStateByKey.has(key)) {
                    return {
                        success: false,
                        statusCode: 400,
                        error: 'invalid_request',
                        message:
                            'duplicate reconciled_records entries are not '
                            + 'allowed',
                        requested_record_count: requestedRecordCount,
                    };
                }

                targetStateByKey.set(key, record.target_state);
            }

            const expectedKeySet = new Set<string>();

            for (const record of draft.target_reconciliation_records) {
                expectedKeySet.add(buildTargetStateLookupKey({
                    record_sys_id: record.record_sys_id,
                    table: record.table,
                }));
            }

            for (const expectedKey of expectedKeySet) {
                if (!targetStateByKey.has(expectedKey)) {
                    return {
                        success: false,
                        statusCode: 400,
                        error: 'invalid_request',
                        message:
                            'reconciled_records is missing required target '
                            + 'reconciliation rows',
                        requested_record_count: requestedRecordCount,
                    };
                }
            }

            for (const providedKey of targetStateByKey.keys()) {
                if (!expectedKeySet.has(providedKey)) {
                    return {
                        success: false,
                        statusCode: 400,
                        error: 'invalid_request',
                        message:
                            'reconciled_records contains rows not present in '
                            + 'the target reconciliation request set',
                        requested_record_count: requestedRecordCount,
                    };
                }
            }

            let finalizedRows: RestorePlanHashRowInput[];

            try {
                finalizedRows = this.rowMaterializationService
                    .finalizeDraftRows({
                        draftRows: draft.draft_rows,
                        targetStateByKey,
                        requireTargetStates: true,
                    });
            } catch (error: unknown) {
                if (error instanceof RestoreRowMaterializationError) {
                    if (error.code === 'target_reconciliation_blocked') {
                        return {
                            success: false,
                            statusCode: 409,
                            error: 'restore_plan_materialization_failed',
                            reasonCode: 'blocked_reference_conflict',
                            message:
                                'target reconciliation blocked finalization: '
                                + error.message,
                            requested_record_count: requestedRecordCount,
                        };
                    }

                    if (error.code === 'target_reconciliation_incomplete') {
                        return {
                            success: false,
                            statusCode: 400,
                            error: 'invalid_request',
                            message:
                                'target reconciliation results are incomplete',
                            requested_record_count: requestedRecordCount,
                        };
                    }

                    return {
                        success: false,
                        statusCode: 409,
                        error: 'restore_plan_materialization_failed',
                        reasonCode: 'failed_internal_error',
                        message:
                            'target reconciliation finalization failed '
                            + `(${error.code}): ${error.message}`,
                        requested_record_count: requestedRecordCount,
                    };
                }

                return {
                    success: false,
                    statusCode: 503,
                    error: 'restore_index_unavailable',
                    reasonCode: 'blocked_freshness_unknown',
                    message:
                        'target reconciliation finalization failed: '
                        + String((error as Error)?.message || error),
                    requested_record_count: requestedRecordCount,
                };
            }

            const requestForPlanning: CreateDryRunPlanRequest = {
                input_mode: 'scope_driven',
                tenant_id: draft.tenant_id,
                instance_id: draft.instance_id,
                source: draft.source,
                plan_id: draft.plan_id,
                requested_by: draft.requested_by,
                pit: cloneUnknown(draft.pit),
                scope: cloneUnknown(draft.scope),
                execution_options: cloneUnknown(draft.execution_options),
                rows: finalizedRows,
                conflicts: cloneUnknown(draft.conflicts),
                delete_candidates: cloneUnknown(draft.delete_candidates),
                media_candidates: cloneUnknown(draft.media_candidates),
                watermarks: cloneUnknown(draft.watermark_hints),
                pit_candidates: [],
                approval: cloneUnknown(draft.approval),
            };
            const finalizedPlanRecord = this.buildPlanRecord({
                authoritativeWatermarks: draft.authoritative_watermarks,
                pitResolutions: draft.pit_resolutions,
                request: requestForPlanning,
            });

            state.plans_by_id[planId] = finalizedPlanRecord;
            delete state.drafts_by_id[planId];

            return {
                success: true,
                statusCode: 201,
                record: finalizedPlanRecord,
                reused_existing_plan: false,
                requested_record_count: requestedRecordCount,
                finalized_record_count: finalizedRows.length,
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

    private buildDraftCreateResult(
        draft: RestoreDryRunPlanDraftRecord,
        statusCode: number,
    ): CreateDryRunPlanResult {
        const requestForPlanning: CreateDryRunPlanRequest = {
            input_mode: 'scope_driven',
            tenant_id: draft.tenant_id,
            instance_id: draft.instance_id,
            source: draft.source,
            plan_id: draft.plan_id,
            requested_by: draft.requested_by,
            pit: cloneUnknown(draft.pit),
            scope: cloneUnknown(draft.scope),
            execution_options: cloneUnknown(draft.execution_options),
            rows: draft.draft_rows.map((draftRow) => {
                return cloneUnknown(draftRow.row);
            }),
            conflicts: cloneUnknown(draft.conflicts),
            delete_candidates: cloneUnknown(draft.delete_candidates),
            media_candidates: cloneUnknown(draft.media_candidates),
            watermarks: cloneUnknown(draft.watermark_hints),
            pit_candidates: [],
            approval: cloneUnknown(draft.approval),
        };
        const previewRecord = this.buildPlanRecord({
            authoritativeWatermarks: draft.authoritative_watermarks,
            pitResolutions: draft.pit_resolutions,
            request: requestForPlanning,
        });

        return {
            success: true,
            statusCode,
            record: previewRecord,
            reconciliation_state: 'draft',
            target_reconciliation_records:
                cloneUnknown(draft.target_reconciliation_records),
        };
    }

    private buildPlanRecord(input: {
        authoritativeWatermarks: RestoreWatermark[];
        pitResolutions: RestorePitResolutionRecord[];
        request: CreateDryRunPlanRequest;
    }): RestoreDryRunPlanRecord {
        const actionCounts = buildActionCounts(
            input.request,
            input.request.rows,
        );
        const planHashInput = buildPlanHashInput(
            input.request,
            actionCounts,
        );
        const planHashData = computeRestorePlanHash(planHashInput);
        const nowIso = normalizeIsoWithMillis(this.now());
        const gate = evaluateGate({
            request: input.request,
            watermarks: input.authoritativeWatermarks,
        });
        const plan = RestorePlan.parse({
            contract_version: RESTORE_CONTRACT_VERSION,
            plan_id: input.request.plan_id,
            plan_hash: planHashData.plan_hash,
            plan_hash_algorithm: PLAN_HASH_ALGORITHM,
            plan_hash_input_version: PLAN_HASH_INPUT_VERSION,
            generated_at: nowIso,
            pit: input.request.pit,
            scope: input.request.scope,
            execution_options: input.request.execution_options,
            action_counts: actionCounts,
            conflicts: input.request.conflicts,
            approval: buildApprovalPlaceholder(input.request.approval),
            metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
        });

        return {
            tenant_id: input.request.tenant_id,
            instance_id: input.request.instance_id,
            source: input.request.source,
            plan,
            plan_hash_input: planHashInput,
            gate,
            delete_candidates: cloneUnknown(input.request.delete_candidates),
            media_candidates: cloneUnknown(input.request.media_candidates),
            pit_resolutions: cloneUnknown(input.pitResolutions),
            watermarks: cloneUnknown(input.authoritativeWatermarks),
        };
    }

    private isScopeMatch(input: {
        tenantId: string;
        instanceId: string;
        source: string;
        claims: AuthTokenClaims;
    }): boolean {
        return (
            input.claims.tenant_id === input.tenantId &&
            input.claims.instance_id === input.instanceId &&
            input.claims.source === input.source
        );
    }

    private async resolvePlanRows(
        request: CreateDryRunPlanRequest,
    ): Promise<ResolvePlanRowsResult> {
        if (request.input_mode === 'legacy_rows') {
            return {
                reconciliationState: 'finalized',
                pitResolutions: buildPitResolutions(request),
                rows: [...request.rows],
            };
        }

        if (!hasIndexedEventLookup(this.restoreIndexStateReader)) {
            return {
                failure: {
                    success: false,
                    statusCode: 503,
                    error: 'restore_index_unavailable',
                    reasonCode: 'blocked_freshness_unknown',
                    freshnessUnknownDetail: 'restore_index_unavailable',
                    message:
                        'scope-driven dry-run requires indexed-event lookup, '
                        + 'but restore index reader does not provide it',
                },
            };
        }

        let lookupResult;

        try {
            lookupResult =
                await this.restoreIndexStateReader.lookupIndexedEventCandidates(
                    {
                        instanceId: request.instance_id,
                        pitCutoff: request.pit.restore_time,
                        recordSysIds: request.scope.record_sys_ids,
                        source: request.source,
                        tables: request.scope.tables,
                        tenantId: request.tenant_id,
                    },
                );
        } catch (error: unknown) {
            return {
                failure: {
                    success: false,
                    statusCode: 503,
                    error: 'restore_index_unavailable',
                    reasonCode: 'blocked_freshness_unknown',
                    freshnessUnknownDetail: 'restore_index_unavailable',
                    message:
                        'authoritative indexed-event lookup failed: '
                        + String((error as Error)?.message || error),
                },
            };
        }

        if (
            lookupResult.coverage !== 'covered'
            || lookupResult.candidates.length === 0
        ) {
            return {
                failure: {
                    success: false,
                    statusCode: 409,
                    error: 'restore_plan_materialization_failed',
                    reasonCode: 'blocked_freshness_unknown',
                    freshnessUnknownDetail: 'no_indexed_coverage',
                    message:
                        'scope/PIT request has no indexed-event coverage for '
                        + 'row materialization',
                },
            };
        }

        const scopeRecords = this.buildMaterializationScopeRecords(request);

        try {
            const materialized = await this.rowMaterializationService
                .materializeRowsForTargetReconciliationDraft({
                    candidates: lookupResult.candidates,
                    instanceId: request.instance_id,
                    scopeRecords,
                    source: request.source,
                    tenantId: request.tenant_id,
                });

            if (materialized.rows.length === 0) {
                return {
                    failure: {
                        success: false,
                        statusCode: 409,
                        error: 'restore_plan_materialization_failed',
                        reasonCode: 'blocked_freshness_unknown',
                        freshnessUnknownDetail: 'no_indexed_coverage',
                        message:
                            'scope/PIT materialization produced zero plan rows',
                    },
                };
            }

            return {
                draftRows: materialized.draftRows,
                reconciliationState: 'draft',
                pitResolutions: materialized.pitResolutions,
                rows: materialized.rows,
            };
        } catch (error: unknown) {
            if (error instanceof RestoreRowMaterializationError) {
                if (error.code === 'missing_pit_candidates') {
                    return {
                        failure: {
                            success: false,
                            statusCode: 409,
                            error: 'restore_plan_materialization_failed',
                            reasonCode: 'blocked_freshness_unknown',
                            freshnessUnknownDetail: 'no_indexed_coverage',
                            message:
                                'scope/PIT row materialization failed '
                            + `(missing PIT candidates): ${error.message}`,
                        },
                    };
                }

                return {
                    failure: {
                        success: false,
                        statusCode: 409,
                        error: 'restore_plan_materialization_failed',
                        reasonCode: 'failed_internal_error',
                        message:
                            'scope/PIT row materialization failed '
                            + `(${error.code}): ${error.message}`,
                    },
                };
            }

            return {
                failure: {
                    success: false,
                    statusCode: 503,
                    error: 'restore_index_unavailable',
                    reasonCode: 'blocked_freshness_unknown',
                    freshnessUnknownDetail: 'restore_index_unavailable',
                    message:
                        'scope/PIT row materialization failed: '
                        + String((error as Error)?.message || error),
                },
            };
        }
    }

    private buildMaterializationScopeRecords(
        request: CreateDryRunPlanRequest,
    ): MaterializationScopeRecord[] {
        const recordSysIds = request.scope.record_sys_ids;

        if (!recordSysIds || recordSysIds.length === 0) {
            return [];
        }

        const tables = [...request.scope.tables]
            .map((table) => table.trim())
            .filter((table) => table.length > 0)
            .sort((left, right) => left.localeCompare(right));
        const normalizedRecordSysIds = [...recordSysIds]
            .map((recordSysId) => recordSysId.trim())
            .filter((recordSysId) => recordSysId.length > 0)
            .sort((left, right) => left.localeCompare(right));
        const keyed = new Map<string, MaterializationScopeRecord>();

        for (const table of tables) {
            for (const recordSysId of normalizedRecordSysIds) {
                const key = `${table}|${recordSysId}`;

                if (keyed.has(key)) {
                    continue;
                }

                keyed.set(key, {
                    recordSysId,
                    table,
                });
            }
        }

        return Array.from(keyed.values());
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
