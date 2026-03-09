import {
    canonicalizeIsoDateTimeWithMillis,
    canonicalizeRestoreOffsetDecimalString,
    type RestoreWatermark,
} from '@rezilient/types';
import type { Pool } from 'pg';
import { RESTORE_CONTRACT_VERSION } from '../constants';

type FreshnessPolicy = {
    staleAfterSeconds: number;
};

type RestoreIndexPartition = {
    partition: number;
    topic: string;
};

type RestoreIndexScope = {
    instanceId: string;
    source: string;
    tenantId: string;
};

type WatermarkInput = RestoreIndexScope & RestoreIndexPartition;

type StoredPartitionWatermark = {
    coverageEnd: string;
    coverageStart: string;
    generationId: string;
    indexedThroughOffset: string;
    indexedThroughTime: string;
};

type PartitionWatermarkRow = {
    coverage_end: Date | string;
    coverage_start: Date | string;
    generation_id: string;
    indexed_through_offset: number | string;
    indexed_through_time: Date | string;
    instance_id: string;
    kafka_partition: number;
    source: string;
    tenant_id: string;
    topic: string;
};

type IndexedEventLookupRow = {
    artifact_key: string | null;
    event_id: string | null;
    event_time: Date | string;
    kafka_offset: number | string;
    kafka_partition: number;
    manifest_key: string | null;
    record_sys_id: string | null;
    sys_mod_count: number | null;
    sys_updated_on: string | null;
    table_name: string | null;
    topic: string;
};

export type RestoreIndexIndexedEventLookupInput = {
    instanceId: string;
    pitCutoff: string;
    recordSysIds?: string[];
    source: string;
    tables: string[];
    tenantId: string;
};

export type RestoreIndexIndexedEventLookupCandidate = {
    artifactKey: string;
    eventId: string;
    eventTime: string;
    manifestKey: string;
    offset: string;
    partition: number;
    recordSysId: string;
    sysModCount: number | null;
    sysUpdatedOn: string | null;
    table: string;
    topic: string;
};

export type RestoreIndexIndexedEventLookupResult = {
    candidates: RestoreIndexIndexedEventLookupCandidate[];
    coverage: 'covered' | 'no_indexed_coverage';
};

export interface RestoreIndexStateReader {
    listWatermarksForSource(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]>;
    readWatermarksForPartitions(input: {
        instanceId: string;
        measuredAt: string;
        partitions: RestoreIndexPartition[];
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]>;
}

export interface RestoreIndexIndexedEventReader {
    lookupIndexedEventCandidates(
        input: RestoreIndexIndexedEventLookupInput,
    ): Promise<RestoreIndexIndexedEventLookupResult>;
}

export type RestoreIndexAuthoritativeReader =
    RestoreIndexStateReader
    & RestoreIndexIndexedEventReader;

export type InMemoryRestoreIndexStateReaderOptions = {
    staleAfterSeconds?: number;
};

export type PostgresRestoreIndexStateReaderOptions = {
    schemaName?: string;
    staleAfterSeconds?: number;
};

const DEFAULT_STALE_AFTER_SECONDS = 120;
const DEFAULT_SCHEMA_NAME = 'rez_restore_index';
const INDEX_EVENTS_TABLE = 'index_events';
const PARTITION_WATERMARKS_TABLE = 'partition_watermarks';

function uniquePartitions(
    partitions: RestoreIndexPartition[],
): RestoreIndexPartition[] {
    const keyed = new Map<string, RestoreIndexPartition>();

    for (const partition of partitions) {
        const key = `${partition.topic}|${partition.partition}`;

        if (!keyed.has(key)) {
            keyed.set(key, partition);
        }
    }

    return Array.from(keyed.values())
        .sort((left, right) => {
            if (left.topic === right.topic) {
                return left.partition - right.partition;
            }

            return left.topic.localeCompare(right.topic);
        });
}

function parseStaleAfterSeconds(
    value: number | undefined,
): number {
    if (
        typeof value !== 'number'
        || !Number.isFinite(value)
        || value < 1
        || !Number.isInteger(value)
    ) {
        return DEFAULT_STALE_AFTER_SECONDS;
    }

    return value;
}

function canonicalizeTimestamp(
    value: Date | string,
    field: string,
): string {
    const input = value instanceof Date ? value.toISOString() : String(value);

    try {
        return canonicalizeIsoDateTimeWithMillis(input);
    } catch {
        throw new Error(`invalid ${field} timestamp`);
    }
}

function readRequiredText(
    value: string,
    field: string,
): string {
    const trimmed = String(value || '').trim();

    if (!trimmed) {
        throw new Error(`${field} is required`);
    }

    return trimmed;
}

function readOptionalText(
    value: unknown,
): string | null {
    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();

    return trimmed.length > 0 ? trimmed : null;
}

function normalizeLookupList(
    values: string[],
    field: string,
): string[] {
    if (!Array.isArray(values)) {
        throw new Error(`${field} must contain at least one value`);
    }

    const unique = new Set<string>();

    for (const value of values) {
        const normalized = String(value || '').trim();

        if (!normalized) {
            continue;
        }

        unique.add(normalized);
    }

    if (unique.size === 0) {
        throw new Error(`${field} must contain at least one value`);
    }

    return Array.from(unique.values()).sort((left, right) => {
        return left.localeCompare(right);
    });
}

type NormalizedIndexedEventLookupInput = {
    instanceId: string;
    pitCutoff: string;
    recordSysIds: string[];
    source: string;
    tables: string[];
    tenantId: string;
};

function normalizeIndexedEventLookupInput(
    input: RestoreIndexIndexedEventLookupInput,
): NormalizedIndexedEventLookupInput {
    return {
        instanceId: readRequiredText(
            input.instanceId,
            'indexed-event lookup instanceId',
        ),
        pitCutoff: canonicalizeTimestamp(
            input.pitCutoff,
            'indexed-event lookup pitCutoff',
        ),
        recordSysIds: Array.isArray(input.recordSysIds)
            ? normalizeLookupList(
                input.recordSysIds,
                'indexed-event lookup recordSysIds',
            )
            : [],
        source: readRequiredText(
            input.source,
            'indexed-event lookup source',
        ),
        tables: normalizeLookupList(
            input.tables,
            'indexed-event lookup tables',
        ),
        tenantId: readRequiredText(
            input.tenantId,
            'indexed-event lookup tenantId',
        ),
    };
}

function parseOptionalNonNegativeInteger(
    value: unknown,
): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);

    if (!Number.isInteger(parsed) || parsed < 0) {
        return null;
    }

    return parsed;
}

function parseServiceNowUtcMillis(
    value: string | null,
): number {
    if (value === null) {
        return Number.NEGATIVE_INFINITY;
    }

    const parsed = Date.parse(value.replace(' ', 'T') + '.000Z');

    if (!Number.isFinite(parsed)) {
        return Number.NEGATIVE_INFINITY;
    }

    return parsed;
}

function parseIsoUtcMillis(
    value: string,
): number {
    const parsed = Date.parse(value);

    if (!Number.isFinite(parsed)) {
        return Number.NEGATIVE_INFINITY;
    }

    return parsed;
}

function parseOffsetBigInt(
    value: string,
): bigint {
    try {
        return BigInt(canonicalizeRestoreOffsetDecimalString(value));
    } catch {
        return BigInt(0);
    }
}

function compareIndexedEventCandidates(
    left: RestoreIndexIndexedEventLookupCandidate,
    right: RestoreIndexIndexedEventLookupCandidate,
): number {
    const tableCompare = left.table.localeCompare(right.table);

    if (tableCompare !== 0) {
        return tableCompare;
    }

    const recordCompare = left.recordSysId.localeCompare(right.recordSysId);

    if (recordCompare !== 0) {
        return recordCompare;
    }

    const sysUpdatedCompare = parseServiceNowUtcMillis(right.sysUpdatedOn)
        - parseServiceNowUtcMillis(left.sysUpdatedOn);

    if (sysUpdatedCompare !== 0) {
        return sysUpdatedCompare;
    }

    const leftSysModCount = left.sysModCount === null
        ? Number.NEGATIVE_INFINITY
        : left.sysModCount;
    const rightSysModCount = right.sysModCount === null
        ? Number.NEGATIVE_INFINITY
        : right.sysModCount;
    const sysModCompare = rightSysModCount - leftSysModCount;

    if (sysModCompare !== 0) {
        return sysModCompare;
    }

    const eventTimeCompare = parseIsoUtcMillis(right.eventTime)
        - parseIsoUtcMillis(left.eventTime);

    if (eventTimeCompare !== 0) {
        return eventTimeCompare;
    }

    const eventIdCompare = right.eventId.localeCompare(left.eventId);

    if (eventIdCompare !== 0) {
        return eventIdCompare;
    }

    const topicCompare = left.topic.localeCompare(right.topic);

    if (topicCompare !== 0) {
        return topicCompare;
    }

    if (left.partition !== right.partition) {
        return left.partition - right.partition;
    }

    const leftOffset = parseOffsetBigInt(left.offset);
    const rightOffset = parseOffsetBigInt(right.offset);

    if (leftOffset !== rightOffset) {
        return rightOffset > leftOffset ? 1 : -1;
    }

    const artifactCompare = left.artifactKey.localeCompare(right.artifactKey);

    if (artifactCompare !== 0) {
        return artifactCompare;
    }

    return left.manifestKey.localeCompare(right.manifestKey);
}

function buildIndexedEventLookupResult(
    candidates: RestoreIndexIndexedEventLookupCandidate[],
): RestoreIndexIndexedEventLookupResult {
    const ordered = [...candidates].sort(compareIndexedEventCandidates);

    return {
        candidates: ordered,
        coverage: ordered.length > 0 ? 'covered' : 'no_indexed_coverage',
    };
}

function evaluateFreshness(
    indexedThroughTime: string,
    measuredAt: string,
    policy: FreshnessPolicy,
): {
    executability: RestoreWatermark['executability'];
    freshness: RestoreWatermark['freshness'];
    reasonCode: RestoreWatermark['reason_code'];
} {
    const indexedMillis = Date.parse(indexedThroughTime);
    const measuredMillis = Date.parse(measuredAt);

    if (!Number.isFinite(indexedMillis) || !Number.isFinite(measuredMillis)) {
        return {
            executability: 'blocked',
            freshness: 'unknown',
            reasonCode: 'blocked_freshness_unknown',
        };
    }

    const lagSeconds = Math.max(
        0,
        Math.floor((measuredMillis - indexedMillis) / 1000),
    );

    if (lagSeconds > policy.staleAfterSeconds) {
        return {
            executability: 'preview_only',
            freshness: 'stale',
            reasonCode: 'blocked_freshness_stale',
        };
    }

    return {
        executability: 'executable',
        freshness: 'fresh',
        reasonCode: 'none',
    };
}

function buildUnknownWatermark(
    input: WatermarkInput,
    measuredAt: string,
): RestoreWatermark {
    return {
        contract_version: RESTORE_CONTRACT_VERSION,
        coverage_end: measuredAt,
        coverage_start: measuredAt,
        executability: 'blocked',
        freshness: 'unknown',
        generation_id: 'unknown',
        indexed_through_offset: '0',
        indexed_through_time: measuredAt,
        instance_id: input.instanceId,
        measured_at: measuredAt,
        partition: input.partition,
        reason_code: 'blocked_freshness_unknown',
        source: input.source,
        tenant_id: input.tenantId,
        topic: input.topic,
    };
}

function buildKnownWatermark(
    input: WatermarkInput,
    state: StoredPartitionWatermark,
    measuredAt: string,
    policy: FreshnessPolicy,
): RestoreWatermark {
    const evaluated = evaluateFreshness(
        state.indexedThroughTime,
        measuredAt,
        policy,
    );

    return {
        contract_version: RESTORE_CONTRACT_VERSION,
        coverage_end: state.coverageEnd,
        coverage_start: state.coverageStart,
        executability: evaluated.executability,
        freshness: evaluated.freshness,
        generation_id: state.generationId,
        indexed_through_offset: state.indexedThroughOffset,
        indexed_through_time: state.indexedThroughTime,
        instance_id: input.instanceId,
        measured_at: measuredAt,
        partition: input.partition,
        reason_code: evaluated.reasonCode,
        source: input.source,
        tenant_id: input.tenantId,
        topic: input.topic,
    };
}

function partitionKey(input: WatermarkInput): string {
    return [
        input.tenantId,
        input.instanceId,
        input.source,
        input.topic,
        String(input.partition),
    ].join('|');
}

function validateSqlIdentifier(
    value: string,
    fieldName: string,
): string {
    const trimmed = String(value || '').trim();

    if (trimmed.length === 0) {
        throw new Error(`${fieldName} is required`);
    }

    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
        throw new Error(
            `${fieldName} must match [A-Za-z_][A-Za-z0-9_]*`,
        );
    }

    return trimmed;
}

type StoredIndexedEventCandidate =
    RestoreIndexIndexedEventLookupCandidate
    & {
        instanceId: string;
        source: string;
        tenantId: string;
    };

function indexedEventCandidateKey(
    candidate: StoredIndexedEventCandidate,
): string {
    return [
        candidate.tenantId,
        candidate.instanceId,
        candidate.source,
        candidate.table,
        candidate.recordSysId,
        candidate.eventId,
        candidate.topic,
        String(candidate.partition),
        candidate.offset,
    ].join('|');
}

export class InMemoryRestoreIndexStateReader
implements RestoreIndexStateReader, RestoreIndexIndexedEventReader {
    private readonly policy: FreshnessPolicy;

    private readonly partitionWatermarks =
        new Map<string, StoredPartitionWatermark>();

    private readonly indexedEventCandidates =
        new Map<string, StoredIndexedEventCandidate>();

    constructor(options: InMemoryRestoreIndexStateReaderOptions = {}) {
        this.policy = {
            staleAfterSeconds: parseStaleAfterSeconds(options.staleAfterSeconds),
        };
    }

    upsertWatermark(watermark: RestoreWatermark): void {
        const key = partitionKey({
            instanceId: watermark.instance_id,
            partition: watermark.partition,
            source: watermark.source,
            tenantId: watermark.tenant_id,
            topic: watermark.topic,
        });

        this.partitionWatermarks.set(key, {
            coverageEnd: canonicalizeTimestamp(
                watermark.coverage_end,
                'in-memory watermark coverage_end',
            ),
            coverageStart: canonicalizeTimestamp(
                watermark.coverage_start,
                'in-memory watermark coverage_start',
            ),
            generationId: String(watermark.generation_id),
            indexedThroughOffset: canonicalizeRestoreOffsetDecimalString(
                watermark.indexed_through_offset,
            ),
            indexedThroughTime: canonicalizeTimestamp(
                watermark.indexed_through_time,
                'in-memory watermark indexed_through_time',
            ),
        });
    }

    upsertIndexedEventCandidate(input: {
        artifactKey: string;
        eventId: string;
        eventTime: string;
        instanceId: string;
        manifestKey: string;
        offset: string;
        partition: number;
        recordSysId: string;
        source: string;
        sysModCount?: number | null;
        sysUpdatedOn?: string | null;
        table: string;
        tenantId: string;
        topic: string;
    }): void {
        const stored: StoredIndexedEventCandidate = {
            artifactKey: readRequiredText(
                input.artifactKey,
                'indexed-event candidate artifactKey',
            ),
            eventId: readRequiredText(
                input.eventId,
                'indexed-event candidate eventId',
            ),
            eventTime: canonicalizeTimestamp(
                input.eventTime,
                'indexed-event candidate eventTime',
            ),
            instanceId: readRequiredText(
                input.instanceId,
                'indexed-event candidate instanceId',
            ),
            manifestKey: readRequiredText(
                input.manifestKey,
                'indexed-event candidate manifestKey',
            ),
            offset: canonicalizeRestoreOffsetDecimalString(input.offset),
            partition: Math.max(0, Math.trunc(input.partition)),
            recordSysId: readRequiredText(
                input.recordSysId,
                'indexed-event candidate recordSysId',
            ),
            source: readRequiredText(
                input.source,
                'indexed-event candidate source',
            ),
            sysModCount: parseOptionalNonNegativeInteger(input.sysModCount),
            sysUpdatedOn: readOptionalText(input.sysUpdatedOn),
            table: readRequiredText(
                input.table,
                'indexed-event candidate table',
            ),
            tenantId: readRequiredText(
                input.tenantId,
                'indexed-event candidate tenantId',
            ),
            topic: readRequiredText(
                input.topic,
                'indexed-event candidate topic',
            ),
        };

        this.indexedEventCandidates.set(
            indexedEventCandidateKey(stored),
            stored,
        );
    }

    async readWatermarksForPartitions(input: {
        instanceId: string;
        measuredAt: string;
        partitions: RestoreIndexPartition[];
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]> {
        const measuredAt = canonicalizeTimestamp(
            input.measuredAt,
            'authoritative measuredAt',
        );
        const partitions = uniquePartitions(input.partitions);

        return partitions.map((partition) => {
            const scoped = {
                instanceId: input.instanceId,
                partition: partition.partition,
                source: input.source,
                tenantId: input.tenantId,
                topic: partition.topic,
            };
            const state = this.partitionWatermarks.get(partitionKey(scoped));

            if (!state) {
                return buildUnknownWatermark(scoped, measuredAt);
            }

            return buildKnownWatermark(scoped, state, measuredAt, this.policy);
        });
    }

    async listWatermarksForSource(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]> {
        const measuredAt = canonicalizeTimestamp(
            input.measuredAt,
            'authoritative measuredAt',
        );
        const rows: RestoreWatermark[] = [];

        for (const [key, state] of this.partitionWatermarks.entries()) {
            const [
                tenantId,
                instanceId,
                source,
                topic,
                partitionRaw,
            ] = key.split('|');

            if (
                tenantId !== input.tenantId
                || instanceId !== input.instanceId
                || source !== input.source
            ) {
                continue;
            }

            rows.push(buildKnownWatermark({
                instanceId,
                partition: Number.parseInt(partitionRaw, 10),
                source,
                tenantId,
                topic,
            }, state, measuredAt, this.policy));
        }

        return rows.sort((left, right) => {
            if (left.topic === right.topic) {
                return left.partition - right.partition;
            }

            return left.topic.localeCompare(right.topic);
        });
    }

    async lookupIndexedEventCandidates(
        input: RestoreIndexIndexedEventLookupInput,
    ): Promise<RestoreIndexIndexedEventLookupResult> {
        const normalized = normalizeIndexedEventLookupInput(input);
        const pitCutoffMillis = parseIsoUtcMillis(normalized.pitCutoff);
        const tableSet = new Set(normalized.tables);
        const recordSysIdSet = normalized.recordSysIds.length > 0
            ? new Set(normalized.recordSysIds)
            : null;
        const candidates: RestoreIndexIndexedEventLookupCandidate[] = [];

        for (const storedCandidate of this.indexedEventCandidates.values()) {
            if (storedCandidate.tenantId !== normalized.tenantId) {
                continue;
            }

            if (storedCandidate.instanceId !== normalized.instanceId) {
                continue;
            }

            if (storedCandidate.source !== normalized.source) {
                continue;
            }

            if (!tableSet.has(storedCandidate.table)) {
                continue;
            }

            if (
                recordSysIdSet
                && !recordSysIdSet.has(storedCandidate.recordSysId)
            ) {
                continue;
            }

            if (parseIsoUtcMillis(storedCandidate.eventTime) > pitCutoffMillis) {
                continue;
            }

            candidates.push({
                artifactKey: storedCandidate.artifactKey,
                eventId: storedCandidate.eventId,
                eventTime: storedCandidate.eventTime,
                manifestKey: storedCandidate.manifestKey,
                offset: storedCandidate.offset,
                partition: storedCandidate.partition,
                recordSysId: storedCandidate.recordSysId,
                sysModCount: storedCandidate.sysModCount,
                sysUpdatedOn: storedCandidate.sysUpdatedOn,
                table: storedCandidate.table,
                topic: storedCandidate.topic,
            });
        }

        return buildIndexedEventLookupResult(candidates);
    }
}

export class PostgresRestoreIndexStateReader
implements RestoreIndexStateReader, RestoreIndexIndexedEventReader {
    private readonly policy: FreshnessPolicy;

    private readonly indexEventsTableQualified: string;

    private readonly partitionWatermarksTableQualified: string;

    constructor(
        private readonly pool: Pool,
        options: PostgresRestoreIndexStateReaderOptions = {},
    ) {
        const schemaName = validateSqlIdentifier(
            options.schemaName || DEFAULT_SCHEMA_NAME,
            'restore-index schema name',
        );

        this.indexEventsTableQualified =
            `"${schemaName}"."${INDEX_EVENTS_TABLE}"`;
        this.partitionWatermarksTableQualified =
            `"${schemaName}"."${PARTITION_WATERMARKS_TABLE}"`;
        this.policy = {
            staleAfterSeconds: parseStaleAfterSeconds(options.staleAfterSeconds),
        };
    }

    async readWatermarksForPartitions(input: {
        instanceId: string;
        measuredAt: string;
        partitions: RestoreIndexPartition[];
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]> {
        const measuredAt = canonicalizeTimestamp(
            input.measuredAt,
            'authoritative measuredAt',
        );
        const partitions = uniquePartitions(input.partitions);

        if (partitions.length === 0) {
            return [];
        }

        const params: Array<string | number> = [
            input.tenantId,
            input.instanceId,
            input.source,
        ];
        const clauses: string[] = [];

        for (const partition of partitions) {
            const topicIndex = params.length + 1;
            const partitionIndex = params.length + 2;

            params.push(partition.topic, partition.partition);
            clauses.push(
                `(topic = $${topicIndex} AND kafka_partition = $${partitionIndex})`,
            );
        }

        const result = await this.pool.query<PartitionWatermarkRow>(
            `SELECT
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                indexed_through_offset,
                indexed_through_time,
                coverage_start,
                coverage_end
            FROM ${this.partitionWatermarksTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3
              AND (${clauses.join(' OR ')})`,
            params,
        );
        const rowsByKey = new Map<string, StoredPartitionWatermark>();

        for (const row of result.rows) {
            const key = partitionKey({
                instanceId: row.instance_id,
                partition: row.kafka_partition,
                source: row.source,
                tenantId: row.tenant_id,
                topic: row.topic,
            });

            rowsByKey.set(key, {
                coverageEnd: canonicalizeTimestamp(
                    row.coverage_end,
                    'db watermark coverage_end',
                ),
                coverageStart: canonicalizeTimestamp(
                    row.coverage_start,
                    'db watermark coverage_start',
                ),
                generationId: String(row.generation_id),
                indexedThroughOffset: canonicalizeRestoreOffsetDecimalString(
                    row.indexed_through_offset,
                ),
                indexedThroughTime: canonicalizeTimestamp(
                    row.indexed_through_time,
                    'db watermark indexed_through_time',
                ),
            });
        }

        return partitions.map((partition) => {
            const scoped = {
                instanceId: input.instanceId,
                partition: partition.partition,
                source: input.source,
                tenantId: input.tenantId,
                topic: partition.topic,
            };
            const row = rowsByKey.get(partitionKey(scoped));

            if (!row) {
                return buildUnknownWatermark(scoped, measuredAt);
            }

            return buildKnownWatermark(scoped, row, measuredAt, this.policy);
        });
    }

    async lookupIndexedEventCandidates(
        input: RestoreIndexIndexedEventLookupInput,
    ): Promise<RestoreIndexIndexedEventLookupResult> {
        const normalized = normalizeIndexedEventLookupInput(input);
        const params: Array<string | string[]> = [
            normalized.tenantId,
            normalized.instanceId,
            normalized.source,
            normalized.pitCutoff,
            normalized.tables,
        ];
        let recordSysIdClause = '';

        if (normalized.recordSysIds.length > 0) {
            const recordSysIdParam = params.length + 1;
            params.push(normalized.recordSysIds);
            recordSysIdClause =
                `AND record_sys_id = ANY($${recordSysIdParam}::text[])`;
        }

        const result = await this.pool.query<IndexedEventLookupRow>(
            `SELECT
                table_name,
                record_sys_id,
                event_id,
                sys_updated_on,
                sys_mod_count,
                event_time,
                topic,
                kafka_partition,
                kafka_offset,
                artifact_key,
                manifest_key
            FROM ${this.indexEventsTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3
              AND event_time <= $4::timestamptz
              AND table_name = ANY($5::text[])
              AND table_name IS NOT NULL
              AND record_sys_id IS NOT NULL
              ${recordSysIdClause}
            ORDER BY
                table_name ASC,
                record_sys_id ASC,
                sys_updated_on DESC NULLS LAST,
                sys_mod_count DESC NULLS LAST,
                event_time DESC,
                event_id DESC,
                topic ASC,
                kafka_partition ASC,
                kafka_offset::numeric DESC,
                artifact_key ASC,
                manifest_key ASC`,
            params,
        );
        const candidates: RestoreIndexIndexedEventLookupCandidate[] = [];

        for (const row of result.rows) {
            const table = readOptionalText(row.table_name);
            const recordSysId = readOptionalText(row.record_sys_id);
            const eventId = readOptionalText(row.event_id);
            const artifactKey = readOptionalText(row.artifact_key);
            const manifestKey = readOptionalText(row.manifest_key);

            if (
                !table
                || !recordSysId
                || !eventId
                || !artifactKey
                || !manifestKey
            ) {
                continue;
            }

            candidates.push({
                artifactKey,
                eventId,
                eventTime: canonicalizeTimestamp(
                    row.event_time,
                    'db indexed-event event_time',
                ),
                manifestKey,
                offset: canonicalizeRestoreOffsetDecimalString(row.kafka_offset),
                partition: Math.max(0, Math.trunc(row.kafka_partition)),
                recordSysId,
                sysModCount: parseOptionalNonNegativeInteger(row.sys_mod_count),
                sysUpdatedOn: readOptionalText(row.sys_updated_on),
                table,
                topic: readRequiredText(
                    row.topic,
                    'db indexed-event topic',
                ),
            });
        }

        return buildIndexedEventLookupResult(candidates);
    }

    async listWatermarksForSource(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<RestoreWatermark[]> {
        const measuredAt = canonicalizeTimestamp(
            input.measuredAt,
            'authoritative measuredAt',
        );
        const result = await this.pool.query<PartitionWatermarkRow>(
            `SELECT
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                indexed_through_offset,
                indexed_through_time,
                coverage_start,
                coverage_end
            FROM ${this.partitionWatermarksTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3
            ORDER BY topic ASC, kafka_partition ASC`,
            [
                input.tenantId,
                input.instanceId,
                input.source,
            ],
        );

        return result.rows.map((row) => {
            return buildKnownWatermark({
                instanceId: row.instance_id,
                partition: row.kafka_partition,
                source: row.source,
                tenantId: row.tenant_id,
                topic: row.topic,
            }, {
                coverageEnd: canonicalizeTimestamp(
                    row.coverage_end,
                    'db watermark coverage_end',
                ),
                coverageStart: canonicalizeTimestamp(
                    row.coverage_start,
                    'db watermark coverage_start',
                ),
                generationId: String(row.generation_id),
                indexedThroughOffset: canonicalizeRestoreOffsetDecimalString(
                    row.indexed_through_offset,
                ),
                indexedThroughTime: canonicalizeTimestamp(
                    row.indexed_through_time,
                    'db watermark indexed_through_time',
                ),
            }, measuredAt, this.policy);
        });
    }
}
