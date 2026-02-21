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

export type InMemoryRestoreIndexStateReaderOptions = {
    staleAfterSeconds?: number;
};

export type PostgresRestoreIndexStateReaderOptions = {
    schemaName?: string;
    staleAfterSeconds?: number;
};

const DEFAULT_STALE_AFTER_SECONDS = 120;
const DEFAULT_SCHEMA_NAME = 'rez_restore_index';
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

export class InMemoryRestoreIndexStateReader implements RestoreIndexStateReader {
    private readonly policy: FreshnessPolicy;

    private readonly partitionWatermarks =
        new Map<string, StoredPartitionWatermark>();

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
}

export class PostgresRestoreIndexStateReader implements RestoreIndexStateReader {
    private readonly policy: FreshnessPolicy;

    private readonly partitionWatermarksTableQualified: string;

    constructor(
        private readonly pool: Pool,
        options: PostgresRestoreIndexStateReaderOptions = {},
    ) {
        const schemaName = validateSqlIdentifier(
            options.schemaName || DEFAULT_SCHEMA_NAME,
            'restore-index schema name',
        );

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
