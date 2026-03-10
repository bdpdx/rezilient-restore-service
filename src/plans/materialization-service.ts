import { createHash } from 'node:crypto';
import {
    canonicalJsonStringify,
    EncryptedPayload as EncryptedPayloadSchema,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RestorePlanHashRowInput as RestorePlanHashRowInputSchema,
    RestorePitRowTuple,
    selectLatestPitRowTuple,
} from '@rezilient/types';
import type {
    EncryptedPayload,
    RestorePlanHashRowInput,
} from '@rezilient/types';
import type {
    RestoreIndexIndexedEventLookupCandidate,
} from '../restore-index/state-reader';
import type { RestorePitResolutionRecord } from './models';
import {
    buildTargetStateLookupKey,
    NoopRestoreTargetStateLookup,
    reconcileSourceOperationWithTargetState,
    type RestoreTargetRecordState,
    type RestoreTargetStateLookup,
} from './target-reconciliation';

export type RestoreArtifactBodyReference = {
    artifactKey: string;
    manifestKey: string;
};

export interface RestoreArtifactBodyReader {
    readArtifactBody(
        reference: RestoreArtifactBodyReference,
    ): Promise<unknown | null>;
}

export type MaterializationScopeRecord = {
    recordSysId: string;
    table: string;
};

export type MaterializeRestoreRowsInput = {
    candidates: RestoreIndexIndexedEventLookupCandidate[];
    instanceId: string;
    scopeRecords?: MaterializationScopeRecord[];
    source: string;
    tenantId: string;
};

export type MaterializeRestoreRowsResult = {
    pitResolutions: RestorePitResolutionRecord[];
    rows: RestorePlanHashRowInput[];
};

export type RestoreRowMaterializationErrorCode =
    | 'ambiguous_pit_candidates'
    | 'invalid_artifact_body'
    | 'missing_artifact_body'
    | 'missing_encrypted_row_material'
    | 'missing_pit_candidates'
    | 'target_reconciliation_blocked';

export class RestoreRowMaterializationError extends Error {
    constructor(
        public readonly code: RestoreRowMaterializationErrorCode,
        message: string,
        public readonly details: Record<string, unknown> = {},
    ) {
        super(message);
        this.name = 'RestoreRowMaterializationError';
    }
}

function sha256Hex(
    value: string,
): string {
    return createHash('sha256')
        .update(value, 'utf8')
        .digest('hex');
}

function cloneUnknown<T>(
    value: T,
): T {
    if (value === undefined) {
        return value;
    }

    return JSON.parse(JSON.stringify(value)) as T;
}

function readRequiredText(
    value: unknown,
    fieldName: string,
): string {
    const normalized = String(value || '').trim();

    if (!normalized) {
        throw new RestoreRowMaterializationError(
            'invalid_artifact_body',
            `${fieldName} is required`,
            {
                field: fieldName,
            },
        );
    }

    return normalized;
}

function readOptionalText(
    value: unknown,
): string | null {
    if (typeof value !== 'string') {
        return null;
    }

    const normalized = value.trim();

    if (!normalized) {
        return null;
    }

    return normalized;
}

function normalizeOperation(
    value: unknown,
): 'D' | 'I' | 'U' | null {
    const normalized = readOptionalText(value)?.toUpperCase();

    if (normalized === 'I' || normalized === 'U' || normalized === 'D') {
        return normalized;
    }

    return null;
}

function parsePositiveInteger(
    value: unknown,
): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);

    if (!Number.isInteger(parsed) || parsed < 1) {
        return null;
    }

    return parsed;
}

function parseEncryptedPayload(
    value: unknown,
    fieldName: string,
): EncryptedPayload | undefined {
    if (value === null || value === undefined) {
        return undefined;
    }

    const parsed = EncryptedPayloadSchema.safeParse(value);

    if (!parsed.success) {
        throw new RestoreRowMaterializationError(
            'invalid_artifact_body',
            `${fieldName} is not a valid EncryptedPayload`,
            {
                field: fieldName,
                issue: parsed.error.issues[0]?.message || 'invalid payload',
            },
        );
    }

    return parsed.data;
}

function asRecord(
    value: unknown,
): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return null;
    }

    return value as Record<string, unknown>;
}

function toAction(
    operation: 'D' | 'I' | 'U',
): 'delete' | 'insert' | 'update' {
    if (operation === 'D') {
        return 'delete';
    }

    if (operation === 'I') {
        return 'insert';
    }

    return 'update';
}

function buildStableRowId(
    table: string,
    recordSysId: string,
): string {
    return `row.${sha256Hex(`${table}|${recordSysId}`)}`;
}

function buildReferenceKey(
    table: string,
    recordSysId: string,
): string {
    return `${table}|${recordSysId}`;
}

function buildArtifactReferenceKey(
    reference: RestoreArtifactBodyReference,
): string {
    return `${reference.artifactKey}|${reference.manifestKey}`;
}

function normalizeScopeRecords(
    scopeRecords: MaterializationScopeRecord[],
): MaterializationScopeRecord[] {
    const unique = new Map<string, MaterializationScopeRecord>();

    for (const record of scopeRecords) {
        const table = String(record.table || '').trim();
        const recordSysId = String(record.recordSysId || '').trim();

        if (!table || !recordSysId) {
            continue;
        }

        unique.set(buildReferenceKey(table, recordSysId), {
            recordSysId,
            table,
        });
    }

    return Array.from(unique.values())
        .sort((left, right) => {
            if (left.table === right.table) {
                return left.recordSysId.localeCompare(right.recordSysId);
            }

            return left.table.localeCompare(right.table);
        });
}

function candidateToPitTuple(
    candidate: RestoreIndexIndexedEventLookupCandidate,
): RestorePitRowTuple {
    const sysUpdatedOn = readOptionalText(candidate.sysUpdatedOn);

    if (!sysUpdatedOn) {
        throw new RestoreRowMaterializationError(
            'missing_pit_candidates',
            'candidate is missing sys_updated_on required for PIT tie-break',
            {
                eventId: candidate.eventId,
                recordSysId: candidate.recordSysId,
                table: candidate.table,
            },
        );
    }

    const tuple: RestorePitRowTuple = {
        __time: candidate.eventTime,
        event_id: candidate.eventId,
        sys_updated_on: sysUpdatedOn,
    };

    if (typeof candidate.sysModCount === 'number') {
        tuple.sys_mod_count = candidate.sysModCount;
    }

    return tuple;
}

function candidateMatchesTuple(
    candidate: RestoreIndexIndexedEventLookupCandidate,
    tuple: RestorePitRowTuple,
): boolean {
    const candidateSysModCount = candidate.sysModCount === null
        ? undefined
        : candidate.sysModCount;

    return (
        candidate.eventId === tuple.event_id &&
        candidate.eventTime === tuple.__time &&
        candidate.sysUpdatedOn === tuple.sys_updated_on &&
        candidateSysModCount === tuple.sys_mod_count
    );
}

function resolvePitWinner(
    candidates: RestoreIndexIndexedEventLookupCandidate[],
): {
    winnerCandidate: RestoreIndexIndexedEventLookupCandidate;
    winnerTuple: RestorePitRowTuple;
} {
    if (candidates.length === 0) {
        throw new RestoreRowMaterializationError(
            'missing_pit_candidates',
            'record has no PIT candidates',
        );
    }

    const tuples = candidates.map(candidateToPitTuple);
    const winnerTuple = selectLatestPitRowTuple(tuples);
    const matching = candidates.filter((candidate) => {
        return candidateMatchesTuple(candidate, winnerTuple);
    });

    if (matching.length === 0) {
        throw new RestoreRowMaterializationError(
            'missing_pit_candidates',
            'PIT winner tuple could not be mapped to a candidate',
            {
                winnerTuple,
            },
        );
    }

    if (matching.length > 1) {
        throw new RestoreRowMaterializationError(
            'ambiguous_pit_candidates',
            'multiple candidates matched PIT winner tuple',
            {
                recordSysId: matching[0].recordSysId,
                table: matching[0].table,
                winnerTuple,
            },
        );
    }

    return {
        winnerCandidate: matching[0],
        winnerTuple,
    };
}

function buildPreconditionHash(input: {
    action: 'delete' | 'insert' | 'update';
    artifactKey: string;
    manifestKey: string;
    operation: 'D' | 'I' | 'U';
    recordSysId: string;
    rowId: string;
    table: string;
    values: NonNullable<RestorePlanHashRowInput['values']>;
    winnerTuple: RestorePitRowTuple;
}): string {
    return sha256Hex(canonicalJsonStringify({
        action: input.action,
        artifact_key: input.artifactKey,
        manifest_key: input.manifestKey,
        operation: input.operation,
        record_sys_id: input.recordSysId,
        row_id: input.rowId,
        table: input.table,
        values: input.values,
        winner_tuple: input.winnerTuple,
    }));
}

function deriveOperationFromArtifact(
    artifactBody: Record<string, unknown>,
): 'D' | 'I' | 'U' {
    const event = asRecord(artifactBody.event);
    const eventData = asRecord(event?.data);
    const media = asRecord(artifactBody.media);
    const operation = normalizeOperation(artifactBody.__op)
        || normalizeOperation(artifactBody.operation)
        || normalizeOperation(artifactBody.op)
        || normalizeOperation(media?.op)
        || normalizeOperation(eventData?.op);

    if (operation) {
        return operation;
    }

    const eventType = readOptionalText(artifactBody.__type)
        || readOptionalText(event?.type);

    if (eventType === 'cdc.delete') {
        return 'D';
    }

    if (eventType === 'cdc.write') {
        return 'U';
    }

    if (artifactBody.__is_tombstone === true) {
        return 'D';
    }

    throw new RestoreRowMaterializationError(
        'invalid_artifact_body',
        'unable to derive CDC operation from artifact body',
    );
}

function deriveEventTypeFromArtifact(
    artifactBody: Record<string, unknown>,
    operation: 'D' | 'I' | 'U',
): string {
    const event = asRecord(artifactBody.event);
    const eventType = readOptionalText(artifactBody.__type)
        || readOptionalText(event?.type);

    if (eventType) {
        return eventType;
    }

    if (operation === 'D') {
        return 'cdc.delete';
    }

    return 'cdc.write';
}

function deriveSchemaVersionFromArtifact(
    artifactBody: Record<string, unknown>,
): number | undefined {
    const event = asRecord(artifactBody.event);
    const eventData = asRecord(event?.data);
    const schemaVersion = parsePositiveInteger(artifactBody.__schema_version)
        || parsePositiveInteger(artifactBody.schema_version)
        || parsePositiveInteger(eventData?.schema_version);

    return schemaVersion || undefined;
}

function deriveEncryptedValuesFromArtifact(
    artifactBody: Record<string, unknown>,
): NonNullable<RestorePlanHashRowInput['values']> {
    const event = asRecord(artifactBody.event);
    const eventData = asRecord(event?.data);
    const diffEnc = parseEncryptedPayload(
        artifactBody.diff_enc,
        'artifact.diff_enc',
    )
        || parseEncryptedPayload(artifactBody.row_enc, 'artifact.row_enc')
        || parseEncryptedPayload(
            eventData?.snapshot_enc,
            'artifact.event.data.snapshot_enc',
        );
    const beforeImageEnc = parseEncryptedPayload(
        artifactBody.before_image_enc,
        'artifact.before_image_enc',
    );
    const afterImageEnc = parseEncryptedPayload(
        artifactBody.after_image_enc,
        'artifact.after_image_enc',
    );

    if (!diffEnc && !beforeImageEnc && !afterImageEnc) {
        throw new RestoreRowMaterializationError(
            'missing_encrypted_row_material',
            'artifact body is missing encrypted row material',
        );
    }

    const values: NonNullable<RestorePlanHashRowInput['values']> = {};

    if (diffEnc) {
        values.diff_enc = diffEnc;
    }

    if (beforeImageEnc) {
        values.before_image_enc = beforeImageEnc;
    }

    if (afterImageEnc) {
        values.after_image_enc = afterImageEnc;
    }

    return values;
}

function groupByRecord(
    candidates: RestoreIndexIndexedEventLookupCandidate[],
): Map<string, RestoreIndexIndexedEventLookupCandidate[]> {
    const grouped =
        new Map<string, RestoreIndexIndexedEventLookupCandidate[]>();

    for (const candidate of candidates) {
        const key = buildReferenceKey(candidate.table, candidate.recordSysId);
        const existing = grouped.get(key);

        if (existing) {
            existing.push(candidate);
            continue;
        }

        grouped.set(key, [candidate]);
    }

    return grouped;
}

type MaterializationWinner = {
    winnerCandidate: RestoreIndexIndexedEventLookupCandidate;
    winnerTuple: RestorePitRowTuple;
};

export class InMemoryRestoreArtifactBodyReader
implements RestoreArtifactBodyReader {
    private readonly bodiesByReference = new Map<string, unknown>();

    setArtifactBody(input: {
        artifactKey: string;
        body: unknown;
        manifestKey: string;
    }): void {
        const reference: RestoreArtifactBodyReference = {
            artifactKey: String(input.artifactKey || '').trim(),
            manifestKey: String(input.manifestKey || '').trim(),
        };

        this.bodiesByReference.set(
            buildArtifactReferenceKey(reference),
            cloneUnknown(input.body),
        );
    }

    async readArtifactBody(
        reference: RestoreArtifactBodyReference,
    ): Promise<unknown | null> {
        const key = buildArtifactReferenceKey(reference);

        if (!this.bodiesByReference.has(key)) {
            return null;
        }

        return cloneUnknown(this.bodiesByReference.get(key));
    }
}

export class RestoreRowMaterializationService {
    constructor(
        private readonly artifactBodyReader: RestoreArtifactBodyReader,
        private readonly targetStateLookup: RestoreTargetStateLookup =
            new NoopRestoreTargetStateLookup(),
    ) {}

    async materializeRows(
        input: MaterializeRestoreRowsInput,
    ): Promise<MaterializeRestoreRowsResult> {
        const instanceId = readRequiredText(input.instanceId, 'instanceId');
        const source = readRequiredText(input.source, 'source');
        const tenantId = readRequiredText(input.tenantId, 'tenantId');
        const groupedCandidates = groupByRecord(input.candidates);
        const normalizedScopeRecords = input.scopeRecords
            ? normalizeScopeRecords(input.scopeRecords)
            : [];

        if (groupedCandidates.size === 0) {
            throw new RestoreRowMaterializationError(
                'missing_pit_candidates',
                'no PIT candidates were provided for materialization',
            );
        }

        if (normalizedScopeRecords.length > 0) {
            for (const scopeRecord of normalizedScopeRecords) {
                const key = buildReferenceKey(
                    scopeRecord.table,
                    scopeRecord.recordSysId,
                );

                if (!groupedCandidates.has(key)) {
                    throw new RestoreRowMaterializationError(
                        'missing_pit_candidates',
                        'scope record has no PIT candidates',
                        {
                            recordSysId: scopeRecord.recordSysId,
                            table: scopeRecord.table,
                        },
                    );
                }
            }
        }

        const materializedRows: RestorePlanHashRowInput[] = [];
        const pitResolutions: RestorePitResolutionRecord[] = [];
        const winners: MaterializationWinner[] = [];
        const targetRecordKeys = normalizedScopeRecords.length > 0
            ? normalizedScopeRecords.map((record) => {
                return buildReferenceKey(record.table, record.recordSysId);
            })
            : Array.from(groupedCandidates.keys()).sort((left, right) => {
                return left.localeCompare(right);
            });

        for (const recordKey of targetRecordKeys) {
            const recordCandidates = groupedCandidates.get(recordKey) || [];
            const {
                winnerCandidate,
                winnerTuple,
            } = resolvePitWinner(recordCandidates);
            winners.push({
                winnerCandidate,
                winnerTuple,
            });
        }

        const targetStateByKey = await this.targetStateLookup.lookupTargetState({
            instance_id: instanceId,
            records: winners.map((winner) => {
                return {
                    record_sys_id: winner.winnerCandidate.recordSysId,
                    table: winner.winnerCandidate.table,
                };
            }),
            source,
            tenant_id: tenantId,
        });

        for (const winner of winners) {
            const winnerCandidate = winner.winnerCandidate;
            const winnerTuple = winner.winnerTuple;
            const artifactReference = {
                artifactKey: winnerCandidate.artifactKey,
                manifestKey: winnerCandidate.manifestKey,
            };
            const artifactBody = await this.artifactBodyReader.readArtifactBody(
                artifactReference,
            );

            if (artifactBody === null) {
                throw new RestoreRowMaterializationError(
                    'missing_artifact_body',
                    'winning PIT candidate artifact body was not found',
                    {
                        artifactKey: winnerCandidate.artifactKey,
                        eventId: winnerCandidate.eventId,
                        manifestKey: winnerCandidate.manifestKey,
                    },
                );
            }

            const parsedArtifactBody = asRecord(artifactBody);

            if (!parsedArtifactBody) {
                throw new RestoreRowMaterializationError(
                    'invalid_artifact_body',
                    'artifact body root must be an object',
                    artifactReference,
                );
            }

            const row = this.buildMaterializedRow({
                artifactBody: parsedArtifactBody,
                instanceId,
                source,
                targetState: targetStateByKey.get(
                    buildTargetStateLookupKey({
                        record_sys_id: winnerCandidate.recordSysId,
                        table: winnerCandidate.table,
                    }),
                ),
                tenantId,
                winnerCandidate,
                winnerTuple,
            });

            materializedRows.push(row);
            pitResolutions.push({
                record_sys_id: winnerCandidate.recordSysId,
                row_id: row.row_id,
                table: winnerCandidate.table,
                winning_event_id: winnerCandidate.eventId,
                winning_event_time: winnerCandidate.eventTime,
                winning_sys_mod_count: winnerCandidate.sysModCount ?? undefined,
                winning_sys_updated_on: winnerTuple.sys_updated_on,
            });
        }

        const rows = [...materializedRows]
            .sort((left, right) => left.row_id.localeCompare(right.row_id));
        const orderedPitResolutions = [...pitResolutions]
            .sort((left, right) => left.row_id.localeCompare(right.row_id));

        return {
            pitResolutions: orderedPitResolutions,
            rows,
        };
    }

    private buildMaterializedRow(input: {
        artifactBody: Record<string, unknown>;
        instanceId: string;
        source: string;
        targetState?: RestoreTargetRecordState;
        tenantId: string;
        winnerCandidate: RestoreIndexIndexedEventLookupCandidate;
        winnerTuple: RestorePitRowTuple;
    }): RestorePlanHashRowInput {
        const winnerCandidate = input.winnerCandidate;
        const operation = deriveOperationFromArtifact(input.artifactBody);
        const action = this.resolveRowAction({
            operation,
            recordSysId: winnerCandidate.recordSysId,
            table: winnerCandidate.table,
            targetState: input.targetState,
        });
        const eventType = deriveEventTypeFromArtifact(
            input.artifactBody,
            operation,
        );
        const schemaVersion = deriveSchemaVersionFromArtifact(
            input.artifactBody,
        );
        const values = deriveEncryptedValuesFromArtifact(input.artifactBody);
        const rowId = buildStableRowId(
            winnerCandidate.table,
            winnerCandidate.recordSysId,
        );
        const preconditionHash = buildPreconditionHash({
            action,
            artifactKey: winnerCandidate.artifactKey,
            manifestKey: winnerCandidate.manifestKey,
            operation,
            recordSysId: winnerCandidate.recordSysId,
            rowId,
            table: winnerCandidate.table,
            values,
            winnerTuple: input.winnerTuple,
        });
        const metadata = {
            allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
            metadata: {
                __time: winnerCandidate.eventTime,
                event_id: winnerCandidate.eventId,
                event_type: eventType,
                instance_id: input.instanceId,
                offset: winnerCandidate.offset,
                operation,
                partition: winnerCandidate.partition,
                record_sys_id: winnerCandidate.recordSysId,
                schema_version: schemaVersion,
                source: input.source,
                sys_mod_count: winnerCandidate.sysModCount ?? undefined,
                sys_updated_on: input.winnerTuple.sys_updated_on,
                table: winnerCandidate.table,
                tenant_id: input.tenantId,
                topic: winnerCandidate.topic,
            },
        };

        return RestorePlanHashRowInputSchema.parse({
            action,
            metadata,
            precondition_hash: preconditionHash,
            record_sys_id: winnerCandidate.recordSysId,
            row_id: rowId,
            table: winnerCandidate.table,
            values,
        });
    }

    private resolveRowAction(input: {
        operation: 'D' | 'I' | 'U';
        recordSysId: string;
        table: string;
        targetState?: RestoreTargetRecordState;
    }): 'delete' | 'insert' | 'update' {
        if (!input.targetState) {
            return toAction(input.operation);
        }

        const reconciliation = reconcileSourceOperationWithTargetState({
            source_operation: input.operation,
            target_state: input.targetState,
        });

        if (reconciliation.decision === 'apply') {
            return reconciliation.plan_action;
        }

        throw new RestoreRowMaterializationError(
            'target_reconciliation_blocked',
            'target reconciliation blocked scope/PIT row materialization',
            {
                blocking_reason: reconciliation.blocking_reason,
                conflict_class: reconciliation.conflict_class,
                policy_case: reconciliation.policy_case,
                record_sys_id: input.recordSysId,
                source_operation: input.operation,
                table: input.table,
                target_state: input.targetState,
            },
        );
    }
}
