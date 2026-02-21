import { type Pool, type PoolConfig } from 'pg';
import { PostgresSnapshotStore } from '../state/postgres-snapshot-store';
import { EvidenceExportRecord } from './models';

interface SnapshotRow {
    version: number;
    state_json: string;
}

export interface RestoreEvidenceState {
    by_job_id: Record<string, EvidenceExportRecord>;
    by_evidence_id: Record<string, EvidenceExportRecord>;
}

export function createEmptyRestoreEvidenceState(): RestoreEvidenceState {
    return {
        by_job_id: {},
        by_evidence_id: {},
    };
}

export function cloneRestoreEvidenceState(
    state: RestoreEvidenceState,
): RestoreEvidenceState {
    return JSON.parse(
        JSON.stringify(state),
    ) as RestoreEvidenceState;
}

export interface RestoreEvidenceStateStore {
    read(): Promise<RestoreEvidenceState>;
    mutate<T>(
        mutator: (state: RestoreEvidenceState) => T | Promise<T>,
    ): Promise<T>;
}

export class InMemoryRestoreEvidenceStateStore
    implements RestoreEvidenceStateStore {
    private state: RestoreEvidenceState;

    constructor(initialState?: RestoreEvidenceState) {
        const source = initialState ?? createEmptyRestoreEvidenceState();
        this.state = cloneRestoreEvidenceState(source);
    }

    async read(): Promise<RestoreEvidenceState> {
        return cloneRestoreEvidenceState(this.state);
    }

    async mutate<T>(
        mutator: (state: RestoreEvidenceState) => T | Promise<T>,
    ): Promise<T> {
        const workingState = cloneRestoreEvidenceState(this.state);
        const result = await mutator(workingState);
        this.state = workingState;

        return result;
    }
}

function parseState(stateJson: string): RestoreEvidenceState {
    const parsed = JSON.parse(stateJson) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('invalid persisted evidence state payload');
    }

    const state = parsed as Partial<RestoreEvidenceState>;

    if (!state.by_job_id || typeof state.by_job_id !== 'object') {
        throw new Error('invalid persisted evidence state by_job_id payload');
    }

    if (!state.by_evidence_id || typeof state.by_evidence_id !== 'object') {
        throw new Error(
            'invalid persisted evidence state by_evidence_id payload',
        );
    }

    return cloneRestoreEvidenceState({
        by_job_id: state.by_job_id as Record<string, EvidenceExportRecord>,
        by_evidence_id:
            state.by_evidence_id as Record<string, EvidenceExportRecord>,
    });
}

export class PostgresRestoreEvidenceStateStore
    implements RestoreEvidenceStateStore {
    private readonly snapshots: PostgresSnapshotStore<RestoreEvidenceState>;

    constructor(
        pgUrl: string,
        options: {
            pool?: Pool;
            poolConfig?: Omit<PoolConfig, 'connectionString'>;
            schemaName?: string;
            tableName?: string;
        } = {},
    ) {
        this.snapshots = new PostgresSnapshotStore(
            pgUrl,
            createEmptyRestoreEvidenceState,
            (raw: unknown) => {
                if (typeof raw === 'string') {
                    return parseState(raw);
                }

                return parseState(JSON.stringify(raw));
            },
            {
                ...options,
                storeKey: 'evidence_state',
            },
        );
    }

    async close(): Promise<void> {
        await this.snapshots.close();
    }

    async read(): Promise<RestoreEvidenceState> {
        return this.snapshots.read();
    }

    async mutate<T>(
        mutator: (state: RestoreEvidenceState) => T | Promise<T>,
    ): Promise<T> {
        return this.snapshots.mutate(mutator);
    }
}

export { PostgresRestoreEvidenceStateStore as SqliteRestoreEvidenceStateStore };
