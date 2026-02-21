import { type Pool, type PoolConfig } from 'pg';
import type { RestoreJournalEntry } from '@rezilient/types';
import { PostgresSnapshotStore } from '../state/postgres-snapshot-store';
import {
    RestoreExecutionRecord,
    RestoreJournalMirrorRecord,
} from './models';

interface SnapshotRow {
    version: number;
    state_json: string;
}

export interface RestoreExecutionState {
    records_by_job_id: Record<string, RestoreExecutionRecord>;
    rollback_journal_by_job_id: Record<string, RestoreJournalEntry[]>;
    sn_mirror_by_job_id: Record<string, RestoreJournalMirrorRecord[]>;
}

export function createEmptyRestoreExecutionState(): RestoreExecutionState {
    return {
        records_by_job_id: {},
        rollback_journal_by_job_id: {},
        sn_mirror_by_job_id: {},
    };
}

export function cloneRestoreExecutionState(
    state: RestoreExecutionState,
): RestoreExecutionState {
    return JSON.parse(
        JSON.stringify(state),
    ) as RestoreExecutionState;
}

export interface RestoreExecutionStateStore {
    read(): Promise<RestoreExecutionState>;
    mutate<T>(
        mutator: (state: RestoreExecutionState) => T | Promise<T>,
    ): Promise<T>;
}

export class InMemoryRestoreExecutionStateStore
    implements RestoreExecutionStateStore {
    private state: RestoreExecutionState;

    constructor(initialState?: RestoreExecutionState) {
        const source = initialState ?? createEmptyRestoreExecutionState();
        this.state = cloneRestoreExecutionState(source);
    }

    async read(): Promise<RestoreExecutionState> {
        return cloneRestoreExecutionState(this.state);
    }

    async mutate<T>(
        mutator: (state: RestoreExecutionState) => T | Promise<T>,
    ): Promise<T> {
        const workingState = cloneRestoreExecutionState(this.state);
        const result = await mutator(workingState);
        this.state = workingState;

        return result;
    }
}

function parseState(stateJson: string): RestoreExecutionState {
    const parsed = JSON.parse(stateJson) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('invalid persisted execution state payload');
    }

    const state = parsed as Partial<RestoreExecutionState>;

    if (
        !state.records_by_job_id ||
        typeof state.records_by_job_id !== 'object'
    ) {
        throw new Error(
            'invalid persisted execution state records_by_job_id payload',
        );
    }

    if (
        !state.rollback_journal_by_job_id ||
        typeof state.rollback_journal_by_job_id !== 'object'
    ) {
        throw new Error(
            'invalid persisted execution state rollback_journal payload',
        );
    }

    if (
        !state.sn_mirror_by_job_id ||
        typeof state.sn_mirror_by_job_id !== 'object'
    ) {
        throw new Error(
            'invalid persisted execution state sn_mirror payload',
        );
    }

    return cloneRestoreExecutionState({
        records_by_job_id:
            state.records_by_job_id as Record<string, RestoreExecutionRecord>,
        rollback_journal_by_job_id:
            state.rollback_journal_by_job_id as Record<
                string,
                RestoreJournalEntry[]
            >,
        sn_mirror_by_job_id:
            state.sn_mirror_by_job_id as Record<
                string,
                RestoreJournalMirrorRecord[]
            >,
    });
}

export class PostgresRestoreExecutionStateStore
    implements RestoreExecutionStateStore {
    private readonly snapshots: PostgresSnapshotStore<RestoreExecutionState>;

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
            createEmptyRestoreExecutionState,
            (raw: unknown) => {
                if (typeof raw === 'string') {
                    return parseState(raw);
                }

                return parseState(JSON.stringify(raw));
            },
            {
                ...options,
                storeKey: 'execution_state',
            },
        );
    }

    async close(): Promise<void> {
        await this.snapshots.close();
    }

    async read(): Promise<RestoreExecutionState> {
        return this.snapshots.read();
    }

    async mutate<T>(
        mutator: (state: RestoreExecutionState) => T | Promise<T>,
    ): Promise<T> {
        return this.snapshots.mutate(mutator);
    }
}

export { PostgresRestoreExecutionStateStore as SqliteRestoreExecutionStateStore };
