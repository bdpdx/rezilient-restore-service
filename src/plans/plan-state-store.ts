import { type Pool, type PoolConfig } from 'pg';
import { PostgresSnapshotStore } from '../state/postgres-snapshot-store';
import { RestoreDryRunPlanRecord } from './models';

interface SnapshotRow {
    version: number;
    state_json: string;
}

export interface RestorePlanState {
    plans_by_id: Record<string, RestoreDryRunPlanRecord>;
}

export function createEmptyRestorePlanState(): RestorePlanState {
    return {
        plans_by_id: {},
    };
}

export function cloneRestorePlanState(
    state: RestorePlanState,
): RestorePlanState {
    return JSON.parse(
        JSON.stringify(state),
    ) as RestorePlanState;
}

export interface RestorePlanStateStore {
    read(): Promise<RestorePlanState>;
    mutate<T>(mutator: (state: RestorePlanState) => T | Promise<T>): Promise<T>;
}

export class InMemoryRestorePlanStateStore implements RestorePlanStateStore {
    private state: RestorePlanState;

    constructor(initialState?: RestorePlanState) {
        const source = initialState ?? createEmptyRestorePlanState();
        this.state = cloneRestorePlanState(source);
    }

    async read(): Promise<RestorePlanState> {
        return cloneRestorePlanState(this.state);
    }

    async mutate<T>(
        mutator: (state: RestorePlanState) => T | Promise<T>,
    ): Promise<T> {
        const workingState = cloneRestorePlanState(this.state);
        const result = await mutator(workingState);
        this.state = workingState;

        return result;
    }
}

function parseState(stateJson: string): RestorePlanState {
    const parsed = JSON.parse(stateJson) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('invalid persisted plan state payload');
    }

    const state = parsed as Partial<RestorePlanState>;

    if (!state.plans_by_id || typeof state.plans_by_id !== 'object') {
        throw new Error('invalid persisted plan state plans_by_id payload');
    }

    return cloneRestorePlanState({
        plans_by_id: state.plans_by_id as Record<string, RestoreDryRunPlanRecord>,
    });
}

export class PostgresRestorePlanStateStore implements RestorePlanStateStore {
    private readonly snapshots: PostgresSnapshotStore<RestorePlanState>;

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
            createEmptyRestorePlanState,
            (raw: unknown) => {
                if (typeof raw === 'string') {
                    return parseState(raw);
                }

                return parseState(JSON.stringify(raw));
            },
            {
                ...options,
                storeKey: 'plan_state',
            },
        );
    }

    async close(): Promise<void> {
        await this.snapshots.close();
    }

    async read(): Promise<RestorePlanState> {
        return this.snapshots.read();
    }

    async mutate<T>(
        mutator: (state: RestorePlanState) => T | Promise<T>,
    ): Promise<T> {
        return this.snapshots.mutate(mutator);
    }
}

export { PostgresRestorePlanStateStore as SqliteRestorePlanStateStore };
