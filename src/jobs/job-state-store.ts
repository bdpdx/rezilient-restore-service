import { type Pool, type PoolConfig } from 'pg';
import { CrossServiceAuditEvent } from '@rezilient/types';
import { RestoreLockManagerState } from '../locks/lock-manager';
import { PostgresSnapshotStore } from '../state/postgres-snapshot-store';
import {
    RestoreJobAuditEvent,
    RestoreJobRecord,
    RestorePlanMetadataRecord,
} from './models';

interface SnapshotRow {
    version: number;
    state_json: string;
}

export interface RestoreJobState {
    plans_by_id: Record<string, RestorePlanMetadataRecord>;
    jobs_by_id: Record<string, RestoreJobRecord>;
    events_by_job_id: Record<string, RestoreJobAuditEvent[]>;
    cross_service_events_by_job_id: Record<string, CrossServiceAuditEvent[]>;
    lock_state: RestoreLockManagerState;
}

export function createEmptyRestoreJobState(): RestoreJobState {
    return {
        plans_by_id: {},
        jobs_by_id: {},
        events_by_job_id: {},
        cross_service_events_by_job_id: {},
        lock_state: {
            running_jobs: [],
            queued_jobs: [],
        },
    };
}

export function cloneRestoreJobState(
    state: RestoreJobState,
): RestoreJobState {
    return JSON.parse(
        JSON.stringify(state),
    ) as RestoreJobState;
}

export interface RestoreJobStateStore {
    read(): Promise<RestoreJobState>;
    mutate<T>(mutator: (state: RestoreJobState) => T | Promise<T>): Promise<T>;
}

export class InMemoryRestoreJobStateStore implements RestoreJobStateStore {
    private state: RestoreJobState;

    constructor(initialState?: RestoreJobState) {
        const source = initialState ?? createEmptyRestoreJobState();
        this.state = cloneRestoreJobState(source);
    }

    async read(): Promise<RestoreJobState> {
        return cloneRestoreJobState(this.state);
    }

    async mutate<T>(
        mutator: (state: RestoreJobState) => T | Promise<T>,
    ): Promise<T> {
        const workingState = cloneRestoreJobState(this.state);
        const result = await mutator(workingState);
        this.state = workingState;

        return result;
    }
}

function parseState(stateJson: string): RestoreJobState {
    const parsed = JSON.parse(stateJson) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('invalid persisted job state payload');
    }

    const state = parsed as Partial<RestoreJobState>;

    if (!state.plans_by_id || typeof state.plans_by_id !== 'object') {
        throw new Error('invalid persisted job state plans_by_id payload');
    }

    if (!state.jobs_by_id || typeof state.jobs_by_id !== 'object') {
        throw new Error('invalid persisted job state jobs_by_id payload');
    }

    if (!state.events_by_job_id || typeof state.events_by_job_id !== 'object') {
        throw new Error('invalid persisted job state events_by_job_id payload');
    }

    const crossServiceEventsByJobId = state.cross_service_events_by_job_id;

    if (
        crossServiceEventsByJobId !== undefined &&
        typeof crossServiceEventsByJobId !== 'object'
    ) {
        throw new Error(
            'invalid persisted job state cross_service_events_by_job_id payload',
        );
    }

    const lockState = state.lock_state;

    if (!lockState || typeof lockState !== 'object') {
        throw new Error('invalid persisted job state lock_state payload');
    }

    if (!Array.isArray(lockState.running_jobs)) {
        throw new Error('invalid persisted job state running_jobs payload');
    }

    if (!Array.isArray(lockState.queued_jobs)) {
        throw new Error('invalid persisted job state queued_jobs payload');
    }

    return cloneRestoreJobState({
        plans_by_id: state.plans_by_id as Record<string, RestorePlanMetadataRecord>,
        jobs_by_id: state.jobs_by_id as Record<string, RestoreJobRecord>,
        events_by_job_id:
            state.events_by_job_id as Record<string, RestoreJobAuditEvent[]>,
        cross_service_events_by_job_id:
            (crossServiceEventsByJobId ||
                {}) as Record<string, CrossServiceAuditEvent[]>,
        lock_state: {
            running_jobs: lockState.running_jobs,
            queued_jobs: lockState.queued_jobs,
        },
    });
}

export class PostgresRestoreJobStateStore implements RestoreJobStateStore {
    private readonly snapshots: PostgresSnapshotStore<RestoreJobState>;

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
            createEmptyRestoreJobState,
            (raw: unknown) => {
                if (typeof raw === 'string') {
                    return parseState(raw);
                }

                return parseState(JSON.stringify(raw));
            },
            {
                ...options,
                storeKey: 'job_state',
            },
        );
    }

    async close(): Promise<void> {
        await this.snapshots.close();
    }

    async read(): Promise<RestoreJobState> {
        return this.snapshots.read();
    }

    async mutate<T>(
        mutator: (state: RestoreJobState) => T | Promise<T>,
    ): Promise<T> {
        return this.snapshots.mutate(mutator);
    }
}

export { PostgresRestoreJobStateStore as SqliteRestoreJobStateStore };
