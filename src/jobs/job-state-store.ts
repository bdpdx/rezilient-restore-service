import { DatabaseSync } from 'node:sqlite';
import { CrossServiceAuditEvent } from '@rezilient/types';
import { RestoreLockManagerState } from '../locks/lock-manager';
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
    read(): RestoreJobState;
    mutate<T>(mutator: (state: RestoreJobState) => T): T;
}

export class InMemoryRestoreJobStateStore implements RestoreJobStateStore {
    private state: RestoreJobState;

    constructor(initialState?: RestoreJobState) {
        const source = initialState ?? createEmptyRestoreJobState();
        this.state = cloneRestoreJobState(source);
    }

    read(): RestoreJobState {
        return cloneRestoreJobState(this.state);
    }

    mutate<T>(mutator: (state: RestoreJobState) => T): T {
        const workingState = cloneRestoreJobState(this.state);
        const result = mutator(workingState);
        this.state = workingState;

        return result;
    }
}

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS rrs_job_state_snapshots (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    version INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
`;

const SELECT_SNAPSHOT_SQL = `
SELECT version, state_json
FROM rrs_job_state_snapshots
WHERE snapshot_id = 1
`;

const INSERT_SNAPSHOT_SQL = `
INSERT INTO rrs_job_state_snapshots (
    snapshot_id,
    version,
    state_json,
    updated_at
) VALUES (
    1,
    ?,
    ?,
    ?
)
`;

const UPDATE_SNAPSHOT_SQL = `
UPDATE rrs_job_state_snapshots
SET version = ?, state_json = ?, updated_at = ?
WHERE snapshot_id = 1
`;

function serializeState(state: RestoreJobState): string {
    return JSON.stringify(state);
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

export class SqliteRestoreJobStateStore implements RestoreJobStateStore {
    private readonly database: DatabaseSync;

    constructor(private readonly dbPath: string) {
        this.database = new DatabaseSync(dbPath);
        this.database.exec('PRAGMA journal_mode = WAL');
        this.database.exec('PRAGMA synchronous = NORMAL');
        this.database.exec(CREATE_TABLE_SQL);
        this.ensureSnapshotRow();
    }

    read(): RestoreJobState {
        const row = this.selectSnapshot();

        if (!row) {
            return createEmptyRestoreJobState();
        }

        return parseState(row.state_json);
    }

    mutate<T>(mutator: (state: RestoreJobState) => T): T {
        this.database.exec('BEGIN IMMEDIATE');

        try {
            const row = this.selectSnapshotForTransaction();
            const currentState = row
                ? parseState(row.state_json)
                : createEmptyRestoreJobState();
            const result = mutator(currentState);
            const nextVersion = (row?.version ?? 0) + 1;
            const updatedAt = new Date().toISOString();
            const updateStatement = this.database.prepare(UPDATE_SNAPSHOT_SQL);

            updateStatement.run(
                nextVersion,
                serializeState(currentState),
                updatedAt,
            );
            this.database.exec('COMMIT');

            return result;
        } catch (error) {
            this.database.exec('ROLLBACK');
            throw error;
        }
    }

    private ensureSnapshotRow(): void {
        const row = this.selectSnapshot();

        if (row) {
            return;
        }

        const insert = this.database.prepare(INSERT_SNAPSHOT_SQL);

        insert.run(
            0,
            serializeState(createEmptyRestoreJobState()),
            new Date().toISOString(),
        );
    }

    private selectSnapshot(): SnapshotRow | undefined {
        const statement = this.database.prepare(SELECT_SNAPSHOT_SQL);

        return statement.get() as SnapshotRow | undefined;
    }

    private selectSnapshotForTransaction(): SnapshotRow | undefined {
        const row = this.selectSnapshot();

        if (row) {
            return row;
        }

        const emptyState = createEmptyRestoreJobState();
        const insert = this.database.prepare(INSERT_SNAPSHOT_SQL);

        insert.run(
            0,
            serializeState(emptyState),
            new Date().toISOString(),
        );

        return {
            version: 0,
            state_json: serializeState(emptyState),
        };
    }
}
