import { DatabaseSync } from 'node:sqlite';
import type { RestoreJournalEntry } from '@rezilient/types';
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
    read(): RestoreExecutionState;
    mutate<T>(mutator: (state: RestoreExecutionState) => T): T;
}

export class InMemoryRestoreExecutionStateStore
    implements RestoreExecutionStateStore {
    private state: RestoreExecutionState;

    constructor(initialState?: RestoreExecutionState) {
        const source = initialState ?? createEmptyRestoreExecutionState();
        this.state = cloneRestoreExecutionState(source);
    }

    read(): RestoreExecutionState {
        return cloneRestoreExecutionState(this.state);
    }

    mutate<T>(mutator: (state: RestoreExecutionState) => T): T {
        const workingState = cloneRestoreExecutionState(this.state);
        const result = mutator(workingState);
        this.state = workingState;

        return result;
    }
}

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS rrs_execution_state_snapshots (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    version INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
`;

const SELECT_SNAPSHOT_SQL = `
SELECT version, state_json
FROM rrs_execution_state_snapshots
WHERE snapshot_id = 1
`;

const INSERT_SNAPSHOT_SQL = `
INSERT INTO rrs_execution_state_snapshots (
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
UPDATE rrs_execution_state_snapshots
SET version = ?, state_json = ?, updated_at = ?
WHERE snapshot_id = 1
`;

function serializeState(state: RestoreExecutionState): string {
    return JSON.stringify(state);
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

export class SqliteRestoreExecutionStateStore
    implements RestoreExecutionStateStore {
    private readonly database: DatabaseSync;

    constructor(private readonly dbPath: string) {
        this.database = new DatabaseSync(dbPath);
        this.database.exec('PRAGMA journal_mode = WAL');
        this.database.exec('PRAGMA synchronous = NORMAL');
        this.database.exec(CREATE_TABLE_SQL);
        this.ensureSnapshotRow();
    }

    read(): RestoreExecutionState {
        const row = this.selectSnapshot();

        if (!row) {
            return createEmptyRestoreExecutionState();
        }

        return parseState(row.state_json);
    }

    mutate<T>(mutator: (state: RestoreExecutionState) => T): T {
        this.database.exec('BEGIN IMMEDIATE');

        try {
            const row = this.selectSnapshotForTransaction();
            const currentState = row
                ? parseState(row.state_json)
                : createEmptyRestoreExecutionState();
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
            serializeState(createEmptyRestoreExecutionState()),
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

        const emptyState = createEmptyRestoreExecutionState();
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
