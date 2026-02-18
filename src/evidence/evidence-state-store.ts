import { DatabaseSync } from 'node:sqlite';
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
    read(): RestoreEvidenceState;
    mutate<T>(mutator: (state: RestoreEvidenceState) => T): T;
}

export class InMemoryRestoreEvidenceStateStore
    implements RestoreEvidenceStateStore {
    private state: RestoreEvidenceState;

    constructor(initialState?: RestoreEvidenceState) {
        const source = initialState ?? createEmptyRestoreEvidenceState();
        this.state = cloneRestoreEvidenceState(source);
    }

    read(): RestoreEvidenceState {
        return cloneRestoreEvidenceState(this.state);
    }

    mutate<T>(mutator: (state: RestoreEvidenceState) => T): T {
        const workingState = cloneRestoreEvidenceState(this.state);
        const result = mutator(workingState);
        this.state = workingState;

        return result;
    }
}

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS rrs_evidence_state_snapshots (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    version INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
`;

const SELECT_SNAPSHOT_SQL = `
SELECT version, state_json
FROM rrs_evidence_state_snapshots
WHERE snapshot_id = 1
`;

const INSERT_SNAPSHOT_SQL = `
INSERT INTO rrs_evidence_state_snapshots (
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
UPDATE rrs_evidence_state_snapshots
SET version = ?, state_json = ?, updated_at = ?
WHERE snapshot_id = 1
`;

function serializeState(state: RestoreEvidenceState): string {
    return JSON.stringify(state);
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

export class SqliteRestoreEvidenceStateStore
    implements RestoreEvidenceStateStore {
    private readonly database: DatabaseSync;

    constructor(private readonly dbPath: string) {
        this.database = new DatabaseSync(dbPath);
        this.database.exec('PRAGMA journal_mode = WAL');
        this.database.exec('PRAGMA synchronous = NORMAL');
        this.database.exec(CREATE_TABLE_SQL);
        this.ensureSnapshotRow();
    }

    read(): RestoreEvidenceState {
        const row = this.selectSnapshot();

        if (!row) {
            return createEmptyRestoreEvidenceState();
        }

        return parseState(row.state_json);
    }

    mutate<T>(mutator: (state: RestoreEvidenceState) => T): T {
        this.database.exec('BEGIN IMMEDIATE');

        try {
            const row = this.selectSnapshotForTransaction();
            const currentState = row
                ? parseState(row.state_json)
                : createEmptyRestoreEvidenceState();
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
            serializeState(createEmptyRestoreEvidenceState()),
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

        const emptyState = createEmptyRestoreEvidenceState();
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
