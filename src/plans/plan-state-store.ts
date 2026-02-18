import { DatabaseSync } from 'node:sqlite';
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
    read(): RestorePlanState;
    mutate<T>(mutator: (state: RestorePlanState) => T): T;
}

export class InMemoryRestorePlanStateStore implements RestorePlanStateStore {
    private state: RestorePlanState;

    constructor(initialState?: RestorePlanState) {
        const source = initialState ?? createEmptyRestorePlanState();
        this.state = cloneRestorePlanState(source);
    }

    read(): RestorePlanState {
        return cloneRestorePlanState(this.state);
    }

    mutate<T>(mutator: (state: RestorePlanState) => T): T {
        const workingState = cloneRestorePlanState(this.state);
        const result = mutator(workingState);
        this.state = workingState;

        return result;
    }
}

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS rrs_plan_state_snapshots (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    version INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
`;

const SELECT_SNAPSHOT_SQL = `
SELECT version, state_json
FROM rrs_plan_state_snapshots
WHERE snapshot_id = 1
`;

const INSERT_SNAPSHOT_SQL = `
INSERT INTO rrs_plan_state_snapshots (
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
UPDATE rrs_plan_state_snapshots
SET version = ?, state_json = ?, updated_at = ?
WHERE snapshot_id = 1
`;

function serializeState(state: RestorePlanState): string {
    return JSON.stringify(state);
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

export class SqliteRestorePlanStateStore implements RestorePlanStateStore {
    private readonly database: DatabaseSync;

    constructor(private readonly dbPath: string) {
        this.database = new DatabaseSync(dbPath);
        this.database.exec('PRAGMA journal_mode = WAL');
        this.database.exec('PRAGMA synchronous = NORMAL');
        this.database.exec(CREATE_TABLE_SQL);
        this.ensureSnapshotRow();
    }

    read(): RestorePlanState {
        const row = this.selectSnapshot();

        if (!row) {
            return createEmptyRestorePlanState();
        }

        return parseState(row.state_json);
    }

    mutate<T>(mutator: (state: RestorePlanState) => T): T {
        this.database.exec('BEGIN IMMEDIATE');

        try {
            const row = this.selectSnapshotForTransaction();
            const currentState = row
                ? parseState(row.state_json)
                : createEmptyRestorePlanState();
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
            serializeState(createEmptyRestorePlanState()),
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

        const emptyState = createEmptyRestorePlanState();
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
