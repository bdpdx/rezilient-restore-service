import { Pool, type PoolClient, type PoolConfig } from 'pg';

interface SnapshotRow {
    version: number;
    state_json: unknown;
}

export interface PostgresSnapshotStoreOptions {
    pool?: Pool;
    poolConfig?: Omit<PoolConfig, 'connectionString'>;
    schemaName?: string;
    tableName?: string;
    storeKey: string;
}

const DEFAULT_SCHEMA_NAME = 'rez_restore_index';
const DEFAULT_TABLE_NAME = 'rrs_state_snapshots';

function cloneValue<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

function parseVersion(raw: number | string): number {
    if (typeof raw === 'number') {
        return raw;
    }

    const parsed = Number(raw);

    if (!Number.isFinite(parsed)) {
        throw new Error('invalid snapshot version');
    }

    return parsed;
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

function validateStoreKey(raw: string): string {
    const trimmed = String(raw || '').trim();

    if (trimmed.length === 0) {
        throw new Error('snapshot store key is required');
    }

    return trimmed;
}

export class PostgresSnapshotStore<T> {
    private readonly ownsPool: boolean;

    private readonly pool: Pool;

    private readonly ready: Promise<void>;

    private readonly storeKey: string;

    private readonly tableQualified: string;

    private state: T;

    constructor(
        pgUrl: string,
        private readonly createEmptyState: () => T,
        private readonly parseState: (raw: unknown) => T,
        options: PostgresSnapshotStoreOptions,
    ) {
        const connectionString = String(pgUrl || '').trim();
        const schemaName = validateSqlIdentifier(
            options.schemaName || DEFAULT_SCHEMA_NAME,
            'snapshot schema name',
        );
        const tableName = validateSqlIdentifier(
            options.tableName || DEFAULT_TABLE_NAME,
            'snapshot table name',
        );

        this.storeKey = validateStoreKey(options.storeKey);
        this.tableQualified = `"${schemaName}"."${tableName}"`;
        this.state = cloneValue(createEmptyState());

        if (options.pool) {
            this.pool = options.pool;
            this.ownsPool = false;
        } else {
            if (connectionString.length === 0) {
                throw new Error('REZ_RESTORE_PG_URL is required');
            }

            this.pool = new Pool({
                allowExitOnIdle: true,
                connectionString,
                idleTimeoutMillis:
                    options.poolConfig?.idleTimeoutMillis || 30000,
                max: options.poolConfig?.max || 10,
                ...options.poolConfig,
            });
            this.ownsPool = true;
        }

        this.ready = this.initialize();
    }

    async close(): Promise<void> {
        if (!this.ownsPool) {
            return;
        }

        await this.pool.end();
    }

    async read(): Promise<T> {
        await this.ready;

        return cloneValue(this.state);
    }

    async mutate<R>(
        mutator: (state: T) => R | Promise<R>,
    ): Promise<R> {
        await this.ready;

        const client = await this.pool.connect();

        try {
            await client.query('BEGIN');

            const row = await this.selectSnapshotForTransaction(client);
            const workingState = this.parseState(row.state_json);
            const result = await mutator(workingState);
            const nextVersion = row.version + 1;

            await client.query(
                `UPDATE ${this.tableQualified}
                SET version = $1::bigint,
                    state_json = $2::jsonb,
                    updated_at = now()
                WHERE store_key = $3`,
                [
                    nextVersion,
                    JSON.stringify(workingState),
                    this.storeKey,
                ],
            );
            await client.query('COMMIT');

            this.state = cloneValue(workingState);

            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    private async initialize(): Promise<void> {
        await this.pool.query(this.createTableSql());

        await this.pool.query(
            `INSERT INTO ${this.tableQualified} (
                store_key,
                version,
                state_json,
                updated_at
            )
            SELECT
                $1,
                0,
                $2::jsonb,
                now()
            WHERE NOT EXISTS (
                SELECT 1
                FROM ${this.tableQualified}
                WHERE store_key = $1
            )`,
            [
                this.storeKey,
                JSON.stringify(this.createEmptyState()),
            ],
        );

        const row = await this.selectSnapshot();

        if (!row) {
            throw new Error('failed to load persisted restore snapshot state');
        }

        this.state = this.parseState(row.state_json);
    }

    private async selectSnapshot(): Promise<SnapshotRow | null> {
        const result = await this.pool.query<SnapshotRow>(
            `SELECT version, state_json
            FROM ${this.tableQualified}
            WHERE store_key = $1
            ORDER BY version DESC
            LIMIT 1`,
            [
                this.storeKey,
            ],
        );

        if (result.rowCount !== 1) {
            return null;
        }

        return {
            version: parseVersion(result.rows[0].version),
            state_json: result.rows[0].state_json,
        };
    }

    private async selectSnapshotForTransaction(
        client: PoolClient,
    ): Promise<SnapshotRow> {
        const result = await client.query<SnapshotRow>(
            `SELECT version, state_json
            FROM ${this.tableQualified}
            WHERE store_key = $1
            ORDER BY version DESC
            LIMIT 1
            FOR UPDATE`,
            [
                this.storeKey,
            ],
        );

        if (result.rowCount === 1) {
            return {
                version: parseVersion(result.rows[0].version),
                state_json: result.rows[0].state_json,
            };
        }

        const emptyState = this.createEmptyState();

        await client.query(
            `INSERT INTO ${this.tableQualified} (
                store_key,
                version,
                state_json,
                updated_at
            ) VALUES (
                $1,
                0,
                $2::jsonb,
                now()
            )`,
            [
                this.storeKey,
                JSON.stringify(emptyState),
            ],
        );

        return {
            version: 0,
            state_json: emptyState,
        };
    }

    private createTableSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.tableQualified} (
    store_key TEXT,
    version BIGINT,
    state_json JSONB,
    updated_at TIMESTAMPTZ
)
`;
    }
}
