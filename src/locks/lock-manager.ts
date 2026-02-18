import { RestoreReasonCode } from '@rezilient/types';

export type LockState = 'running' | 'queued';

export interface LockRequest {
    jobId: string;
    tenantId: string;
    instanceId: string;
    tables: string[];
}

export interface LockDecision {
    state: LockState;
    reasonCode: RestoreReasonCode;
    blockedTables: string[];
    queuePosition?: number;
}

export interface LockPromotion {
    jobId: string;
    reasonCode: RestoreReasonCode;
}

export interface ReleaseResult {
    released: boolean;
    promoted: LockPromotion[];
}

export interface RestoreLockQueueEntry {
    jobId: string;
    tenantId: string;
    instanceId: string;
    tables: string[];
    reasonCode: RestoreReasonCode;
}

export interface RestoreLockManagerState {
    running_jobs: RestoreLockQueueEntry[];
    queued_jobs: RestoreLockQueueEntry[];
}

type QueueEntry = RestoreLockQueueEntry;

function normalizeTables(tables: string[]): string[] {
    const normalized = new Set<string>();

    for (const table of tables) {
        const trimmed = table.trim();

        if (trimmed.length === 0) {
            continue;
        }

        normalized.add(trimmed);
    }

    return Array.from(normalized).sort((left, right) =>
        left.localeCompare(right),
    );
}

function lockKey(
    tenantId: string,
    instanceId: string,
    table: string,
): string {
    return `${tenantId}|${instanceId}|${table}`;
}

function normalizeQueueEntry(entry: RestoreLockQueueEntry): QueueEntry {
    const tables = normalizeTables(entry.tables);

    if (tables.length === 0) {
        throw new Error('lock state entry must include at least one table');
    }

    return {
        jobId: entry.jobId,
        tenantId: entry.tenantId,
        instanceId: entry.instanceId,
        tables,
        reasonCode: entry.reasonCode,
    };
}

export class RestoreLockManager {
    private readonly activeLocks = new Map<string, string>();

    private readonly runningJobs = new Map<string, QueueEntry>();

    private readonly queue: QueueEntry[] = [];

    constructor(initialState?: RestoreLockManagerState) {
        if (!initialState) {
            return;
        }

        this.loadState(initialState);
    }

    loadState(state: RestoreLockManagerState): void {
        this.activeLocks.clear();
        this.runningJobs.clear();
        this.queue.length = 0;

        for (const running of state.running_jobs) {
            const normalized = normalizeQueueEntry(running);

            if (this.runningJobs.has(normalized.jobId)) {
                throw new Error(
                    `duplicate running lock state for job ${normalized.jobId}`,
                );
            }

            this.runningJobs.set(normalized.jobId, normalized);

            for (const table of normalized.tables) {
                const key = lockKey(
                    normalized.tenantId,
                    normalized.instanceId,
                    table,
                );

                if (this.activeLocks.has(key)) {
                    throw new Error(
                        `duplicate running lock for ${key} in persisted state`,
                    );
                }

                this.activeLocks.set(key, normalized.jobId);
            }
        }

        for (const queued of state.queued_jobs) {
            this.queue.push(normalizeQueueEntry(queued));
        }
    }

    exportState(): RestoreLockManagerState {
        return {
            running_jobs: Array.from(this.runningJobs.values()).map((entry) => ({
                jobId: entry.jobId,
                tenantId: entry.tenantId,
                instanceId: entry.instanceId,
                tables: [...entry.tables],
                reasonCode: entry.reasonCode,
            })),
            queued_jobs: this.queue.map((entry) => ({
                jobId: entry.jobId,
                tenantId: entry.tenantId,
                instanceId: entry.instanceId,
                tables: [...entry.tables],
                reasonCode: entry.reasonCode,
            })),
        };
    }

    acquire(request: LockRequest): LockDecision {
        const tables = normalizeTables(request.tables);

        if (tables.length === 0) {
            throw new Error('lock request must include at least one table');
        }

        if (this.runningJobs.has(request.jobId)) {
            throw new Error('job already holds running locks');
        }

        const blockedTables = this.findBlockedTables(
            request.tenantId,
            request.instanceId,
            tables,
        );

        if (blockedTables.length > 0) {
            const queued: QueueEntry = {
                jobId: request.jobId,
                tenantId: request.tenantId,
                instanceId: request.instanceId,
                tables,
                reasonCode: 'queued_scope_lock',
            };

            this.queue.push(queued);

            return {
                state: 'queued',
                reasonCode: 'queued_scope_lock',
                blockedTables,
                queuePosition: this.queue.length,
            };
        }

        const running: QueueEntry = {
            jobId: request.jobId,
            tenantId: request.tenantId,
            instanceId: request.instanceId,
            tables,
            reasonCode: 'none',
        };

        this.runningJobs.set(request.jobId, running);

        for (const table of tables) {
            this.activeLocks.set(
                lockKey(request.tenantId, request.instanceId, table),
                request.jobId,
            );
        }

        return {
            state: 'running',
            reasonCode: 'none',
            blockedTables: [],
        };
    }

    release(jobId: string): ReleaseResult {
        const running = this.runningJobs.get(jobId);

        if (running) {
            for (const table of running.tables) {
                this.activeLocks.delete(
                    lockKey(running.tenantId, running.instanceId, table),
                );
            }

            this.runningJobs.delete(jobId);
        }

        this.dequeue(jobId);

        return {
            released: running !== undefined,
            promoted: this.promoteEligibleQueuedJobs(),
        };
    }

    dequeue(jobId: string): boolean {
        const index = this.queue.findIndex((entry) => entry.jobId === jobId);

        if (index === -1) {
            return false;
        }

        this.queue.splice(index, 1);

        return true;
    }

    getQueuePosition(jobId: string): number | null {
        const index = this.queue.findIndex((entry) => entry.jobId === jobId);

        if (index === -1) {
            return null;
        }

        return index + 1;
    }

    snapshot(): {
        running: Array<{ jobId: string; tables: string[] }>;
        queued: Array<{ jobId: string; tables: string[] }>;
    } {
        return {
            running: Array.from(this.runningJobs.values()).map((entry) => ({
                jobId: entry.jobId,
                tables: [...entry.tables],
            })),
            queued: this.queue.map((entry) => ({
                jobId: entry.jobId,
                tables: [...entry.tables],
            })),
        };
    }

    private findBlockedTables(
        tenantId: string,
        instanceId: string,
        tables: string[],
    ): string[] {
        const blocked: string[] = [];

        for (const table of tables) {
            const key = lockKey(tenantId, instanceId, table);

            if (this.activeLocks.has(key)) {
                blocked.push(table);
            }
        }

        return blocked;
    }

    private canRunQueued(entry: QueueEntry): boolean {
        const blockedTables = this.findBlockedTables(
            entry.tenantId,
            entry.instanceId,
            entry.tables,
        );

        return blockedTables.length === 0;
    }

    private promoteEligibleQueuedJobs(): LockPromotion[] {
        const promoted: LockPromotion[] = [];
        let promotedAny = true;

        while (promotedAny) {
            promotedAny = false;

            for (let index = 0; index < this.queue.length; index += 1) {
                const entry = this.queue[index];

                if (!this.canRunQueued(entry)) {
                    continue;
                }

                this.queue.splice(index, 1);
                index -= 1;
                promotedAny = true;

                this.runningJobs.set(entry.jobId, {
                    ...entry,
                    reasonCode: 'none',
                });

                for (const table of entry.tables) {
                    this.activeLocks.set(
                        lockKey(entry.tenantId, entry.instanceId, table),
                        entry.jobId,
                    );
                }

                promoted.push({
                    jobId: entry.jobId,
                    reasonCode: 'none',
                });
            }
        }

        return promoted;
    }
}
