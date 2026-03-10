export const RESTORE_TARGET_EXISTENCE_CHECK_POLICY = {
    dry_run_plan_visibility: true,
    execute_time_revalidation: true,
} as const;

export type RestoreTargetRecordState = 'exists' | 'missing';

export type RestoreTargetRecordReference = {
    record_sys_id: string;
    table: string;
};

export type RestoreTargetStateLookupRequest = {
    instance_id: string;
    records: RestoreTargetRecordReference[];
    source: string;
    tenant_id: string;
};

export interface RestoreTargetStateLookup {
    lookupTargetState(
        input: RestoreTargetStateLookupRequest,
    ): Promise<Map<string, RestoreTargetRecordState>>;
}

export class RestoreTargetStateLookupUnavailableError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'RestoreTargetStateLookupUnavailableError';
    }
}

export type RestoreSourceOperation = 'D' | 'I' | 'U';

export type RestoreTargetActionReconciliationResult =
    | {
        decision: 'apply';
        plan_action: 'delete' | 'insert' | 'update';
        policy_case:
            | 'source_delete'
            | 'source_insert_target_exists'
            | 'source_insert_target_missing'
            | 'source_update_target_exists';
    }
    | {
        decision: 'block';
        policy_case: 'source_update_target_missing';
        conflict_class: 'missing_row_conflict';
        blocking_reason: 'target_missing_for_source_update';
    };

function normalizeKeyPart(
    value: string,
    fieldName: string,
): string {
    const normalized = String(value || '').trim();

    if (!normalized) {
        throw new Error(`${fieldName} is required for target lookup key`);
    }

    return normalized;
}

export function buildTargetStateLookupKey(
    reference: RestoreTargetRecordReference,
): string {
    const table = normalizeKeyPart(reference.table, 'table');
    const recordSysId = normalizeKeyPart(
        reference.record_sys_id,
        'record_sys_id',
    );

    return `${table}|${recordSysId}`;
}

export function reconcileSourceOperationWithTargetState(
    input: {
        source_operation: RestoreSourceOperation;
        target_state: RestoreTargetRecordState;
    },
): RestoreTargetActionReconciliationResult {
    if (input.source_operation === 'D') {
        return {
            decision: 'apply',
            plan_action: 'delete',
            policy_case: 'source_delete',
        };
    }

    if (input.source_operation === 'I') {
        if (input.target_state === 'exists') {
            return {
                decision: 'apply',
                plan_action: 'update',
                policy_case: 'source_insert_target_exists',
            };
        }

        return {
            decision: 'apply',
            plan_action: 'insert',
            policy_case: 'source_insert_target_missing',
        };
    }

    if (input.target_state === 'exists') {
        return {
            decision: 'apply',
            plan_action: 'update',
            policy_case: 'source_update_target_exists',
        };
    }

    return {
        decision: 'block',
        policy_case: 'source_update_target_missing',
        conflict_class: 'missing_row_conflict',
        blocking_reason: 'target_missing_for_source_update',
    };
}

export class InMemoryRestoreTargetStateLookup
implements RestoreTargetStateLookup {
    private readonly stateByKey = new Map<string, RestoreTargetRecordState>();

    setTargetRecordState(input: {
        record_sys_id: string;
        state: RestoreTargetRecordState;
        table: string;
    }): void {
        const key = buildTargetStateLookupKey({
            record_sys_id: input.record_sys_id,
            table: input.table,
        });

        this.stateByKey.set(key, input.state);
    }

    clear(): void {
        this.stateByKey.clear();
    }

    async lookupTargetState(
        input: RestoreTargetStateLookupRequest,
    ): Promise<Map<string, RestoreTargetRecordState>> {
        const states = new Map<string, RestoreTargetRecordState>();

        for (const reference of input.records) {
            const key = buildTargetStateLookupKey(reference);
            const state = this.stateByKey.get(key) || 'missing';

            states.set(key, state);
        }

        return states;
    }
}

export class FailClosedRestoreTargetStateLookup
implements RestoreTargetStateLookup {
    async lookupTargetState(): Promise<Map<string, RestoreTargetRecordState>> {
        throw new RestoreTargetStateLookupUnavailableError(
            'target state lookup runtime support is unavailable',
        );
    }
}

export class NoopRestoreTargetStateLookup
implements RestoreTargetStateLookup {
    async lookupTargetState(): Promise<Map<string, RestoreTargetRecordState>> {
        return new Map<string, RestoreTargetRecordState>();
    }
}
