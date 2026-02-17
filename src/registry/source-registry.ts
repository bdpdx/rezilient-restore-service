import { RestoreReasonCode } from '@rezilient/types';

export interface SourceMappingInput {
    tenantId: string;
    instanceId: string;
    source: string;
}

export interface SourceMappingRecord {
    tenantId: string;
    instanceId: string;
    source: string;
    updatedAt: string;
}

export interface SourceMappingValidation {
    allowed: boolean;
    reasonCode: RestoreReasonCode;
    message: string;
}

function mappingKey(tenantId: string, instanceId: string): string {
    return `${tenantId}|${instanceId}`;
}

export class SourceRegistry {
    private readonly mappings = new Map<string, SourceMappingRecord>();

    constructor(initialMappings: SourceMappingInput[] = []) {
        for (const mapping of initialMappings) {
            this.upsert(mapping);
        }
    }

    upsert(mapping: SourceMappingInput): SourceMappingRecord {
        const now = new Date().toISOString();
        const record: SourceMappingRecord = {
            tenantId: mapping.tenantId,
            instanceId: mapping.instanceId,
            source: mapping.source,
            updatedAt: now,
        };

        this.mappings.set(
            mappingKey(mapping.tenantId, mapping.instanceId),
            record,
        );

        return record;
    }

    resolve(
        tenantId: string,
        instanceId: string,
    ): SourceMappingRecord | null {
        return this.mappings.get(mappingKey(tenantId, instanceId)) || null;
    }

    validateScope(input: {
        tenantId: string;
        instanceId: string;
        source: string;
    }): SourceMappingValidation {
        const mapping = this.resolve(input.tenantId, input.instanceId);

        if (!mapping) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'No source mapping found for tenant/instance',
            };
        }

        if (mapping.source !== input.source) {
            return {
                allowed: false,
                reasonCode: 'blocked_unknown_source_mapping',
                message: 'Source does not match canonical mapping',
            };
        }

        return {
            allowed: true,
            reasonCode: 'none',
            message: 'Scope mapping validated',
        };
    }

    list(): SourceMappingRecord[] {
        return Array.from(this.mappings.values());
    }
}
