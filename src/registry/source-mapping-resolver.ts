import {
    AcpResolveSourceMappingResult,
    ResolveSourceMappingInput,
} from './acp-source-mapping-client';
import { SourceRegistry } from './source-registry';

export interface SourceMappingResolver {
    resolveSourceMapping(
        input: ResolveSourceMappingInput,
    ): Promise<AcpResolveSourceMappingResult>;
}

export function createSourceRegistryBackedResolver(
    sourceRegistry: SourceRegistry,
): SourceMappingResolver {
    return {
        async resolveSourceMapping(
            input: ResolveSourceMappingInput,
        ): Promise<AcpResolveSourceMappingResult> {
            const mapping = sourceRegistry.resolve(
                input.tenantId,
                input.instanceId,
            );

            if (!mapping) {
                return {
                    status: 'not_found',
                };
            }

            const requestedServiceScope = input.serviceScope || 'rrs';
            const allowedServices = ['rrs'];

            return {
                status: 'found',
                mapping: {
                    tenantId: mapping.tenantId,
                    instanceId: mapping.instanceId,
                    source: mapping.source,
                    tenantState: 'active',
                    entitlementState: 'active',
                    instanceState: 'active',
                    allowedServices,
                    updatedAt: mapping.updatedAt,
                    requestedServiceScope,
                    serviceAllowed: allowedServices.includes(
                        requestedServiceScope,
                    ),
                },
            };
        },
    };
}
