import {
    AcpListSourceMappingsResult,
    AcpResolveSourceMappingFound,
    AcpResolveSourceMappingNotFound,
    AcpResolveSourceMappingResult,
    AcpResolvedSourceMapping,
    AcpSourceMappingClient,
    AcpSourceMappingOutage,
    ListSourceMappingsInput,
    ResolveSourceMappingInput,
} from './acp-source-mapping-client';

export interface CachedAcpSourceMappingProviderConfig {
    positiveTtlSeconds: number;
    negativeTtlSeconds: number;
    now?: () => Date;
}

export interface AcpSourceMappingProviderClient {
    resolveSourceMapping(
        input: ResolveSourceMappingInput,
    ): Promise<AcpResolveSourceMappingResult>;
    listSourceMappings(
        input?: ListSourceMappingsInput,
    ): Promise<AcpListSourceMappingsResult>;
}

export type CachedAcpResolveSourceMappingResult =
    | (AcpResolveSourceMappingFound & {
        cacheHit: boolean;
    })
    | (AcpResolveSourceMappingNotFound & {
        cacheHit: boolean;
    })
    | (AcpSourceMappingOutage & {
        cacheHit: false;
    });

type CacheableResolveResult =
    | AcpResolveSourceMappingFound
    | AcpResolveSourceMappingNotFound;

interface ResolveCacheEntry {
    expiresAtMs: number;
    result: CacheableResolveResult;
}

function parseNonNegativeInteger(
    value: number,
    fieldName: string,
): number {
    if (!Number.isInteger(value) || value < 0) {
        throw new Error(`${fieldName} must be a non-negative integer`);
    }

    return value;
}

function cloneResolvedMapping(
    mapping: AcpResolvedSourceMapping,
): AcpResolvedSourceMapping {
    return {
        ...mapping,
        allowedServices: [...mapping.allowedServices],
    };
}

function cloneResolveResult(
    result: CacheableResolveResult,
): CacheableResolveResult {
    if (result.status === 'found') {
        return {
            status: 'found',
            mapping: cloneResolvedMapping(result.mapping),
        };
    }

    return {
        status: 'not_found',
    };
}

function resolveCacheKey(input: ResolveSourceMappingInput): string {
    const serviceScope = input.serviceScope || 'rrs';

    return `${input.tenantId}|${input.instanceId}|${serviceScope}`;
}

export class CachedAcpSourceMappingProvider {
    private readonly resolveCache = new Map<string, ResolveCacheEntry>();

    private readonly positiveTtlMs: number;

    private readonly negativeTtlMs: number;

    private readonly now: () => Date;

    constructor(
        private readonly client: AcpSourceMappingProviderClient,
        config: CachedAcpSourceMappingProviderConfig,
    ) {
        this.positiveTtlMs = parseNonNegativeInteger(
            config.positiveTtlSeconds,
            'positiveTtlSeconds',
        ) * 1000;
        this.negativeTtlMs = parseNonNegativeInteger(
            config.negativeTtlSeconds,
            'negativeTtlSeconds',
        ) * 1000;
        this.now = config.now || (() => new Date());
    }

    async resolveSourceMapping(
        input: ResolveSourceMappingInput,
    ): Promise<CachedAcpResolveSourceMappingResult> {
        const key = resolveCacheKey(input);
        const nowMs = this.now().getTime();
        const cached = this.resolveCache.get(key);

        if (cached && cached.expiresAtMs > nowMs) {
            return {
                ...cloneResolveResult(cached.result),
                cacheHit: true,
            };
        }

        if (cached) {
            this.resolveCache.delete(key);
        }

        const result = await this.client.resolveSourceMapping(input);

        if (result.status === 'outage') {
            return {
                ...result,
                cacheHit: false,
            };
        }

        const ttlMs = result.status === 'found'
            ? this.positiveTtlMs
            : this.negativeTtlMs;

        if (ttlMs > 0) {
            this.resolveCache.set(key, {
                expiresAtMs: nowMs + ttlMs,
                result: cloneResolveResult(result),
            });
        }

        return {
            ...cloneResolveResult(result),
            cacheHit: false,
        };
    }

    async listSourceMappings(
        input: ListSourceMappingsInput = {},
    ): Promise<AcpListSourceMappingsResult> {
        return this.client.listSourceMappings(input);
    }

    clearResolveCache(): void {
        this.resolveCache.clear();
    }

    getResolveCacheSize(): number {
        return this.resolveCache.size;
    }
}

export function createCachedAcpSourceMappingProvider(
    client: AcpSourceMappingClient,
    config: CachedAcpSourceMappingProviderConfig,
): CachedAcpSourceMappingProvider {
    return new CachedAcpSourceMappingProvider(client, config);
}
