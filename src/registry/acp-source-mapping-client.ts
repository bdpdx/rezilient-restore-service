export type AcpServiceScope = 'reg' | 'rrs';

export type AcpState = 'active' | 'suspended' | 'disabled';

export interface AcpSourceMappingRecord {
    tenantId: string;
    instanceId: string;
    source: string;
    tenantState: AcpState;
    entitlementState: AcpState;
    instanceState: AcpState;
    allowedServices: string[];
    updatedAt: string;
}

export interface AcpResolvedSourceMapping extends AcpSourceMappingRecord {
    requestedServiceScope: AcpServiceScope;
    serviceAllowed: boolean;
}

export interface ResolveSourceMappingInput {
    tenantId: string;
    instanceId: string;
    serviceScope?: AcpServiceScope;
}

export interface ListSourceMappingsInput {
    tenantId?: string;
    instanceId?: string;
    serviceScope?: AcpServiceScope;
    tenantState?: AcpState;
    entitlementState?: AcpState;
    instanceState?: AcpState;
}

export interface AcpResolveSourceMappingFound {
    status: 'found';
    mapping: AcpResolvedSourceMapping;
}

export interface AcpResolveSourceMappingNotFound {
    status: 'not_found';
}

export interface AcpSourceMappingOutage {
    status: 'outage';
    message: string;
    statusCode?: number;
}

export type AcpResolveSourceMappingResult =
    | AcpResolveSourceMappingFound
    | AcpResolveSourceMappingNotFound
    | AcpSourceMappingOutage;

export interface AcpListSourceMappingsOk {
    status: 'ok';
    mappings: AcpSourceMappingRecord[];
}

export type AcpListSourceMappingsResult =
    | AcpListSourceMappingsOk
    | AcpSourceMappingOutage;

type FetchLike = (
    input: string,
    init?: RequestInit,
) => Promise<Response>;

export interface AcpSourceMappingClientConfig {
    baseUrl: string;
    internalToken: string;
    timeoutMs: number;
    defaultServiceScope?: AcpServiceScope;
    fetchImpl?: FetchLike;
}

function trimTrailingSlash(value: string): string {
    while (value.endsWith('/')) {
        value = value.slice(0, -1);
    }

    return value;
}

function parseRequiredString(value: unknown, fieldName: string): string {
    if (typeof value !== 'string') {
        throw new Error(`${fieldName} is required`);
    }

    const trimmed = value.trim();

    if (!trimmed) {
        throw new Error(`${fieldName} is required`);
    }

    return trimmed;
}

function parseServiceScope(
    value: unknown,
): AcpServiceScope | undefined {
    if (value !== 'reg' && value !== 'rrs') {
        return undefined;
    }

    return value;
}

function parseState(value: unknown): AcpState | undefined {
    if (
        value !== 'active' &&
        value !== 'suspended' &&
        value !== 'disabled'
    ) {
        return undefined;
    }

    return value;
}

function parseStringArray(value: unknown): string[] | undefined {
    if (!Array.isArray(value)) {
        return undefined;
    }

    const parsed: string[] = [];

    for (const item of value) {
        if (typeof item !== 'string' || item.trim() === '') {
            return undefined;
        }

        parsed.push(item);
    }

    return parsed;
}

function asObject(value: unknown): Record<string, unknown> | undefined {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return undefined;
    }

    return value as Record<string, unknown>;
}

function parseSourceMappingRecord(
    value: unknown,
): AcpSourceMappingRecord | undefined {
    const record = asObject(value);

    if (!record) {
        return undefined;
    }

    const tenantId = record.tenant_id;
    const instanceId = record.instance_id;
    const source = record.source;
    const tenantState = parseState(record.tenant_state);
    const entitlementState = parseState(record.entitlement_state);
    const instanceState = parseState(record.instance_state);
    const allowedServices = parseStringArray(record.allowed_services);
    const updatedAt = record.updated_at;

    if (typeof tenantId !== 'string' || tenantId.trim() === '') {
        return undefined;
    }

    if (typeof instanceId !== 'string' || instanceId.trim() === '') {
        return undefined;
    }

    if (typeof source !== 'string' || source.trim() === '') {
        return undefined;
    }

    if (!tenantState || !entitlementState || !instanceState) {
        return undefined;
    }

    if (!allowedServices) {
        return undefined;
    }

    if (typeof updatedAt !== 'string' || updatedAt.trim() === '') {
        return undefined;
    }

    return {
        tenantId,
        instanceId,
        source,
        tenantState,
        entitlementState,
        instanceState,
        allowedServices,
        updatedAt,
    };
}

function buildTimeoutOutage(
    timeoutMs: number,
): AcpSourceMappingOutage {
    return {
        status: 'outage',
        message: `ACP request timed out after ${timeoutMs}ms`,
    };
}

function buildTransportOutage(
    error: unknown,
): AcpSourceMappingOutage {
    if (error instanceof Error && error.message.trim() !== '') {
        return {
            status: 'outage',
            message: `ACP request failed: ${error.message}`,
        };
    }

    return {
        status: 'outage',
        message: 'ACP request failed',
    };
}

function buildHttpOutage(
    path: string,
    statusCode: number,
    body: Record<string, unknown> | undefined,
): AcpSourceMappingOutage {
    const reasonCode = body?.reason_code;
    const reasonCodeSuffix = typeof reasonCode === 'string'
        ? ` (${reasonCode})`
        : '';
    const message = statusCode === 403 &&
            reasonCode === 'internal_auth_required'
        ? 'ACP internal authentication failed'
        : `ACP request to ${path} failed with status `
            + `${statusCode}${reasonCodeSuffix}`;

    return {
        status: 'outage',
        message,
        statusCode,
    };
}

async function readResponseBody(
    response: Response,
): Promise<Record<string, unknown> | undefined> {
    const text = await response.text();

    if (text.trim() === '') {
        return {};
    }

    let parsed: unknown;

    try {
        parsed = JSON.parse(text);
    } catch {
        return undefined;
    }

    return asObject(parsed);
}

export class AcpSourceMappingClient {
    private readonly baseUrl: string;

    private readonly internalToken: string;

    private readonly timeoutMs: number;

    private readonly defaultServiceScope: AcpServiceScope;

    private readonly fetchImpl: FetchLike;

    constructor(config: AcpSourceMappingClientConfig) {
        const baseUrl = parseRequiredString(config.baseUrl, 'baseUrl');
        const internalToken = parseRequiredString(
            config.internalToken,
            'internalToken',
        );

        let parsedBaseUrl: URL;

        try {
            parsedBaseUrl = new URL(baseUrl);
        } catch {
            throw new Error('baseUrl must be a valid URL');
        }

        if (
            !Number.isInteger(config.timeoutMs) ||
            config.timeoutMs <= 0
        ) {
            throw new Error('timeoutMs must be a positive integer');
        }

        const defaultServiceScope = config.defaultServiceScope || 'rrs';

        if (defaultServiceScope !== 'reg' && defaultServiceScope !== 'rrs') {
            throw new Error(
                'defaultServiceScope must be one of: reg, rrs',
            );
        }

        this.baseUrl = trimTrailingSlash(parsedBaseUrl.toString());
        this.internalToken = internalToken;
        this.timeoutMs = config.timeoutMs;
        this.defaultServiceScope = defaultServiceScope;
        this.fetchImpl = config.fetchImpl || fetch;
    }

    async resolveSourceMapping(
        input: ResolveSourceMappingInput,
    ): Promise<AcpResolveSourceMappingResult> {
        const tenantId = parseRequiredString(input.tenantId, 'tenantId');
        const instanceId = parseRequiredString(input.instanceId, 'instanceId');
        const serviceScope = input.serviceScope || this.defaultServiceScope;
        const response = await this.postJson(
            '/v1/internal/source-mapping/resolve',
            {
                tenant_id: tenantId,
                instance_id: instanceId,
                service_scope: serviceScope,
            },
        );

        if (response.status === 'outage') {
            return response;
        }

        const { body, httpResponse } = response;

        if (
            httpResponse.status === 404 &&
            body?.reason_code === 'tenant_instance_mapping_not_found'
        ) {
            return {
                status: 'not_found',
            };
        }

        if (httpResponse.status !== 200) {
            return buildHttpOutage(
                '/v1/internal/source-mapping/resolve',
                httpResponse.status,
                body,
            );
        }

        if (!body) {
            return {
                status: 'outage',
                message: 'ACP resolve response body is invalid JSON',
            };
        }

        const mapping = parseSourceMappingRecord(body);

        if (!mapping) {
            return {
                status: 'outage',
                message: 'ACP resolve response missing mapping fields',
            };
        }

        const requestedServiceScope = parseServiceScope(
            body.requested_service_scope,
        ) || serviceScope;
        const serviceAllowed = typeof body.service_allowed === 'boolean'
            ? body.service_allowed
            : mapping.allowedServices.includes(requestedServiceScope);

        return {
            status: 'found',
            mapping: {
                ...mapping,
                requestedServiceScope,
                serviceAllowed,
            },
        };
    }

    async listSourceMappings(
        input: ListSourceMappingsInput = {},
    ): Promise<AcpListSourceMappingsResult> {
        const response = await this.postJson(
            '/v1/internal/source-mappings/list',
            {
                tenant_id: input.tenantId,
                instance_id: input.instanceId,
                service_scope: input.serviceScope,
                tenant_state: input.tenantState,
                entitlement_state: input.entitlementState,
                instance_state: input.instanceState,
            },
        );

        if (response.status === 'outage') {
            return response;
        }

        const { body, httpResponse } = response;

        if (httpResponse.status !== 200) {
            return buildHttpOutage(
                '/v1/internal/source-mappings/list',
                httpResponse.status,
                body,
            );
        }

        if (!body) {
            return {
                status: 'outage',
                message: 'ACP list response body is invalid JSON',
            };
        }

        const rawMappings = body.source_mappings;

        if (!Array.isArray(rawMappings)) {
            return {
                status: 'outage',
                message: 'ACP list response missing source_mappings array',
            };
        }

        const mappings: AcpSourceMappingRecord[] = [];

        for (const rawMapping of rawMappings) {
            const mapping = parseSourceMappingRecord(rawMapping);

            if (!mapping) {
                return {
                    status: 'outage',
                    message: 'ACP list response contains invalid mapping record',
                };
            }

            mappings.push(mapping);
        }

        return {
            status: 'ok',
            mappings,
        };
    }

    private async postJson(
        path: string,
        body: Record<string, unknown>,
    ): Promise<
        | {
            status: 'ok';
            httpResponse: Response;
            body: Record<string, unknown> | undefined;
        }
        | AcpSourceMappingOutage
    > {
        const controller = new AbortController();
        const timeoutHandle = setTimeout(() => {
            controller.abort();
        }, this.timeoutMs);

        try {
            const httpResponse = await this.fetchImpl(
                `${this.baseUrl}${path}`,
                {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/json',
                        'x-rezilient-internal-token': this.internalToken,
                    },
                    body: JSON.stringify(body),
                    signal: controller.signal,
                },
            );
            const responseBody = await readResponseBody(httpResponse);

            return {
                status: 'ok',
                httpResponse,
                body: responseBody,
            };
        } catch (error: unknown) {
            if (error instanceof Error && error.name === 'AbortError') {
                return buildTimeoutOutage(this.timeoutMs);
            }

            return buildTransportOutage(error);
        } finally {
            clearTimeout(timeoutHandle);
        }
    }
}
