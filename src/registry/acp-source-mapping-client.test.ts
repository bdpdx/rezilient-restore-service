import { strict as assert } from 'node:assert';
import { once } from 'node:events';
import {
    createServer,
    IncomingHttpHeaders,
    IncomingMessage,
    Server,
    ServerResponse,
} from 'node:http';
import { describe, test } from 'node:test';
import { AcpSourceMappingClient } from './acp-source-mapping-client';

interface JsonRequestRecord {
    method: string;
    pathname: string;
    headers: IncomingHttpHeaders;
    body: Record<string, unknown>;
}

interface JsonResponseRecord {
    statusCode: number;
    body?: Record<string, unknown>;
    rawBody?: string;
    delayMs?: number;
}

async function listen(server: Server): Promise<string> {
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const address = server.address();

    if (!address || typeof address === 'string') {
        throw new Error('server address unavailable');
    }

    return `http://127.0.0.1:${address.port}`;
}

async function closeServer(server: Server): Promise<void> {
    await new Promise<void>((resolve) => {
        server.close(() => {
            resolve();
        });
    });
}

async function readJsonBody(
    request: IncomingMessage,
): Promise<Record<string, unknown>> {
    const buffers: Buffer[] = [];

    for await (const chunk of request) {
        buffers.push(
            Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
        );
    }

    const rawBody = Buffer.concat(buffers).toString('utf8').trim();

    if (!rawBody) {
        return {};
    }

    const parsed = JSON.parse(rawBody) as unknown;

    if (
        !parsed ||
        typeof parsed !== 'object' ||
        Array.isArray(parsed)
    ) {
        throw new Error('request body must be a JSON object');
    }

    return parsed as Record<string, unknown>;
}

function writeJsonResponse(
    response: ServerResponse,
    payload: JsonResponseRecord,
): void {
    response.statusCode = payload.statusCode;
    response.setHeader('content-type', 'application/json');

    if (payload.rawBody !== undefined) {
        response.end(payload.rawBody);

        return;
    }

    response.end(JSON.stringify(payload.body || {}));
}

function createJsonServer(
    handler: (
        request: JsonRequestRecord,
    ) => Promise<JsonResponseRecord> | JsonResponseRecord,
): Server {
    return createServer(async (request, response) => {
        const body = await readJsonBody(request);
        const result = await handler({
            method: request.method || 'GET',
            pathname: request.url || '/',
            headers: request.headers,
            body,
        });

        if (result.delayMs && result.delayMs > 0) {
            await new Promise<void>((resolve) => {
                setTimeout(() => {
                    resolve();
                }, result.delayMs);
            });
        }

        writeJsonResponse(response, result);
    });
}

describe('AcpSourceMappingClient', () => {
    test('resolveSourceMapping sends auth header and parses found mapping', async () => {
        const capturedRequests: JsonRequestRecord[] = [];
        const server = createJsonServer((request) => {
            capturedRequests.push(request);

            return {
                statusCode: 200,
                body: {
                    ok: true,
                    tenant_id: 'tenant-acme',
                    instance_id: 'instance-acme',
                    source: 'sn://acme-dev.service-now.com',
                    tenant_state: 'active',
                    entitlement_state: 'active',
                    instance_state: 'active',
                    allowed_services: ['rrs', 'reg'],
                    updated_at: '2026-02-26T00:00:00.000Z',
                    requested_service_scope: 'rrs',
                    service_allowed: true,
                },
            };
        });
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'internal-secret',
            timeoutMs: 2000,
        });

        try {
            const result = await client.resolveSourceMapping({
                tenantId: 'tenant-acme',
                instanceId: 'instance-acme',
            });

            assert.equal(result.status, 'found');
            assert.equal(
                result.mapping.source,
                'sn://acme-dev.service-now.com',
            );
            assert.equal(result.mapping.requestedServiceScope, 'rrs');
            assert.equal(result.mapping.serviceAllowed, true);
            const capturedRequest = capturedRequests[0];

            if (!capturedRequest) {
                throw new Error('expected request to be captured');
            }

            assert.equal(
                capturedRequest.pathname,
                '/v1/internal/source-mapping/resolve',
            );
            assert.equal(capturedRequest.method, 'POST');
            assert.equal(
                capturedRequest.body.tenant_id,
                'tenant-acme',
            );
            assert.equal(
                capturedRequest.body.instance_id,
                'instance-acme',
            );
            assert.equal(capturedRequest.body.service_scope, 'rrs');
            assert.equal(
                capturedRequest.headers['x-rezilient-internal-token'],
                'internal-secret',
            );
        } finally {
            await closeServer(server);
        }
    });

    test('resolveSourceMapping returns not_found for missing mapping', async () => {
        const server = createJsonServer(() => ({
            statusCode: 404,
            body: {
                error: 'not_found',
                reason_code: 'tenant_instance_mapping_not_found',
            },
        }));
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'internal-secret',
            timeoutMs: 2000,
        });

        try {
            const result = await client.resolveSourceMapping({
                tenantId: 'tenant-missing',
                instanceId: 'instance-missing',
            });

            assert.equal(result.status, 'not_found');
        } finally {
            await closeServer(server);
        }
    });

    test('resolveSourceMapping maps ACP internal auth failures to outage', async () => {
        const server = createJsonServer(() => ({
            statusCode: 403,
            body: {
                error: 'forbidden',
                reason_code: 'internal_auth_required',
            },
        }));
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'wrong-secret',
            timeoutMs: 2000,
        });

        try {
            const result = await client.resolveSourceMapping({
                tenantId: 'tenant-acme',
                instanceId: 'instance-acme',
            });

            assert.equal(result.status, 'outage');
            assert.equal(result.statusCode, 403);
            assert.equal(
                result.message,
                'ACP internal authentication failed',
            );
        } finally {
            await closeServer(server);
        }
    });

    test('resolveSourceMapping returns outage on timeout', async () => {
        const server = createJsonServer(() => ({
            statusCode: 200,
            body: {
                ok: true,
                tenant_id: 'tenant-timeout',
                instance_id: 'instance-timeout',
                source: 'sn://timeout.service-now.com',
                tenant_state: 'active',
                entitlement_state: 'active',
                instance_state: 'active',
                allowed_services: ['rrs'],
                updated_at: '2026-02-26T00:00:00.000Z',
                requested_service_scope: 'rrs',
                service_allowed: true,
            },
            delayMs: 200,
        }));
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'internal-secret',
            timeoutMs: 25,
        });

        try {
            const result = await client.resolveSourceMapping({
                tenantId: 'tenant-timeout',
                instanceId: 'instance-timeout',
            });

            assert.equal(result.status, 'outage');
            assert.equal(
                result.message,
                'ACP request timed out after 25ms',
            );
        } finally {
            await closeServer(server);
        }
    });

    test('listSourceMappings sends filters and parses records', async () => {
        const capturedRequests: JsonRequestRecord[] = [];
        const server = createJsonServer((request) => {
            capturedRequests.push(request);

            return {
                statusCode: 200,
                body: {
                    ok: true,
                    source_mappings: [
                        {
                            tenant_id: 'tenant-acme',
                            instance_id: 'instance-acme',
                            source: 'sn://acme-dev.service-now.com',
                            tenant_state: 'active',
                            entitlement_state: 'active',
                            instance_state: 'active',
                            allowed_services: ['rrs', 'reg'],
                            updated_at: '2026-02-26T00:00:00.000Z',
                        },
                    ],
                },
            };
        });
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'internal-secret',
            timeoutMs: 2000,
        });

        try {
            const result = await client.listSourceMappings({
                serviceScope: 'rrs',
                instanceState: 'active',
            });

            assert.equal(result.status, 'ok');
            assert.equal(result.mappings.length, 1);
            assert.equal(
                result.mappings[0].instanceId,
                'instance-acme',
            );
            const capturedRequest = capturedRequests[0];

            if (!capturedRequest) {
                throw new Error('expected request to be captured');
            }

            assert.equal(
                capturedRequest.pathname,
                '/v1/internal/source-mappings/list',
            );
            assert.equal(capturedRequest.method, 'POST');
            assert.equal(capturedRequest.body.service_scope, 'rrs');
            assert.equal(capturedRequest.body.instance_state, 'active');
            assert.equal(
                capturedRequest.headers['x-rezilient-internal-token'],
                'internal-secret',
            );
        } finally {
            await closeServer(server);
        }
    });

    test('listSourceMappings returns outage for invalid ACP payloads', async () => {
        const server = createJsonServer(() => ({
            statusCode: 200,
            body: {
                ok: true,
                source_mappings: [
                    {
                        tenant_id: 'tenant-acme',
                    },
                ],
            },
        }));
        const baseUrl = await listen(server);
        const client = new AcpSourceMappingClient({
            baseUrl,
            internalToken: 'internal-secret',
            timeoutMs: 2000,
        });

        try {
            const result = await client.listSourceMappings();
            assert.equal(result.status, 'outage');
            assert.equal(
                result.message,
                'ACP list response contains invalid mapping record',
            );
        } finally {
            await closeServer(server);
        }
    });
});
