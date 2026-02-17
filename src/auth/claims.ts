import {
    AuthDenyReasonCode,
    ServiceScope,
    SERVICE_SCOPES,
} from '../constants';

export interface AuthTokenClaims {
    iss: string;
    sub: string;
    aud: string;
    jti: string;
    iat: number;
    exp: number;
    service_scope: ServiceScope;
    tenant_id: string;
    instance_id: string;
    source: string;
}

export interface ParseClaimsResult {
    success: boolean;
    claims?: AuthTokenClaims;
    reasonCode?: AuthDenyReasonCode;
}

function asString(
    payload: Record<string, unknown>,
    fieldName: string,
): string {
    const value = payload[fieldName];

    if (typeof value !== 'string' || value.trim() === '') {
        throw new Error(`${fieldName} must be a non-empty string`);
    }

    return value;
}

function asNumber(
    payload: Record<string, unknown>,
    fieldName: string,
): number {
    const value = payload[fieldName];

    if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new Error(`${fieldName} must be a valid number`);
    }

    return value;
}

function isServiceScope(value: string): value is ServiceScope {
    return SERVICE_SCOPES.includes(value as ServiceScope);
}

export function parseTokenClaims(
    payload: Record<string, unknown>,
): ParseClaimsResult {
    try {
        const serviceScope = asString(payload, 'service_scope');

        if (!isServiceScope(serviceScope)) {
            return {
                success: false,
                reasonCode: 'denied_token_malformed',
            };
        }

        return {
            success: true,
            claims: {
                iss: asString(payload, 'iss'),
                sub: asString(payload, 'sub'),
                aud: asString(payload, 'aud'),
                jti: asString(payload, 'jti'),
                iat: asNumber(payload, 'iat'),
                exp: asNumber(payload, 'exp'),
                service_scope: serviceScope,
                tenant_id: asString(payload, 'tenant_id'),
                instance_id: asString(payload, 'instance_id'),
                source: asString(payload, 'source'),
            },
        };
    } catch {
        return {
            success: false,
            reasonCode: 'denied_token_malformed',
        };
    }
}
