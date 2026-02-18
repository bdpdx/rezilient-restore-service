import { signJwt } from './auth/jwt';

export interface BuildTokenInput {
    audience?: string;
    expiresInSeconds?: number;
    issuedAt?: number;
    issuer?: string;
    scope?: 'reg' | 'rrs';
    signingKey: string;
    source?: string;
    tenantId?: string;
    instanceId?: string;
}

export const TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM = [
    '-----BEGIN PRIVATE KEY-----',
    'MC4CAQAwBQYDK2VwBCIEIGBOo7UP8KJKc4bpjCwHXmmC16FfJrstu4xENR7ylDJr',
    '-----END PRIVATE KEY-----',
].join('\n');

export const TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM = [
    '-----BEGIN PUBLIC KEY-----',
    'MCowBQYDK2VwAyEAeL8BvtaY4GJPBLvWIR185RUBiyJXz9dBlxsAU20c6GA=',
    '-----END PUBLIC KEY-----',
].join('\n');

export function buildScopedToken(input: BuildTokenInput): string {
    const now = input.issuedAt || Math.floor(Date.now() / 1000);
    const scope = input.scope || 'rrs';

    return signJwt(
        {
            iss: input.issuer || 'rez-auth-control-plane',
            sub: 'client-test',
            aud: input.audience || `rezilient:${scope}`,
            jti: `tok-${now}`,
            iat: now,
            exp: now + (input.expiresInSeconds || 300),
            service_scope: scope,
            tenant_id: input.tenantId || 'tenant-acme',
            instance_id: input.instanceId || 'sn-dev-01',
            source: input.source || 'sn://acme-dev.service-now.com',
        },
        input.signingKey,
    );
}
