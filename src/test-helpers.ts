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
