import {
    AuthDenyReasonCode,
    RRS_SERVICE_SCOPE,
} from '../constants';
import {
    AuthTokenClaims,
    parseTokenClaims,
} from './claims';
import { verifyJwt } from './jwt';

export interface RestoreRequestAuth {
    claims: AuthTokenClaims;
}

export interface RequestAuthenticatorConfig {
    signingKey: string;
    tokenClockSkewSeconds: number;
    expectedIssuer?: string;
    now?: () => Date;
}

export interface AuthenticateSuccess {
    success: true;
    auth: RestoreRequestAuth;
}

export interface AuthenticateFailure {
    success: false;
    reasonCode: AuthDenyReasonCode;
}

export type AuthenticateResult = AuthenticateSuccess | AuthenticateFailure;

function parseBearerToken(authorizationHeader: string | undefined): string | null {
    if (!authorizationHeader) {
        return null;
    }

    const [scheme, token] = authorizationHeader.split(' ', 2);

    if (!scheme || !token) {
        return null;
    }

    if (scheme.toLowerCase() !== 'bearer') {
        return null;
    }

    if (token.trim() === '') {
        return null;
    }

    return token.trim();
}

export class RequestAuthenticator {
    private readonly nowFn: () => Date;

    constructor(private readonly config: RequestAuthenticatorConfig) {
        this.nowFn = config.now || (() => new Date());
    }

    authenticate(
        authorizationHeader: string | undefined,
    ): AuthenticateResult {
        const token = parseBearerToken(authorizationHeader);

        if (!token) {
            return {
                success: false,
                reasonCode: 'denied_token_malformed',
            };
        }

        const verified = verifyJwt(token, this.config.signingKey);

        if (!verified.valid) {
            return {
                success: false,
                reasonCode: verified.reason === 'invalid_signature'
                    ? 'denied_token_invalid_signature'
                    : 'denied_token_malformed',
            };
        }

        const claimsResult = parseTokenClaims(
            verified.payload as Record<string, unknown>,
        );

        if (!claimsResult.success || !claimsResult.claims) {
            return {
                success: false,
                reasonCode: claimsResult.reasonCode ||
                    'denied_token_malformed',
            };
        }

        const claims = claimsResult.claims;

        if (claims.service_scope !== RRS_SERVICE_SCOPE) {
            return {
                success: false,
                reasonCode: 'denied_token_wrong_service_scope',
            };
        }

        const expectedAudience = `rezilient:${RRS_SERVICE_SCOPE}`;

        if (claims.aud !== expectedAudience) {
            return {
                success: false,
                reasonCode: 'denied_token_wrong_service_scope',
            };
        }

        if (
            this.config.expectedIssuer &&
            claims.iss !== this.config.expectedIssuer
        ) {
            return {
                success: false,
                reasonCode: 'denied_token_malformed',
            };
        }

        const nowSeconds = Math.floor(this.nowFn().getTime() / 1000);

        if (nowSeconds > claims.exp + this.config.tokenClockSkewSeconds) {
            return {
                success: false,
                reasonCode: 'denied_token_expired',
            };
        }

        if (claims.iat > nowSeconds + this.config.tokenClockSkewSeconds) {
            return {
                success: false,
                reasonCode: 'denied_token_malformed',
            };
        }

        if (claims.iat >= claims.exp) {
            return {
                success: false,
                reasonCode: 'denied_token_malformed',
            };
        }

        return {
            success: true,
            auth: {
                claims,
            },
        };
    }
}
