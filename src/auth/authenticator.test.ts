import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { buildScopedToken } from '../test-helpers';
import {
    RequestAuthenticator,
    RequestAuthenticatorConfig,
} from './authenticator';
import { signJwt } from './jwt';

const SIGNING_KEY = 'test-signing-key-256-bits-long!!';

function buildAuthenticator(
    overrides: Partial<RequestAuthenticatorConfig> = {},
): RequestAuthenticator {
    return new RequestAuthenticator({
        signingKey: SIGNING_KEY,
        tokenClockSkewSeconds: 30,
        ...overrides,
    });
}

describe('RequestAuthenticator', () => {
    test('authenticate succeeds with valid bearer token', () => {
        const auth = buildAuthenticator();
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, true);
        if (result.success) {
            assert.equal(
                result.auth.claims.tenant_id,
                'tenant-acme',
            );
            assert.equal(
                result.auth.claims.service_scope,
                'rrs',
            );
        }
    });

    test('authenticate rejects missing authorization header', () => {
        const auth = buildAuthenticator();
        const result = auth.authenticate(undefined);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_malformed',
            );
        }
    });

    test('authenticate rejects non-Bearer scheme', () => {
        const auth = buildAuthenticator();
        const result = auth.authenticate('Basic abc123');
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_malformed',
            );
        }
    });

    test('authenticate rejects empty token after Bearer', () => {
        const auth = buildAuthenticator();
        const result = auth.authenticate('Bearer ');
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_malformed',
            );
        }
    });

    test('authenticate rejects invalid JWT signature', () => {
        const auth = buildAuthenticator();
        const token = buildScopedToken({
            signingKey: 'wrong-key-not-the-right-one!!!!',
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_invalid_signature',
            );
        }
    });

    test('authenticate rejects expired token', () => {
        const now = Math.floor(Date.now() / 1000);
        const auth = buildAuthenticator({
            tokenClockSkewSeconds: 30,
            now: () => new Date(),
        });
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            issuedAt: now - 600,
            expiresInSeconds: 300,
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_expired',
            );
        }
    });

    test('authenticate accepts token within clock skew', () => {
        const now = Math.floor(Date.now() / 1000);
        const auth = buildAuthenticator({
            tokenClockSkewSeconds: 30,
            now: () => new Date(now * 1000),
        });
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            issuedAt: now - 310,
            expiresInSeconds: 300,
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, true);
    });

    test('authenticate rejects token beyond clock skew', () => {
        const now = Math.floor(Date.now() / 1000);
        const auth = buildAuthenticator({
            tokenClockSkewSeconds: 30,
            now: () => new Date(now * 1000),
        });
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            issuedAt: now - 400,
            expiresInSeconds: 300,
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_expired',
            );
        }
    });

    test('authenticate rejects iat >= exp', () => {
        const now = Math.floor(Date.now() / 1000);
        const auth = buildAuthenticator({
            now: () => new Date(now * 1000),
        });
        const token = signJwt(
            {
                iss: 'rez-auth-control-plane',
                sub: 'client-test',
                aud: 'rezilient:rrs',
                jti: 'tok-iat-exp',
                iat: now,
                exp: now,
                service_scope: 'rrs',
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
            SIGNING_KEY,
        );
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_malformed',
            );
        }
    });

    test('authenticate rejects wrong service scope', () => {
        const auth = buildAuthenticator();
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            scope: 'reg',
            audience: 'rezilient:reg',
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_wrong_service_scope',
            );
        }
    });

    test('authenticate rejects wrong audience', () => {
        const auth = buildAuthenticator();
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            audience: 'rezilient:wrong',
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_wrong_service_scope',
            );
        }
    });

    test('authenticate validates issuer when expectedIssuer is set', () => {
        const auth = buildAuthenticator({
            expectedIssuer: 'expected-issuer',
        });
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            issuer: 'wrong-issuer',
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, false);
        if (!result.success) {
            assert.equal(
                result.reasonCode,
                'denied_token_malformed',
            );
        }
    });

    test('authenticate skips issuer check when expectedIssuer is unset', () => {
        const auth = buildAuthenticator();
        const token = buildScopedToken({
            signingKey: SIGNING_KEY,
            issuer: 'any-random-issuer',
        });
        const result = auth.authenticate(`Bearer ${token}`);
        assert.equal(result.success, true);
    });
});
