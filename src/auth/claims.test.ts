import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { parseTokenClaims } from './claims';

function buildValidPayload(): Record<string, unknown> {
    return {
        iss: 'rez-auth-control-plane',
        sub: 'client-test',
        aud: 'rezilient:rrs',
        jti: 'tok-1',
        iat: 1700000000,
        exp: 1700000300,
        service_scope: 'rrs',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
    };
}

describe('parseTokenClaims', () => {
    test('returns all fields for valid payload', () => {
        const result = parseTokenClaims(buildValidPayload());
        assert.equal(result.success, true);
        assert.ok(result.claims);
        assert.equal(result.claims.iss, 'rez-auth-control-plane');
        assert.equal(result.claims.sub, 'client-test');
        assert.equal(result.claims.aud, 'rezilient:rrs');
        assert.equal(result.claims.jti, 'tok-1');
        assert.equal(result.claims.iat, 1700000000);
        assert.equal(result.claims.exp, 1700000300);
        assert.equal(result.claims.service_scope, 'rrs');
        assert.equal(result.claims.tenant_id, 'tenant-acme');
        assert.equal(result.claims.instance_id, 'sn-dev-01');
        assert.equal(
            result.claims.source,
            'sn://acme-dev.service-now.com',
        );
    });

    test('rejects missing service_scope', () => {
        const payload = buildValidPayload();
        delete payload.service_scope;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects invalid service_scope value', () => {
        const payload = buildValidPayload();
        payload.service_scope = 'admin';
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing tenant_id', () => {
        const payload = buildValidPayload();
        delete payload.tenant_id;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing instance_id', () => {
        const payload = buildValidPayload();
        delete payload.instance_id;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing source', () => {
        const payload = buildValidPayload();
        delete payload.source;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing aud', () => {
        const payload = buildValidPayload();
        delete payload.aud;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects non-string aud', () => {
        const payload = buildValidPayload();
        payload.aud = 123;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing exp', () => {
        const payload = buildValidPayload();
        delete payload.exp;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects non-finite exp', () => {
        const payload = buildValidPayload();
        payload.exp = Infinity;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });

    test('rejects missing iat', () => {
        const payload = buildValidPayload();
        delete payload.iat;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });
});

describe('asString (indirect via parseTokenClaims)', () => {
    test('returns value for valid string', () => {
        const payload = buildValidPayload();
        payload.iss = 'hello';
        const result = parseTokenClaims(payload);
        assert.equal(result.success, true);
        assert.equal(result.claims?.iss, 'hello');
    });

    test('throws for non-string value', () => {
        const payload = buildValidPayload();
        payload.iss = 42;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });
});

describe('asNumber (indirect via parseTokenClaims)', () => {
    test('returns value for finite number', () => {
        const payload = buildValidPayload();
        payload.iat = 42;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, true);
        assert.equal(result.claims?.iat, 42);
    });

    test('throws for NaN', () => {
        const payload = buildValidPayload();
        payload.iat = NaN;
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
        assert.equal(result.reasonCode, 'denied_token_malformed');
    });
});

describe('isServiceScope (indirect via parseTokenClaims)', () => {
    test('accepts reg and rrs', () => {
        for (const scope of ['reg', 'rrs']) {
            const payload = buildValidPayload();
            payload.service_scope = scope;
            const result = parseTokenClaims(payload);
            assert.equal(result.success, true);
            assert.equal(result.claims?.service_scope, scope);
        }
    });

    test('rejects unknown scope', () => {
        const payload = buildValidPayload();
        payload.service_scope = 'admin';
        const result = parseTokenClaims(payload);
        assert.equal(result.success, false);
    });
});
