import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { signJwt, verifyJwt } from './jwt';

describe('signJwt', () => {
    test('produces valid three-segment token', () => {
        const token = signJwt({ sub: 'test' }, 'secret');
        const segments = token.split('.');
        assert.equal(segments.length, 3);
        for (const seg of segments) {
            assert.ok(seg.length > 0, 'segment must be non-empty');
        }
    });

    test('always sets alg to HS256 and typ to JWT', () => {
        const token = signJwt({ sub: 'test' }, 'secret');
        const headerSegment = token.split('.')[0];
        const headerJson = Buffer.from(
            headerSegment, 'base64url'
        ).toString('utf8');
        const header = JSON.parse(headerJson);
        assert.equal(header.alg, 'HS256');
        assert.equal(header.typ, 'JWT');
    });
});

describe('verifyJwt', () => {
    test('round-trips a signed token', () => {
        const payload = {
            sub: 'client-1',
            iss: 'test-issuer',
            count: 42,
        };
        const token = signJwt(payload, 'my-key');
        const result = verifyJwt(token, 'my-key');
        assert.equal(result.valid, true);
        assert.equal(result.payload?.sub, 'client-1');
        assert.equal(result.payload?.iss, 'test-issuer');
        assert.equal(result.payload?.count, 42);
    });

    test('rejects token with wrong signing key', () => {
        const token = signJwt({ sub: 'test' }, 'key-a');
        const result = verifyJwt(token, 'key-b');
        assert.equal(result.valid, false);
        assert.equal(result.reason, 'invalid_signature');
    });

    test('rejects token with tampered payload', () => {
        const token = signJwt({ sub: 'test' }, 'secret');
        const parts = token.split('.');
        const tamperedPayload = Buffer.from(
            JSON.stringify({ sub: 'hacked' }), 'utf8'
        ).toString('base64url');
        const tampered = `${parts[0]}.${tamperedPayload}.${parts[2]}`;
        const result = verifyJwt(tampered, 'secret');
        assert.equal(result.valid, false);
        assert.equal(result.reason, 'invalid_signature');
    });

    test('rejects token with fewer than 3 segments', () => {
        const result = verifyJwt('a.b', 'secret');
        assert.equal(result.valid, false);
        assert.equal(result.reason, 'malformed');
    });

    test('rejects token with invalid base64url segments', () => {
        const result = verifyJwt('!!!.@@@.###', 'secret');
        assert.equal(result.valid, false);
    });
});

describe('base64url encoding (indirect)', () => {
    test('round-trips binary-like data through sign/verify', () => {
        const payload = {
            data: Buffer.from([0xff, 0xfe, 0xfd, 0x00, 0x01])
                .toString('base64'),
        };
        const token = signJwt(payload, 'key');
        const result = verifyJwt(token, 'key');
        assert.equal(result.valid, true);
        assert.equal(result.payload?.data, payload.data);
    });

    test('token segments contain no +, /, or = characters', () => {
        const payload = {
            bin: Buffer.from([
                0xfb, 0xff, 0xbe, 0xef, 0x3e, 0x3f, 0xfc, 0xfd,
            ]).toString('base64'),
        };
        const token = signJwt(payload, 'secret-key-12345');
        assert.ok(!token.includes('+'), 'no + in token');
        assert.ok(!token.includes('/'), 'no / in token');
        assert.ok(!token.includes('='), 'no = in token');
    });
});
