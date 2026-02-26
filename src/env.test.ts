import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from './test-helpers';
import { parseRestoreServiceEnv } from './env';

function buildValidEnv(): Record<string, string> {
    return {
        RRS_ADMIN_TOKEN: 'admin-tok-123',
        RRS_AUTH_SIGNING_KEY: 'signing-key-for-tests',
        RRS_ACP_BASE_URL: 'http://127.0.0.1:3010',
        RRS_ACP_INTERNAL_TOKEN: 'internal-token-for-tests',
        RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
        RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        REZ_RESTORE_PG_URL:
            'postgres://user:pass@localhost:5432/restore',
    };
}

describe('parseRestoreServiceEnv', () => {
    test('succeeds with all required vars', () => {
        const env = buildValidEnv();
        const result = parseRestoreServiceEnv(env);
        assert.equal(result.adminToken, 'admin-tok-123');
        assert.equal(
            result.authSigningKey,
            'signing-key-for-tests',
        );
        assert.equal(
            result.evidenceSigningPrivateKeyPem,
            TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
        );
        assert.equal(
            result.evidenceSigningPublicKeyPem,
            TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        );
        assert.ok(result.restorePgUrl.includes('localhost'));
    });

    test('applies defaults for optional vars', () => {
        const env = buildValidEnv();
        const result = parseRestoreServiceEnv(env);
        assert.equal(result.port, 3100);
        assert.equal(result.authClockSkewSeconds, 30);
        assert.equal(result.authExpectedIssuer, undefined);
        assert.equal(result.maxJsonBodyBytes, 1048576);
        assert.equal(result.restoreIndexStaleAfterSeconds, 120);
        assert.equal(result.executeDefaultChunkSize, 100);
        assert.equal(result.executeMaxRows, 10000);
        assert.equal(
            result.executeElevatedSkipRatioPercent,
            20,
        );
        assert.equal(result.executeMaxChunksPerAttempt, 0);
        assert.equal(result.executeMediaChunkSize, 25);
        assert.equal(result.executeMediaMaxItems, 1000);
        assert.equal(result.executeMediaMaxBytes, 104857600);
        assert.equal(result.executeMediaMaxRetryAttempts, 3);
        assert.equal(result.evidenceImmutableWormEnabled, false);
        assert.equal(
            result.evidenceImmutableRetentionClass,
            'standard-30d',
        );
        assert.equal(result.stagingModeEnabled, false);
        assert.equal(result.gaRunbooksSignedOff, false);
        assert.equal(result.acpBaseUrl, 'http://127.0.0.1:3010');
        assert.equal(
            result.acpInternalToken,
            'internal-token-for-tests',
        );
        assert.equal(result.acpRequestTimeoutMs, 2000);
        assert.equal(result.acpPositiveCacheTtlSeconds, 30);
        assert.equal(result.acpNegativeCacheTtlSeconds, 5);
    });

    test('throws for missing RRS_ADMIN_TOKEN', () => {
        const env = buildValidEnv();
        delete env.RRS_ADMIN_TOKEN;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_ADMIN_TOKEN is required/,
        );
    });

    test('throws for missing RRS_AUTH_SIGNING_KEY', () => {
        const env = buildValidEnv();
        delete env.RRS_AUTH_SIGNING_KEY;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_AUTH_SIGNING_KEY is required/,
        );
    });

    test('throws for missing RRS_ACP_BASE_URL', () => {
        const env = buildValidEnv();
        delete env.RRS_ACP_BASE_URL;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_ACP_BASE_URL is required/,
        );
    });

    test('throws for invalid RRS_ACP_BASE_URL', () => {
        const env = buildValidEnv();
        env.RRS_ACP_BASE_URL = 'not-a-url';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_ACP_BASE_URL must be a valid URL/,
        );
    });

    test('throws for missing RRS_ACP_INTERNAL_TOKEN', () => {
        const env = buildValidEnv();
        delete env.RRS_ACP_INTERNAL_TOKEN;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_ACP_INTERNAL_TOKEN is required/,
        );
    });

    test('throws for missing evidence private PEM', () => {
        const env = buildValidEnv();
        delete env.RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is required/,
        );
    });

    test('throws for missing evidence public PEM', () => {
        const env = buildValidEnv();
        delete env.RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is required/,
        );
    });

    test('throws for missing PG URL', () => {
        const env = buildValidEnv();
        delete env.REZ_RESTORE_PG_URL;
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /REZ_RESTORE_PG_URL is required/,
        );
    });

    test('accepts RRS_RESTORE_PG_URL as fallback', () => {
        const env = buildValidEnv();
        delete env.REZ_RESTORE_PG_URL;
        env.RRS_RESTORE_PG_URL =
            'postgres://user:pass@localhost:5432/alt';
        const result = parseRestoreServiceEnv(env);
        assert.ok(result.restorePgUrl.includes('alt'));
    });
});

describe('parseNonNegativeInteger (indirect)', () => {
    test('rejects negative value', () => {
        const env = buildValidEnv();
        env.PORT = '-1';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /PORT must be a non-negative integer/,
        );
    });

    test('rejects non-numeric string', () => {
        const env = buildValidEnv();
        env.PORT = 'abc';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /PORT must be a non-negative integer/,
        );
    });
});

describe('parseStrictPositiveInteger (indirect)', () => {
    test('rejects zero', () => {
        const env = buildValidEnv();
        env.RRS_MAX_JSON_BODY_BYTES = '0';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_MAX_JSON_BODY_BYTES must be greater than zero/,
        );
    });

    test('rejects zero ACP request timeout', () => {
        const env = buildValidEnv();
        env.RRS_ACP_REQUEST_TIMEOUT_MS = '0';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /RRS_ACP_REQUEST_TIMEOUT_MS must be greater than zero/,
        );
    });
});

describe('parsePercentage (indirect)', () => {
    test('rejects value above 100', () => {
        const env = buildValidEnv();
        env.RRS_EXECUTE_ELEVATED_SKIP_RATIO_PERCENT = '101';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /must be a number between 0 and 100/,
        );
    });

    test('rejects negative value', () => {
        const env = buildValidEnv();
        env.RRS_EXECUTE_ELEVATED_SKIP_RATIO_PERCENT = '-1';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /must be a number between 0 and 100/,
        );
    });
});

describe('parseBoolean (indirect)', () => {
    test('parses true/false/1/0', () => {
        for (const [input, expected] of [
            ['true', true],
            ['false', false],
            ['1', true],
            ['0', false],
        ] as [string, boolean][]) {
            const env = buildValidEnv();
            env.RRS_STAGING_MODE_ENABLED = input;
            const result = parseRestoreServiceEnv(env);
            assert.equal(
                result.stagingModeEnabled,
                expected,
                `expected ${input} => ${expected}`,
            );
        }
    });

    test('rejects non-boolean string', () => {
        const env = buildValidEnv();
        env.RRS_STAGING_MODE_ENABLED = 'maybe';
        assert.throws(
            () => parseRestoreServiceEnv(env),
            /must be a boolean value/,
        );
    });
});

describe('unknown env vars (indirect)', () => {
    test('ignores unrelated env vars', () => {
        const env = buildValidEnv();
        env.RRS_UNUSED_TEST_FLAG = 'not-json-and-now-ignored';
        const result = parseRestoreServiceEnv(env);
        assert.equal(result.acpBaseUrl, 'http://127.0.0.1:3010');
    });
});

describe('parseOptionalString (indirect)', () => {
    test('returns undefined for empty/missing', () => {
        const env = buildValidEnv();
        const result = parseRestoreServiceEnv(env);
        assert.equal(result.authExpectedIssuer, undefined);
    });
});

describe('all numeric defaults are correct', () => {
    test('defaults match expected values', () => {
        const env = buildValidEnv();
        const result = parseRestoreServiceEnv(env);
        assert.equal(result.port, 3100);
        assert.equal(result.executeDefaultChunkSize, 100);
        assert.equal(result.executeMaxRows, 10000);
        assert.equal(
            result.executeElevatedSkipRatioPercent,
            20,
        );
        assert.equal(result.executeMaxChunksPerAttempt, 0);
        assert.equal(result.authClockSkewSeconds, 30);
        assert.equal(result.maxJsonBodyBytes, 1048576);
        assert.equal(result.restoreIndexStaleAfterSeconds, 120);
        assert.equal(result.executeMediaChunkSize, 25);
        assert.equal(result.executeMediaMaxItems, 1000);
        assert.equal(result.executeMediaMaxBytes, 104857600);
        assert.equal(result.executeMediaMaxRetryAttempts, 3);
        assert.equal(
            result.evidenceSignerKeyId,
            'rrs-dev-ed25519-v1',
        );
        assert.equal(
            result.evidenceImmutableRetentionClass,
            'standard-30d',
        );
        assert.equal(result.acpRequestTimeoutMs, 2000);
        assert.equal(result.acpPositiveCacheTtlSeconds, 30);
        assert.equal(result.acpNegativeCacheTtlSeconds, 5);
    });
});
