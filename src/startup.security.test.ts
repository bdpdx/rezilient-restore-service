import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { parseRestoreServiceEnv } from './env';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from './test-helpers';

function buildEnv(
    overrides: Record<string, string | undefined>,
): NodeJS.ProcessEnv {
    return {
        RRS_ADMIN_TOKEN: 'admin-token-0123456789abcdef',
        RRS_AUTH_SIGNING_KEY: '0123456789abcdef0123456789abcdef',
        RRS_ACP_BASE_URL: 'http://127.0.0.1:3010',
        RRS_ACP_INTERNAL_TOKEN: 'internal-token-0123456789abcdef',
        REZ_RESTORE_PG_URL:
            'postgres://rez_restore_service_rw:pw@127.0.0.1:5432/rez_restore',
        RRS_EVIDENCE_SIGNER_KEY_ID: 'rrs-test-ed25519-v1',
        RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
        RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        ...overrides,
    };
}

test('startup config fails when RRS_ADMIN_TOKEN is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_ADMIN_TOKEN: undefined,
        }));
    }, /RRS_ADMIN_TOKEN is required/);
});

test('startup config fails when RRS_AUTH_SIGNING_KEY is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_AUTH_SIGNING_KEY: undefined,
        }));
    }, /RRS_AUTH_SIGNING_KEY is required/);
});

test('startup config does not allow AUTH_SIGNING_KEY fallback', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_AUTH_SIGNING_KEY: undefined,
            AUTH_SIGNING_KEY: 'legacy-shared-secret',
        }));
    }, /RRS_AUTH_SIGNING_KEY is required/);
});

test('startup config fails when RRS_ACP_BASE_URL is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_ACP_BASE_URL: undefined,
        }));
    }, /RRS_ACP_BASE_URL is required/);
});

test('startup config fails when RRS_ACP_INTERNAL_TOKEN is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_ACP_INTERNAL_TOKEN: undefined,
        }));
    }, /RRS_ACP_INTERNAL_TOKEN is required/);
});

test('startup config fails when RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM: undefined,
        }));
    }, /RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is required/);
});

test('startup config fails when RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM: undefined,
        }));
    }, /RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is required/);
});

test('startup config fails when REZ_RESTORE_PG_URL is missing', async () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            REZ_RESTORE_PG_URL: undefined,
        }));
    }, /REZ_RESTORE_PG_URL is required/);
});
