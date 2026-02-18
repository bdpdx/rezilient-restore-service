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
        RRS_EVIDENCE_SIGNER_KEY_ID: 'rrs-test-ed25519-v1',
        RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
        RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM:
            TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        ...overrides,
    };
}

test('startup config fails when RRS_ADMIN_TOKEN is missing', () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_ADMIN_TOKEN: undefined,
        }));
    }, /RRS_ADMIN_TOKEN is required/);
});

test('startup config fails when RRS_AUTH_SIGNING_KEY is missing', () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_AUTH_SIGNING_KEY: undefined,
        }));
    }, /RRS_AUTH_SIGNING_KEY is required/);
});

test('startup config does not allow AUTH_SIGNING_KEY fallback', () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_AUTH_SIGNING_KEY: undefined,
            AUTH_SIGNING_KEY: 'legacy-shared-secret',
        }));
    }, /RRS_AUTH_SIGNING_KEY is required/);
});

test('startup config fails when RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is missing', () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM: undefined,
        }));
    }, /RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is required/);
});

test('startup config fails when RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is missing', () => {
    assert.throws(() => {
        parseRestoreServiceEnv(buildEnv({
            RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM: undefined,
        }));
    }, /RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is required/);
});
