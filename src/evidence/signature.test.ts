import { strict as assert } from 'node:assert';
import { generateKeyPairSync } from 'node:crypto';
import { test } from 'node:test';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from '../test-helpers';
import {
    createEvidenceSigner,
    signCanonicalPayload,
    verifyCanonicalSignature,
} from './signature';

test('createEvidenceSigner fails when private key PEM is missing', async () => {
    assert.throws(() => {
        createEvidenceSigner({
            signer_key_id: 'rrs-test-signer',
            private_key_pem: undefined,
            public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
        });
    }, /RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is required/);
});

test('createEvidenceSigner fails when public key PEM is missing', async () => {
    assert.throws(() => {
        createEvidenceSigner({
            signer_key_id: 'rrs-test-signer',
            private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
            public_key_pem: undefined,
        });
    }, /RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is required/);
});

test('createEvidenceSigner fails when private/public keypair does not match', async () => {
    const mismatchedPublicKey = generateKeyPairSync('ed25519')
        .publicKey
        .export({
            type: 'spki',
            format: 'pem',
        })
        .toString();

    assert.throws(() => {
        createEvidenceSigner({
            signer_key_id: 'rrs-test-signer',
            private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
            public_key_pem: mismatchedPublicKey,
        });
    }, /RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM must match/);
});

test('createEvidenceSigner accepts escaped-newline PEM and signs/verifies', async () => {
    const signer = createEvidenceSigner({
        signer_key_id: 'rrs-test-signer',
        private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM.replace(
            /\n/g,
            '\\n',
        ),
        public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM.replace(
            /\n/g,
            '\\n',
        ),
    });
    const payload = {
        id: 'evidence-1',
        status: 'ok',
    };
    const signature = signCanonicalPayload(payload, signer);

    assert.equal(verifyCanonicalSignature(payload, signature, signer), true);
    assert.equal(
        signer.public_key_pem,
        TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
    );
});
