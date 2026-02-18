import {
    createPrivateKey,
    createPublicKey,
    KeyObject,
    sign,
    verify,
} from 'node:crypto';
import { canonicalJsonStringify } from '@rezilient/types';
import { EvidenceSignerConfig } from './models';

export interface EvidenceSigner {
    signature_algorithm: 'ed25519';
    signer_key_id: string;
    private_key_pem: string;
    public_key_pem: string;
    private_key: KeyObject;
    public_key: KeyObject;
}

function normalizePem(raw: string | undefined): string | undefined {
    if (!raw) {
        return undefined;
    }

    const trimmed = raw.trim();

    if (!trimmed) {
        return undefined;
    }

    return trimmed.includes('\\n')
        ? trimmed.replace(/\\n/g, '\n')
        : trimmed;
}

export function createEvidenceSigner(
    config: EvidenceSignerConfig,
): EvidenceSigner {
    const privatePem = normalizePem(config.private_key_pem);
    const publicPem = normalizePem(config.public_key_pem);

    if (!privatePem) {
        throw new Error('RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM is required');
    }

    if (!publicPem) {
        throw new Error('RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM is required');
    }

    const privateKey = createPrivateKey(privatePem);
    const derivedPublicKey = createPublicKey(privateKey);
    const publicKey = createPublicKey(publicPem);
    const derivedPublicSpki = derivedPublicKey.export({
        type: 'spki',
        format: 'der',
    }) as Buffer;
    const explicitPublicSpki = publicKey.export({
        type: 'spki',
        format: 'der',
    }) as Buffer;

    if (!derivedPublicSpki.equals(explicitPublicSpki)) {
        throw new Error(
            'RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM must match ' +
            'RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM',
        );
    }

    return {
        signature_algorithm: 'ed25519',
        signer_key_id: config.signer_key_id,
        private_key_pem: privatePem,
        public_key_pem: publicPem,
        private_key: privateKey,
        public_key: publicKey,
    };
}

export function signCanonicalPayload(
    payload: unknown,
    signer: EvidenceSigner,
): string {
    const message = Buffer.from(canonicalJsonStringify(payload), 'utf8');

    return sign(null, message, signer.private_key).toString('base64');
}

export function verifyCanonicalSignature(
    payload: unknown,
    signatureBase64: string,
    signer: EvidenceSigner,
): boolean {
    let signature: Buffer;

    try {
        signature = Buffer.from(signatureBase64, 'base64');
    } catch {
        return false;
    }

    const message = Buffer.from(canonicalJsonStringify(payload), 'utf8');

    return verify(null, message, signer.public_key, signature);
}
