import {
    createPrivateKey,
    createPublicKey,
    KeyObject,
    sign,
    verify,
} from 'node:crypto';
import { canonicalJsonStringify } from '@rezilient/types';
import { EvidenceSignerConfig } from './models';

const DEFAULT_PRIVATE_KEY_PEM = [
    '-----BEGIN PRIVATE KEY-----',
    'MC4CAQAwBQYDK2VwBCIEIGBOo7UP8KJKc4bpjCwHXmmC16FfJrstu4xENR7ylDJr',
    '-----END PRIVATE KEY-----',
].join('\n');

const DEFAULT_PUBLIC_KEY_PEM = [
    '-----BEGIN PUBLIC KEY-----',
    'MCowBQYDK2VwAyEAeL8BvtaY4GJPBLvWIR185RUBiyJXz9dBlxsAU20c6GA=',
    '-----END PUBLIC KEY-----',
].join('\n');

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
    const privatePem = normalizePem(config.private_key_pem) ||
        DEFAULT_PRIVATE_KEY_PEM;
    const explicitPublicPem = normalizePem(config.public_key_pem);
    const privateKey = createPrivateKey(privatePem);
    const derivedPublicKey = createPublicKey(privateKey);
    const publicPem = explicitPublicPem ||
        derivedPublicKey.export({
            type: 'spki',
            format: 'pem',
        }).toString();
    const publicKey = createPublicKey(publicPem || DEFAULT_PUBLIC_KEY_PEM);

    return {
        signature_algorithm: 'ed25519',
        signer_key_id: config.signer_key_id,
        private_key_pem: privatePem,
        public_key_pem: publicPem || DEFAULT_PUBLIC_KEY_PEM,
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
