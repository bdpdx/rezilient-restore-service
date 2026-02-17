import {
    createHmac,
    timingSafeEqual,
} from 'node:crypto';

export interface JwtPayload {
    [key: string]: unknown;
}

export interface VerifiedJwt {
    valid: boolean;
    payload?: JwtPayload;
    reason?: 'malformed' | 'invalid_signature';
}

function base64UrlEncode(input: string): string {
    return Buffer.from(input, 'utf8').toString('base64url');
}

function base64UrlDecode(input: string): string {
    return Buffer.from(input, 'base64url').toString('utf8');
}

export function signJwt(payload: JwtPayload, signingKey: string): string {
    const header = {
        alg: 'HS256',
        typ: 'JWT',
    };
    const headerSegment = base64UrlEncode(JSON.stringify(header));
    const payloadSegment = base64UrlEncode(JSON.stringify(payload));
    const signingInput = `${headerSegment}.${payloadSegment}`;
    const signature = createHmac('sha256', signingKey)
        .update(signingInput)
        .digest('base64url');

    return `${signingInput}.${signature}`;
}

export function verifyJwt(token: string, signingKey: string): VerifiedJwt {
    const parts = token.split('.');

    if (parts.length !== 3) {
        return {
            valid: false,
            reason: 'malformed',
        };
    }

    const [headerSegment, payloadSegment, signatureSegment] = parts;
    const signingInput = `${headerSegment}.${payloadSegment}`;
    const expectedSignature = createHmac('sha256', signingKey)
        .update(signingInput)
        .digest('base64url');
    const expectedBuffer = Buffer.from(expectedSignature, 'utf8');
    const suppliedBuffer = Buffer.from(signatureSegment, 'utf8');

    if (
        expectedBuffer.length !== suppliedBuffer.length ||
        !timingSafeEqual(expectedBuffer, suppliedBuffer)
    ) {
        return {
            valid: false,
            reason: 'invalid_signature',
        };
    }

    try {
        const payloadJson = base64UrlDecode(payloadSegment);
        const payload = JSON.parse(payloadJson) as JwtPayload;

        return {
            valid: true,
            payload,
        };
    } catch {
        return {
            valid: false,
            reason: 'malformed',
        };
    }
}
