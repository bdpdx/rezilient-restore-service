import {
    GetObjectCommand,
    S3Client,
    type S3ClientConfig,
} from '@aws-sdk/client-s3';
import type {
    RestoreArtifactBodyReader,
    RestoreArtifactBodyReference,
} from './materialization-service';

export type S3RestoreArtifactBodyReaderConfig = {
    accessKeyId?: string;
    bucket: string;
    endpoint?: string;
    forcePathStyle?: boolean;
    region: string;
    secretAccessKey?: string;
    sessionToken?: string;
};

function toUtf8String(
    value: Uint8Array,
): string {
    return Buffer.from(value).toString('utf8');
}

async function readStreamBody(
    body: AsyncIterable<unknown>,
): Promise<string> {
    const chunks: Buffer[] = [];

    for await (const chunk of body) {
        if (typeof chunk === 'string') {
            chunks.push(Buffer.from(chunk, 'utf8'));
            continue;
        }

        if (chunk instanceof Uint8Array) {
            chunks.push(Buffer.from(chunk));
            continue;
        }

        throw new Error('unsupported object-store body chunk type');
    }

    return Buffer.concat(chunks).toString('utf8');
}

async function readBodyAsString(
    body: unknown,
): Promise<string> {
    if (typeof body === 'string') {
        return body;
    }

    if (body instanceof Uint8Array) {
        return toUtf8String(body);
    }

    const maybeTransform = body as {
        transformToString?: (encoding?: string) => Promise<string>;
    };

    if (typeof maybeTransform.transformToString === 'function') {
        return maybeTransform.transformToString('utf8');
    }

    const maybeStream = body as AsyncIterable<unknown>;

    if (maybeStream && Symbol.asyncIterator in maybeStream) {
        return readStreamBody(maybeStream);
    }

    throw new Error('unsupported object-store body type');
}

function isNotFoundError(
    error: unknown,
): boolean {
    if (!error || typeof error !== 'object') {
        return false;
    }

    const record = error as {
        $metadata?: {
            httpStatusCode?: number;
        };
        Code?: string;
        code?: string;
        name?: string;
    };

    return (
        record.name === 'NoSuchKey'
        || record.name === 'NotFound'
        || record.Code === 'NoSuchKey'
        || record.Code === 'NotFound'
        || record.code === 'NoSuchKey'
        || record.code === 'NotFound'
        || record.$metadata?.httpStatusCode === 404
    );
}

export class S3RestoreArtifactBodyReader
implements RestoreArtifactBodyReader {
    private readonly bucket: string;

    constructor(
        private readonly client: S3Client,
        config: S3RestoreArtifactBodyReaderConfig,
    ) {
        this.bucket = String(config.bucket || '').trim();

        if (!this.bucket) {
            throw new Error('restore artifact bucket must not be empty');
        }
    }

    async readArtifactBody(
        reference: RestoreArtifactBodyReference,
    ): Promise<unknown | null> {
        const artifactKey = String(reference.artifactKey || '').trim();

        if (!artifactKey) {
            return null;
        }

        try {
            const response = await this.client.send(
                new GetObjectCommand({
                    Bucket: this.bucket,
                    Key: artifactKey,
                }),
            );

            if (!response.Body) {
                return null;
            }

            return JSON.parse(await readBodyAsString(response.Body)) as unknown;
        } catch (error: unknown) {
            if (isNotFoundError(error)) {
                return null;
            }

            throw error;
        }
    }
}

export function createS3RestoreArtifactBodyReader(
    config: S3RestoreArtifactBodyReaderConfig,
): RestoreArtifactBodyReader {
    const clientConfig: S3ClientConfig = {
        forcePathStyle: Boolean(config.forcePathStyle),
        region: config.region,
    };

    if (config.endpoint) {
        clientConfig.endpoint = config.endpoint;
    }

    if (config.accessKeyId && config.secretAccessKey) {
        clientConfig.credentials = {
            accessKeyId: config.accessKeyId,
            secretAccessKey: config.secretAccessKey,
            sessionToken: config.sessionToken,
        };
    }

    const client = new S3Client(clientConfig);

    return new S3RestoreArtifactBodyReader(client, config);
}
