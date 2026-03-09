import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import type { S3Client } from '@aws-sdk/client-s3';
import {
    S3RestoreArtifactBodyReader,
} from './s3-artifact-body-reader';

type GetResponse = {
    Body?: unknown;
};

function createMockS3Client(options: {
    getResponses?: Record<string, GetResponse>;
    getError?: unknown;
}) {
    const getCalls: string[] = [];

    return {
        client: {
            send: async (command: unknown) => {
                const cmd = command as {
                    input?: Record<string, unknown>;
                };

                if (!cmd.input || !('Key' in cmd.input)) {
                    throw new Error('unknown command');
                }

                const key = String(cmd.input.Key || '');
                getCalls.push(key);

                if (options.getError) {
                    throw options.getError;
                }

                const response = options.getResponses?.[key];

                if (!response) {
                    throw new Error(`key not found: ${key}`);
                }

                return response;
            },
        } as unknown as S3Client,
        getCalls,
    };
}

function createReader(
    client: S3Client,
    bucket = 'test-bucket',
) {
    return new S3RestoreArtifactBodyReader(client, {
        bucket,
        region: 'us-west-2',
    });
}

describe('S3RestoreArtifactBodyReader constructor', () => {
    it('throws when bucket is empty', () => {
        const { client } = createMockS3Client({});
        assert.throws(
            () => createReader(client, ''),
            /bucket must not be empty/,
        );
    });
});

describe('S3RestoreArtifactBodyReader.readArtifactBody', () => {
    it('returns null when artifact key is empty', async () => {
        const { client, getCalls } = createMockS3Client({});
        const reader = createReader(client);
        const value = await reader.readArtifactBody({
            artifactKey: '',
            manifestKey: 'manifest',
        });
        assert.equal(value, null);
        assert.deepEqual(getCalls, []);
    });

    it('parses JSON body from string response', async () => {
        const { client, getCalls } = createMockS3Client({
            getResponses: {
                'rez/key.artifact.json': {
                    Body: '{"hello":"world"}',
                },
            },
        });
        const reader = createReader(client);
        const value = await reader.readArtifactBody({
            artifactKey: 'rez/key.artifact.json',
            manifestKey: 'rez/key.manifest.json',
        });

        assert.deepEqual(value, {
            hello: 'world',
        });
        assert.deepEqual(getCalls, ['rez/key.artifact.json']);
    });

    it('parses JSON body from async iterable response', async () => {
        async function* bodyStream() {
            yield Buffer.from('{"chunk":');
            yield Buffer.from('"ok"}');
        }

        const { client } = createMockS3Client({
            getResponses: {
                'rez/key.artifact.json': {
                    Body: bodyStream(),
                },
            },
        });
        const reader = createReader(client);
        const value = await reader.readArtifactBody({
            artifactKey: 'rez/key.artifact.json',
            manifestKey: 'rez/key.manifest.json',
        });

        assert.deepEqual(value, {
            chunk: 'ok',
        });
    });

    it('returns null when S3 object is not found', async () => {
        const { client } = createMockS3Client({
            getError: {
                name: 'NoSuchKey',
                $metadata: {
                    httpStatusCode: 404,
                },
            },
        });
        const reader = createReader(client);
        const value = await reader.readArtifactBody({
            artifactKey: 'rez/missing.artifact.json',
            manifestKey: 'rez/missing.manifest.json',
        });

        assert.equal(value, null);
    });

    it('returns null when response body is missing', async () => {
        const { client } = createMockS3Client({
            getResponses: {
                'rez/key.artifact.json': {},
            },
        });
        const reader = createReader(client);
        const value = await reader.readArtifactBody({
            artifactKey: 'rez/key.artifact.json',
            manifestKey: 'rez/key.manifest.json',
        });

        assert.equal(value, null);
    });

    it('propagates invalid JSON bodies', async () => {
        const { client } = createMockS3Client({
            getResponses: {
                'rez/key.artifact.json': {
                    Body: '{not-json}',
                },
            },
        });
        const reader = createReader(client);

        await assert.rejects(
            reader.readArtifactBody({
                artifactKey: 'rez/key.artifact.json',
                manifestKey: 'rez/key.manifest.json',
            }),
            /JSON|property name/,
        );
    });
});
