import { SourceMappingInput } from './registry/source-registry';

export interface RestoreServiceEnv {
    port: number;
    adminToken: string;
    authSigningKey: string;
    authClockSkewSeconds: number;
    authExpectedIssuer?: string;
    maxJsonBodyBytes: number;
    restorePgUrl: string;
    restoreIndexStaleAfterSeconds: number;
    executeDefaultChunkSize: number;
    executeMaxRows: number;
    executeElevatedSkipRatioPercent: number;
    executeMaxChunksPerAttempt: number;
    executeMediaChunkSize: number;
    executeMediaMaxItems: number;
    executeMediaMaxBytes: number;
    executeMediaMaxRetryAttempts: number;
    evidenceSignerKeyId: string;
    evidenceSigningPrivateKeyPem: string;
    evidenceSigningPublicKeyPem: string;
    evidenceImmutableWormEnabled: boolean;
    evidenceImmutableRetentionClass: string;
    stagingModeEnabled: boolean;
    gaRunbooksSignedOff: boolean;
    sourceMappings: SourceMappingInput[];
}

function parseNonNegativeInteger(
    raw: string | undefined,
    fieldName: string,
    defaultValue: number,
): number {
    if (!raw || raw.trim() === '') {
        return defaultValue;
    }

    const parsed = Number(raw);

    if (!Number.isInteger(parsed) || parsed < 0) {
        throw new Error(`${fieldName} must be a non-negative integer`);
    }

    return parsed;
}

function parseStrictPositiveInteger(
    raw: string | undefined,
    fieldName: string,
    defaultValue: number,
): number {
    const parsed = parseNonNegativeInteger(raw, fieldName, defaultValue);

    if (parsed <= 0) {
        throw new Error(`${fieldName} must be greater than zero`);
    }

    return parsed;
}

function parsePercentage(
    raw: string | undefined,
    fieldName: string,
    defaultValue: number,
): number {
    if (!raw || raw.trim() === '') {
        return defaultValue;
    }

    const parsed = Number(raw);

    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 100) {
        throw new Error(
            `${fieldName} must be a number between 0 and 100`,
        );
    }

    return parsed;
}

function parseSourceMappings(
    raw: string | undefined,
): SourceMappingInput[] {
    if (!raw || raw.trim() === '') {
        return [];
    }

    let parsed: unknown;

    try {
        parsed = JSON.parse(raw);
    } catch {
        throw new Error('RRS_SOURCE_MAPPINGS_JSON must be valid JSON');
    }

    if (!Array.isArray(parsed)) {
        throw new Error(
            'RRS_SOURCE_MAPPINGS_JSON must be a JSON array',
        );
    }

    return parsed.map((entry, index) => {
        if (!entry || typeof entry !== 'object') {
            throw new Error(
                `RRS_SOURCE_MAPPINGS_JSON[${index}] must be an object`,
            );
        }

        const mapping = entry as Record<string, unknown>;
        const tenantId = mapping.tenant_id;
        const instanceId = mapping.instance_id;
        const source = mapping.source;

        if (typeof tenantId !== 'string' || tenantId.trim() === '') {
            throw new Error(
                `RRS_SOURCE_MAPPINGS_JSON[${index}].tenant_id ` +
                'must be non-empty',
            );
        }

        if (typeof instanceId !== 'string' || instanceId.trim() === '') {
            throw new Error(
                `RRS_SOURCE_MAPPINGS_JSON[${index}].instance_id ` +
                'must be non-empty',
            );
        }

        if (typeof source !== 'string' || source.trim() === '') {
            throw new Error(
                `RRS_SOURCE_MAPPINGS_JSON[${index}].source ` +
                'must be non-empty',
            );
        }

        return {
            tenantId,
            instanceId,
            source,
        };
    });
}

function parseBoolean(
    raw: string | undefined,
    fieldName: string,
    defaultValue: boolean,
): boolean {
    if (!raw || raw.trim() === '') {
        return defaultValue;
    }

    const normalized = raw.trim().toLowerCase();

    if (
        normalized === '1' ||
        normalized === 'true' ||
        normalized === 'yes' ||
        normalized === 'on'
    ) {
        return true;
    }

    if (
        normalized === '0' ||
        normalized === 'false' ||
        normalized === 'no' ||
        normalized === 'off'
    ) {
        return false;
    }

    throw new Error(`${fieldName} must be a boolean value`);
}

function parseOptionalString(raw: string | undefined): string | undefined {
    if (!raw) {
        return undefined;
    }

    const trimmed = raw.trim();

    return trimmed ? trimmed : undefined;
}

function parseRequiredString(
    raw: string | undefined,
    fieldName: string,
): string {
    const parsed = parseOptionalString(raw);

    if (!parsed) {
        throw new Error(`${fieldName} is required`);
    }

    return parsed;
}

export function parseRestoreServiceEnv(
    env: NodeJS.ProcessEnv,
): RestoreServiceEnv {
    const authExpectedIssuer = parseOptionalString(
        env.RRS_AUTH_EXPECTED_ISSUER,
    );

    return {
        port: parseNonNegativeInteger(env.PORT, 'PORT', 3100),
        adminToken: parseRequiredString(
            env.RRS_ADMIN_TOKEN,
            'RRS_ADMIN_TOKEN',
        ),
        authSigningKey: parseRequiredString(
            env.RRS_AUTH_SIGNING_KEY,
            'RRS_AUTH_SIGNING_KEY',
        ),
        authClockSkewSeconds: parseNonNegativeInteger(
            env.RRS_AUTH_TOKEN_CLOCK_SKEW_SECONDS,
            'RRS_AUTH_TOKEN_CLOCK_SKEW_SECONDS',
            30,
        ),
        authExpectedIssuer,
        maxJsonBodyBytes: parseStrictPositiveInteger(
            env.RRS_MAX_JSON_BODY_BYTES,
            'RRS_MAX_JSON_BODY_BYTES',
            1048576,
        ),
        restorePgUrl: parseRequiredString(
            env.REZ_RESTORE_PG_URL || env.RRS_RESTORE_PG_URL,
            'REZ_RESTORE_PG_URL',
        ),
        restoreIndexStaleAfterSeconds: parseStrictPositiveInteger(
            env.RRS_RESTORE_INDEX_STALE_AFTER_SECONDS,
            'RRS_RESTORE_INDEX_STALE_AFTER_SECONDS',
            120,
        ),
        executeDefaultChunkSize: parseStrictPositiveInteger(
            env.RRS_EXECUTE_DEFAULT_CHUNK_SIZE,
            'RRS_EXECUTE_DEFAULT_CHUNK_SIZE',
            100,
        ),
        executeMaxRows: parseStrictPositiveInteger(
            env.RRS_EXECUTE_MAX_ROWS,
            'RRS_EXECUTE_MAX_ROWS',
            10000,
        ),
        executeElevatedSkipRatioPercent: parsePercentage(
            env.RRS_EXECUTE_ELEVATED_SKIP_RATIO_PERCENT,
            'RRS_EXECUTE_ELEVATED_SKIP_RATIO_PERCENT',
            20,
        ),
        executeMaxChunksPerAttempt: parseNonNegativeInteger(
            env.RRS_EXECUTE_MAX_CHUNKS_PER_ATTEMPT,
            'RRS_EXECUTE_MAX_CHUNKS_PER_ATTEMPT',
            0,
        ),
        executeMediaChunkSize: parseStrictPositiveInteger(
            env.RRS_MEDIA_CHUNK_SIZE,
            'RRS_MEDIA_CHUNK_SIZE',
            25,
        ),
        executeMediaMaxItems: parseStrictPositiveInteger(
            env.RRS_MEDIA_MAX_ITEMS,
            'RRS_MEDIA_MAX_ITEMS',
            1000,
        ),
        executeMediaMaxBytes: parseStrictPositiveInteger(
            env.RRS_MEDIA_MAX_BYTES,
            'RRS_MEDIA_MAX_BYTES',
            104857600,
        ),
        executeMediaMaxRetryAttempts: parseStrictPositiveInteger(
            env.RRS_MEDIA_MAX_RETRY_ATTEMPTS,
            'RRS_MEDIA_MAX_RETRY_ATTEMPTS',
            3,
        ),
        evidenceSignerKeyId: parseOptionalString(
            env.RRS_EVIDENCE_SIGNER_KEY_ID,
        ) || 'rrs-dev-ed25519-v1',
        evidenceSigningPrivateKeyPem: parseRequiredString(
            env.RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
            'RRS_EVIDENCE_SIGNING_PRIVATE_KEY_PEM',
        ),
        evidenceSigningPublicKeyPem: parseRequiredString(
            env.RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
            'RRS_EVIDENCE_SIGNING_PUBLIC_KEY_PEM',
        ),
        evidenceImmutableWormEnabled: parseBoolean(
            env.RRS_EVIDENCE_IMMUTABLE_WORM_ENABLED,
            'RRS_EVIDENCE_IMMUTABLE_WORM_ENABLED',
            false,
        ),
        evidenceImmutableRetentionClass: parseOptionalString(
            env.RRS_EVIDENCE_IMMUTABLE_RETENTION_CLASS,
        ) || 'standard-30d',
        stagingModeEnabled: parseBoolean(
            env.RRS_STAGING_MODE_ENABLED,
            'RRS_STAGING_MODE_ENABLED',
            false,
        ),
        gaRunbooksSignedOff: parseBoolean(
            env.RRS_GA_RUNBOOKS_SIGNED_OFF,
            'RRS_GA_RUNBOOKS_SIGNED_OFF',
            false,
        ),
        sourceMappings: parseSourceMappings(env.RRS_SOURCE_MAPPINGS_JSON),
    };
}
