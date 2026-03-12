import { createRestoreServiceServer } from './server';
import { parseRestoreServiceEnv } from './env';
import { RequestAuthenticator } from './auth/authenticator';
import { RestoreOpsAdminService } from './admin/ops-admin-service';
import { RestoreEvidenceService } from './evidence/evidence-service';
import { PostgresRestoreEvidenceStateStore } from './evidence/evidence-state-store';
import { RestoreExecutionService } from './execute/execute-service';
import { PostgresRestoreExecutionStateStore } from './execute/execute-state-store';
import { NoopRestoreTargetWriter } from './execute/models';
import { RestoreLockManager } from './locks/lock-manager';
import { PostgresRestoreJobStateStore } from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { PostgresRestorePlanStateStore } from './plans/plan-state-store';
import { RestorePlanService } from './plans/plan-service';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationService,
} from './plans/materialization-service';
import {
    createS3RestoreArtifactBodyReader,
} from './plans/s3-artifact-body-reader';
import { AcpSourceMappingClient } from './registry/acp-source-mapping-client';
import { createCachedAcpSourceMappingProvider } from './registry/acp-source-mapping-provider';
import { PostgresRestoreIndexStateReader } from './restore-index/state-reader';
import { Pool } from 'pg';

export * from './constants';

async function main(): Promise<void> {
    const env = parseRestoreServiceEnv(process.env);
    const acpSourceMappingClient = new AcpSourceMappingClient({
        baseUrl: env.acpBaseUrl,
        internalToken: env.acpInternalToken,
        timeoutMs: env.acpRequestTimeoutMs,
        defaultServiceScope: 'rrs',
    });
    const acpSourceMappingProvider = createCachedAcpSourceMappingProvider(
        acpSourceMappingClient,
        {
            positiveTtlSeconds: env.acpPositiveCacheTtlSeconds,
            negativeTtlSeconds: env.acpNegativeCacheTtlSeconds,
        },
    );
    const statePool = new Pool({
        allowExitOnIdle: false,
        connectionString: env.restorePgUrl,
        idleTimeoutMillis: 30000,
        max: 10,
    });
    const planStateStore = new PostgresRestorePlanStateStore(
        env.restorePgUrl,
        {
            pool: statePool,
        },
    );
    const jobStateStore = new PostgresRestoreJobStateStore(
        env.restorePgUrl,
        {
            pool: statePool,
        },
    );
    const executeStateStore = new PostgresRestoreExecutionStateStore(
        env.restorePgUrl,
        {
            pool: statePool,
        },
    );
    const evidenceStateStore = new PostgresRestoreEvidenceStateStore(
        env.restorePgUrl,
        {
            pool: statePool,
        },
    );
    const lockManager = new RestoreLockManager();
    const restoreIndexStateReader = new PostgresRestoreIndexStateReader(
        statePool,
        {
            staleAfterSeconds: env.restoreIndexStaleAfterSeconds,
        },
    );
    const artifactBodyReader = env.objectStoreBucket && env.objectStoreRegion
        ? createS3RestoreArtifactBodyReader({
            accessKeyId: env.objectStoreAccessKeyId,
            bucket: env.objectStoreBucket,
            endpoint: env.objectStoreEndpoint,
            forcePathStyle: env.objectStoreForcePathStyle,
            region: env.objectStoreRegion,
            secretAccessKey: env.objectStoreSecretAccessKey,
            sessionToken: env.objectStoreSessionToken,
        })
        : new InMemoryRestoreArtifactBodyReader();
    const plans = new RestorePlanService(
        undefined,
        undefined,
        planStateStore,
        restoreIndexStateReader,
        acpSourceMappingProvider,
        new RestoreRowMaterializationService(artifactBodyReader),
    );
    const jobs = new RestoreJobService(
        lockManager,
        undefined,
        undefined,
        jobStateStore,
        acpSourceMappingProvider,
        {
            async getFinalizedPlan(planId: string) {
                const plan = await plans.getPlan(planId);

                if (!plan) {
                    return null;
                }

                return {
                    plan_id: plan.plan.plan_id,
                    plan_hash: plan.plan.plan_hash,
                    gate: {
                        executability: plan.gate.executability,
                        reason_code: plan.gate.reason_code,
                    },
                };
            },
        },
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {
            defaultChunkSize: env.executeDefaultChunkSize,
            maxRows: env.executeMaxRows,
            elevatedSkipRatioPercent: env.executeElevatedSkipRatioPercent,
            maxChunksPerAttempt: env.executeMaxChunksPerAttempt,
            mediaChunkSize: env.executeMediaChunkSize,
            mediaMaxItems: env.executeMediaMaxItems,
            mediaMaxBytes: env.executeMediaMaxBytes,
            mediaMaxRetryAttempts: env.executeMediaMaxRetryAttempts,
        },
        undefined,
        executeStateStore,
        new NoopRestoreTargetWriter(),
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        {
            signer: {
                signer_key_id: env.evidenceSignerKeyId,
                private_key_pem: env.evidenceSigningPrivateKeyPem,
                public_key_pem: env.evidenceSigningPublicKeyPem,
            },
            immutable_storage: {
                worm_enabled: env.evidenceImmutableWormEnabled,
                retention_class: env.evidenceImmutableRetentionClass,
            },
        },
        undefined,
        evidenceStateStore,
    );
    const authenticator = new RequestAuthenticator({
        signingKey: env.authSigningKey,
        tokenClockSkewSeconds: env.authClockSkewSeconds,
        expectedIssuer: env.authExpectedIssuer,
    });
    const admin = new RestoreOpsAdminService(
        jobs,
        plans,
        evidence,
        execute,
        acpSourceMappingProvider,
        restoreIndexStateReader,
        {
            stagingModeEnabled: env.stagingModeEnabled,
            runbooksSignedOff: env.gaRunbooksSignedOff,
        },
    );
    const server = createRestoreServiceServer({
        admin,
        authenticator,
        evidence,
        execute,
        jobs,
        plans,
    }, {
        adminToken: env.adminToken,
        maxJsonBodyBytes: env.maxJsonBodyBytes,
        executePreflightReconcileStaleAfterMs:
            env.executePreflightReconcileStaleAfterMs,
    });

    await new Promise<void>((resolve) => {
        server.listen(env.port, '0.0.0.0', () => {
            resolve();
        });
    });

    console.log('restore-service listening', {
        execute_default_chunk_size: env.executeDefaultChunkSize,
        execute_max_rows: env.executeMaxRows,
        execute_skip_ratio_percent: env.executeElevatedSkipRatioPercent,
        execute_max_chunks_per_attempt: env.executeMaxChunksPerAttempt,
        execute_preflight_reconcile_stale_after_ms:
            env.executePreflightReconcileStaleAfterMs,
        media_chunk_size: env.executeMediaChunkSize,
        media_max_items: env.executeMediaMaxItems,
        media_max_bytes: env.executeMediaMaxBytes,
        media_max_retry_attempts: env.executeMediaMaxRetryAttempts,
        evidence_signer_key_id: env.evidenceSignerKeyId,
        evidence_immutable_worm_enabled: env.evidenceImmutableWormEnabled,
        evidence_immutable_retention_class: env.evidenceImmutableRetentionClass,
        restore_index_stale_after_seconds: env.restoreIndexStaleAfterSeconds,
        staging_mode_enabled: env.stagingModeEnabled,
        ga_runbooks_signed_off: env.gaRunbooksSignedOff,
        max_json_body_bytes: env.maxJsonBodyBytes,
        object_store_bucket_configured: Boolean(env.objectStoreBucket),
        restore_pg_url_configured: env.restorePgUrl.length > 0,
        port: env.port,
    });
}

if (require.main === module) {
    main().catch((error: unknown) => {
        console.error('restore-service failed to start', error);
        process.exitCode = 1;
    });
}
