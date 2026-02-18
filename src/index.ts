import { createRestoreServiceServer } from './server';
import { parseRestoreServiceEnv } from './env';
import { RequestAuthenticator } from './auth/authenticator';
import { RestoreOpsAdminService } from './admin/ops-admin-service';
import { RestoreEvidenceService } from './evidence/evidence-service';
import { RestoreExecutionService } from './execute/execute-service';
import { RestoreLockManager } from './locks/lock-manager';
import { SqliteRestoreJobStateStore } from './jobs/job-state-store';
import { RestoreJobService } from './jobs/job-service';
import { SqliteRestorePlanStateStore } from './plans/plan-state-store';
import { RestorePlanService } from './plans/plan-service';
import { SourceRegistry } from './registry/source-registry';

export * from './constants';

async function main(): Promise<void> {
    const env = parseRestoreServiceEnv(process.env);
    const sourceRegistry = new SourceRegistry(env.sourceMappings);
    const planStateStore = new SqliteRestorePlanStateStore(
        env.coreStateDbPath,
    );
    const jobStateStore = new SqliteRestoreJobStateStore(
        env.coreStateDbPath,
    );
    const lockManager = new RestoreLockManager();
    const jobs = new RestoreJobService(
        lockManager,
        sourceRegistry,
        undefined,
        jobStateStore,
    );
    const plans = new RestorePlanService(
        sourceRegistry,
        undefined,
        planStateStore,
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
        media_chunk_size: env.executeMediaChunkSize,
        media_max_items: env.executeMediaMaxItems,
        media_max_bytes: env.executeMediaMaxBytes,
        media_max_retry_attempts: env.executeMediaMaxRetryAttempts,
        evidence_signer_key_id: env.evidenceSignerKeyId,
        evidence_immutable_worm_enabled: env.evidenceImmutableWormEnabled,
        evidence_immutable_retention_class: env.evidenceImmutableRetentionClass,
        staging_mode_enabled: env.stagingModeEnabled,
        ga_runbooks_signed_off: env.gaRunbooksSignedOff,
        max_json_body_bytes: env.maxJsonBodyBytes,
        core_state_db_path: env.coreStateDbPath,
        mapping_count: sourceRegistry.list().length,
        port: env.port,
    });
}

if (require.main === module) {
    main().catch((error: unknown) => {
        console.error('restore-service failed to start', error);
        process.exitCode = 1;
    });
}
