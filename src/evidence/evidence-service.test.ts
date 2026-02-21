import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';
import { RestoreExecutionService } from '../execute/execute-service';
import { RestoreJobService } from '../jobs/job-service';
import { RestoreLockManager } from '../locks/lock-manager';
import { RestorePlanService } from '../plans/plan-service';
import { SourceRegistry } from '../registry/source-registry';
import { InMemoryRestoreIndexStateReader } from '../restore-index/state-reader';
import {
    TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
    TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
} from '../test-helpers';
import { RestoreEvidenceService } from './evidence-service';

const FIXED_NOW = new Date('2026-02-17T01:02:03.000Z');

function now(): Date {
    return new Date(FIXED_NOW.getTime());
}

function claims() {
    return {
        iss: 'rez-auth-control-plane',
        sub: 'client-1',
        aud: 'rezilient:rrs',
        jti: 'tok-1',
        iat: 100,
        exp: 200,
        service_scope: 'rrs' as const,
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
    };
}

function createRow(rowId: string) {
    return {
        row_id: rowId,
        table: 'incident',
        record_sys_id: `rec-${rowId}`,
        action: 'update' as const,
        precondition_hash: 'a'.repeat(64),
        metadata: {
            allowlist_version: 'rrs.metadata.allowlist.v1',
            metadata: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                table: 'incident',
                record_sys_id: `rec-${rowId}`,
                event_id: `evt-${rowId}`,
                event_type: 'cdc.write',
                operation: 'U',
                schema_version: 3,
                sys_updated_on: '2026-02-16 11:59:59',
                sys_mod_count: 2,
                __time: '2026-02-16T11:59:59.000Z',
                topic: 'rez.cdc',
                partition: 1,
                offset: '100',
            },
        },
        values: {
            diff_enc: {
                alg: 'AES-256-CBC',
                module: 'x_rezrp_rezilient.encrypter',
                format: 'kmf',
                compression: 'none',
                ciphertext: `cipher-${rowId}`,
            },
        },
    };
}

function createAuthoritativeWatermark(): Record<string, unknown> {
    const indexedThrough = FIXED_NOW.toISOString();

    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 1,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: indexedThrough,
        coverage_start: '2026-02-16T00:00:00.000Z',
        coverage_end: indexedThrough,
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: indexedThrough,
    };
}

async function createFixture(): Promise<{
    evidence: RestoreEvidenceService;
    jobId: string;
}> {
    const sourceRegistry = new SourceRegistry([
        {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
        },
    ]);
    const restoreIndexReader = new InMemoryRestoreIndexStateReader();

    restoreIndexReader.upsertWatermark(RestoreWatermarkSchema.parse(
        createAuthoritativeWatermark(),
    ));

    const plans = new RestorePlanService(
        sourceRegistry,
        now,
        undefined,
        restoreIndexReader,
    );
    const jobs = new RestoreJobService(
        new RestoreLockManager(),
        sourceRegistry,
        now,
    );
    const execute = new RestoreExecutionService(
        jobs,
        plans,
        {},
        now,
    );
    const evidence = new RestoreEvidenceService(
        jobs,
        plans,
        execute,
        {
            signer: {
                signer_key_id: 'rrs-test-signer',
                private_key_pem: TEST_EVIDENCE_SIGNING_PRIVATE_KEY_PEM,
                public_key_pem: TEST_EVIDENCE_SIGNING_PUBLIC_KEY_PEM,
            },
            immutable_storage: {
                worm_enabled: true,
                retention_class: 'compliance-7y',
            },
        },
        now,
    );
    const dryRun = await plans.createDryRunPlan(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: 'plan-evidence-1',
            requested_by: 'operator@example.com',
            pit: {
                restore_time: '2026-02-16T12:00:00.000Z',
                restore_timezone: 'UTC',
                pit_algorithm_version:
                    'pit.v1.sys_updated_on-sys_mod_count-__time-event_id',
                tie_breaker: [
                    'sys_updated_on',
                    'sys_mod_count',
                    '__time',
                    'event_id',
                ],
                tie_breaker_fallback: [
                    'sys_updated_on',
                    '__time',
                    'event_id',
                ],
            },
            scope: {
                mode: 'record',
                tables: ['incident'],
                encoded_query: 'active=true',
            },
            execution_options: {
                missing_row_mode: 'existing_only',
                conflict_policy: 'review_required',
                schema_compatibility_mode: 'compatible_only',
                workflow_mode: 'suppressed_default',
            },
            rows: [createRow('row-01'), createRow('row-02')],
            conflicts: [],
            delete_candidates: [],
            media_candidates: [],
            watermarks: [
                {
                    contract_version: 'restore.contracts.v1',
                    tenant_id: 'tenant-acme',
                    instance_id: 'sn-dev-01',
                    source: 'sn://acme-dev.service-now.com',
                    topic: 'rez.cdc',
                    partition: 1,
                    generation_id: 'gen-01',
                    indexed_through_offset: '100',
                    indexed_through_time: '2026-02-16T12:00:00.000Z',
                    coverage_start: '2026-02-16T00:00:00.000Z',
                    coverage_end: '2026-02-16T12:00:00.000Z',
                    freshness: 'fresh',
                    executability: 'executable',
                    reason_code: 'none',
                    measured_at: '2026-02-16T12:00:00.000Z',
                },
            ],
            pit_candidates: [],
        },
        claims(),
    );

    assert.equal(dryRun.success, true);
    if (!dryRun.success) {
        throw new Error('failed to create dry-run fixture');
    }

    const job = await jobs.createJob(
        {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            plan_id: dryRun.record.plan.plan_id,
            plan_hash: dryRun.record.plan.plan_hash,
            lock_scope_tables: ['incident'],
            required_capabilities: ['restore_execute'],
            requested_by: 'operator@example.com',
        },
        claims(),
    );

    assert.equal(job.success, true);
    if (!job.success) {
        throw new Error('failed to create job fixture');
    }

    const executed = await execute.executeJob(
        job.job.job_id,
        {
            operator_id: 'operator@example.com',
            operator_capabilities: ['restore_execute'],
        },
        claims(),
    );

    assert.equal(executed.success, true);
    if (!executed.success) {
        throw new Error('failed to execute job fixture');
    }

    assert.equal(executed.record.status, 'completed');

    return {
        evidence,
        jobId: job.job.job_id,
    };
}

test('evidence export is deterministic and includes signed verification status', async () => {
    const fixture = await createFixture();
    const first = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(first.success, true);
    if (!first.success) {
        return;
    }

    assert.equal(first.statusCode, 201);
    assert.equal(first.reused, false);
    assert.equal(first.record.evidence.plan_hash.length, 64);
    assert.equal(first.record.evidence.report_hash.length, 64);
    assert.equal(
        first.record.evidence.manifest_signature.signature_verification,
        'verified',
    );
    assert.ok(first.record.evidence.artifact_hashes.length >= 3);
    assert.ok(first.record.evidence.resume_metadata.checkpoint_id);

    const second = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(second.success, true);
    if (!second.success) {
        return;
    }

    assert.equal(second.statusCode, 200);
    assert.equal(second.reused, true);
    assert.equal(
        second.record.evidence.evidence_id,
        first.record.evidence.evidence_id,
    );
    assert.equal(
        second.record.evidence.report_hash,
        first.record.evidence.report_hash,
    );
});

test('evidence verification detects report-hash tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.evidence.execution_outcomes.rows_applied = 999;

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_report_hash_mismatch',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});

test('evidence verification detects artifact hash tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.artifacts[0].canonical_json =
        tampered.artifacts[0].canonical_json + ' ';

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_artifact_hash_mismatch',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});

test('evidence verification detects signature tampering', async () => {
    const fixture = await createFixture();
    const exported = await fixture.evidence.exportEvidence(fixture.jobId);

    assert.equal(exported.success, true);
    if (!exported.success) {
        return;
    }

    const tampered = JSON.parse(
        JSON.stringify(exported.record),
    ) as typeof exported.record;

    tampered.evidence.manifest_signature.signature = 'not-a-valid-signature';

    const verification = fixture.evidence.validateEvidenceRecord(tampered);

    assert.equal(
        verification.reason_code,
        'failed_evidence_signature_verification',
    );
    assert.equal(verification.signature_verification, 'verification_failed');
});
