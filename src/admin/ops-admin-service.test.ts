import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import {
    RestoreOpsAdminService,
} from './ops-admin-service';
import { RestoreJobService } from '../jobs/job-service';
import { RestorePlanService } from '../plans/plan-service';
import { RestoreEvidenceService } from '../evidence/evidence-service';
import { RestoreExecutionService } from '../execute/execute-service';
import { SourceRegistry } from '../registry/source-registry';
import {
    InMemoryRestoreIndexStateReader,
} from '../restore-index/state-reader';
import { RestoreWatermark as RestoreWatermarkSchema } from '@rezilient/types';

const NOW = new Date('2026-02-18T15:30:00.000Z');

function fixedNow(): Date {
    return new Date(NOW);
}

function buildMockJobService(
    jobs: Record<string, unknown>[] = [],
): RestoreJobService {
    return {
        listJobs: async () => jobs,
        getLockSnapshot: async () => ({
            running: [],
            queued: [],
        }),
    } as unknown as RestoreJobService;
}

function buildMockPlanService(
    plans: Record<string, unknown>[] = [],
): RestorePlanService {
    return {
        listPlans: async () => plans,
    } as unknown as RestorePlanService;
}

function buildMockEvidenceService(
    evidences: Record<string, unknown>[] = [],
): RestoreEvidenceService {
    return {
        listEvidence: async () => evidences,
    } as unknown as RestoreEvidenceService;
}

function buildMockExecutionService(
    executions: Record<string, unknown>[] = [],
): RestoreExecutionService {
    return {
        listExecutions: async () => executions,
    } as unknown as RestoreExecutionService;
}

function buildWatermark(
    overrides: Record<string, unknown> = {},
) {
    return {
        contract_version: 'restore.contracts.v1',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        topic: 'rez.cdc',
        partition: 0,
        generation_id: 'gen-01',
        indexed_through_offset: '100',
        indexed_through_time: '2026-02-18T15:29:30.000Z',
        coverage_start: '2026-02-18T00:00:00.000Z',
        coverage_end: '2026-02-18T15:29:30.000Z',
        freshness: 'fresh',
        executability: 'executable',
        reason_code: 'none',
        measured_at: '2026-02-18T15:30:00.000Z',
        ...overrides,
    };
}

describe('RestoreOpsAdminService', () => {
    test('getQueueDashboard returns running and queued jobs', async () => {
        const jobs = [
            {
                job_id: 'job-r1',
                status: 'running',
                requested_at: '2026-02-18T15:00:00.000Z',
                wait_reason_code: null,
            },
            {
                job_id: 'job-q1',
                status: 'queued',
                requested_at: '2026-02-18T15:01:00.000Z',
                wait_reason_code: 'queued_scope_lock',
            },
        ];
        const service = new RestoreOpsAdminService(
            buildMockJobService(jobs),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const dashboard = await service.getQueueDashboard();
        const totals = dashboard.totals as Record<
            string,
            number
        >;
        assert.equal(totals.running_jobs, 1);
        assert.equal(totals.queued_jobs, 1);
    });

    test('getQueueDashboard returns empty when no jobs', async () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const dashboard = await service.getQueueDashboard();
        const totals = dashboard.totals as Record<
            string,
            number
        >;
        assert.equal(totals.running_jobs, 0);
        assert.equal(totals.queued_jobs, 0);
        assert.equal(totals.paused_jobs, 0);
    });

    test('getFreshnessDashboard aggregates partition states', async () => {
        const registry = new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]);
        const indexReader =
            new InMemoryRestoreIndexStateReader();
        indexReader.upsertWatermark(
            RestoreWatermarkSchema.parse(
                buildWatermark({ partition: 0 }),
            ),
        );
        indexReader.upsertWatermark(
            RestoreWatermarkSchema.parse(
                buildWatermark({
                    partition: 1,
                    indexed_through_time:
                        '2026-02-18T15:27:00.000Z',
                }),
            ),
        );
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            registry,
            indexReader,
            { now: fixedNow },
        );
        const dashboard =
            await service.getFreshnessDashboard();
        const sources = dashboard.sources as Array<
            Record<string, unknown>
        >;
        assert.equal(sources.length, 1);
        assert.equal(sources[0].fresh_partitions, 1);
        assert.equal(sources[0].stale_partitions, 1);
    });

    test('getEvidenceDashboard counts verification statuses', async () => {
        const evidences = [
            {
                evidence: {
                    job_id: 'job-01',
                    evidence_id: 'ev-01',
                    plan_hash: 'h1',
                    report_hash: 'rh1',
                },
                generated_at: '2026-02-18T15:00:00.000Z',
                verification: {
                    signature_verification: 'verified',
                    reason_code: 'none',
                },
            },
            {
                evidence: {
                    job_id: 'job-02',
                    evidence_id: 'ev-02',
                    plan_hash: 'h2',
                    report_hash: 'rh2',
                },
                generated_at: '2026-02-18T15:01:00.000Z',
                verification: {
                    signature_verification:
                        'verification_failed',
                    reason_code: 'signature_mismatch',
                },
            },
        ];
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(evidences),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const dashboard =
            await service.getEvidenceDashboard();
        const totals = dashboard.totals as Record<
            string,
            number
        >;
        assert.equal(totals.total, 2);
        assert.equal(totals.verified, 1);
        assert.equal(totals.verification_failed, 1);
    });

    test('getSloDashboard compares against SLO targets', async () => {
        const executions = [
            {
                job_id: 'job-01',
                status: 'completed',
                started_at: '2026-02-18T15:00:00.000Z',
                completed_at: '2026-02-18T15:01:00.000Z',
            },
        ];
        const jobs = [
            {
                job_id: 'job-01',
                status: 'completed',
                requested_at: '2026-02-18T14:59:50.000Z',
                started_at: '2026-02-18T15:00:00.000Z',
                completed_at: '2026-02-18T15:01:00.000Z',
                status_reason_code: 'none',
            },
        ];
        const service = new RestoreOpsAdminService(
            buildMockJobService(jobs),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(executions),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const dashboard = await service.getSloDashboard();
        const burnRate = dashboard.burn_rate as Record<
            string,
            unknown
        >;
        assert.equal(burnRate.status, 'within_budget');
        assert.equal(burnRate.severity, 'normal');
    });

    test('getSloDashboard detects SLO violations', async () => {
        const executions = [
            {
                job_id: 'job-fail',
                status: 'failed',
                started_at: '2026-02-18T15:00:00.000Z',
                completed_at: '2026-02-18T15:01:00.000Z',
            },
        ];
        const jobs = [
            {
                job_id: 'job-fail',
                status: 'completed',
                requested_at: '2026-02-18T14:59:50.000Z',
                started_at: '2026-02-18T15:00:00.000Z',
                completed_at: '2026-02-18T15:01:00.000Z',
                status_reason_code: 'none',
            },
        ];
        const service = new RestoreOpsAdminService(
            buildMockJobService(jobs),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(executions),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const dashboard = await service.getSloDashboard();
        const burnRate = dashboard.burn_rate as Record<
            string,
            unknown
        >;
        assert.equal(burnRate.status, 'breached');
        assert.equal(burnRate.severity, 'critical');
    });

    test('getGaReadinessDashboard reflects staging mode', async () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            {
                now: fixedNow,
                stagingModeEnabled: true,
            },
        );
        const dashboard =
            await service.getGaReadinessDashboard();
        const staging = dashboard.staging_mode as Record<
            string,
            unknown
        >;
        assert.equal(staging.enabled, true);
    });

    test('getGaReadinessDashboard reflects runbook signoff', async () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            {
                now: fixedNow,
                runbooksSignedOff: true,
            },
        );
        const dashboard =
            await service.getGaReadinessDashboard();
        const runbooks = dashboard.runbooks as Record<
            string,
            unknown
        >;
        assert.equal(runbooks.signed_off, true);
    });

    test('getGaReadinessDashboard reflects failure drill status', async () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            {
                now: fixedNow,
                requiredFailureDrills: ['auth_outage'],
            },
        );
        service.recordFailureDrillResult(
            'auth_outage',
            'passed',
            'admin@example.com',
        );
        const dashboard =
            await service.getGaReadinessDashboard();
        const drills = dashboard.failure_drills as Record<
            string,
            unknown
        >;
        const totals = drills.totals as Record<
            string,
            number
        >;
        assert.equal(totals.passed, 1);
        assert.equal(totals.pending, 0);
    });

    test('setStagingMode toggles with audit record', () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const result = service.setStagingMode(
            true,
            'admin@example.com',
        );
        const staging = result.staging_mode as Record<
            string,
            unknown
        >;
        assert.equal(staging.enabled, true);
        assert.equal(
            staging.updated_by,
            'admin@example.com',
        );

        const result2 = service.setStagingMode(
            false,
            'ops@example.com',
        );
        const staging2 = result2.staging_mode as Record<
            string,
            unknown
        >;
        assert.equal(staging2.enabled, false);
        assert.equal(
            staging2.updated_by,
            'ops@example.com',
        );
    });

    test('setRunbookSignoff toggles with audit record', () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            { now: fixedNow },
        );
        const result = service.setRunbookSignoff(
            true,
            'sre@example.com',
        );
        const runbooks = result.runbooks as Record<
            string,
            unknown
        >;
        assert.equal(runbooks.signed_off, true);
        assert.equal(
            runbooks.updated_by,
            'sre@example.com',
        );
    });

    test('recordFailureDrillResult persists drill outcome', () => {
        const service = new RestoreOpsAdminService(
            buildMockJobService(),
            buildMockPlanService(),
            buildMockEvidenceService(),
            buildMockExecutionService(),
            new SourceRegistry([]),
            new InMemoryRestoreIndexStateReader(),
            {
                now: fixedNow,
                requiredFailureDrills: ['crash_resume'],
            },
        );
        const result = service.recordFailureDrillResult(
            'crash_resume',
            'passed',
            'sre@example.com',
            'drill completed successfully',
        );
        assert.equal(result.success, true);
        assert.ok(result.record);
        assert.equal(result.record.drill_id, 'crash_resume');
        assert.equal(result.record.status, 'passed');
        assert.equal(
            result.record.notes,
            'drill completed successfully',
        );

        const unknown = service.recordFailureDrillResult(
            'nonexistent_drill',
            'passed',
            'sre@example.com',
        );
        assert.equal(unknown.success, false);
    });
});
