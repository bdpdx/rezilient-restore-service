import { RestoreEvidenceService } from '../evidence/evidence-service';
import { RestoreJobService } from '../jobs/job-service';
import { RestorePlanService } from '../plans/plan-service';

interface FreshnessSummaryRow {
    tenant_id: string;
    instance_id: string;
    source: string;
    fresh_partitions: number;
    stale_partitions: number;
    unknown_partitions: number;
    executable_partitions: number;
    preview_only_partitions: number;
    blocked_partitions: number;
    coverage_start: string | null;
    coverage_end: string | null;
    latest_measured_at: string | null;
    backfill_status: 'up_to_date' | 'gap_repair_required' | 'bootstrap_required';
}

interface FreshnessAccumulator {
    tenant_id: string;
    instance_id: string;
    source: string;
    fresh_partitions: number;
    stale_partitions: number;
    unknown_partitions: number;
    executable_partitions: number;
    preview_only_partitions: number;
    blocked_partitions: number;
    coverage_start: string | null;
    coverage_end: string | null;
    latest_measured_at: string | null;
}

function sourceKey(input: {
    tenant_id: string;
    instance_id: string;
    source: string;
}): string {
    return `${input.tenant_id}|${input.instance_id}|${input.source}`;
}

function minIso(left: string | null, right: string): string {
    if (!left) {
        return right;
    }

    return left.localeCompare(right) <= 0 ? left : right;
}

function maxIso(left: string | null, right: string): string {
    if (!left) {
        return right;
    }

    return left.localeCompare(right) >= 0 ? left : right;
}

function toFreshnessRow(acc: FreshnessAccumulator): FreshnessSummaryRow {
    const backfillStatus = acc.unknown_partitions > 0
        ? 'bootstrap_required'
        : acc.stale_partitions > 0
        ? 'gap_repair_required'
        : 'up_to_date';

    return {
        tenant_id: acc.tenant_id,
        instance_id: acc.instance_id,
        source: acc.source,
        fresh_partitions: acc.fresh_partitions,
        stale_partitions: acc.stale_partitions,
        unknown_partitions: acc.unknown_partitions,
        executable_partitions: acc.executable_partitions,
        preview_only_partitions: acc.preview_only_partitions,
        blocked_partitions: acc.blocked_partitions,
        coverage_start: acc.coverage_start,
        coverage_end: acc.coverage_end,
        latest_measured_at: acc.latest_measured_at,
        backfill_status: backfillStatus,
    };
}

export class RestoreOpsAdminService {
    constructor(
        private readonly jobs: RestoreJobService,
        private readonly plans: RestorePlanService,
        private readonly evidence: RestoreEvidenceService,
    ) {}

    getQueueDashboard(): Record<string, unknown> {
        const jobs = this.jobs.listJobs();
        const runningJobs = jobs.filter((job) => job.status === 'running');
        const queuedJobs = jobs.filter((job) => job.status === 'queued');
        const pausedJobs = jobs.filter((job) => job.status === 'paused');
        const waitReasonCounts: Record<string, number> = {};

        for (const job of queuedJobs) {
            const reasonCode = job.wait_reason_code || 'queued_scope_lock';

            waitReasonCounts[reasonCode] = (waitReasonCounts[reasonCode] || 0) + 1;
        }

        return {
            totals: {
                running_jobs: runningJobs.length,
                queued_jobs: queuedJobs.length,
                paused_jobs: pausedJobs.length,
            },
            wait_reason_counts: waitReasonCounts,
            lock_snapshot: this.jobs.getLockSnapshot(),
            running_jobs: runningJobs,
            queued_jobs: queuedJobs,
            paused_jobs: pausedJobs,
        };
    }

    getFreshnessDashboard(): Record<string, unknown> {
        const accumulators = new Map<string, FreshnessAccumulator>();
        const plans = this.plans.listPlans();

        for (const plan of plans) {
            for (const watermark of plan.watermarks) {
                const key = sourceKey({
                    tenant_id: watermark.tenant_id,
                    instance_id: watermark.instance_id,
                    source: watermark.source,
                });
                const existing = accumulators.get(key) || {
                    tenant_id: watermark.tenant_id,
                    instance_id: watermark.instance_id,
                    source: watermark.source,
                    fresh_partitions: 0,
                    stale_partitions: 0,
                    unknown_partitions: 0,
                    executable_partitions: 0,
                    preview_only_partitions: 0,
                    blocked_partitions: 0,
                    coverage_start: null,
                    coverage_end: null,
                    latest_measured_at: null,
                };

                if (watermark.freshness === 'fresh') {
                    existing.fresh_partitions += 1;
                } else if (watermark.freshness === 'stale') {
                    existing.stale_partitions += 1;
                } else if (watermark.freshness === 'unknown') {
                    existing.unknown_partitions += 1;
                }

                if (watermark.executability === 'executable') {
                    existing.executable_partitions += 1;
                } else if (watermark.executability === 'preview_only') {
                    existing.preview_only_partitions += 1;
                } else if (watermark.executability === 'blocked') {
                    existing.blocked_partitions += 1;
                }

                existing.coverage_start = minIso(
                    existing.coverage_start,
                    watermark.coverage_start,
                );
                existing.coverage_end = maxIso(
                    existing.coverage_end,
                    watermark.coverage_end,
                );
                existing.latest_measured_at = maxIso(
                    existing.latest_measured_at,
                    watermark.measured_at,
                );
                accumulators.set(key, existing);
            }
        }

        const sources = Array.from(accumulators.values())
            .map((acc) => toFreshnessRow(acc))
            .sort((left, right) => {
                const leftKey = sourceKey(left);
                const rightKey = sourceKey(right);

                return leftKey.localeCompare(rightKey);
            });

        const totals = {
            source_count: sources.length,
            stale_source_count: sources.filter((source) =>
                source.stale_partitions > 0
            ).length,
            unknown_source_count: sources.filter((source) =>
                source.unknown_partitions > 0
            ).length,
            bootstrap_required_count: sources.filter((source) =>
                source.backfill_status === 'bootstrap_required'
            ).length,
            gap_repair_required_count: sources.filter((source) =>
                source.backfill_status === 'gap_repair_required'
            ).length,
        };

        return {
            totals,
            sources,
        };
    }

    getEvidenceDashboard(): Record<string, unknown> {
        const evidences = this.evidence.listEvidence()
            .map((record) => ({
                job_id: record.evidence.job_id,
                evidence_id: record.evidence.evidence_id,
                plan_hash: record.evidence.plan_hash,
                report_hash: record.evidence.report_hash,
                generated_at: record.generated_at,
                signature_verification: record.verification.signature_verification,
                reason_code: record.verification.reason_code,
                drilldown_links: {
                    evidence: `/v1/jobs/${record.evidence.job_id}/evidence`,
                    export: `/v1/jobs/${record.evidence.job_id}/evidence/export`,
                },
            }))
            .sort((left, right) => {
                return left.generated_at.localeCompare(right.generated_at);
            });

        return {
            totals: {
                total: evidences.length,
                verified: evidences.filter((entry) =>
                    entry.signature_verification === 'verified'
                ).length,
                verification_failed: evidences.filter((entry) =>
                    entry.signature_verification === 'verification_failed'
                ).length,
            },
            evidences,
        };
    }
}
