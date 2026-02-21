import { normalizeIsoWithMillis } from '../jobs/models';
import { RestoreEvidenceService } from '../evidence/evidence-service';
import { RestoreExecutionService } from '../execute/execute-service';
import { RestoreJobService } from '../jobs/job-service';
import { RestorePlanService } from '../plans/plan-service';
import { SourceRegistry } from '../registry/source-registry';
import { RestoreIndexStateReader } from '../restore-index/state-reader';

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

interface SloTargetConfig {
    execute_success_rate_min_percent: number;
    queue_wait_p95_max_ms: number;
    execute_duration_p95_max_ms: number;
    stale_source_count_max: number;
    evidence_verification_failed_max: number;
    auth_pause_rate_max_percent: number;
}

type FailureDrillStatus = 'pending' | 'passed' | 'failed';

export interface RestoreOpsAdminConfig {
    stagingModeEnabled?: boolean;
    runbooksSignedOff?: boolean;
    requiredFailureDrills?: string[];
    now?: () => Date;
}

interface FailureDrillRecord {
    drill_id: string;
    status: FailureDrillStatus;
    notes: string | null;
    updated_at: string;
    updated_by: string;
}

interface SloCheck {
    id: string;
    description: string;
    severity: 'critical' | 'warning';
    within_target: boolean;
    observed_value: number | null;
    target_value: number;
    comparator: '<=' | '>=';
}

const DEFAULT_REQUIRED_FAILURE_DRILLS = [
    'auth_outage',
    'sidecar_lag',
    'pg_saturation',
    'entitlement_disable',
    'crash_resume',
    'evidence_audit_export',
];

const DEFAULT_SLO_TARGETS: SloTargetConfig = {
    execute_success_rate_min_percent: 99,
    queue_wait_p95_max_ms: 10000,
    execute_duration_p95_max_ms: 120000,
    stale_source_count_max: 0,
    evidence_verification_failed_max: 0,
    auth_pause_rate_max_percent: 5,
};

const AUTH_PAUSE_REASON_CODES = new Set([
    'paused_token_refresh_grace_exhausted',
    'paused_entitlement_disabled',
    'paused_instance_disabled',
    'blocked_auth_control_plane_outage',
]);

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

function toEpochMillis(value: string | null | undefined): number | null {
    if (!value) {
        return null;
    }

    const parsed = Date.parse(value);

    if (Number.isNaN(parsed)) {
        return null;
    }

    return parsed;
}

function durationMs(
    start: string | null | undefined,
    end: string | null | undefined,
): number | null {
    const startMs = toEpochMillis(start);
    const endMs = toEpochMillis(end);

    if (startMs === null || endMs === null) {
        return null;
    }

    if (endMs < startMs) {
        return 0;
    }

    return endMs - startMs;
}

function roundTwo(value: number): number {
    return Math.round(value * 100) / 100;
}

function ratioPercent(
    numerator: number,
    denominator: number,
): number | null {
    if (denominator <= 0) {
        return null;
    }

    return roundTwo((numerator / denominator) * 100);
}

function asPercentile(values: number[], percentile: number): number | null {
    if (values.length === 0) {
        return null;
    }

    const sorted = [...values].sort((left, right) => left - right);
    const index = Math.ceil((percentile / 100) * sorted.length) - 1;
    const clamped = Math.max(0, Math.min(sorted.length - 1, index));

    return sorted[clamped];
}

function pushTimestamp(
    values: string[],
    value: string | null | undefined,
): void {
    if (value && value.trim()) {
        values.push(value);
    }
}

function normalizeFailureDrillStatus(
    status: string,
): FailureDrillStatus | null {
    if (status === 'pending') {
        return 'pending';
    }

    if (status === 'pass' || status === 'passed') {
        return 'passed';
    }

    if (status === 'fail' || status === 'failed') {
        return 'failed';
    }

    return null;
}

function evaluateCheckWithinTarget(check: SloCheck): boolean {
    if (check.observed_value === null) {
        return false;
    }

    if (check.comparator === '>=') {
        return check.observed_value >= check.target_value;
    }

    return check.observed_value <= check.target_value;
}

export class RestoreOpsAdminService {
    private readonly now: () => Date;

    private readonly requiredFailureDrills: string[];

    private readonly stagingMode: {
        enabled: boolean;
        updated_at: string;
        updated_by: string;
    };

    private readonly runbookSignoff: {
        signed_off: boolean;
        updated_at: string;
        updated_by: string;
    };

    private readonly failureDrills = new Map<string, FailureDrillRecord>();

    constructor(
        private readonly jobs: RestoreJobService,
        private readonly plans: RestorePlanService,
        private readonly evidence: RestoreEvidenceService,
        private readonly execute: RestoreExecutionService,
        private readonly sourceRegistry: SourceRegistry,
        private readonly restoreIndexStateReader: RestoreIndexStateReader,
        config?: RestoreOpsAdminConfig,
    ) {
        this.now = config?.now || (() => new Date());
        this.requiredFailureDrills = (
            config?.requiredFailureDrills ||
            DEFAULT_REQUIRED_FAILURE_DRILLS
        )
            .map((value) => value.trim())
            .filter((value, index, values) => {
                return value.length > 0 && values.indexOf(value) === index;
            });

        const nowIso = normalizeIsoWithMillis(this.now());
        this.stagingMode = {
            enabled: config?.stagingModeEnabled || false,
            updated_at: nowIso,
            updated_by: 'system',
        };
        this.runbookSignoff = {
            signed_off: config?.runbooksSignedOff || false,
            updated_at: nowIso,
            updated_by: 'system',
        };
    }

    setStagingMode(
        enabled: boolean,
        actor: string,
    ): Record<string, unknown> {
        const nowIso = normalizeIsoWithMillis(this.now());
        this.stagingMode.enabled = enabled;
        this.stagingMode.updated_at = nowIso;
        this.stagingMode.updated_by = actor;

        return {
            staging_mode: {
                enabled: this.stagingMode.enabled,
                updated_at: this.stagingMode.updated_at,
                updated_by: this.stagingMode.updated_by,
            },
        };
    }

    setRunbookSignoff(
        signedOff: boolean,
        actor: string,
    ): Record<string, unknown> {
        const nowIso = normalizeIsoWithMillis(this.now());
        this.runbookSignoff.signed_off = signedOff;
        this.runbookSignoff.updated_at = nowIso;
        this.runbookSignoff.updated_by = actor;

        return {
            runbooks: {
                signed_off: this.runbookSignoff.signed_off,
                updated_at: this.runbookSignoff.updated_at,
                updated_by: this.runbookSignoff.updated_by,
            },
        };
    }

    recordFailureDrillResult(
        drillId: string,
        status: string,
        actor: string,
        notes?: string,
    ): {
        success: boolean;
        message?: string;
        record?: FailureDrillRecord;
    } {
        if (!this.requiredFailureDrills.includes(drillId)) {
            return {
                success: false,
                message: 'unknown drill_id',
            };
        }

        const normalized = normalizeFailureDrillStatus(status);

        if (!normalized) {
            return {
                success: false,
                message: 'status must be pending, pass/passed, or fail/failed',
            };
        }

        const record: FailureDrillRecord = {
            drill_id: drillId,
            status: normalized,
            notes: notes ? notes : null,
            updated_at: normalizeIsoWithMillis(this.now()),
            updated_by: actor,
        };

        this.failureDrills.set(drillId, record);

        return {
            success: true,
            record,
        };
    }

    async getQueueDashboard(): Promise<Record<string, unknown>> {
        const jobs = await this.jobs.listJobs();
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
            lock_snapshot: await this.jobs.getLockSnapshot(),
            running_jobs: runningJobs,
            queued_jobs: queuedJobs,
            paused_jobs: pausedJobs,
        };
    }

    async getFreshnessDashboard(): Promise<Record<string, unknown>> {
        const accumulators = new Map<string, FreshnessAccumulator>();
        const measuredAt = normalizeIsoWithMillis(this.now());
        const mappings = this.sourceRegistry.list();

        for (const mapping of mappings) {
            const key = sourceKey({
                tenant_id: mapping.tenantId,
                instance_id: mapping.instanceId,
                source: mapping.source,
            });
            const existing = accumulators.get(key) || {
                tenant_id: mapping.tenantId,
                instance_id: mapping.instanceId,
                source: mapping.source,
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
            const watermarks =
                await this.restoreIndexStateReader.listWatermarksForSource({
                    instanceId: mapping.instanceId,
                    measuredAt,
                    source: mapping.source,
                    tenantId: mapping.tenantId,
                });

            if (watermarks.length === 0) {
                existing.unknown_partitions += 1;
                existing.blocked_partitions += 1;
                existing.latest_measured_at = maxIso(
                    existing.latest_measured_at,
                    measuredAt,
                );
                accumulators.set(key, existing);
                continue;
            }

            for (const watermark of watermarks) {
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
            }

            accumulators.set(key, existing);
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

    async getEvidenceDashboard(): Promise<Record<string, unknown>> {
        const evidences = (await this.evidence.listEvidence())
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

    async getSloDashboard(): Promise<Record<string, unknown>> {
        const plans = await this.plans.listPlans();
        const jobs = await this.jobs.listJobs();
        const executions = await this.execute.listExecutions();
        const freshness = await this.getFreshnessDashboard();
        const evidence = await this.getEvidenceDashboard();
        const timestamps: string[] = [];
        const queueWaitDurations: number[] = [];
        const executeDurations: number[] = [];

        for (const plan of plans) {
            pushTimestamp(timestamps, plan.plan.generated_at);
        }

        for (const job of jobs) {
            pushTimestamp(timestamps, job.requested_at);
            pushTimestamp(timestamps, job.started_at);
            pushTimestamp(timestamps, job.completed_at);

            const queueWait = durationMs(job.requested_at, job.started_at);

            if (queueWait !== null) {
                queueWaitDurations.push(queueWait);
            }
        }

        for (const execution of executions) {
            pushTimestamp(timestamps, execution.started_at);
            pushTimestamp(timestamps, execution.completed_at);

            const duration = durationMs(
                execution.started_at,
                execution.completed_at,
            );

            if (duration !== null) {
                executeDurations.push(duration);
            }
        }

        const evidenceRows = evidence.evidences as Array<Record<string, unknown>>;

        for (const row of evidenceRows) {
            const generatedAt = row.generated_at;

            if (typeof generatedAt === 'string') {
                pushTimestamp(timestamps, generatedAt);
            }
        }

        const dryRunTotal = plans.length;
        const dryRunExecutable = plans.filter((plan) =>
            plan.gate.executability === 'executable'
        ).length;
        const dryRunPreviewOnly = plans.filter((plan) =>
            plan.gate.executability === 'preview_only'
        ).length;
        const dryRunBlocked = plans.filter((plan) =>
            plan.gate.executability === 'blocked'
        ).length;

        const terminalExecutions = executions.filter((execution) => {
            return execution.status === 'completed' || execution.status === 'failed';
        });
        const completedExecutions = terminalExecutions.filter((execution) => {
            return execution.status === 'completed';
        });
        const failedExecutions = terminalExecutions.filter((execution) => {
            return execution.status === 'failed';
        });

        const authPausedJobs = jobs.filter((job) => {
            return (
                job.status === 'paused' &&
                AUTH_PAUSE_REASON_CODES.has(job.status_reason_code)
            );
        });

        const evidenceTotals = evidence.totals as Record<string, unknown>;
        const freshnessTotals = freshness.totals as Record<string, unknown>;
        const staleSourceCount = Number(freshnessTotals.stale_source_count || 0);
        const verificationFailedCount = Number(
            evidenceTotals.verification_failed || 0,
        );
        const executeSuccessRate = ratioPercent(
            completedExecutions.length,
            terminalExecutions.length,
        );
        const queueWaitP95 = asPercentile(queueWaitDurations, 95);
        const executeDurationP95 = asPercentile(executeDurations, 95);
        const authPauseRate = ratioPercent(authPausedJobs.length, jobs.length);

        const checks: SloCheck[] = [
            {
                id: 'execute_success_rate',
                description: 'Completed execute success rate',
                severity: 'critical',
                within_target: false,
                observed_value: executeSuccessRate,
                comparator: '>=',
                target_value: DEFAULT_SLO_TARGETS.execute_success_rate_min_percent,
            },
            {
                id: 'queue_wait_p95_ms',
                description: 'Queue wait p95',
                severity: 'warning',
                within_target: false,
                observed_value: queueWaitP95,
                comparator: '<=',
                target_value: DEFAULT_SLO_TARGETS.queue_wait_p95_max_ms,
            },
            {
                id: 'execute_duration_p95_ms',
                description: 'Execute duration p95',
                severity: 'warning',
                within_target: false,
                observed_value: executeDurationP95,
                comparator: '<=',
                target_value: DEFAULT_SLO_TARGETS.execute_duration_p95_max_ms,
            },
            {
                id: 'stale_source_count',
                description: 'Stale source count',
                severity: 'critical',
                within_target: false,
                observed_value: staleSourceCount,
                comparator: '<=',
                target_value: DEFAULT_SLO_TARGETS.stale_source_count_max,
            },
            {
                id: 'evidence_verification_failed',
                description: 'Evidence verification failures',
                severity: 'critical',
                within_target: false,
                observed_value: verificationFailedCount,
                comparator: '<=',
                target_value:
                    DEFAULT_SLO_TARGETS.evidence_verification_failed_max,
            },
            {
                id: 'auth_pause_rate_percent',
                description: 'Auth pause rate',
                severity: 'warning',
                within_target: false,
                observed_value: authPauseRate,
                comparator: '<=',
                target_value: DEFAULT_SLO_TARGETS.auth_pause_rate_max_percent,
            },
        ];

        for (const check of checks) {
            check.within_target = evaluateCheckWithinTarget(check);
        }

        const criticalFailed = checks.some((check) => {
            return check.severity === 'critical' && !check.within_target;
        });
        const warningFailed = checks.some((check) => {
            return check.severity === 'warning' && !check.within_target;
        });
        const burnRateSeverity = criticalFailed
            ? 'critical'
            : warningFailed
            ? 'warning'
            : 'normal';
        const burnRateStatus = criticalFailed
            ? 'breached'
            : warningFailed
            ? 'at_risk'
            : 'within_budget';
        const sortedTimestamps = [...timestamps].sort((left, right) => {
            return left.localeCompare(right);
        });
        const measurementStart = sortedTimestamps[0] || null;
        const measurementEnd = sortedTimestamps[sortedTimestamps.length - 1] || null;
        const measurementDurationMs = durationMs(measurementStart, measurementEnd);

        return {
            generated_at: normalizeIsoWithMillis(this.now()),
            measurement_window: {
                start: measurementStart,
                end: measurementEnd,
                duration_ms: measurementDurationMs,
            },
            targets: {
                ...DEFAULT_SLO_TARGETS,
            },
            dry_run: {
                total: dryRunTotal,
                executable: dryRunExecutable,
                preview_only: dryRunPreviewOnly,
                blocked: dryRunBlocked,
                executable_rate_percent: ratioPercent(
                    dryRunExecutable,
                    dryRunTotal,
                ),
            },
            execution: {
                total: executions.length,
                terminal: terminalExecutions.length,
                completed: completedExecutions.length,
                failed: failedExecutions.length,
                success_rate_percent: executeSuccessRate,
                execute_duration_p95_ms: executeDurationP95,
            },
            queue: {
                queue_wait_samples: queueWaitDurations.length,
                queue_wait_p95_ms: queueWaitP95,
            },
            auth_dependency: {
                paused_jobs_total: authPausedJobs.length,
                auth_pause_rate_percent: authPauseRate,
            },
            evidence: {
                total: Number(evidenceTotals.total || 0),
                verification_failed: verificationFailedCount,
            },
            freshness: {
                stale_source_count: staleSourceCount,
            },
            burn_rate: {
                status: burnRateStatus,
                severity: burnRateSeverity,
                failed_check_count: checks.filter((check) =>
                    !check.within_target
                ).length,
                checks,
            },
        };
    }

    async getGaReadinessDashboard(): Promise<Record<string, unknown>> {
        const nowIso = normalizeIsoWithMillis(this.now());
        const slo = await this.getSloDashboard();
        const burnRate = slo.burn_rate as Record<string, unknown>;
        const burnRateStatus = String(burnRate.status || 'breached');
        const evidence = await this.getEvidenceDashboard();
        const evidenceTotals = evidence.totals as Record<string, unknown>;
        const evidenceVerificationFailed = Number(
            evidenceTotals.verification_failed || 0,
        );
        const failureDrills = this.requiredFailureDrills.map((drillId) => {
            return this.failureDrills.get(drillId) || {
                drill_id: drillId,
                status: 'pending',
                notes: null,
                updated_at: null,
                updated_by: null,
            };
        });
        const failureDrillPassed = failureDrills.filter((drill) => {
            return drill.status === 'passed';
        }).length;
        const failureDrillPending = failureDrills.filter((drill) => {
            return drill.status === 'pending';
        }).length;
        const failureDrillFailed = failureDrills.filter((drill) => {
            return drill.status === 'failed';
        }).length;

        const checks = [
            {
                id: 'staging_mode_enabled',
                ok: this.stagingMode.enabled,
                detail: this.stagingMode.enabled
                    ? 'staging mode enabled'
                    : 'staging mode disabled',
            },
            {
                id: 'slo_within_budget',
                ok: burnRateStatus === 'within_budget',
                detail: `burn-rate status=${burnRateStatus}`,
            },
            {
                id: 'runbooks_signed_off',
                ok: this.runbookSignoff.signed_off,
                detail: this.runbookSignoff.signed_off
                    ? 'runbooks signed off'
                    : 'runbooks not signed off',
            },
            {
                id: 'failure_drills_passed',
                ok: failureDrillPending === 0 && failureDrillFailed === 0,
                detail:
                    `${failureDrillPassed}/${this.requiredFailureDrills.length}` +
                    ' required drills passed',
            },
            {
                id: 'evidence_verification_clean',
                ok: evidenceVerificationFailed === 0,
                detail: evidenceVerificationFailed === 0
                    ? 'no evidence verification failures'
                    : `${evidenceVerificationFailed} evidence verification failures`,
            },
        ];
        const blockedReasons = checks
            .filter((check) => !check.ok)
            .map((check) => check.id);

        return {
            generated_at: nowIso,
            ga_ready: blockedReasons.length === 0,
            blocked_reasons: blockedReasons,
            staging_mode: {
                enabled: this.stagingMode.enabled,
                updated_at: this.stagingMode.updated_at,
                updated_by: this.stagingMode.updated_by,
            },
            runbooks: {
                signed_off: this.runbookSignoff.signed_off,
                updated_at: this.runbookSignoff.updated_at,
                updated_by: this.runbookSignoff.updated_by,
            },
            failure_drills: {
                required: [...this.requiredFailureDrills],
                totals: {
                    passed: failureDrillPassed,
                    pending: failureDrillPending,
                    failed: failureDrillFailed,
                },
                drills: failureDrills,
            },
            checks,
            slo_summary: {
                burn_rate_status: burnRateStatus,
                burn_rate_severity: burnRate.severity,
                failed_check_count: burnRate.failed_check_count,
            },
        };
    }
}
