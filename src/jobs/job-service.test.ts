import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    RestoreFinalizedPlanReader,
    RestoreJobService,
} from './job-service';
import {
    AcpResolveSourceMappingResult,
    ResolveSourceMappingInput,
} from '../registry/acp-source-mapping-client';
import {
    SourceMappingResolver,
} from '../registry/source-mapping-resolver';
import { SourceRegistry } from '../registry/source-registry';
import { RestoreLockManager } from '../locks/lock-manager';
import { InMemoryRestoreJobStateStore } from './job-state-store';

const FIXED_NOW = new Date('2026-02-16T12:00:00.000Z');

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

function baseRequest(planId: string, table: string) {
    return {
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        plan_id: planId,
        plan_hash: 'a'.repeat(64),
        lock_scope_tables: [table],
        required_capabilities: ['restore_execute'],
        requested_by: 'operator@example.com',
    };
}

interface FinalizedPlanReaderOptions {
    plansById?: Record<string, string>;
    defaultPlanHash?: string | null;
}

function createFinalizedPlanReader(
    options: FinalizedPlanReaderOptions = {},
): RestoreFinalizedPlanReader {
    const planHashesById = new Map(Object.entries(options.plansById || {}));
    const defaultPlanHash = options.defaultPlanHash === undefined
        ? 'a'.repeat(64)
        : options.defaultPlanHash;

    return {
        async getFinalizedPlan(planId: string) {
            const planHash = planHashesById.get(planId);

            if (planHash) {
                return {
                    plan_id: planId,
                    plan_hash: planHash,
                };
            }

            if (defaultPlanHash === null) {
                return null;
            }

            return {
                plan_id: planId,
                plan_hash: defaultPlanHash,
            };
        },
    };
}

type FoundMapping = Extract<
    AcpResolveSourceMappingResult,
    { status: 'found' }
>;

function createResolveResult(
    overrides: Partial<FoundMapping['mapping']> = {},
): FoundMapping {
    return {
        status: 'found',
        mapping: {
            tenantId: 'tenant-acme',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantState: 'active',
            entitlementState: 'active',
            instanceState: 'active',
            allowedServices: ['rrs'],
            updatedAt: '2026-02-16T12:00:00.000Z',
            requestedServiceScope: 'rrs',
            serviceAllowed: true,
            ...overrides,
        },
    };
}

function createResolver(
    resolveHandler?: (
        input: ResolveSourceMappingInput,
    ) => Promise<AcpResolveSourceMappingResult>,
): SourceMappingResolver {
    return {
        async resolveSourceMapping(
            input: ResolveSourceMappingInput,
        ): Promise<AcpResolveSourceMappingResult> {
            if (resolveHandler) {
                return resolveHandler(input);
            }

            return createResolveResult();
        },
    };
}

interface CreateServiceOptions {
    resolver?: SourceMappingResolver;
    nowFn?: () => Date;
    stateStore?: InMemoryRestoreJobStateStore;
    sourceRegistry?: SourceRegistry;
    finalizedPlanReader?: RestoreFinalizedPlanReader;
}

function createService(options: CreateServiceOptions = {}): RestoreJobService {
    return new RestoreJobService(
        new RestoreLockManager(),
        options.sourceRegistry || new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
        ]),
        options.nowFn || now,
        options.stateStore,
        options.resolver,
        options.finalizedPlanReader || createFinalizedPlanReader(),
    );
}

test('parallel non-overlapping jobs run immediately', async () => {
    const service = createService();

    const first = await service.createJob(baseRequest('plan-1', 'incident'), claims());
    const second = await service.createJob(baseRequest('plan-2', 'cmdb_ci'), claims());

    assert.equal(first.success, true);
    assert.equal(second.success, true);

    if (!first.success || !second.success) {
        return;
    }

    assert.equal(first.job.status, 'running');
    assert.equal(second.job.status, 'running');
    assert.equal(first.job.status_reason_code, 'none');
    assert.equal(second.job.status_reason_code, 'none');
});

test('queued job is promoted after overlapping running job completes', async () => {
    const service = createService();

    const running = await service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = await service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    assert.equal(queued.job.status, 'queued');
    assert.equal(queued.job.status_reason_code, 'queued_scope_lock');

    const completion = await service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    if (!completion.success) {
        return;
    }

    assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);

    const promoted = await service.getJob(queued.job.job_id);

    assert.notEqual(promoted, null);
    assert.equal(promoted?.status, 'running');
    assert.equal(promoted?.status_reason_code, 'none');
    assert.equal(promoted?.queue_position, null);
});

test(
    'createJob hydrates legacy lock state entries without source from job metadata',
    async () => {
        const stateStore = new InMemoryRestoreJobStateStore();
        const service = createService({
            stateStore,
        });
        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );

        assert.equal(running.success, true);

        if (!running.success) {
            return;
        }

        await stateStore.mutate((state) => {
            state.lock_state.running_jobs = state.lock_state.running_jobs.map(
                (entry) => {
                    const next = {
                        ...entry,
                    };

                    delete next.source;

                    return next;
                },
            );
        });

        const queued = await service.createJob(
            baseRequest('plan-2', 'incident'),
            claims(),
        );

        assert.equal(queued.success, true);

        if (!queued.success) {
            return;
        }

        assert.equal(queued.job.status, 'queued');
        assert.equal(queued.job.wait_reason_code, 'queued_scope_lock');
    },
);

test('running job can pause and resume without releasing scope lock', async () => {
    const service = createService();

    const running = await service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = await service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const pause = await service.pauseJob(
        running.job.job_id,
        'paused_token_refresh_grace_exhausted',
    );

    assert.equal(pause.success, true);

    if (!pause.success) {
        return;
    }

    assert.equal(pause.job.status, 'paused');
    assert.equal(queued.job.status, 'queued');

    const resumed = await service.resumePausedJob(running.job.job_id);

    assert.equal(resumed.success, true);

    if (!resumed.success) {
        return;
    }

    assert.equal(resumed.job.status, 'running');

    const completion = await service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    if (!completion.success) {
        return;
    }

    assert.deepEqual(completion.promoted_job_ids, [queued.job.job_id]);
});

test('job lifecycle events emit normalized cross-service audit events', async () => {
    const service = createService();

    const running = await service.createJob(baseRequest('plan-1', 'incident'), claims());
    const queued = await service.createJob(baseRequest('plan-2', 'incident'), claims());

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const completion = await service.completeJob(running.job.job_id, {
        status: 'completed',
    });

    assert.equal(completion.success, true);

    const runningCrossService = await service.listCrossServiceJobEvents(
        running.job.job_id,
    );
    const queuedCrossService = await service.listCrossServiceJobEvents(
        queued.job.job_id,
    );

    assert.ok(runningCrossService.length >= 2);
    assert.equal(runningCrossService[0].contract_version, 'audit.contracts.v1');
    assert.equal(runningCrossService[0].schema_version, 'audit.event.v1');
    assert.equal(runningCrossService[0].service, 'rrs');
    assert.equal(runningCrossService[0].tenant_id, 'tenant-acme');
    assert.equal(runningCrossService[0].instance_id, 'sn-dev-01');
    assert.equal(
        runningCrossService[0].source,
        'sn://acme-dev.service-now.com',
    );
    assert.equal(runningCrossService[0].plan_id, 'plan-1');
    assert.equal(runningCrossService[0].job_id, running.job.job_id);

    const hasPlanCreated = runningCrossService.some((event) =>
        event.lifecycle === 'plan' &&
        event.action === 'job_created' &&
        event.outcome === 'accepted'
    );
    const hasCompleted = runningCrossService.some((event) =>
        event.lifecycle === 'execute' &&
        event.action === 'completed' &&
        event.outcome === 'completed'
    );

    assert.equal(hasPlanCreated, true);
    assert.equal(hasCompleted, true);

    const hasQueued = queuedCrossService.some((event) =>
        event.lifecycle === 'execute' &&
        event.action === 'queued_for_lock' &&
        event.outcome === 'queued'
    );

    assert.equal(hasQueued, true);
});

test('createJob rejects mismatched scope', async () => {
    const service = createService();

    const mismatchedClaims = claims();
    mismatchedClaims.tenant_id = 'tenant-wrong';

    const result = await service.createJob(
        baseRequest('plan-bad', 'incident'),
        mismatchedClaims,
    );

    assert.equal(result.success, false);
});

test('createJob rejects missing finalized plan before mutating state', async () => {
    const stateStore = new InMemoryRestoreJobStateStore();
    const beforeState = await stateStore.read();
    const service = createService({
        stateStore,
        finalizedPlanReader: createFinalizedPlanReader({
            defaultPlanHash: null,
        }),
    });
    const result = await service.createJob(
        baseRequest('plan-missing', 'incident'),
        claims(),
    );

    assert.equal(result.success, false);

    if (!result.success) {
        assert.equal(result.statusCode, 409);
        assert.equal(result.error, 'plan_missing');
        assert.equal(result.reasonCode, 'blocked_plan_unavailable');
        assert.equal(result.message, 'job plan is unavailable in plan store');
    }

    const afterState = await stateStore.read();

    assert.deepEqual(afterState.jobs_by_id, beforeState.jobs_by_id);
    assert.deepEqual(afterState.plans_by_id, beforeState.plans_by_id);
    assert.deepEqual(afterState.lock_state, beforeState.lock_state);
    assert.deepEqual(afterState.events_by_job_id, beforeState.events_by_job_id);
});

test('createJob rejects draft-only plan id (not finalized)', async () => {
    const stateStore = new InMemoryRestoreJobStateStore();
    const beforeState = await stateStore.read();
    const service = createService({
        stateStore,
        finalizedPlanReader: createFinalizedPlanReader({
            plansById: {
                'plan-finalized': 'a'.repeat(64),
            },
            defaultPlanHash: null,
        }),
    });
    const result = await service.createJob(
        baseRequest('plan-draft-only', 'incident'),
        claims(),
    );

    assert.equal(result.success, false);

    if (!result.success) {
        assert.equal(result.statusCode, 409);
        assert.equal(result.error, 'plan_missing');
        assert.equal(result.reasonCode, 'blocked_plan_unavailable');
        assert.equal(result.message, 'job plan is unavailable in plan store');
    }

    const afterState = await stateStore.read();

    assert.deepEqual(afterState.jobs_by_id, beforeState.jobs_by_id);
    assert.deepEqual(afterState.plans_by_id, beforeState.plans_by_id);
    assert.deepEqual(afterState.lock_state, beforeState.lock_state);
    assert.deepEqual(afterState.events_by_job_id, beforeState.events_by_job_id);
});

test(
    'createJob rejects finalized-plan hash mismatch deterministically '
    + 'without side effects',
    async () => {
        const stateStore = new InMemoryRestoreJobStateStore();
        const beforeState = await stateStore.read();
        const service = createService({
            stateStore,
            finalizedPlanReader: createFinalizedPlanReader({
                plansById: {
                    'plan-finalized-mismatch': 'b'.repeat(64),
                },
                defaultPlanHash: null,
            }),
        });
        const result = await service.createJob(
            {
                ...baseRequest('plan-finalized-mismatch', 'incident'),
                plan_hash: 'a'.repeat(64),
            },
            claims(),
        );

        assert.equal(result.success, false);

        if (!result.success) {
            assert.equal(result.statusCode, 409);
            assert.equal(result.error, 'plan_hash_mismatch');
            assert.equal(result.reasonCode, 'blocked_plan_hash_mismatch');
            assert.equal(
                result.message,
                'plan_id already exists with a different plan_hash',
            );
        }

        const afterState = await stateStore.read();

        assert.deepEqual(afterState.jobs_by_id, beforeState.jobs_by_id);
        assert.deepEqual(afterState.plans_by_id, beforeState.plans_by_id);
        assert.deepEqual(afterState.lock_state, beforeState.lock_state);
        assert.deepEqual(
            afterState.events_by_job_id,
            beforeState.events_by_job_id,
        );
    },
);

test('createJob succeeds when finalized plan exists', async () => {
    const service = createService({
        finalizedPlanReader: createFinalizedPlanReader({
            plansById: {
                'plan-finalized': 'a'.repeat(64),
            },
            defaultPlanHash: null,
        }),
    });
    const result = await service.createJob(
        baseRequest('plan-finalized', 'incident'),
        claims(),
    );

    assert.equal(result.success, true);

    if (!result.success) {
        return;
    }

    assert.equal(result.job.plan_id, 'plan-finalized');
    assert.equal(result.job.plan_hash, 'a'.repeat(64));
    assert.equal(result.job.status, 'running');
});

test('createJob rejects missing ACP mapping', async () => {
    const service = createService({
        resolver: createResolver(async () => ({
            status: 'not_found',
        })),
    });
    const result = await service.createJob(
        baseRequest('plan-missing-mapping', 'incident'),
        claims(),
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 403);
        assert.equal(
            result.reasonCode,
            'blocked_unknown_source_mapping',
        );
    }
});

test('createJob rejects ACP mapping when service is not allowed', async () => {
    const service = createService({
        resolver: createResolver(async () =>
            createResolveResult({
                allowedServices: ['reg'],
                serviceAllowed: false,
            })),
    });
    const result = await service.createJob(
        baseRequest('plan-service-not-allowed', 'incident'),
        claims(),
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 403);
        assert.equal(
            result.reasonCode,
            'blocked_unknown_source_mapping',
        );
    }
});

test('createJob rejects ACP outages explicitly', async () => {
    const service = createService({
        resolver: createResolver(async () => ({
            status: 'outage',
            message: 'ACP unavailable',
        })),
    });
    const result = await service.createJob(
        baseRequest('plan-acp-outage', 'incident'),
        claims(),
    );

    assert.equal(result.success, false);
    if (!result.success) {
        assert.equal(result.statusCode, 503);
        assert.equal(
            result.reasonCode,
            'blocked_auth_control_plane_outage',
        );
    }
});

test('getJob returns null for unknown job', async () => {
    const service = createService();

    const job = await service.getJob('nonexistent');

    assert.equal(job, null);
});

test('completeJob rejects non-terminal status', async () => {
    const service = createService();

    const created = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );

    assert.equal(created.success, true);

    if (!created.success) {
        return;
    }

    const result = await service.completeJob(
        created.job.job_id,
        { status: 'running' },
    );

    assert.equal(result.success, false);
});

test('completeJob rejects completion for already-terminal job', async () => {
    const service = createService();

    const created = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );

    assert.equal(created.success, true);

    if (!created.success) {
        return;
    }

    await service.completeJob(created.job.job_id, {
        status: 'completed',
    });

    const result = await service.completeJob(
        created.job.job_id,
        { status: 'completed' },
    );

    assert.equal(result.success, false);
});

test('reconcileQueueLocks dry-run detects stale non-terminal jobs', async () => {
    const service = createService();

    const running = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );
    const queued = await service.createJob(
        baseRequest('plan-2', 'incident'),
        claims(),
    );

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const beforeRunning = await service.getJob(running.job.job_id);
    const beforeQueued = await service.getJob(queued.job.job_id);
    const preview = await service.reconcileQueueLocks({
        dry_run: true,
        stale_after_ms: 0,
        scope: {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            lock_scope_tables: ['incident'],
        },
    });

    assert.equal(preview.dry_run, true);
    assert.equal(preview.applied, false);
    assert.equal(preview.totals.stale_jobs_in_scope, 2);
    assert.equal(preview.forced_transitions.length, 0);

    const staleAnomalyCount = preview.anomalies.filter((anomaly) =>
        anomaly.code === 'stale_non_terminal_job'
    ).length;

    assert.equal(staleAnomalyCount, 2);

    const afterRunning = await service.getJob(running.job.job_id);
    const afterQueued = await service.getJob(queued.job.job_id);

    assert.equal(beforeRunning?.status, 'running');
    assert.equal(beforeQueued?.status, 'queued');
    assert.equal(afterRunning?.status, 'running');
    assert.equal(afterQueued?.status, 'queued');
});

test(
    'reconcileQueueLocks force-stales stale blocker and promotes queued job',
    async () => {
        let nowMillis = Date.parse('2026-02-16T12:00:00.000Z');
        const dynamicNow = (): Date => new Date(nowMillis);
        const service = createService({
            nowFn: dynamicNow,
        });

        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );

        assert.equal(running.success, true);

        if (!running.success) {
            return;
        }

        nowMillis += 2 * 60 * 1000;

        const queued = await service.createJob(
            baseRequest('plan-2', 'incident'),
            claims(),
        );

        assert.equal(queued.success, true);

        if (!queued.success) {
            return;
        }

        nowMillis += 8 * 60 * 1000;

        const applied = await service.reconcileQueueLocks({
            dry_run: false,
            stale_after_ms: 5 * 60 * 1000,
            force_stale_status: 'failed',
            force_reason_code: 'failed_internal_error',
            preserve_stale_job_ids: [queued.job.job_id],
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });

        assert.equal(applied.applied, true);
        assert.equal(applied.forced_transitions.length, 1);
        assert.deepEqual(
            applied.forced_transitions.map((entry) => entry.job_id),
            [running.job.job_id],
        );
        assert.equal(
            applied.forced_transitions[0].job_id,
            running.job.job_id,
        );
        assert.equal(
            applied.forced_transitions[0].to_status,
            'failed',
        );
        assert.deepEqual(applied.promoted_job_ids, [queued.job.job_id]);

        const failed = await service.getJob(running.job.job_id);
        const promoted = await service.getJob(queued.job.job_id);

        assert.notEqual(failed, null);
        assert.notEqual(promoted, null);
        assert.equal(failed?.status, 'failed');
        assert.equal(failed?.status_reason_code, 'failed_internal_error');
        assert.equal(failed?.completed_at, '2026-02-16T12:10:00.000Z');
        assert.equal(promoted?.status, 'running');
        assert.equal(promoted?.queue_position, null);
        assert.equal(promoted?.wait_reason_code, null);
        assert.equal(promoted?.started_at, '2026-02-16T12:10:00.000Z');

        const failedEvents = await service.listJobEvents(running.job.job_id);
        const hasForcedFailedEvent = failedEvents.some((event) => {
            if (event.event_type !== 'job_failed') {
                return false;
            }

            const details = event.details as {
                reconcile_forced_terminal?: boolean;
            };

            return details.reconcile_forced_terminal === true;
        });

        assert.equal(hasForcedFailedEvent, true);
    },
);

test(
    'reconcileQueueLocks defaults stale forced reason code when omitted',
    async () => {
        let nowMillis = Date.parse('2026-02-16T12:00:00.000Z');
        const dynamicNow = (): Date => new Date(nowMillis);
        const service = createService({
            nowFn: dynamicNow,
        });

        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );

        assert.equal(running.success, true);

        if (!running.success) {
            return;
        }

        nowMillis += 8 * 60 * 1000;

        const applied = await service.reconcileQueueLocks({
            dry_run: false,
            stale_after_ms: 5 * 60 * 1000,
            force_stale_status: 'failed',
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });

        assert.equal(applied.forced_transitions.length, 1);
        assert.equal(
            applied.forced_transitions[0].reason_code,
            'failed_stale_lock_recovered',
        );

        const failed = await service.getJob(running.job.job_id);
        assert.equal(failed?.status, 'failed');
        assert.equal(
            failed?.status_reason_code,
            'failed_stale_lock_recovered',
        );

        const failedEvents = await service.listJobEvents(running.job.job_id);
        const forced = failedEvents.find((event) => {
            return (
                event.event_type === 'job_failed'
                && event.reason_code === 'failed_stale_lock_recovered'
            );
        });

        assert.notEqual(forced, undefined);
    },
);

test('reconcileQueueLocks does not force active jobs that are not stale', async () => {
    let nowMillis = Date.parse('2026-02-16T12:00:00.000Z');
    const dynamicNow = (): Date => new Date(nowMillis);
    const service = createService({
        nowFn: dynamicNow,
    });
    const running = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );
    nowMillis += 60 * 1000;
    const queued = await service.createJob(
        baseRequest('plan-2', 'incident'),
        claims(),
    );
    nowMillis += 60 * 1000;

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const applied = await service.reconcileQueueLocks({
        dry_run: false,
        stale_after_ms: 10 * 60 * 1000,
        force_stale_status: 'failed',
        force_reason_code: 'failed_internal_error',
        scope: {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            lock_scope_tables: ['incident'],
        },
    });

    assert.deepEqual(applied.forced_transitions, []);
    assert.deepEqual(applied.promoted_job_ids, []);
    assert.equal(applied.totals.stale_jobs_in_scope, 0);

    const runningAfter = await service.getJob(running.job.job_id);
    const queuedAfter = await service.getJob(queued.job.job_id);

    assert.equal(runningAfter?.status, 'running');
    assert.equal(queuedAfter?.status, 'queued');
    assert.equal(queuedAfter?.wait_reason_code, 'queued_scope_lock');
});

test('reconcileQueueLocks force-stales stale queued jobs when not preserved', async () => {
    let nowMillis = Date.parse('2026-02-16T12:00:00.000Z');
    const dynamicNow = (): Date => new Date(nowMillis);
    const service = createService({
        nowFn: dynamicNow,
    });

    const running = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );
    nowMillis += 2 * 60 * 1000;
    const queued = await service.createJob(
        baseRequest('plan-2', 'incident'),
        claims(),
    );
    nowMillis += 8 * 60 * 1000;

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    const applied = await service.reconcileQueueLocks({
        dry_run: false,
        stale_after_ms: 5 * 60 * 1000,
        force_stale_status: 'failed',
        force_reason_code: 'failed_internal_error',
        scope: {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            lock_scope_tables: ['incident'],
        },
    });

    assert.deepEqual(applied.promoted_job_ids, []);
    assert.equal(applied.forced_transitions.length, 2);
    assert.deepEqual(
        applied.forced_transitions.map((transition) => transition.job_id).sort(),
        [running.job.job_id, queued.job.job_id].sort(),
    );

    const runningAfter = await service.getJob(running.job.job_id);
    const queuedAfter = await service.getJob(queued.job.job_id);

    assert.equal(runningAfter?.status, 'failed');
    assert.equal(queuedAfter?.status, 'failed');
    assert.equal(queuedAfter?.status_reason_code, 'failed_internal_error');
});

test(
    'reconcileQueueLocks drains stale queued backlog while preserving target queued job',
    async () => {
        let nowMillis = Date.parse('2026-02-16T12:00:00.000Z');
        const dynamicNow = (): Date => new Date(nowMillis);
        const service = createService({
            nowFn: dynamicNow,
        });

        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );
        nowMillis += 2 * 60 * 1000;
        const staleQueuedA = await service.createJob(
            baseRequest('plan-2', 'incident'),
            claims(),
        );
        nowMillis += 2 * 60 * 1000;
        const staleQueuedB = await service.createJob(
            baseRequest('plan-3', 'incident'),
            claims(),
        );
        nowMillis += 2 * 60 * 1000;
        const targetQueued = await service.createJob(
            baseRequest('plan-4', 'incident'),
            claims(),
        );
        nowMillis += 8 * 60 * 1000;

        assert.equal(running.success, true);
        assert.equal(staleQueuedA.success, true);
        assert.equal(staleQueuedB.success, true);
        assert.equal(targetQueued.success, true);

        if (
            !running.success
            || !staleQueuedA.success
            || !staleQueuedB.success
            || !targetQueued.success
        ) {
            return;
        }

        const applied = await service.reconcileQueueLocks({
            dry_run: false,
            stale_after_ms: 5 * 60 * 1000,
            force_stale_status: 'failed',
            force_reason_code: 'failed_internal_error',
            preserve_stale_job_ids: [targetQueued.job.job_id],
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });

        assert.equal(applied.forced_transitions.length, 3);
        assert.deepEqual(
            applied.forced_transitions.map((transition) => transition.job_id).sort(),
            [
                running.job.job_id,
                staleQueuedA.job.job_id,
                staleQueuedB.job.job_id,
            ].sort(),
        );
        assert.deepEqual(applied.promoted_job_ids, [targetQueued.job.job_id]);

        const staleQueuedAAfter = await service.getJob(staleQueuedA.job.job_id);
        const staleQueuedBAfter = await service.getJob(staleQueuedB.job.job_id);
        const targetAfter = await service.getJob(targetQueued.job.job_id);

        assert.equal(staleQueuedAAfter?.status, 'failed');
        assert.equal(staleQueuedBAfter?.status, 'failed');
        assert.equal(targetAfter?.status, 'running');
        assert.equal(targetAfter?.queue_position, null);
        assert.equal(targetAfter?.wait_reason_code, null);
    },
);

test('reconcileQueueLocks scope limits forced stale transitions', async () => {
    const sourceB = 'sn://acme-qa.service-now.com';
    const instanceB = 'sn-dev-02';
    const service = createService({
        sourceRegistry: new SourceRegistry([
            {
                tenantId: 'tenant-acme',
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
            },
            {
                tenantId: 'tenant-acme',
                instanceId: instanceB,
                source: sourceB,
            },
        ]),
    });
    const scopedRunning = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );
    const scopedQueued = await service.createJob(
        baseRequest('plan-2', 'incident'),
        claims(),
    );

    const sourceBClaims = {
        ...claims(),
        instance_id: instanceB,
        source: sourceB,
    };
    const sourceBRequest = {
        ...baseRequest('plan-3', 'incident'),
        instance_id: instanceB,
        source: sourceB,
    };
    const otherScopeRunning = await service.createJob(
        sourceBRequest,
        sourceBClaims,
    );

    assert.equal(scopedRunning.success, true);
    assert.equal(scopedQueued.success, true);
    assert.equal(otherScopeRunning.success, true);

    if (
        !scopedRunning.success
        || !scopedQueued.success
        || !otherScopeRunning.success
    ) {
        return;
    }

    const applied = await service.reconcileQueueLocks({
        dry_run: false,
        stale_after_ms: 0,
        force_stale_status: 'failed',
        force_reason_code: 'failed_internal_error',
        preserve_stale_job_ids: [scopedQueued.job.job_id],
        scope: {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            lock_scope_tables: ['incident'],
        },
    });

    assert.deepEqual(
        applied.forced_transitions.map((entry) => entry.job_id),
        [scopedRunning.job.job_id],
    );
    assert.deepEqual(applied.promoted_job_ids, [scopedQueued.job.job_id]);

    const scopedRunningAfter = await service.getJob(scopedRunning.job.job_id);
    const scopedQueuedAfter = await service.getJob(scopedQueued.job.job_id);
    const otherScopeAfter = await service.getJob(otherScopeRunning.job.job_id);

    assert.equal(scopedRunningAfter?.status, 'failed');
    assert.equal(scopedQueuedAfter?.status, 'running');
    assert.equal(otherScopeAfter?.status, 'running');
});

test('reconcileQueueLocks reports lock membership mismatch anomalies', async () => {
    const stateStore = new InMemoryRestoreJobStateStore();
    const service = createService({
        stateStore,
    });

    const running = await service.createJob(
        baseRequest('plan-1', 'incident'),
        claims(),
    );
    const queued = await service.createJob(
        baseRequest('plan-2', 'incident'),
        claims(),
    );

    assert.equal(running.success, true);
    assert.equal(queued.success, true);

    if (!running.success || !queued.success) {
        return;
    }

    await stateStore.mutate((state) => {
        state.lock_state.queued_jobs = [];
    });

    const preview = await service.reconcileQueueLocks({
        dry_run: true,
        scope: {
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            lock_scope_tables: ['incident'],
        },
    });

    const mismatch = preview.anomalies.find((anomaly) =>
        anomaly.code === 'lock_membership_mismatch'
        && anomaly.job_id === queued.job.job_id
    );

    assert.notEqual(mismatch, undefined);
});

test(
    'reconcileQueueLocks flags queued missing blocker and cleans orphaned lock',
    async () => {
        const stateStore = new InMemoryRestoreJobStateStore();
        const service = createService({
            stateStore,
        });
        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );
        const queued = await service.createJob(
            baseRequest('plan-2', 'incident'),
            claims(),
        );

        assert.equal(running.success, true);
        assert.equal(queued.success, true);

        if (!running.success || !queued.success) {
            return;
        }

        await stateStore.mutate((state) => {
            const staleRunning = state.jobs_by_id[running.job.job_id];

            state.jobs_by_id[running.job.job_id] = {
                ...staleRunning,
                status: 'failed',
                status_reason_code: 'failed_internal_error',
                queue_position: null,
                wait_reason_code: null,
                wait_tables: [],
                completed_at: '2026-02-16T12:00:00.000Z',
                updated_at: '2026-02-16T12:00:00.000Z',
            };
        });

        const preview = await service.reconcileQueueLocks({
            dry_run: true,
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });
        const missingBlocker = preview.anomalies.find((anomaly) => {
            return (
                anomaly.code === 'queued_missing_blocker' &&
                anomaly.job_id === queued.job.job_id
            );
        });

        assert.notEqual(missingBlocker, undefined);
        assert.deepEqual(
            (
                missingBlocker?.details.orphaned_blocker_job_ids as
                    string[] | undefined
            ),
            [running.job.job_id],
        );

        const applied = await service.reconcileQueueLocks({
            dry_run: false,
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });

        assert.deepEqual(applied.promoted_job_ids, [queued.job.job_id]);

        const promoted = await service.getJob(queued.job.job_id);
        const lockSnapshot = await service.getLockSnapshot();

        assert.equal(promoted?.status, 'running');
        assert.deepEqual(
            lockSnapshot.running.map((entry) => entry.jobId),
            [queued.job.job_id],
        );
        assert.deepEqual(lockSnapshot.queued, []);
    },
);

test(
    'reconcileQueueLocks keeps queued job when real blocker is still active',
    async () => {
        const service = createService();
        const running = await service.createJob(
            baseRequest('plan-1', 'incident'),
            claims(),
        );
        const queued = await service.createJob(
            baseRequest('plan-2', 'incident'),
            claims(),
        );

        assert.equal(running.success, true);
        assert.equal(queued.success, true);

        if (!running.success || !queued.success) {
            return;
        }

        const applied = await service.reconcileQueueLocks({
            dry_run: false,
            scope: {
                tenant_id: 'tenant-acme',
                instance_id: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                lock_scope_tables: ['incident'],
            },
        });
        const missingBlocker = applied.anomalies.find((anomaly) => {
            return (
                anomaly.code === 'queued_missing_blocker' &&
                anomaly.job_id === queued.job.job_id
            );
        });
        const queuedAfter = await service.getJob(queued.job.job_id);

        assert.deepEqual(applied.promoted_job_ids, []);
        assert.equal(missingBlocker, undefined);
        assert.equal(queuedAfter?.status, 'queued');
        assert.equal(queuedAfter?.wait_reason_code, 'queued_scope_lock');
    },
);

test('listJobEvents returns empty for unknown job', async () => {
    const service = createService();

    const events = await service.listJobEvents(
        'nonexistent',
    );

    assert.deepEqual(events, []);
});
