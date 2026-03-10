import assert from 'node:assert/strict';
import { test } from 'node:test';
import type {
    RestoreIndexIndexedEventLookupCandidate,
} from '../restore-index/state-reader';
import {
    InMemoryRestoreArtifactBodyReader,
    RestoreRowMaterializationError,
    RestoreRowMaterializationService,
} from './materialization-service';
import {
    buildTargetStateLookupKey,
    InMemoryRestoreTargetStateLookup,
    NoopRestoreTargetStateLookup,
    reconcileSourceOperationWithTargetState,
    RESTORE_TARGET_EXISTENCE_CHECK_POLICY,
    type RestoreTargetStateLookup,
} from './target-reconciliation';

function encryptedPayload(
    ciphertext: string,
): Record<string, unknown> {
    return {
        alg: 'AES-256-GCM',
        ciphertext,
    };
}

function buildCandidate(
    overrides: Partial<RestoreIndexIndexedEventLookupCandidate> = {},
): RestoreIndexIndexedEventLookupCandidate {
    return {
        artifactKey: 'rez/restore/event=evt-01.artifact.json',
        eventId: 'evt-01',
        eventTime: '2026-02-21T10:00:00.000Z',
        manifestKey: 'rez/restore/event=evt-01.manifest.json',
        offset: '100',
        partition: 0,
        recordSysId: 'rec-01',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 10:00:00',
        table: 'x_app.ticket',
        topic: 'rez.cdc',
        ...overrides,
    };
}

function buildArtifact(
    overrides: Record<string, unknown> = {},
): Record<string, unknown> {
    return {
        __op: 'U',
        __schema_version: 3,
        __type: 'cdc.write',
        row_enc: encryptedPayload('row-ciphertext'),
        ...overrides,
    };
}

function buildServiceFixture(options?: {
    targetStateLookup?: RestoreTargetStateLookup;
}): {
    reader: InMemoryRestoreArtifactBodyReader;
    service: RestoreRowMaterializationService;
} {
    const reader = new InMemoryRestoreArtifactBodyReader();
    const service = new RestoreRowMaterializationService(
        reader,
        options?.targetStateLookup || new NoopRestoreTargetStateLookup(),
    );

    return {
        reader,
        service,
    };
}

test(
    'materializeRows fails closed by default when target lookup support is missing',
    async () => {
        const reader = new InMemoryRestoreArtifactBodyReader();
        const service = new RestoreRowMaterializationService(reader);
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-default.artifact.json',
            eventId: 'evt-default',
            manifestKey: 'rez/restore/event=evt-default.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'U',
                row_enc: encryptedPayload('default-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        await assert.rejects(
            async () => service.materializeRows({
                candidates: [candidate],
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                tenantId: 'tenant-acme',
            }),
            (error: unknown) => {
                assert.ok(error instanceof RestoreRowMaterializationError);
                assert.equal(error.code, 'target_state_lookup_unavailable');
                assert.match(error.message, /target state lookup/i);

                return true;
            },
        );
    },
);

test(
    'target existence checks are enabled for dry-run visibility and execute',
    () => {
        assert.equal(
            RESTORE_TARGET_EXISTENCE_CHECK_POLICY.dry_run_plan_visibility,
            true,
        );
        assert.equal(
            RESTORE_TARGET_EXISTENCE_CHECK_POLICY.execute_time_revalidation,
            true,
        );
    },
);

test('target state lookup key is table + record_sys_id', () => {
    const key = buildTargetStateLookupKey({
        record_sys_id: ' rec-01 ',
        table: ' x_app.ticket ',
    });

    assert.equal(key, 'x_app.ticket|rec-01');
});

test('in-memory target lookup resolves state and defaults to missing', async () => {
    const lookup = new InMemoryRestoreTargetStateLookup();
    lookup.setTargetRecordState({
        record_sys_id: 'rec-01',
        state: 'exists',
        table: 'x_app.ticket',
    });

    const result = await lookup.lookupTargetState({
        instance_id: 'sn-dev-01',
        records: [
            {
                record_sys_id: 'rec-01',
                table: 'x_app.ticket',
            },
            {
                record_sys_id: 'rec-02',
                table: 'x_app.ticket',
            },
        ],
        source: 'sn://acme-dev.service-now.com',
        tenant_id: 'tenant-acme',
    });

    assert.equal(result.get('x_app.ticket|rec-01'), 'exists');
    assert.equal(result.get('x_app.ticket|rec-02'), 'missing');
});

test(
    'materializeRowsForTargetReconciliationDraft returns deterministic '
    + 'request rows with source actions',
    async () => {
        const { reader, service } = buildServiceFixture();
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-draft.artifact.json',
            eventId: 'evt-draft',
            manifestKey: 'rez/restore/event=evt-draft.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'I',
                row_enc: encryptedPayload('draft-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        const draft = await service.materializeRowsForTargetReconciliationDraft({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });

        assert.equal(draft.rows.length, 1);
        assert.equal(draft.draftRows.length, 1);
        assert.equal(draft.rows[0].action, 'insert');
        assert.equal(draft.draftRows[0].source_operation, 'I');
        assert.equal(draft.pitResolutions.length, 1);
    },
);

test(
    'finalizeDraftRows recomputes row action and precondition hash from '
    + 'target-state results',
    async () => {
        const { reader, service } = buildServiceFixture();
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-finalize.artifact.json',
            eventId: 'evt-finalize',
            manifestKey: 'rez/restore/event=evt-finalize.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'I',
                row_enc: encryptedPayload('finalize-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        const draft = await service.materializeRowsForTargetReconciliationDraft({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const key = buildTargetStateLookupKey({
            record_sys_id: 'rec-01',
            table: 'x_app.ticket',
        });
        const targetStateByKey = new Map([
            [key, 'exists' as const],
        ]);

        const reconciledRows = service.finalizeDraftRows({
            draftRows: draft.draftRows,
            targetStateByKey,
            requireTargetStates: true,
        });

        assert.equal(reconciledRows.length, 1);
        assert.equal(reconciledRows[0].action, 'update');
        assert.notEqual(
            reconciledRows[0].precondition_hash,
            draft.rows[0].precondition_hash,
        );
    },
);

test('finalizeDraftRows rejects missing required target-state results', async () => {
    const { reader, service } = buildServiceFixture();
    const candidate = buildCandidate({
        artifactKey: 'rez/restore/event=evt-finalize-missing.artifact.json',
        eventId: 'evt-finalize-missing',
        manifestKey: 'rez/restore/event=evt-finalize-missing.manifest.json',
    });

    reader.setArtifactBody({
        artifactKey: candidate.artifactKey,
        body: buildArtifact({
            __op: 'U',
            row_enc: encryptedPayload('finalize-missing-ciphertext'),
        }),
        manifestKey: candidate.manifestKey,
    });

    const draft = await service.materializeRowsForTargetReconciliationDraft({
        candidates: [candidate],
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    });

    assert.throws(
        () => {
            service.finalizeDraftRows({
                draftRows: draft.draftRows,
                targetStateByKey: new Map(),
                requireTargetStates: true,
            });
        },
        (error: unknown) => {
            assert.ok(error instanceof RestoreRowMaterializationError);
            assert.equal(error.code, 'target_reconciliation_incomplete');

            return true;
        },
    );
});

test('reconciliation policy covers required source/target combinations', () => {
    const sourceInsertTargetExists = reconcileSourceOperationWithTargetState({
        source_operation: 'I',
        target_state: 'exists',
    });
    const sourceInsertTargetMissing = reconcileSourceOperationWithTargetState({
        source_operation: 'I',
        target_state: 'missing',
    });
    const sourceUpdateTargetMissing = reconcileSourceOperationWithTargetState({
        source_operation: 'U',
        target_state: 'missing',
    });
    const sourceDelete = reconcileSourceOperationWithTargetState({
        source_operation: 'D',
        target_state: 'missing',
    });

    assert.deepEqual(sourceInsertTargetExists, {
        decision: 'apply',
        plan_action: 'update',
        policy_case: 'source_insert_target_exists',
    });
    assert.deepEqual(sourceInsertTargetMissing, {
        decision: 'apply',
        plan_action: 'insert',
        policy_case: 'source_insert_target_missing',
    });
    assert.deepEqual(sourceUpdateTargetMissing, {
        blocking_reason: 'target_missing_for_source_update',
        conflict_class: 'missing_row_conflict',
        decision: 'block',
        policy_case: 'source_update_target_missing',
    });
    assert.deepEqual(sourceDelete, {
        decision: 'apply',
        plan_action: 'delete',
        policy_case: 'source_delete',
    });
});

test(
    'materializeRows reconciles source insert to update when target exists',
    async () => {
        const targetLookup = new InMemoryRestoreTargetStateLookup();
        targetLookup.setTargetRecordState({
            record_sys_id: 'rec-01',
            state: 'exists',
            table: 'x_app.ticket',
        });
        const { reader, service } = buildServiceFixture({
            targetStateLookup: targetLookup,
        });
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-insert.artifact.json',
            eventId: 'evt-insert',
            manifestKey: 'rez/restore/event=evt-insert.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'I',
                row_enc: encryptedPayload('insert-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        const result = await service.materializeRows({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });

        assert.equal(result.rows.length, 1);
        assert.equal(result.rows[0].action, 'update');
        assert.equal(result.rows[0].metadata.metadata.operation, 'I');
    },
);

test(
    'materializeRows keeps source insert as insert when target is missing',
    async () => {
        const targetLookup = new InMemoryRestoreTargetStateLookup();
        targetLookup.setTargetRecordState({
            record_sys_id: 'rec-01',
            state: 'missing',
            table: 'x_app.ticket',
        });
        const { reader, service } = buildServiceFixture({
            targetStateLookup: targetLookup,
        });
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-missing.artifact.json',
            eventId: 'evt-missing',
            manifestKey: 'rez/restore/event=evt-missing.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'I',
                row_enc: encryptedPayload('missing-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        const result = await service.materializeRows({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });

        assert.equal(result.rows.length, 1);
        assert.equal(result.rows[0].action, 'insert');
        assert.equal(result.rows[0].metadata.metadata.operation, 'I');
    },
);

test(
    'materializeRows fails closed when source update targets missing row',
    async () => {
        const targetLookup = new InMemoryRestoreTargetStateLookup();
        targetLookup.setTargetRecordState({
            record_sys_id: 'rec-01',
            state: 'missing',
            table: 'x_app.ticket',
        });
        const { reader, service } = buildServiceFixture({
            targetStateLookup: targetLookup,
        });
        const candidate = buildCandidate({
            artifactKey: 'rez/restore/event=evt-update.artifact.json',
            eventId: 'evt-update',
            manifestKey: 'rez/restore/event=evt-update.manifest.json',
        });

        reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body: buildArtifact({
                __op: 'U',
                row_enc: encryptedPayload('update-ciphertext'),
            }),
            manifestKey: candidate.manifestKey,
        });

        await assert.rejects(
            async () => service.materializeRows({
                candidates: [candidate],
                instanceId: 'sn-dev-01',
                source: 'sn://acme-dev.service-now.com',
                tenantId: 'tenant-acme',
            }),
            (error: unknown) => {
                assert.ok(error instanceof RestoreRowMaterializationError);
                assert.equal(error.code, 'target_reconciliation_blocked');
                assert.equal(
                    error.details.blocking_reason,
                    'target_missing_for_source_update',
                );

                return true;
            },
        );
    },
);

test('materializeRows selects PIT winner with tie-break tuple ordering',
async () => {
    const { reader, service } = buildServiceFixture();
    const older = buildCandidate({
        artifactKey: 'rez/restore/event=evt-older.artifact.json',
        eventId: 'evt-older',
        manifestKey: 'rez/restore/event=evt-older.manifest.json',
        sysModCount: 1,
    });
    const winner = buildCandidate({
        artifactKey: 'rez/restore/event=evt-winner.artifact.json',
        eventId: 'evt-winner',
        manifestKey: 'rez/restore/event=evt-winner.manifest.json',
        sysModCount: 2,
    });

    reader.setArtifactBody({
        artifactKey: older.artifactKey,
        body: buildArtifact({
            row_enc: encryptedPayload('older-ciphertext'),
        }),
        manifestKey: older.manifestKey,
    });
    reader.setArtifactBody({
        artifactKey: winner.artifactKey,
        body: buildArtifact({
            row_enc: encryptedPayload('winner-ciphertext'),
        }),
        manifestKey: winner.manifestKey,
    });

    const result = await service.materializeRows({
        candidates: [older, winner],
        instanceId: 'sn-dev-01',
        scopeRecords: [{
            recordSysId: 'rec-01',
            table: 'x_app.ticket',
        }],
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    });

    assert.equal(result.rows.length, 1);
    assert.equal(result.pitResolutions.length, 1);
    assert.equal(result.pitResolutions[0].winning_event_id, 'evt-winner');
    assert.equal(
        result.rows[0].metadata.metadata.event_id,
        'evt-winner',
    );
    assert.equal(result.rows[0].action, 'update');
});

test('materializeRows fails closed when winning artifact body is missing',
async () => {
    const { service } = buildServiceFixture();
    const candidate = buildCandidate({
        artifactKey: 'rez/restore/event=evt-missing.artifact.json',
        eventId: 'evt-missing',
        manifestKey: 'rez/restore/event=evt-missing.manifest.json',
    });

    await assert.rejects(
        async () => service.materializeRows({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        }),
        (error: unknown) => {
            assert.ok(error instanceof RestoreRowMaterializationError);
            assert.equal(error.code, 'missing_artifact_body');

            return true;
        },
    );
});

test('materializeRows fails closed when scope record has no PIT candidates',
async () => {
    const { reader, service } = buildServiceFixture();
    const candidate = buildCandidate({
        artifactKey: 'rez/restore/event=evt-only.artifact.json',
        eventId: 'evt-only',
        manifestKey: 'rez/restore/event=evt-only.manifest.json',
        recordSysId: 'rec-01',
    });

    reader.setArtifactBody({
        artifactKey: candidate.artifactKey,
        body: buildArtifact(),
        manifestKey: candidate.manifestKey,
    });

    await assert.rejects(
        async () => service.materializeRows({
            candidates: [candidate],
            instanceId: 'sn-dev-01',
            scopeRecords: [
                {
                    recordSysId: 'rec-01',
                    table: 'x_app.ticket',
                },
                {
                    recordSysId: 'rec-02',
                    table: 'x_app.ticket',
                },
            ],
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        }),
        (error: unknown) => {
            assert.ok(error instanceof RestoreRowMaterializationError);
            assert.equal(error.code, 'missing_pit_candidates');

            return true;
        },
    );
});

test('materializeRows fails closed for ambiguous PIT winner candidates',
async () => {
    const { service } = buildServiceFixture();
    const candidateA = buildCandidate({
        artifactKey: 'rez/restore/event=evt-dupe-a.artifact.json',
        eventId: 'evt-dup',
        manifestKey: 'rez/restore/event=evt-dupe-a.manifest.json',
    });
    const candidateB = buildCandidate({
        artifactKey: 'rez/restore/event=evt-dupe-b.artifact.json',
        eventId: 'evt-dup',
        manifestKey: 'rez/restore/event=evt-dupe-b.manifest.json',
    });

    await assert.rejects(
        async () => service.materializeRows({
            candidates: [candidateA, candidateB],
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        }),
        (error: unknown) => {
            assert.ok(error instanceof RestoreRowMaterializationError);
            assert.equal(error.code, 'ambiguous_pit_candidates');

            return true;
        },
    );
});

test('materializeRows assembles deterministic rows and pit resolutions',
async () => {
    const fixtureA = buildServiceFixture();
    const fixtureB = buildServiceFixture();

    const alphaOlder = buildCandidate({
        artifactKey: 'rez/restore/event=evt-alpha-old.artifact.json',
        eventId: 'evt-alpha-old',
        eventTime: '2026-02-21T09:59:00.000Z',
        manifestKey: 'rez/restore/event=evt-alpha-old.manifest.json',
        recordSysId: 'rec-alpha',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 09:59:00',
    });
    const alphaWinner = buildCandidate({
        artifactKey: 'rez/restore/event=evt-alpha-win.artifact.json',
        eventId: 'evt-alpha-win',
        eventTime: '2026-02-21T10:00:00.000Z',
        manifestKey: 'rez/restore/event=evt-alpha-win.manifest.json',
        recordSysId: 'rec-alpha',
        sysModCount: 2,
        sysUpdatedOn: '2026-02-21 10:00:00',
    });
    const bravoWinner = buildCandidate({
        artifactKey: 'rez/restore/event=evt-bravo-win.artifact.json',
        eventId: 'evt-bravo-win',
        eventTime: '2026-02-21T10:01:00.000Z',
        manifestKey: 'rez/restore/event=evt-bravo-win.manifest.json',
        recordSysId: 'rec-bravo',
        sysModCount: 1,
        sysUpdatedOn: '2026-02-21 10:01:00',
    });

    const allCandidates = [
        alphaOlder,
        alphaWinner,
        bravoWinner,
    ];

    const artifactsByKey = new Map<string, Record<string, unknown>>([
        [
            `${alphaOlder.artifactKey}|${alphaOlder.manifestKey}`,
            buildArtifact({
                __op: 'U',
                row_enc: encryptedPayload('alpha-older'),
            }),
        ],
        [
            `${alphaWinner.artifactKey}|${alphaWinner.manifestKey}`,
            buildArtifact({
                __op: 'I',
                row_enc: encryptedPayload('alpha-winner'),
            }),
        ],
        [
            `${bravoWinner.artifactKey}|${bravoWinner.manifestKey}`,
            buildArtifact({
                __op: 'D',
                row_enc: encryptedPayload('bravo-winner'),
            }),
        ],
    ]);

    for (const candidate of allCandidates) {
        const key = `${candidate.artifactKey}|${candidate.manifestKey}`;
        const body = artifactsByKey.get(key);

        assert.ok(body);
        fixtureA.reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body,
            manifestKey: candidate.manifestKey,
        });
    }

    for (const candidate of [...allCandidates].reverse()) {
        const key = `${candidate.artifactKey}|${candidate.manifestKey}`;
        const body = artifactsByKey.get(key);

        assert.ok(body);
        fixtureB.reader.setArtifactBody({
            artifactKey: candidate.artifactKey,
            body,
            manifestKey: candidate.manifestKey,
        });
    }

    const resultA = await fixtureA.service.materializeRows({
        candidates: [
            bravoWinner,
            alphaWinner,
            alphaOlder,
        ],
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    });
    const resultB = await fixtureB.service.materializeRows({
        candidates: [
            alphaOlder,
            bravoWinner,
            alphaWinner,
        ],
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    });

    assert.deepEqual(resultA.rows, resultB.rows);
    assert.deepEqual(resultA.pitResolutions, resultB.pitResolutions);
    assert.equal(resultA.rows[0].row_id < resultA.rows[1].row_id, true);
    assert.equal(resultA.rows[0].action, 'insert');
    assert.equal(resultA.rows[1].action, 'delete');
    assert.equal(
        resultA.pitResolutions.map((resolution) => resolution.winning_event_id)
            .join(','),
        'evt-alpha-win,evt-bravo-win',
    );
});
