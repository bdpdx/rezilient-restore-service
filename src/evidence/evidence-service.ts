import { createHash } from 'node:crypto';
import {
    canonicalJsonStringify,
    EVIDENCE_CANONICALIZATION_VERSION,
    PIT_ALGORITHM_VERSION,
    RESTORE_CONTRACT_VERSION,
    RestoreEvidence,
    type RestoreEvidenceArtifactHash,
} from '@rezilient/types';
import { RestoreExecutionService } from '../execute/execute-service';
import {
    normalizeIsoWithMillis,
    RestoreJobRecord,
} from '../jobs/models';
import { RestoreJobService } from '../jobs/job-service';
import { RestoreDryRunPlanRecord } from '../plans/models';
import { RestorePlanService } from '../plans/plan-service';
import {
    InMemoryRestoreEvidenceStateStore,
    RestoreEvidenceState,
    RestoreEvidenceStateStore,
} from './evidence-state-store';
import {
    EvidenceArtifactMaterial,
    EvidenceArtifactRecord,
    EvidenceExportRecord,
    EvidenceServiceConfig,
    EvidenceVerificationRecord,
    ExportEvidenceResult,
} from './models';
import {
    createEvidenceSigner,
    EvidenceSigner,
    signCanonicalPayload,
    verifyCanonicalSignature,
} from './signature';

function toSha256Hex(canonicalJson: string): string {
    return createHash('sha256')
        .update(canonicalJson, 'utf8')
        .digest('hex');
}

function withoutUndefined(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.map((entry) => withoutUndefined(entry));
    }

    if (value && typeof value === 'object') {
        const out: Record<string, unknown> = {};

        for (const [key, entryValue] of Object.entries(
            value as Record<string, unknown>,
        )) {
            if (entryValue === undefined) {
                continue;
            }

            out[key] = withoutUndefined(entryValue);
        }

        return out;
    }

    return value;
}

function cloneRecord(record: EvidenceExportRecord): EvidenceExportRecord {
    return JSON.parse(JSON.stringify(record)) as EvidenceExportRecord;
}

function toArtifactRecord(
    material: EvidenceArtifactMaterial,
): EvidenceArtifactRecord {
    const canonicalJson = canonicalJsonStringify(
        withoutUndefined(material.payload),
    );

    return {
        artifact_id: material.artifact_id,
        canonical_json: canonicalJson,
        sha256: toSha256Hex(canonicalJson),
        bytes: Buffer.byteLength(canonicalJson, 'utf8'),
    };
}

function normalizeArtifactHashes(
    hashes: RestoreEvidenceArtifactHash[],
): RestoreEvidenceArtifactHash[] {
    return [...hashes]
        .sort((left, right) => {
            return left.artifact_id.localeCompare(right.artifact_id);
        })
        .map((hash) => ({
            artifact_id: hash.artifact_id,
            sha256: hash.sha256,
            bytes: hash.bytes,
        }));
}

function reportHashInputFromEvidence(
    evidence: RestoreEvidence,
): Record<string, unknown> {
    return {
        contract_version: evidence.contract_version,
        evidence_id: evidence.evidence_id,
        job_id: evidence.job_id,
        plan_hash: evidence.plan_hash,
        pit_algorithm_version: evidence.pit_algorithm_version,
        backup_timestamp: evidence.backup_timestamp,
        approved_scope: evidence.approved_scope,
        schema_drift_summary: evidence.schema_drift_summary,
        conflict_summary: evidence.conflict_summary,
        delete_decision_summary: evidence.delete_decision_summary,
        execution_outcomes: evidence.execution_outcomes,
        resume_metadata: evidence.resume_metadata,
        artifact_hashes: evidence.artifact_hashes,
        canonicalization_version: evidence.canonicalization_version,
        immutable_storage: evidence.immutable_storage,
        approval: evidence.approval,
    };
}

function buildManifestPayload(
    evidence: RestoreEvidence,
): Record<string, unknown> {
    return {
        contract_version: evidence.contract_version,
        evidence_id: evidence.evidence_id,
        job_id: evidence.job_id,
        plan_hash: evidence.plan_hash,
        report_hash: evidence.report_hash,
        artifact_hashes: evidence.artifact_hashes,
        canonicalization_version: evidence.canonicalization_version,
        immutable_storage: evidence.immutable_storage,
    };
}

function buildSchemaDriftSummary(
    plan: RestoreDryRunPlanRecord,
): {
    compatible_columns: number;
    incompatible_columns: number;
    override_applied: boolean;
} {
    const incompatible = plan.plan.conflicts.filter((conflict) =>
        conflict.class === 'schema_conflict'
    ).length;

    return {
        compatible_columns: Math.max(
            0,
            plan.plan_hash_input.rows.length - incompatible,
        ),
        incompatible_columns: incompatible,
        override_applied:
            plan.plan.execution_options.schema_compatibility_mode ===
            'manual_override',
    };
}

function buildConflictSummary(
    plan: RestoreDryRunPlanRecord,
): {
    total: number;
    unresolved: number;
} {
    return {
        total: plan.plan.conflicts.length,
        unresolved: plan.plan.conflicts.filter((conflict) => {
            return !conflict.resolution;
        }).length,
    };
}

function buildDeleteDecisionSummary(
    plan: RestoreDryRunPlanRecord,
): {
    allow_deletion: number;
    skip_deletion: number;
} {
    let allowDeletion = 0;
    let skipDeletion = 0;

    for (const candidate of plan.delete_candidates) {
        if (candidate.decision === 'allow_deletion') {
            allowDeletion += 1;
        } else if (candidate.decision === 'skip_deletion') {
            skipDeletion += 1;
        }
    }

    return {
        allow_deletion: allowDeletion,
        skip_deletion: skipDeletion,
    };
}

function buildEvidenceId(
    job: RestoreJobRecord,
    completedAt: string | null,
): string {
    const checksum = createHash('sha256')
        .update(
            `${job.job_id}:${job.plan_hash}:${completedAt || ''}`,
            'utf8',
        )
        .digest('hex');

    return `evidence_${checksum.slice(0, 24)}`;
}

export class RestoreEvidenceService {
    private readonly byJobId = new Map<string, EvidenceExportRecord>();

    private readonly byEvidenceId = new Map<string, EvidenceExportRecord>();

    private readonly signer: EvidenceSigner;

    private initialized = false;

    private initializationPromise: Promise<void> | null = null;

    constructor(
        private readonly jobs: RestoreJobService,
        private readonly plans: RestorePlanService,
        private readonly execute: RestoreExecutionService,
        private readonly config: EvidenceServiceConfig,
        private readonly now: () => Date = () => new Date(),
        private readonly stateStore: RestoreEvidenceStateStore =
            new InMemoryRestoreEvidenceStateStore(),
    ) {
        this.signer = createEvidenceSigner(config.signer);
    }

    async exportEvidence(jobId: string): Promise<ExportEvidenceResult> {
        await this.ensureInitialized();

        const existing = this.byJobId.get(jobId);

        if (existing) {
            return {
                success: true,
                statusCode: 200,
                record: cloneRecord(existing),
                reused: true,
            };
        }

        const job = await this.jobs.getJob(jobId);

        if (!job) {
            return {
                success: false,
                statusCode: 404,
                error: 'not_found',
                message: 'job not found',
            };
        }

        const plan = await this.plans.getPlan(job.plan_id);

        if (!plan) {
            return {
                success: false,
                statusCode: 409,
                error: 'plan_missing',
                reasonCode: 'failed_internal_error',
                message: 'immutable plan record is unavailable for evidence',
            };
        }

        const execution = await this.execute.getExecution(jobId);

        if (
            !execution ||
            (execution.status !== 'completed' && execution.status !== 'failed')
        ) {
            return {
                success: false,
                statusCode: 409,
                error: 'evidence_not_ready',
                reasonCode: 'blocked_evidence_not_ready',
                message: 'evidence export requires terminal execution state',
            };
        }

        const rollbackJournal = await this.execute.getRollbackJournal(jobId) || {
            journal_entries: [],
            sn_mirror_entries: [],
        };
        const events = await this.jobs.listJobEvents(jobId);
        const generatedAt = normalizeIsoWithMillis(this.now());
        const evidenceId = buildEvidenceId(job, execution.completed_at);
        const artifacts = [
            toArtifactRecord({
                artifact_id: 'plan.json',
                payload: {
                    plan: plan.plan,
                    plan_hash_input: plan.plan_hash_input,
                    gate: plan.gate,
                    delete_candidates: plan.delete_candidates,
                    media_candidates: plan.media_candidates,
                    pit_resolutions: plan.pit_resolutions,
                    watermarks: plan.watermarks,
                },
            }),
            toArtifactRecord({
                artifact_id: 'execution.json',
                payload: execution,
            }),
            toArtifactRecord({
                artifact_id: 'rollback-journal.json',
                payload: {
                    rollback_journal: rollbackJournal.journal_entries,
                    sn_mirror: rollbackJournal.sn_mirror_entries,
                },
            }),
            toArtifactRecord({
                artifact_id: 'job-events.json',
                payload: events,
            }),
        ].sort((left, right) => {
            return left.artifact_id.localeCompare(right.artifact_id);
        });
        const artifactHashes = artifacts.map((artifact) => ({
            artifact_id: artifact.artifact_id,
            sha256: artifact.sha256,
            bytes: artifact.bytes,
        }));
        const reportHashInput = {
            contract_version: RESTORE_CONTRACT_VERSION,
            evidence_id: evidenceId,
            job_id: job.job_id,
            plan_hash: plan.plan.plan_hash,
            pit_algorithm_version: PIT_ALGORITHM_VERSION,
            backup_timestamp: plan.plan.pit.restore_time,
            approved_scope: plan.plan.scope,
            schema_drift_summary: buildSchemaDriftSummary(plan),
            conflict_summary: buildConflictSummary(plan),
            delete_decision_summary: buildDeleteDecisionSummary(plan),
            execution_outcomes: {
                rows_applied: execution.summary.applied_rows,
                rows_skipped: execution.summary.skipped_rows,
                rows_failed: execution.summary.failed_rows,
                attachments_applied: execution.summary.attachments_applied,
                attachments_skipped: execution.summary.attachments_skipped,
                attachments_failed: execution.summary.attachments_failed,
            },
            resume_metadata: {
                resume_attempt_count: execution.resume_attempt_count,
                checkpoint_id: execution.checkpoint.checkpoint_id,
                next_chunk_index: execution.checkpoint.next_chunk_index,
                total_chunks: execution.checkpoint.total_chunks,
                last_chunk_id: execution.checkpoint.last_chunk_id,
                plan_checksum: execution.plan_checksum,
                precondition_checksum: execution.precondition_checksum,
            },
            artifact_hashes: artifactHashes,
            canonicalization_version: EVIDENCE_CANONICALIZATION_VERSION,
            immutable_storage: {
                worm_enabled: this.config.immutable_storage.worm_enabled,
                retention_class: this.config.immutable_storage.retention_class,
            },
            approval: plan.plan.approval,
        };
        const reportHash = toSha256Hex(
            canonicalJsonStringify(withoutUndefined(reportHashInput)),
        );
        const manifestSignature: {
            signature_algorithm: 'ed25519';
            signer_key_id: string;
            signature: string;
            signature_verification:
                | 'verified'
                | 'verification_pending'
                | 'verification_failed';
            signed_at: string;
        } = {
            signature_algorithm: 'ed25519' as const,
            signer_key_id: this.signer.signer_key_id,
            signature: '',
            signature_verification: 'verification_pending' as const,
            signed_at: generatedAt,
        };
        const evidenceCandidate = {
            ...reportHashInput,
            report_hash: reportHash,
            manifest_signature: manifestSignature,
        };
        const manifestPayload = buildManifestPayload(
            evidenceCandidate as RestoreEvidence,
        );
        const signature = signCanonicalPayload(manifestPayload, this.signer);
        evidenceCandidate.manifest_signature.signature = signature;
        evidenceCandidate.manifest_signature.signature_verification =
            verifyCanonicalSignature(manifestPayload, signature, this.signer)
                ? 'verified'
                : 'verification_failed';
        const parsedEvidence = RestoreEvidence.safeParse(evidenceCandidate);

        if (!parsedEvidence.success) {
            return {
                success: false,
                statusCode: 500,
                error: 'evidence_contract_violation',
                reasonCode: 'failed_internal_error',
                message:
                    parsedEvidence.error.issues[0]?.message ||
                    'failed to build valid evidence package',
            };
        }

        const record: EvidenceExportRecord = {
            evidence: parsedEvidence.data,
            generated_at: generatedAt,
            manifest_payload: manifestPayload,
            manifest_sha256: toSha256Hex(
                canonicalJsonStringify(withoutUndefined(manifestPayload)),
            ),
            artifacts,
            verification: {
                signature_verification:
                    parsedEvidence.data.manifest_signature
                        .signature_verification === 'verified'
                        ? 'verified'
                        : 'verification_failed',
                reason_code:
                    parsedEvidence.data.manifest_signature
                        .signature_verification === 'verified'
                        ? 'none'
                        : 'failed_evidence_signature_verification',
                report_hash_matches: true,
                artifact_hashes_match: true,
                signature_valid:
                    parsedEvidence.data.manifest_signature
                        .signature_verification === 'verified',
                verified_at: generatedAt,
            },
        };
        const verification = this.validateEvidenceRecord(record);
        record.verification = verification;
        record.evidence.manifest_signature.signature_verification =
            verification.signature_verification;
        await this.setEvidenceRecord(jobId, record);

        return {
            success: true,
            statusCode: 201,
            record: cloneRecord(record),
            reused: false,
        };
    }

    async ensureEvidence(jobId: string): Promise<ExportEvidenceResult> {
        return this.exportEvidence(jobId);
    }

    async getEvidence(jobId: string): Promise<EvidenceExportRecord | null> {
        await this.ensureInitialized();

        const existing = this.byJobId.get(jobId);

        if (!existing) {
            return null;
        }

        const updated = cloneRecord(existing);
        updated.verification = this.validateEvidenceRecord(updated);
        updated.evidence.manifest_signature.signature_verification =
            updated.verification.signature_verification;
        await this.setEvidenceRecord(jobId, updated);

        return updated;
    }

    async getEvidenceById(
        evidenceId: string,
    ): Promise<EvidenceExportRecord | null> {
        await this.ensureInitialized();

        const existing = this.byEvidenceId.get(evidenceId);

        if (!existing) {
            return null;
        }

        return cloneRecord(existing);
    }

    async listEvidence(): Promise<EvidenceExportRecord[]> {
        await this.ensureInitialized();

        return Array.from(this.byJobId.values())
            .map((record) => cloneRecord(record))
            .sort((left, right) => {
                return left.generated_at.localeCompare(right.generated_at);
            });
    }

    private async setEvidenceRecord(
        jobId: string,
        record: EvidenceExportRecord,
    ): Promise<void> {
        this.byJobId.set(jobId, cloneRecord(record));
        this.byEvidenceId.set(record.evidence.evidence_id, cloneRecord(record));
        await this.persistState();
    }

    private async persistState(): Promise<void> {
        const snapshot = this.snapshotState();

        await this.stateStore.mutate((state) => {
            state.by_job_id = snapshot.by_job_id;
            state.by_evidence_id = snapshot.by_evidence_id;
        });
    }

    private snapshotState(): RestoreEvidenceState {
        const byJobId: Record<string, EvidenceExportRecord> = {};

        for (const [jobId, record] of this.byJobId.entries()) {
            byJobId[jobId] = record;
        }

        const byEvidenceId: Record<string, EvidenceExportRecord> = {};

        for (const [evidenceId, record] of this.byEvidenceId.entries()) {
            byEvidenceId[evidenceId] = record;
        }

        return JSON.parse(
            JSON.stringify({
                by_job_id: byJobId,
                by_evidence_id: byEvidenceId,
            }),
        ) as RestoreEvidenceState;
    }

    validateEvidenceRecord(
        record: EvidenceExportRecord,
    ): EvidenceVerificationRecord {
        const verifiedAt = normalizeIsoWithMillis(this.now());
        const artifactHashesFromArtifacts = normalizeArtifactHashes(
            record.artifacts.map((artifact) => {
                return {
                    artifact_id: artifact.artifact_id,
                    sha256: toSha256Hex(artifact.canonical_json),
                    bytes: Buffer.byteLength(artifact.canonical_json, 'utf8'),
                };
            }),
        );
        const artifactHashesFromEvidence = normalizeArtifactHashes(
            record.evidence.artifact_hashes,
        );
        const artifact_hashes_match = canonicalJsonStringify(
            artifactHashesFromArtifacts,
        ) === canonicalJsonStringify(artifactHashesFromEvidence);
        const computedReportHash = toSha256Hex(
            canonicalJsonStringify(
                withoutUndefined(reportHashInputFromEvidence(record.evidence)),
            ),
        );
        const report_hash_matches =
            computedReportHash === record.evidence.report_hash;
        const manifestPayload = buildManifestPayload(record.evidence);
        const signature_valid = verifyCanonicalSignature(
            manifestPayload,
            record.evidence.manifest_signature.signature,
            this.signer,
        );

        if (!artifact_hashes_match) {
            return {
                signature_verification: 'verification_failed',
                reason_code: 'failed_evidence_artifact_hash_mismatch',
                report_hash_matches,
                artifact_hashes_match,
                signature_valid,
                verified_at: verifiedAt,
            };
        }

        if (!report_hash_matches) {
            return {
                signature_verification: 'verification_failed',
                reason_code: 'failed_evidence_report_hash_mismatch',
                report_hash_matches,
                artifact_hashes_match,
                signature_valid,
                verified_at: verifiedAt,
            };
        }

        if (!signature_valid) {
            return {
                signature_verification: 'verification_failed',
                reason_code: 'failed_evidence_signature_verification',
                report_hash_matches,
                artifact_hashes_match,
                signature_valid,
                verified_at: verifiedAt,
            };
        }

        return {
            signature_verification: 'verified',
            reason_code: 'none',
            report_hash_matches,
            artifact_hashes_match,
            signature_valid,
            verified_at: verifiedAt,
        };
    }

    private async ensureInitialized(): Promise<void> {
        if (this.initialized) {
            return;
        }

        if (!this.initializationPromise) {
            this.initializationPromise = this.initialize();
        }

        await this.initializationPromise;
    }

    private async initialize(): Promise<void> {
        const state = await this.stateStore.read();

        for (const [jobId, record] of Object.entries(state.by_job_id)) {
            this.byJobId.set(jobId, record);
        }

        for (const [evidenceId, record] of Object.entries(state.by_evidence_id)) {
            this.byEvidenceId.set(evidenceId, record);
        }

        this.initialized = true;
    }
}
