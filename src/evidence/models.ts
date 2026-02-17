import type { RestoreEvidence } from '@rezilient/types';

export interface EvidenceArtifactMaterial {
    artifact_id: string;
    payload: unknown;
}

export interface EvidenceArtifactRecord {
    artifact_id: string;
    canonical_json: string;
    sha256: string;
    bytes: number;
}

export interface EvidenceImmutableStorageConfig {
    worm_enabled: boolean;
    retention_class: string;
}

export interface EvidenceSignerConfig {
    signer_key_id: string;
    private_key_pem?: string;
    public_key_pem?: string;
}

export interface EvidenceServiceConfig {
    signer: EvidenceSignerConfig;
    immutable_storage: EvidenceImmutableStorageConfig;
}

export interface EvidenceVerificationRecord {
    signature_verification: 'verified' | 'verification_failed';
    reason_code:
        | 'none'
        | 'failed_evidence_report_hash_mismatch'
        | 'failed_evidence_artifact_hash_mismatch'
        | 'failed_evidence_signature_verification';
    report_hash_matches: boolean;
    artifact_hashes_match: boolean;
    signature_valid: boolean;
    verified_at: string;
}

export interface EvidenceExportRecord {
    evidence: RestoreEvidence;
    generated_at: string;
    manifest_payload: Record<string, unknown>;
    manifest_sha256: string;
    artifacts: EvidenceArtifactRecord[];
    verification: EvidenceVerificationRecord;
}

export interface ExportEvidenceSuccess {
    success: true;
    statusCode: number;
    record: EvidenceExportRecord;
    reused: boolean;
}

export interface ExportEvidenceFailure {
    success: false;
    statusCode: number;
    error: string;
    reasonCode?: string;
    message: string;
}

export type ExportEvidenceResult = ExportEvidenceSuccess | ExportEvidenceFailure;
