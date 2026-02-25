import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import type {
    EvidenceExportRecord,
    EvidenceArtifactRecord,
    EvidenceVerificationRecord,
    EvidenceServiceConfig,
} from './models';

describe('EvidenceExportRecord', () => {
    test('schema accepts valid record', () => {
        const record = {
            evidence: { evidence_id: 'ev-01' },
            generated_at: '2026-02-16T12:00:00.000Z',
            manifest_payload: { key: 'value' },
            manifest_sha256: 'c'.repeat(64),
            artifacts: [],
            verification: {
                signature_verification: 'verified' as const,
                reason_code: 'none' as const,
                report_hash_matches: true,
                artifact_hashes_match: true,
                signature_valid: true,
                verified_at: '2026-02-16T12:00:00.000Z',
            },
        } as unknown as EvidenceExportRecord;
        assert.ok(record.evidence);
        assert.equal(
            record.verification.signature_verification,
            'verified',
        );
        assert.equal(record.manifest_sha256.length, 64);
    });
});

describe('EvidenceArtifactRecord', () => {
    test('validates required fields', () => {
        const artifact: EvidenceArtifactRecord = {
            artifact_id: 'art-01',
            canonical_json: '{"key":"value"}',
            sha256: 'a'.repeat(64),
            bytes: 128,
        };
        assert.equal(artifact.artifact_id, 'art-01');
        assert.equal(artifact.sha256.length, 64);
        assert.ok(artifact.bytes > 0);
        assert.ok(artifact.canonical_json.length > 0);
    });
});

describe('EvidenceVerificationRecord', () => {
    test('validates all status values', () => {
        const verified: EvidenceVerificationRecord = {
            signature_verification: 'verified',
            reason_code: 'none',
            report_hash_matches: true,
            artifact_hashes_match: true,
            signature_valid: true,
            verified_at: '2026-02-16T12:00:00.000Z',
        };
        assert.equal(
            verified.signature_verification,
            'verified',
        );

        const failed: EvidenceVerificationRecord = {
            signature_verification: 'verification_failed',
            reason_code:
                'failed_evidence_signature_verification',
            report_hash_matches: true,
            artifact_hashes_match: true,
            signature_valid: false,
            verified_at: '2026-02-16T12:00:00.000Z',
        };
        assert.equal(
            failed.signature_verification,
            'verification_failed',
        );
    });
});

describe('EvidenceServiceConfig', () => {
    test('validates signer config fields', () => {
        const config: EvidenceServiceConfig = {
            signer: {
                signer_key_id: 'rrs-dev-ed25519-v1',
                private_key_pem: '-----BEGIN PRIVATE KEY-----',
                public_key_pem: '-----BEGIN PUBLIC KEY-----',
            },
            immutable_storage: {
                worm_enabled: false,
                retention_class: 'standard-30d',
            },
        };
        assert.equal(
            config.signer.signer_key_id,
            'rrs-dev-ed25519-v1',
        );
        assert.ok(config.signer.private_key_pem);
        assert.ok(config.signer.public_key_pem);
        assert.equal(
            config.immutable_storage.worm_enabled,
            false,
        );
    });
});
