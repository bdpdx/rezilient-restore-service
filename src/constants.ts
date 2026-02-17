import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from '@rezilient/types';

export const RESTORE_INDEX_SCHEMA_VERSION =
    'restore.index.schema.v1';
export const RESTORE_DB_ROLE_PROFILE_VERSION =
    'restore.index.db-roles.v1';

export const RESTORE_JOB_SCHEMA_VERSION =
    'restore.job.schema.v1';
export const RESTORE_JOB_QUEUE_VERSION =
    'restore.job.queue.v1';
export const RESTORE_JOB_AUDIT_VERSION =
    'restore.job.audit.v1';

export const AUTH_SERVICE_SCOPE_VERSION = 'auth.service-scope.v1';
export const AUTH_TOKEN_CLAIMS_VERSION = 'auth.token-claims.v1';

export const SERVICE_SCOPES = ['reg', 'rrs'] as const;

export type ServiceScope = (typeof SERVICE_SCOPES)[number];

export const RRS_SERVICE_SCOPE: ServiceScope = 'rrs';

export const AUTH_DENY_REASON_CODES = [
    'denied_auth_control_plane_outage',
    'denied_invalid_client',
    'denied_invalid_secret',
    'denied_invalid_grant',
    'denied_tenant_not_entitled',
    'denied_tenant_suspended',
    'denied_tenant_disabled',
    'denied_instance_suspended',
    'denied_instance_disabled',
    'denied_service_not_allowed',
    'denied_invalid_enrollment_code',
    'denied_enrollment_code_expired',
    'denied_enrollment_code_used',
    'denied_token_expired',
    'denied_token_invalid_signature',
    'denied_token_wrong_service_scope',
    'denied_token_malformed',
] as const;

export type AuthDenyReasonCode = (typeof AUTH_DENY_REASON_CODES)[number];

export {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
};
