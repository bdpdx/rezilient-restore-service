BEGIN;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE
    rez_restore_index.restore_execution_checkpoints,
    rez_restore_index.restore_rollback_journal,
    rez_restore_index.restore_rollback_sn_mirror
TO rez_restore_service_rw;

GRANT SELECT
ON TABLE
    rez_restore_index.restore_execution_checkpoints,
    rez_restore_index.restore_rollback_journal,
    rez_restore_index.restore_rollback_sn_mirror
TO rez_restore_service_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rez_restore_service_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT ON TABLES TO rez_restore_service_ro;

COMMIT;
