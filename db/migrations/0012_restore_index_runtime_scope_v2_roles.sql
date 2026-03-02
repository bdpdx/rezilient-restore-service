BEGIN;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE
    rez_restore_index.source_progress_v2,
    rez_restore_index.source_leader_leases_v2
TO rez_restore_indexer_rw;

GRANT SELECT
ON TABLE
    rez_restore_index.source_progress_v2,
    rez_restore_index.source_leader_leases_v2
TO rez_restore_service_ro;

COMMIT;
