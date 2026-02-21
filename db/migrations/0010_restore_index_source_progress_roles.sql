BEGIN;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE rez_restore_index.source_progress
TO rez_restore_indexer_rw;

GRANT SELECT
ON TABLE rez_restore_index.source_progress
TO rez_restore_service_ro;

COMMIT;
