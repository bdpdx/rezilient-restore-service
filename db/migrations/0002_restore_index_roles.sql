BEGIN;

DO $$
BEGIN
    CREATE ROLE rez_restore_indexer_rw
    NOINHERIT
    LOGIN
    PASSWORD 'rez_restore_indexer_dev';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

DO $$
BEGIN
    CREATE ROLE rez_restore_service_ro
    NOINHERIT
    LOGIN
    PASSWORD 'rez_restore_service_dev';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

ALTER ROLE rez_restore_indexer_rw PASSWORD 'rez_restore_indexer_dev';
ALTER ROLE rez_restore_service_ro PASSWORD 'rez_restore_service_dev';

GRANT USAGE ON SCHEMA rez_restore_index TO rez_restore_indexer_rw;
GRANT USAGE ON SCHEMA rez_restore_index TO rez_restore_service_ro;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA rez_restore_index
TO rez_restore_indexer_rw;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA rez_restore_index
TO rez_restore_indexer_rw;

GRANT SELECT
ON ALL TABLES IN SCHEMA rez_restore_index
TO rez_restore_service_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rez_restore_indexer_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT USAGE, SELECT ON SEQUENCES TO rez_restore_indexer_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT ON TABLES TO rez_restore_service_ro;

COMMIT;
