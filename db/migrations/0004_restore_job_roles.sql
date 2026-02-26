BEGIN;

DO $$
BEGIN
    CREATE ROLE rez_restore_service_rw
    NOINHERIT
    LOGIN
    PASSWORD 'rez_restore_service_dev';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

ALTER ROLE rez_restore_service_rw PASSWORD 'rez_restore_service_dev';

GRANT CONNECT, CREATE ON DATABASE rez_restore TO rez_restore_service_rw;

GRANT USAGE, CREATE ON SCHEMA rez_restore_index TO rez_restore_service_rw;

GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE
    rez_restore_index.partition_watermarks,
    rez_restore_index.source_registry,
    rez_restore_index.restore_plans,
    rez_restore_index.restore_jobs,
    rez_restore_index.restore_job_locks,
    rez_restore_index.restore_job_events
TO rez_restore_service_rw;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA rez_restore_index
TO rez_restore_service_rw;

GRANT SELECT
ON TABLE
    rez_restore_index.partition_watermarks,
    rez_restore_index.source_registry,
    rez_restore_index.restore_plans,
    rez_restore_index.restore_jobs,
    rez_restore_index.restore_job_locks,
    rez_restore_index.restore_job_events
TO rez_restore_service_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rez_restore_service_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT USAGE, SELECT ON SEQUENCES TO rez_restore_service_rw;

ALTER DEFAULT PRIVILEGES IN SCHEMA rez_restore_index
GRANT SELECT ON TABLES TO rez_restore_service_ro;

COMMIT;
