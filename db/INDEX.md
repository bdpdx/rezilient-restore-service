# db Index

- `migrations/0001_restore_index_plane.sql`: creates restore index schema and
  RS-06 data-plane tables.
- `migrations/0002_restore_index_roles.sql`: creates scoped DB roles and grant
  policies for indexer write and restore-service read responsibilities.
- `migrations/0003_restore_job_plane.sql`: creates RS-07 source mapping,
  restore plan metadata, restore job metadata, lock queue, and job audit
  tables with approval placeholder fields.
- `migrations/0004_restore_job_roles.sql`: grants restore-service read/write
  table privileges for RS-07 job/queue state and read-only visibility for
  `rez_restore_service_ro`.
- `migrations/0005_restore_resume_journal_plane.sql`: creates RS-10 resume
  checkpoints and authoritative rollback journal tables, plus compact SN mirror
  linkage table.
- `migrations/0006_restore_resume_journal_roles.sql`: grants restore-service
  read/write and read-only role access for RS-10 resume/journal tables.
