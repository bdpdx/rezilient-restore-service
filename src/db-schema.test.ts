import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { test } from 'node:test';

function readMigration(file: string): string {
    const migrationPath = resolve(
        __dirname,
        '..',
        'db',
        'migrations',
        file,
    );

    return readFileSync(migrationPath, 'utf8');
}

const indexSchemaSql = readMigration('0001_restore_index_plane.sql');
const indexRoleSql = readMigration('0002_restore_index_roles.sql');
const jobSchemaSql = readMigration('0003_restore_job_plane.sql');
const jobRoleSql = readMigration('0004_restore_job_roles.sql');
const resumeSchemaSql = readMigration('0005_restore_resume_journal_plane.sql');
const resumeRoleSql = readMigration('0006_restore_resume_journal_roles.sql');
const runtimeStateSchemaSql = readMigration(
    '0007_restore_runtime_state_plane.sql',
);
const runtimeStateRoleSql = readMigration(
    '0008_restore_runtime_state_roles.sql',
);
const sourceProgressSchemaSql = readMigration(
    '0009_restore_index_source_progress_plane.sql',
);
const sourceProgressRoleSql = readMigration(
    '0010_restore_index_source_progress_roles.sql',
);

test('RS-06 schema migration defines restore index data-plane tables', async () => {
    assert.match(
        indexSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.index_events/i,
    );
    assert.match(
        indexSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.partition_watermarks/i,
    );
    assert.match(
        indexSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.partition_generations/i,
    );
    assert.match(
        indexSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.source_coverage/i,
    );
    assert.match(
        indexSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.backfill_runs/i,
    );
    assert.match(
        indexSchemaSql,
        /UNIQUE\s*\([\s\S]*generation_id[\s\S]*event_id[\s\S]*\)/i,
    );
});

test('RS-06 role migration scopes rw and ro DB privileges', async () => {
    assert.match(
        indexRoleSql,
        /CREATE ROLE\s+rez_restore_indexer_rw/i,
    );
    assert.match(
        indexRoleSql,
        /CREATE ROLE\s+rez_restore_service_ro/i,
    );

    assert.match(
        indexRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+TO rez_restore_indexer_rw/i,
    );
    assert.match(
        indexRoleSql,
        /GRANT SELECT\s+ON ALL TABLES[\s\S]+TO rez_restore_service_ro/i,
    );

    assert.equal(
        /GRANT\s+[^;]*INSERT[^;]*TO\s+rez_restore_service_ro/i
            .test(indexRoleSql),
        false,
    );
});

test('RS-07 schema migration defines job, plan, and lock queue tables', async () => {
    assert.match(
        jobSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.source_registry/i,
    );
    assert.match(
        jobSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_plans/i,
    );
    assert.match(
        jobSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_jobs/i,
    );
    assert.match(
        jobSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_job_locks/i,
    );
    assert.match(
        jobSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_job_events/i,
    );
    assert.match(
        jobSchemaSql,
        /UNIQUE INDEX IF NOT EXISTS\s+ux_restore_job_running_locks/i,
    );
});

test('RS-07 schema migration persists approval placeholder metadata fields', async () => {
    assert.match(jobSchemaSql, /approval_required\s+BOOLEAN\s+NOT NULL/i);
    assert.match(jobSchemaSql, /approval_state\s+TEXT\s+NOT NULL/i);
    assert.match(
        jobSchemaSql,
        /approval_placeholder_mode\s+TEXT\s+NOT NULL\s+DEFAULT\s+'mvp_not_enforced'/i,
    );
});

test('RS-07 schema migration excludes plaintext restore payload columns', async () => {
    const combinedSql = `${indexSchemaSql}\n${jobSchemaSql}`;

    assert.equal(/\bdiff_plain\b/i.test(combinedSql), false);
    assert.equal(/\bbefore_image_plain\b/i.test(combinedSql), false);
    assert.equal(/\bafter_image_plain\b/i.test(combinedSql), false);
});

test('RS-07 role migration grants restore service write scope', async () => {
    assert.match(
        jobRoleSql,
        /CREATE ROLE\s+rez_restore_service_rw/i,
    );
    assert.match(
        jobRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+TO rez_restore_service_rw/i,
    );
    assert.match(
        jobRoleSql,
        /GRANT SELECT\s+ON TABLE[\s\S]+TO rez_restore_service_ro/i,
    );
});

test('RS-10 schema migration defines checkpoint and rollback journal tables', async () => {
    assert.match(
        resumeSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_execution_checkpoints/i,
    );
    assert.match(
        resumeSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_rollback_journal/i,
    );
    assert.match(
        resumeSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.restore_rollback_sn_mirror/i,
    );
    assert.match(
        resumeSchemaSql,
        /UNIQUE\s*\(job_id,\s*row_attempt_key\)/i,
    );
});

test('RS-10 schema migration keeps journal before-image encrypted only', async () => {
    assert.match(resumeSchemaSql, /before_image_enc\s+JSONB/i);
    assert.equal(/\bbefore_image_plain\b/i.test(resumeSchemaSql), false);
});

test('RS-10 role migration grants resume/journal table privileges', async () => {
    assert.match(
        resumeRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+restore_execution_checkpoints/i,
    );
    assert.match(
        resumeRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+restore_rollback_journal/i,
    );
    assert.match(
        resumeRoleSql,
        /GRANT SELECT[\s\S]+restore_rollback_sn_mirror/i,
    );
});

test('RS-10 schema migration defines runtime snapshot state table', async () => {
    assert.match(
        runtimeStateSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.rrs_state_snapshots/i,
    );
    assert.match(runtimeStateSchemaSql, /store_key\s+TEXT/i);
    assert.match(runtimeStateSchemaSql, /state_json\s+JSONB/i);
});

test('RS-10 role migration grants runtime snapshot table privileges', async () => {
    assert.match(
        runtimeStateRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+rrs_state_snapshots/i,
    );
    assert.match(
        runtimeStateRoleSql,
        /GRANT SELECT[\s\S]+rrs_state_snapshots/i,
    );
});

test('RS-06 schema migration defines source-progress checkpoint table', async () => {
    assert.match(
        sourceProgressSchemaSql,
        /CREATE TABLE IF NOT EXISTS\s+rez_restore_index\.source_progress/i,
    );
    assert.match(sourceProgressSchemaSql, /last_indexed_offset\s+BIGINT/i);
    assert.match(sourceProgressSchemaSql, /processed_count\s+BIGINT/i);
});

test('RS-06 role migration grants source-progress table privileges', async () => {
    assert.match(
        sourceProgressRoleSql,
        /GRANT SELECT, INSERT, UPDATE, DELETE[\s\S]+source_progress/i,
    );
    assert.match(
        sourceProgressRoleSql,
        /GRANT SELECT[\s\S]+source_progress/i,
    );
});
