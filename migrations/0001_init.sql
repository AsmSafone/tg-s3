-- Initial schema: all tables for fresh deployments.
-- This is the BASELINE table set. Columns introduced by later migrations
-- (is_public -> 0002, optimize_config -> 0003, default_encryption -> 0004)
-- MUST NOT be declared here, otherwise their `ALTER TABLE ... ADD COLUMN`
-- statements fail on a fresh deploy with "duplicate column name", which
-- also halts every subsequent migration.

CREATE TABLE IF NOT EXISTS buckets (
    name            TEXT    PRIMARY KEY,
    created_at      TEXT    NOT NULL,
    tg_chat_id      TEXT    NOT NULL,
    tg_topic_id     INTEGER,
    description     TEXT,
    object_count    INTEGER NOT NULL DEFAULT 0,
    total_size      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS objects (
    bucket          TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    size            INTEGER NOT NULL,
    etag            TEXT    NOT NULL,
    content_type    TEXT    NOT NULL DEFAULT 'application/octet-stream',
    last_modified   TEXT    NOT NULL,
    storage_class   TEXT    NOT NULL DEFAULT 'STANDARD',
    tg_chat_id      TEXT    NOT NULL,
    tg_message_id   INTEGER NOT NULL,
    tg_file_id      TEXT    NOT NULL,
    tg_file_unique_id TEXT  NOT NULL,
    user_metadata   TEXT,
    system_metadata TEXT,
    derived_from    TEXT,
    PRIMARY KEY (bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_objects_modified ON objects (bucket, last_modified);
CREATE INDEX IF NOT EXISTS idx_objects_file_uid ON objects (tg_file_unique_id);
CREATE INDEX IF NOT EXISTS idx_objects_derived ON objects (bucket, derived_from);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id       TEXT    PRIMARY KEY,
    bucket          TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    content_type    TEXT,
    user_metadata   TEXT,
    system_metadata TEXT
);

CREATE INDEX IF NOT EXISTS idx_multipart_created ON multipart_uploads (created_at);
CREATE INDEX IF NOT EXISTS idx_multipart_bucket_key ON multipart_uploads (bucket, key);

CREATE TABLE IF NOT EXISTS multipart_parts (
    upload_id       TEXT    NOT NULL,
    part_number     INTEGER NOT NULL,
    size            INTEGER NOT NULL,
    etag            TEXT    NOT NULL,
    tg_chat_id      TEXT    NOT NULL,
    tg_message_id   INTEGER NOT NULL,
    tg_file_id      TEXT    NOT NULL,
    created_at      TEXT,
    PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE IF NOT EXISTS share_tokens (
    token           TEXT    PRIMARY KEY,
    bucket          TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    expires_at      TEXT,
    password_hash   TEXT,
    max_downloads   INTEGER,
    download_count  INTEGER NOT NULL DEFAULT 0,
    creator         TEXT,
    note            TEXT
);

CREATE INDEX IF NOT EXISTS idx_share_expires ON share_tokens (expires_at);
CREATE INDEX IF NOT EXISTS idx_share_object ON share_tokens (bucket, key);

CREATE TABLE IF NOT EXISTS chunks (
    bucket          TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    chunk_index     INTEGER NOT NULL,
    offset          INTEGER NOT NULL,
    size            INTEGER NOT NULL,
    tg_chat_id      TEXT    NOT NULL,
    tg_message_id   INTEGER NOT NULL,
    tg_file_id      TEXT    NOT NULL,
    PRIMARY KEY (bucket, key, chunk_index)
);

CREATE TABLE IF NOT EXISTS user_preferences (
    chat_id         TEXT    NOT NULL,
    pref_key        TEXT    NOT NULL,
    pref_value      TEXT    NOT NULL,
    PRIMARY KEY (chat_id, pref_key)
);

CREATE TABLE IF NOT EXISTS share_password_attempts (
    token           TEXT    NOT NULL,
    ip              TEXT    NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    locked_until    TEXT,
    last_attempt    TEXT    NOT NULL,
    PRIMARY KEY (token, ip)
);

CREATE TABLE IF NOT EXISTS credentials (
    access_key_id       TEXT    PRIMARY KEY,
    secret_access_key   TEXT    NOT NULL,
    name                TEXT    NOT NULL DEFAULT '',
    buckets             TEXT    NOT NULL DEFAULT '*',
    permission          TEXT    NOT NULL DEFAULT 'admin',
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    last_used_at        TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS object_tags (
    bucket          TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    tag_key         TEXT    NOT NULL,
    tag_value       TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (bucket, key, tag_key),
    FOREIGN KEY (bucket, key) REFERENCES objects (bucket, key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lifecycle_rules (
    id              TEXT    PRIMARY KEY,
    bucket          TEXT    NOT NULL,
    prefix          TEXT    NOT NULL DEFAULT '',
    expiration_days INTEGER NOT NULL,
    tag_key         TEXT,
    tag_value       TEXT,
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (bucket) REFERENCES buckets (name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_bucket ON lifecycle_rules (bucket);
