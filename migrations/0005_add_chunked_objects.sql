-- Persist S3 multipart uploads as multiple Telegram documents instead of
-- consolidating them into one Telegram file.
ALTER TABLE objects ADD COLUMN is_chunked INTEGER NOT NULL DEFAULT 0;
ALTER TABLE objects ADD COLUMN chunk_count INTEGER;

CREATE INDEX IF NOT EXISTS idx_objects_chunked ON objects (is_chunked, bucket, key);
