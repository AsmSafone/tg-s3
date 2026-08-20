import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { PROMOTE_MULTIPART_CHUNKS_SQL } from '../src/storage/metadata.ts';

function quote(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function runSql(db: string, sql: string): string {
  const result = spawnSync('sqlite3', [db], { input: sql, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

function fixture(): { dir: string; db: string } {
  const dir = mkdtempSync(join(tmpdir(), 'tg-s3-chunks-'));
  const db = join(dir, 'test.db');
  runSql(db, readFileSync(new URL('../src/storage/schema.sql', import.meta.url), 'utf8'));
  runSql(db, `
    INSERT INTO buckets(name, created_at, tg_chat_id) VALUES('b', '2026-01-01', 'chat');
  `);
  return { dir, db };
}

test('Complete promotes selected parts, preserves order, and discards the upload session', () => {
  const { dir, db } = fixture();
  try {
    runSql(db, `
      INSERT INTO multipart_uploads(upload_id,bucket,key,created_at) VALUES('u','b','large.bin','2026-01-01');
      INSERT INTO multipart_parts VALUES
        ('u',1,5242880,'"a"','chat',11,'f1','2026-01-01'),
        ('u',2,7340032,'"b"','chat',12,'f2','2026-01-01'),
        ('u',4,1048576,'"d"','chat',14,'f4','2026-01-01');
    `);

    const promote = PROMOTE_MULTIPART_CHUNKS_SQL
      .replaceAll('?1', quote('b'))
      .replaceAll('?2', quote('large.bin'))
      .replaceAll('?3', quote('[1,4]'))
      .replaceAll('?4', quote('u'));
    runSql(db, `
      BEGIN;
      DELETE FROM chunks WHERE bucket='b' AND key='large.bin';
      ${promote};
      INSERT INTO objects(
        bucket,key,size,etag,content_type,last_modified,tg_chat_id,tg_message_id,
        tg_file_id,tg_file_unique_id,is_chunked,chunk_count
      ) VALUES('b','large.bin',6291456,'"etag-2"','application/octet-stream','2026-01-01','chat',0,'__chunked__','__chunked__',1,2);
      DELETE FROM multipart_uploads WHERE upload_id='u';
      DELETE FROM multipart_parts WHERE upload_id='u';
      COMMIT;
    `);

    assert.equal(
      runSql(db, "SELECT chunk_index || ':' || offset || ':' || size || ':' || tg_message_id FROM chunks ORDER BY chunk_index;"),
      '0:0:5242880:11\n1:5242880:1048576:14',
    );
    assert.equal(runSql(db, "SELECT is_chunked || ':' || chunk_count || ':' || tg_file_id FROM objects WHERE key='large.bin';"), '1:2:__chunked__');
    assert.equal(runSql(db, "SELECT COUNT(*) FROM multipart_uploads WHERE upload_id='u';"), '0');
    assert.equal(runSql(db, "SELECT COUNT(*) FROM multipart_parts WHERE upload_id='u';"), '0');
    // A duplicate Complete observes no upload session and therefore cannot
    // promote or delete the permanent chunk rows a second time.
    assert.equal(runSql(db, "SELECT COUNT(*) FROM chunks WHERE bucket='b' AND key='large.bin';"), '2');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('Abort removes temporary parts without creating permanent chunks', () => {
  const { dir, db } = fixture();
  try {
    runSql(db, `
      INSERT INTO multipart_uploads(upload_id,bucket,key,created_at) VALUES('abort','b','x','2026-01-01');
      INSERT INTO multipart_parts VALUES('abort',1,10,'"a"','chat',21,'temp','2026-01-01');
      DELETE FROM multipart_uploads WHERE upload_id='abort';
      DELETE FROM multipart_parts WHERE upload_id='abort';
    `);
    assert.equal(runSql(db, "SELECT COUNT(*) FROM multipart_uploads WHERE upload_id='abort';"), '0');
    assert.equal(runSql(db, "SELECT COUNT(*) FROM multipart_parts WHERE upload_id='abort';"), '0');
    assert.equal(runSql(db, "SELECT COUNT(*) FROM chunks WHERE key='x';"), '0');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('Delete removes every permanent chunk row for the logical object', () => {
  const { dir, db } = fixture();
  try {
    runSql(db, `
      INSERT INTO objects(
        bucket,key,size,etag,content_type,last_modified,tg_chat_id,tg_message_id,
        tg_file_id,tg_file_unique_id,is_chunked,chunk_count
      ) VALUES('b','delete.bin',20,'"e"','application/octet-stream','2026-01-01','chat',0,'__chunked__','__chunked__',1,2);
      INSERT INTO chunks VALUES
        ('b','delete.bin',0,0,10,'chat',31,'c1'),
        ('b','delete.bin',1,10,10,'chat',32,'c2');
      DELETE FROM objects WHERE bucket='b' AND key='delete.bin';
      DELETE FROM chunks WHERE bucket='b' AND key='delete.bin';
    `);
    assert.equal(runSql(db, "SELECT COUNT(*) FROM objects WHERE key='delete.bin';"), '0');
    assert.equal(runSql(db, "SELECT COUNT(*) FROM chunks WHERE key='delete.bin';"), '0');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
