import test from 'node:test';
import assert from 'node:assert/strict';
import {
  isChunkedObject,
  selectChunkSlices,
  validateChunkLayout,
} from '../src/storage/chunk-layout.ts';

const MiB = 1024 * 1024;

function chunks(count: number, size: number) {
  return Array.from({ length: count }, (_, index) => ({
    bucket: 'bucket',
    key: 'large.bin',
    chunk_index: index,
    offset: index * size,
    size,
    tg_chat_id: 'chat',
    tg_message_id: index + 1,
    tg_file_id: `file-${index}`,
  }));
}

test('validates a logical object larger than 2GiB', () => {
  const rows = chunks(110, 20 * MiB);
  const total = rows.reduce((sum, row) => sum + row.size, 0);
  assert.ok(total > 2 * 1024 * MiB);
  assert.equal(validateChunkLayout(rows, total, rows.length), null);
});

test('maps a Range crossing a chunk boundary', () => {
  const rows = chunks(3, 20 * MiB);
  const start = 20 * MiB - 10;
  const end = 20 * MiB + 19;
  const slices = selectChunkSlices(rows, { start, end });

  assert.equal(slices.length, 2);
  assert.deepEqual(
    slices.map(slice => [slice.chunk.chunk_index, slice.start, slice.endExclusive]),
    [[0, 20 * MiB - 10, 20 * MiB], [1, 0, 20]],
  );
  assert.equal(slices.reduce((sum, slice) => sum + slice.endExclusive - slice.start, 0), 30);
});

test('selects only the chunk touched by a narrow Range', () => {
  const rows = chunks(4, 8 * MiB);
  const slices = selectChunkSlices(rows, { start: 9 * MiB, end: 9 * MiB + 99 });
  assert.equal(slices.length, 1);
  assert.equal(slices[0].chunk.chunk_index, 1);
  assert.equal(slices[0].start, MiB);
  assert.equal(slices[0].endExclusive, MiB + 100);
});

test('rejects missing, overlapping, and miscounted chunks', () => {
  const rows = chunks(3, 5 * MiB);
  rows[1].offset += 1;
  assert.match(validateChunkLayout(rows, 15 * MiB, 3) || '', /offset/);
  assert.match(validateChunkLayout(chunks(2, 5 * MiB), 10 * MiB, 3) || '', /Expected 3 chunks/);
});

test('recognizes both the schema flag and sentinel for compatibility', () => {
  assert.equal(isChunkedObject({ is_chunked: 1, tg_file_id: 'anything' }), true);
  assert.equal(isChunkedObject({ is_chunked: 0, tg_file_id: '__chunked__' }), true);
  assert.equal(isChunkedObject({ is_chunked: 0, tg_file_id: 'normal-file' }), false);
});
