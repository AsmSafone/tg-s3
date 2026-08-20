import type { ChunkRow, ObjectRow } from '../types';

export interface ByteRange {
  start: number;
  end: number;
}

export interface ChunkSlice {
  chunk: ChunkRow;
  start: number;
  endExclusive: number;
}

export function isChunkedObject(obj: Pick<ObjectRow, 'is_chunked' | 'tg_file_id'>): boolean {
  return obj.is_chunked === 1 || obj.tg_file_id === '__chunked__';
}

/** Validate that chunks cover exactly [0, objectSize) with no gaps or overlap. */
export function validateChunkLayout(chunks: ChunkRow[], objectSize: number, expectedCount?: number | null): string | null {
  if (expectedCount !== undefined && expectedCount !== null && chunks.length !== expectedCount) {
    return `Expected ${expectedCount} chunks, found ${chunks.length}.`;
  }
  if (objectSize > 0 && chunks.length === 0) return 'Chunked object has no chunks.';

  let offset = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    if (chunk.chunk_index !== i) return `Expected chunk index ${i}, found ${chunk.chunk_index}.`;
    if (chunk.offset !== offset) return `Expected chunk ${i} at offset ${offset}, found ${chunk.offset}.`;
    if (!Number.isSafeInteger(chunk.size) || chunk.size <= 0) return `Chunk ${i} has invalid size ${chunk.size}.`;
    offset += chunk.size;
    if (!Number.isSafeInteger(offset)) return 'Chunked object size exceeds JavaScript safe integer range.';
  }
  return offset === objectSize ? null : `Chunk layout totals ${offset} bytes, object metadata says ${objectSize}.`;
}

/** Map an object byte range onto the minimal set of physical chunk slices. */
export function selectChunkSlices(chunks: ChunkRow[], range: ByteRange): ChunkSlice[] {
  const slices: ChunkSlice[] = [];
  for (const chunk of chunks) {
    const chunkEnd = chunk.offset + chunk.size - 1;
    if (chunkEnd < range.start) continue;
    if (chunk.offset > range.end) break;
    slices.push({
      chunk,
      start: Math.max(0, range.start - chunk.offset),
      endExclusive: Math.min(chunk.size, range.end - chunk.offset + 1),
    });
  }
  return slices;
}
