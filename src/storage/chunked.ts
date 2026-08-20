import type { ChunkRow, Env } from '../types';
import { downloadFromTelegram } from '../telegram/download';
import { decrypt, decryptS3 } from '../utils/sse';
import { selectChunkSlices, type ByteRange } from './chunk-layout';
export { isChunkedObject, selectChunkSlices, validateChunkLayout } from './chunk-layout';
import { BOT_API_GETFILE_LIMIT } from '../constants';
import { VpsClient } from '../media/vps-client';

export interface ChunkDecryption {
  sseKeyBase64?: string;
  sseS3KeyBase64?: string;
}

export async function downloadChunkPlaintext(
  chunk: ChunkRow,
  env: Env,
  decryption: ChunkDecryption = {},
): Promise<ArrayBuffer> {
  const encrypted = !!(decryption.sseKeyBase64 || decryption.sseS3KeyBase64);
  const physicalSize = chunk.size + (encrypted ? 28 : 0);
  const response = physicalSize > BOT_API_GETFILE_LIMIT && env.VPS_URL
    ? await new VpsClient(env).proxyGet(chunk.tg_file_id)
    : await downloadFromTelegram(chunk.tg_file_id, env);
  let data = await response.arrayBuffer();
  if (decryption.sseKeyBase64) {
    data = await decrypt(data, decryption.sseKeyBase64);
  } else if (decryption.sseS3KeyBase64) {
    data = await decryptS3(data, decryption.sseS3KeyBase64);
  }
  if (data.byteLength !== chunk.size) {
    throw new Error(`Chunk ${chunk.chunk_index} size mismatch: expected ${chunk.size}, got ${data.byteLength}.`);
  }
  return data;
}

/** Stream only the selected bytes, downloading chunks sequentially to bound memory. */
export function streamChunkRange(
  chunks: ChunkRow[],
  range: ByteRange,
  env: Env,
  decryption: ChunkDecryption = {},
): ReadableStream<Uint8Array> {
  const slices = selectChunkSlices(chunks, range);
  let sliceIndex = 0;
  let activeReader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let activeReceived = 0;
  let activeExpected = 0;

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        while (sliceIndex < slices.length) {
          if (activeReader) {
            const { done, value } = await activeReader.read();
            if (!done) {
              activeReceived += value.byteLength;
              controller.enqueue(value);
              return;
            }
            if (activeReceived !== activeExpected) {
              throw new Error(`Chunk ${slices[sliceIndex].chunk.chunk_index} range returned ${activeReceived} bytes; expected ${activeExpected}.`);
            }
            activeReader.releaseLock();
            activeReader = null;
            sliceIndex++;
            continue;
          }

          const slice = slices[sliceIndex];
          // Large VPS chunks can be forwarded as a true stream. Encrypted
          // chunks are decrypted and ranged by the processor, avoiding a
          // multi-gigabyte AES-GCM buffer in Worker memory.
          const decryptKey = decryption.sseKeyBase64 || decryption.sseS3KeyBase64;
          const physicalSize = slice.chunk.size + (decryptKey ? 28 : 0);
          if (physicalSize > BOT_API_GETFILE_LIMIT && env.VPS_URL) {
            const vps = new VpsClient(env);
            const response = decryptKey
              ? await vps.proxyGetDecrypt(slice.chunk.tg_file_id, decryptKey, slice.start, slice.endExclusive - 1)
              : await vps.proxyRange(slice.chunk.tg_file_id, slice.start, slice.endExclusive - 1);
            if (!response.body) throw new Error(`Chunk ${slice.chunk.chunk_index} returned no response body.`);
            activeReader = response.body.getReader();
            activeReceived = 0;
            activeExpected = slice.endExclusive - slice.start;
            continue;
          }
          const data = await downloadChunkPlaintext(slice.chunk, env, decryption);
          controller.enqueue(new Uint8Array(data, slice.start, slice.endExclusive - slice.start));
          sliceIndex++;
          return;
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
    async cancel(reason) {
      if (activeReader) {
        try { await activeReader.cancel(reason); } catch { /* best effort */ }
      }
    },
  });
}

/** Buffer a bounded range for APIs such as UploadPartCopy that require an ArrayBuffer. */
export async function readChunkRange(
  chunks: ChunkRow[],
  range: ByteRange,
  env: Env,
  decryption: ChunkDecryption = {},
  maxBytes = Number.MAX_SAFE_INTEGER,
): Promise<ArrayBuffer> {
  const length = range.end - range.start + 1;
  if (length > maxBytes) throw new Error(`Requested chunk range is ${length} bytes; limit is ${maxBytes}.`);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const slice of selectChunkSlices(chunks, range)) {
    let bytes: Uint8Array;
    const decryptKey = decryption.sseKeyBase64 || decryption.sseS3KeyBase64;
    const physicalSize = slice.chunk.size + (decryptKey ? 28 : 0);
    if (physicalSize > BOT_API_GETFILE_LIMIT && env.VPS_URL) {
      const vps = new VpsClient(env);
      const response = decryptKey
        ? await vps.proxyGetDecrypt(slice.chunk.tg_file_id, decryptKey, slice.start, slice.endExclusive - 1)
        : await vps.proxyRange(slice.chunk.tg_file_id, slice.start, slice.endExclusive - 1);
      bytes = new Uint8Array(await response.arrayBuffer());
    } else {
      const data = await downloadChunkPlaintext(slice.chunk, env, decryption);
      bytes = new Uint8Array(data, slice.start, slice.endExclusive - slice.start);
    }
    const expected = slice.endExclusive - slice.start;
    if (bytes.byteLength !== expected) {
      throw new Error(`Chunk ${slice.chunk.chunk_index} range returned ${bytes.byteLength} bytes; expected ${expected}.`);
    }
    result.set(bytes, offset);
    offset += bytes.byteLength;
  }
  if (offset !== length) throw new Error(`Chunk range returned ${offset} bytes; expected ${length}.`);
  return result.buffer as ArrayBuffer;
}
