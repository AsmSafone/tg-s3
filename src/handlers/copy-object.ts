import type { Env, S3Request, ObjectRow, BucketRow } from '../types';
import { MetadataStore } from '../storage/metadata';
import { TelegramClient } from '../telegram/client';
import { downloadFromTelegram } from '../telegram/download';
import { uploadToTelegram } from '../telegram/upload';
import { computeEtag } from '../utils/crypto';
import { extractUserMetadata, extractSystemMetadata, etagMatches } from '../utils/headers';
import { copyObjectXml, xmlResponse, errorResponse } from '../xml/builder';
import { purgeCdnCache, purgeR2Cache } from './get-object';
import { deleteDerivatives, deleteChunks } from './delete-object';
import {
  parseSseCHeaders, validateKeyMd5, encrypt, decrypt,
  isEncrypted, getStoredKeyMd5, addSseMetadata, SseCError,
  SSE_HEADERS, SSE_COPY_HEADERS,
  isEncryptedS3, decryptS3, encryptS3, addSseS3Metadata,
} from '../utils/sse';
import { BOT_API_GETFILE_LIMIT } from '../constants';
import { VpsClient } from '../media/vps-client';
import { downloadChunkPlaintext, isChunkedObject, validateChunkLayout } from '../storage/chunked';

export async function handleCopyObject(s3: S3Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);

  // Parse x-amz-copy-source header (strip ?versionId= if present, we don't support versioning)
  const copySource = s3.headers.get('x-amz-copy-source') || '';
  let decoded: string;
  try {
    decoded = decodeURIComponent(copySource.split('?')[0]);
  } catch {
    return errorResponse(400, 'InvalidArgument', 'Invalid copy source encoding.');
  }
  const trimmed = decoded.startsWith('/') ? decoded.slice(1) : decoded;
  const slashIdx = trimmed.indexOf('/');
  if (slashIdx < 0) return errorResponse(400, 'InvalidArgument', 'Invalid copy source.');

  const srcBucket = trimmed.slice(0, slashIdx);
  const srcKey = trimmed.slice(slashIdx + 1);

  // x-amz-metadata-directive: COPY (default) or REPLACE
  const metadataDirective = (s3.headers.get('x-amz-metadata-directive') || 'COPY').toUpperCase();
  if (metadataDirective !== 'COPY' && metadataDirective !== 'REPLACE') {
    return errorResponse(400, 'InvalidArgument', `Unknown metadata directive '${metadataDirective}'.`);
  }

  // S3 rejects copy-to-self with COPY directive (must use REPLACE to modify metadata)
  if (srcBucket === s3.bucket && srcKey === s3.key && metadataDirective !== 'REPLACE') {
    return errorResponse(400, 'InvalidRequest',
      'This copy request is illegal because it is trying to copy an object to itself without changing the object\'s metadata, storage class, website redirect location or encryption attributes.');
  }

  // Check source bucket exists
  const srcBucketRow = await store.getBucket(srcBucket);
  if (!srcBucketRow) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', srcBucket);

  // Get source object
  const srcObj = await store.getObject(srcBucket, srcKey);
  if (!srcObj) return errorResponse(404, 'NoSuchKey', 'The specified key does not exist.', `/${srcBucket}/${srcKey}`);

  // Conditional copy headers (S3 precedence: if-match overrides if-unmodified-since,
  // if-none-match overrides if-modified-since)
  const copyIfMatch = s3.headers.get('x-amz-copy-source-if-match');
  if (copyIfMatch && !etagMatches(copyIfMatch, srcObj.etag, true)) {
    return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
  }
  if (!copyIfMatch) {
    const copyIfUnmodifiedSince = s3.headers.get('x-amz-copy-source-if-unmodified-since');
    if (copyIfUnmodifiedSince && new Date(srcObj.last_modified).getTime() > new Date(copyIfUnmodifiedSince).getTime()) {
      return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
    }
  }
  const copyIfNoneMatch = s3.headers.get('x-amz-copy-source-if-none-match');
  if (copyIfNoneMatch && etagMatches(copyIfNoneMatch, srcObj.etag)) {
    return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
  }
  if (!copyIfNoneMatch) {
    const copyIfModifiedSince = s3.headers.get('x-amz-copy-source-if-modified-since');
    if (copyIfModifiedSince && new Date(srcObj.last_modified).getTime() <= new Date(copyIfModifiedSince).getTime()) {
      return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
    }
  }

  // SSE-C: parse source and destination encryption headers
  let srcSse: ReturnType<typeof parseSseCHeaders> = null;
  let destSse: ReturnType<typeof parseSseCHeaders> = null;
  try {
    srcSse = parseSseCHeaders(s3.headers, SSE_COPY_HEADERS);
    if (srcSse) await validateKeyMd5(srcSse);
    destSse = parseSseCHeaders(s3.headers, SSE_HEADERS);
    if (destSse) await validateKeyMd5(destSse);
  } catch (e) {
    if (e instanceof SseCError) return errorResponse(400, 'InvalidArgument', e.message);
    throw e;
  }

  // If source is encrypted (SSE-C), require source SSE-C headers
  const srcEncrypted = isEncrypted(srcObj.system_metadata);
  const srcEncryptedS3 = isEncryptedS3(srcObj.system_metadata);
  if (srcEncryptedS3 && !env.SSE_MASTER_KEY) {
    return errorResponse(500, 'InternalError', 'Source object is SSE-S3 encrypted but SSE_MASTER_KEY is not configured.');
  }
  if (srcEncrypted && !srcSse) {
    return errorResponse(400, 'InvalidRequest', 'The source object was stored using SSE-C. You must provide the source encryption key headers.');
  }
  if (srcEncrypted && srcSse) {
    const storedMd5 = getStoredKeyMd5(srcObj.system_metadata);
    if (storedMd5 && srcSse.keyMd5 !== storedMd5) {
      return errorResponse(403, 'AccessDenied', 'The provided source encryption key does not match.');
    }
  }

  // Check destination bucket exists
  const destBucket = await store.getBucket(s3.bucket);
  if (!destBucket) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', s3.bucket);

  // Determine if destination wants SSE-S3
  const sseS3Header = s3.headers.get('x-amz-server-side-encryption');
  if (sseS3Header && sseS3Header !== 'AES256') {
    return errorResponse(400, 'InvalidArgument', 'The encryption method specified is not supported. Supported: AES256.');
  }
  if (sseS3Header === 'AES256' && !env.SSE_MASTER_KEY) {
    return errorResponse(400, 'InvalidArgument', 'SSE-S3 is not configured. Set SSE_MASTER_KEY secret.');
  }
  const destUseSseS3 = !destSse && !!env.SSE_MASTER_KEY && (
    sseS3Header === 'AES256' || !!destBucket.default_encryption
  );

  // Determine if re-encryption is needed (source encrypted, or dest wants encryption)
  const needsReEncrypt = srcEncrypted || srcEncryptedS3 || !!destSse || destUseSseS3;

  let userMetadata: Record<string, string> | undefined;
  let systemMetadata: Record<string, string> | undefined;
  let contentType = srcObj.content_type;

  if (metadataDirective === 'REPLACE') {
    userMetadata = extractUserMetadata(s3.headers);
    if (Object.keys(userMetadata).length === 0) userMetadata = undefined;
    systemMetadata = extractSystemMetadata(s3.headers);
    contentType = s3.headers.get('content-type') || 'application/octet-stream';
  } else {
    if (srcObj.user_metadata) {
      try { userMetadata = JSON.parse(srcObj.user_metadata); } catch { /* ignore corrupt */ }
    }
    if (srcObj.system_metadata) {
      try { systemMetadata = JSON.parse(srcObj.system_metadata); } catch { /* ignore corrupt */ }
    }
  }

  // Add destination encryption metadata (or strip source encryption metadata if no dest encryption)
  if (destSse) {
    systemMetadata = addSseMetadata(systemMetadata || {}, destSse);
    if (systemMetadata) delete systemMetadata._sse_s3;
  } else if (destUseSseS3) {
    systemMetadata = addSseS3Metadata(systemMetadata || {});
    if (systemMetadata) { delete systemMetadata._sse; delete systemMetadata._sse_key_md5; }
  } else if ((srcEncrypted || srcEncryptedS3) && systemMetadata) {
    delete systemMetadata._sse;
    delete systemMetadata._sse_key_md5;
    delete systemMetadata._sse_s3;
    if (Object.keys(systemMetadata).length === 0) systemMetadata = undefined;
  }

  if (isChunkedObject(srcObj)) {
    return copyChunkedObject({
      s3, env, ctx, store, srcObj, srcBucket, srcKey, destBucket,
      srcEncrypted, srcEncryptedS3, srcSse, destSse, destUseSseS3,
      needsReEncrypt, userMetadata, systemMetadata, contentType,
    });
  }

  let tgChatId = srcObj.tg_chat_id;
  let tgMessageId = srcObj.tg_message_id;
  let tgFileId = srcObj.tg_file_id;

  if (needsReEncrypt && srcObj.size > 0) {
    // Must download, decrypt source, re-encrypt for dest, re-upload
    let data: ArrayBuffer;
    if (srcObj.size <= BOT_API_GETFILE_LIMIT) {
      const tgRes = await downloadFromTelegram(srcObj.tg_file_id, env);
      data = await tgRes.arrayBuffer();
    } else if (env.VPS_URL) {
      const vps = new VpsClient(env);
      const vpsRes = await vps.proxyGet(srcObj.tg_file_id);
      data = await vpsRes.arrayBuffer();
    } else {
      return errorResponse(503, 'ServiceUnavailable', 'Source file exceeds 20MB and requires VPS proxy which is not configured.');
    }

    // Decrypt source
    if (srcEncrypted && srcSse) {
      data = await decrypt(data, srcSse.keyBase64);
    } else if (srcEncryptedS3 && env.SSE_MASTER_KEY) {
      data = await decryptS3(data, env.SSE_MASTER_KEY);
    }

    // Re-encrypt for destination
    if (destSse) {
      data = await encrypt(data, destSse.keyBase64);
    } else if (destUseSseS3) {
      data = await encryptS3(data, env.SSE_MASTER_KEY!);
    }

    const result = await uploadToTelegram(data, destBucket.tg_chat_id, s3.key, srcObj.content_type, env, destBucket.tg_topic_id);
    tgChatId = result.tgChatId;
    tgMessageId = result.tgMessageId;
    tgFileId = result.tgFileId;
  } else if (srcObj.size > 0 && srcBucketRow && destBucket.tg_chat_id !== srcBucketRow.tg_chat_id) {
    // No encryption change, different bucket: forward/re-send TG message
    const tg = new TelegramClient(env);
    if (srcObj.tg_message_id === 0) {
      const sendRes = await tg.sendDocumentByFileId(destBucket.tg_chat_id, srcObj.tg_file_id, destBucket.tg_topic_id);
      tgChatId = destBucket.tg_chat_id;
      tgMessageId = sendRes.result.message_id;
      if (sendRes.result.document) tgFileId = sendRes.result.document.file_id;
    } else {
      const fwdRes = await tg.forwardMessage(srcObj.tg_chat_id, destBucket.tg_chat_id, srcObj.tg_message_id, destBucket.tg_topic_id);
      tgChatId = destBucket.tg_chat_id;
      tgMessageId = fwdRes.result.message_id;
      if (fwdRes.result.document) tgFileId = fwdRes.result.document.file_id;
    }
  } else if (srcObj.size > 0 && !(srcBucket === s3.bucket && srcKey === s3.key)) {
    // Same TG chat but different object: create new TG message reference for isolation.
    // Without this, deleting the source would orphan the copy (shared tg_message_id).
    const tg = new TelegramClient(env);
    if (srcObj.tg_message_id === 0) {
      const sendRes = await tg.sendDocumentByFileId(destBucket.tg_chat_id, srcObj.tg_file_id, destBucket.tg_topic_id);
      tgChatId = destBucket.tg_chat_id;
      tgMessageId = sendRes.result.message_id;
      if (sendRes.result.document) tgFileId = sendRes.result.document.file_id;
    } else {
      const fwdRes = await tg.forwardMessage(srcObj.tg_chat_id, destBucket.tg_chat_id, srcObj.tg_message_id, destBucket.tg_topic_id);
      tgChatId = destBucket.tg_chat_id;
      tgMessageId = fwdRes.result.message_id;
      if (fwdRes.result.document) tgFileId = fwdRes.result.document.file_id;
    }
  } else if (srcObj.size === 0) {
    tgChatId = destBucket.tg_chat_id;
  }

  // Check for existing object at destination (for old TG message cleanup)
  const oldObj = await store.getObject(s3.bucket, s3.key);

  const now = new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
  await store.putObject({
    bucket: s3.bucket,
    key: s3.key,
    size: srcObj.size,
    etag: srcObj.etag,
    contentType,
    tgChatId,
    tgMessageId,
    tgFileId,
    tgFileUniqueId: srcObj.tg_file_unique_id,
    userMetadata,
    systemMetadata,
  }, oldObj);

  await applyCopyTags(s3, srcBucket, srcKey, store, ctx);

  // Async cleanup: delete old TG message + stale derivatives if destination was overwritten
  if (oldObj && !isChunkedObject(oldObj) && oldObj.tg_file_id !== '__zero__'
      && oldObj.tg_message_id !== 0 && oldObj.tg_file_id !== tgFileId) {
    const tg = new TelegramClient(env);
    ctx.waitUntil(tg.deleteMessage(oldObj.tg_chat_id, oldObj.tg_message_id).catch(() => {}));
  }
  // Only clean derivatives/chunks if the underlying file changed (not metadata-only update)
  if (oldObj && oldObj.tg_file_id !== tgFileId) {
    ctx.waitUntil(deleteDerivatives(s3.bucket, s3.key, env, store));
    ctx.waitUntil(deleteChunks(s3.bucket, s3.key, env, store));
  }

  // Purge CDN + R2 cache for destination key
  ctx.waitUntil(purgeCdnCache(s3.url.origin, s3.bucket, s3.key));
  ctx.waitUntil(purgeR2Cache(env, s3.bucket, s3.key));

  const copyResp = xmlResponse(copyObjectXml(srcObj.etag, now));
  if (destSse) {
    copyResp.headers.set('x-amz-server-side-encryption-customer-algorithm', 'AES256');
    copyResp.headers.set('x-amz-server-side-encryption-customer-key-MD5', destSse.keyMd5);
  } else if (destUseSseS3) {
    copyResp.headers.set('x-amz-server-side-encryption', 'AES256');
  }
  return copyResp;
}

async function copyChunkedObject(input: {
  s3: S3Request;
  env: Env;
  ctx: ExecutionContext;
  store: MetadataStore;
  srcObj: ObjectRow;
  srcBucket: string;
  srcKey: string;
  destBucket: BucketRow;
  srcEncrypted: boolean;
  srcEncryptedS3: boolean;
  srcSse: ReturnType<typeof parseSseCHeaders>;
  destSse: ReturnType<typeof parseSseCHeaders>;
  destUseSseS3: boolean;
  needsReEncrypt: boolean;
  userMetadata?: Record<string, string>;
  systemMetadata?: Record<string, string>;
  contentType: string;
}): Promise<Response> {
  const {
    s3, env, ctx, store, srcObj, srcBucket, srcKey, destBucket,
    srcEncrypted, srcEncryptedS3, srcSse, destSse, destUseSseS3,
    needsReEncrypt, userMetadata, contentType,
  } = input;
  let systemMetadata = input.systemMetadata;
  const sourceChunks = await store.getChunks(srcBucket, srcKey);
  const layoutError = validateChunkLayout(sourceChunks, srcObj.size, srcObj.chunk_count);
  if (layoutError) return errorResponse(500, 'InternalError', `Invalid source chunk metadata: ${layoutError}`);
  if (input.needsReEncrypt && sourceChunks.some(chunk => chunk.size > BOT_API_GETFILE_LIMIT)) {
    return errorResponse(400, 'EntityTooLarge',
      'Re-encrypting a chunk larger than 20MiB is not supported by CopyObject; use multipart UploadPartCopy with bounded ranges.');
  }

  const oldObj = await store.getObject(s3.bucket, s3.key);
  const copyToSelf = srcBucket === s3.bucket && srcKey === s3.key;
  const now = new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
  systemMetadata = { ...(systemMetadata || {}) };
  systemMetadata._mp_part_sizes = JSON.stringify(sourceChunks.map(chunk => chunk.size));
  systemMetadata._chunk_layout = 'multipart-v1';

  // Metadata-only self-copy can retain the existing physical chunk messages.
  if (copyToSelf && !needsReEncrypt) {
    await store.putObject({
      bucket: s3.bucket, key: s3.key, size: srcObj.size, etag: srcObj.etag,
      contentType, tgChatId: srcObj.tg_chat_id, tgMessageId: 0,
      tgFileId: '__chunked__', tgFileUniqueId: '__chunked__',
      userMetadata, systemMetadata, isChunked: true, chunkCount: sourceChunks.length,
    }, oldObj);
  } else {
    const uploadId = crypto.randomUUID();
    await store.createMultipartUpload(uploadId, s3.bucket, s3.key, contentType);
    const createdMessages: Array<{ tg_chat_id: string; tg_message_id: number }> = [];
    let committed = false;
    try {
      const tg = new TelegramClient(env);
      for (let i = 0; i < sourceChunks.length; i++) {
        const chunk = sourceChunks[i];
        let tgChatId: string;
        let tgMessageId: number;
        let tgFileId: string;

        if (needsReEncrypt) {
          let data = await downloadChunkPlaintext(chunk, env, {
            sseKeyBase64: srcEncrypted ? srcSse?.keyBase64 : undefined,
            sseS3KeyBase64: srcEncryptedS3 ? env.SSE_MASTER_KEY : undefined,
          });
          if (destSse) data = await encrypt(data, destSse.keyBase64);
          else if (destUseSseS3) data = await encryptS3(data, env.SSE_MASTER_KEY!);
          const result = await uploadToTelegram(
            data, destBucket.tg_chat_id,
            `${s3.key}.part${(i + 1).toString().padStart(4, '0')}`,
            'application/octet-stream', env, destBucket.tg_topic_id,
          );
          tgChatId = result.tgChatId;
          tgMessageId = result.tgMessageId;
          tgFileId = result.tgFileId;
        } else {
          const forwarded = await tg.forwardMessage(
            chunk.tg_chat_id, destBucket.tg_chat_id, chunk.tg_message_id, destBucket.tg_topic_id,
          );
          const document = forwarded.result.document;
          if (!document) throw new Error(`Telegram copy of chunk ${i} did not return a document.`);
          tgChatId = destBucket.tg_chat_id;
          tgMessageId = forwarded.result.message_id;
          tgFileId = document.file_id;
        }

        createdMessages.push({ tg_chat_id: tgChatId, tg_message_id: tgMessageId });
        await store.putMultipartPart({
          uploadId, partNumber: i + 1, size: chunk.size,
          etag: `"chunk-${i}"`, tgChatId, tgMessageId, tgFileId,
        });
      }

      const replaced = await store.completeMultipartAsChunked({
        uploadId, bucket: s3.bucket, key: s3.key, size: srcObj.size,
        etag: srcObj.etag, contentType, tgChatId: destBucket.tg_chat_id,
        partNumbers: sourceChunks.map((_, i) => i + 1),
        userMetadata, systemMetadata,
      }, oldObj);
      committed = true;

      if (replaced.oldChunks.length > 0) ctx.waitUntil(deleteChunkMessages(replaced.oldChunks, env));
      if (replaced.oldObject && !isChunkedObject(replaced.oldObject)
          && replaced.oldObject.tg_file_id !== '__zero__' && replaced.oldObject.tg_message_id !== 0) {
        ctx.waitUntil(new TelegramClient(env).deleteMessage(
          replaced.oldObject.tg_chat_id, replaced.oldObject.tg_message_id,
        ).then(() => {}).catch(() => {}));
      }
      if (replaced.oldObject) ctx.waitUntil(deleteDerivatives(s3.bucket, s3.key, env, store));
    } catch (error) {
      if (!committed) {
        await store.deleteMultipartUpload(uploadId).catch(() => {});
        await deleteChunkMessages(createdMessages, env);
      }
      return errorResponse(503, 'ServiceUnavailable', error instanceof Error ? error.message : 'Chunk copy failed.');
    }
  }

  await applyCopyTags(s3, srcBucket, srcKey, store, ctx);
  ctx.waitUntil(purgeCdnCache(s3.url.origin, s3.bucket, s3.key));
  ctx.waitUntil(purgeR2Cache(env, s3.bucket, s3.key));

  const response = xmlResponse(copyObjectXml(srcObj.etag, now));
  if (destSse) {
    response.headers.set('x-amz-server-side-encryption-customer-algorithm', 'AES256');
    response.headers.set('x-amz-server-side-encryption-customer-key-MD5', destSse.keyMd5);
  } else if (destUseSseS3) {
    response.headers.set('x-amz-server-side-encryption', 'AES256');
  }
  return response;
}

async function deleteChunkMessages(
  chunks: Array<{ tg_chat_id: string; tg_message_id: number }>,
  env: Env,
): Promise<void> {
  const tg = new TelegramClient(env);
  await Promise.allSettled(chunks.map(chunk =>
    tg.deleteMessage(chunk.tg_chat_id, chunk.tg_message_id).catch(() => {}),
  ));
}

async function applyCopyTags(
  s3: S3Request,
  srcBucket: string,
  srcKey: string,
  store: MetadataStore,
  ctx: ExecutionContext,
): Promise<void> {
  const taggingDirective = (s3.headers.get('x-amz-tagging-directive') || 'COPY').toUpperCase();
  if (taggingDirective === 'COPY') {
    const srcTags = await store.getObjectTags(srcBucket, srcKey);
    if (srcTags.length > 0) ctx.waitUntil(store.putObjectTags(s3.bucket, s3.key, srcTags).catch(() => {}));
  } else if (taggingDirective === 'REPLACE') {
    const taggingHeader = s3.headers.get('x-amz-tagging');
    if (taggingHeader) {
      const tags = taggingHeader.split('&').map(pair => {
        const [key, value] = pair.split('=');
        return { key: decodeURIComponent(key || ''), value: decodeURIComponent(value || '') };
      }).filter(tag => tag.key);
      if (tags.length > 0 && tags.length <= 10) {
        ctx.waitUntil(store.putObjectTags(s3.bucket, s3.key, tags).catch(() => {}));
      }
    }
  }
}
