import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import type Database from 'better-sqlite3';
import type {
  CodexContentItem,
  CodexResponseItemLine,
  CodexSessionMetaLine,
  ContentBlockRow,
  MessageRow,
} from './types.js';
import {
  applyCodexSessionMeta,
  getSessionCwd,
  insertContentBlock,
  insertMessage,
  refreshSessionMessageCount,
  upsertIngestLog,
  upsertIngestOffset,
  upsertSession,
} from './db.js';
import { readCompleteLines, resolveStart, type IngestFileResult, type IngestStart } from './ingest.js';

// ── Codex rollouts (CAD-T-618) ──────────────────────────────────
//
// Codex writes each conversation to `<CODEX_HOME>/sessions/YYYY/MM/DD/
// rollout-*.jsonl`. Its prompts land in the same tables as Claude's, so the
// fleet's submit receipts (parallel-code `find-tags`) can confirm a Codex
// delivery the way they confirm a Claude one.

/**
 * Codex's own home: `CODEX_HOME` when set and non-empty, else `~/.codex`. The
 * same rule as parallel-code's `codexSessionsDir` (electron/ipc/codex-resume.ts),
 * restated so flightlog reads the directory Codex writes and the fleet scans.
 */
export function resolveCodexHome(env: NodeJS.ProcessEnv = process.env): string {
  const home = env['CODEX_HOME'];
  return home !== undefined && home !== '' ? home : path.join(os.homedir(), '.codex');
}

export function codexSessionsRoot(codexHome: string): string {
  return path.join(codexHome, 'sessions');
}

export const ROLLOUT_NAME = /^rollout-.*\.jsonl$/;

/**
 * One spelling per rollout, for its `ingest_offsets` key and its message
 * uuids. Three callers hand in paths (the watcher, `ingestAll`, parallel-code's
 * sweep) and Windows paths compare case-insensitively; two spellings of one
 * file would otherwise read it twice under two keys and store every message
 * twice under two uuids.
 */
export function codexFileKey(filePath: string): string {
  const resolved = path.resolve(filePath);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

/** True when `filePath` lies under `<codexHome>/sessions`. */
export function isUnderCodexSessions(filePath: string, codexHome: string = resolveCodexHome()): boolean {
  const rel = path.relative(codexFileKey(codexSessionsRoot(codexHome)), codexFileKey(filePath));
  return rel !== '' && !rel.startsWith('..') && !path.isAbsolute(rel);
}

/**
 * Round-4 NF2: the Claude `ingestFile` refuses a rollout by name. It would
 * store nothing (no Claude line types) yet advance the offset that
 * `ingestCodexFile` shares, so the rollout's rows could never be stored.
 */
export class CodexRolloutRefusedError extends Error {
  constructor(filePath: string) {
    super(`${filePath} is a Codex rollout under the Codex sessions root; ingest it with ingestCodexFile, not ingestFile`);
    this.name = 'CodexRolloutRefusedError';
  }
}

// ── Discovery ───────────────────────────────────────────────────

const missingHomeLogged = new Set<string>();

/**
 * Lists `<codexHome>/sessions/*\/*\/*\/rollout-*.jsonl` (the YYYY/MM/DD
 * layout). A missing sessions root means Codex never ran here: logged once
 * per process (ingestAll runs per query and on a 5 s poll) and answered with
 * no files, never an error.
 */
export function discoverCodexRollouts(codexHome: string = resolveCodexHome()): string[] {
  const root = codexSessionsRoot(codexHome);
  if (!fs.existsSync(root)) {
    if (!missingHomeLogged.has(root)) {
      missingHomeLogged.add(root);
      process.stderr.write(`flightlog: no Codex sessions at ${root}, Codex ingest is a no-op\n`);
    }
    return [];
  }

  const files: string[] = [];
  const subdirs = (dir: string): string[] =>
    fs.readdirSync(dir, { withFileTypes: true }).filter(e => e.isDirectory()).map(e => path.join(dir, e.name));
  for (const year of subdirs(root)) {
    for (const month of subdirs(year)) {
      for (const day of subdirs(month)) {
        for (const entry of fs.readdirSync(day, { withFileTypes: true })) {
          if (entry.isFile() && ROLLOUT_NAME.test(entry.name)) files.push(path.join(day, entry.name));
        }
      }
    }
  }
  return files;
}

// ── Line mapping ────────────────────────────────────────────────

/**
 * Stable uuid for the message on line `lineNumber` of a rollout (round-3 B17):
 * keyed by file + line, never by session id + line, so a re-read (or another
 * server reading the same file) maps every line to the same row. Formatted as
 * a v5-style uuid over a SHA-1 of the key.
 */
export function codexMessageUuid(fileKey: string, lineNumber: number): string {
  const h = crypto.createHash('sha1').update(`codex:${fileKey}:${lineNumber}`).digest();
  h[6] = (h[6]! & 0x0f) | 0x50;
  h[8] = (h[8]! & 0x3f) | 0x80;
  const hex = h.subarray(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Codex timestamps as UTC `Z` ISO strings, or null when unparseable.
 * parallel-code's `find-tags` compares `messages.timestamp >= since` as
 * STRINGS, so a `+hh:mm` offset would sort wrong against a `Z` bound.
 */
export function toUtcIso(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : new Date(ms).toISOString();
}

/** Text item type per role; the block type matches flightlog's own (round-4 NF1). */
const TEXT_ITEM: Record<'user' | 'assistant', { item: string; block: string }> = {
  user: { item: 'input_text', block: 'user_text' },
  assistant: { item: 'output_text', block: 'text' },
};

function bump(counts: Record<string, number>, key: string): void {
  counts[key] = (counts[key] ?? 0) + 1;
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' && value !== '' ? value : null;
}

// ── Single rollout ingestion ────────────────────────────────────

export interface CodexIngestFileResult extends IngestFileResult {
  /**
   * Complete lines not stored, by type: a top-level `type`, a non-message
   * `response_item:<payload type>`, or `response_item:message:<role>` for a
   * role other than user/assistant. Also `response_item:message:no-timestamp`.
   */
  skippedLineTypes: Record<string, number>;
  /** Content items of a stored message that carry no text, by item type. */
  skippedContentTypes: Record<string, number>;
}

/**
 * Ingests one Codex rollout into the tables Claude transcripts use, tailing
 * from its `ingest_offsets` position. The per-file entry point (round-4 NF2):
 * the Codex watcher, `ingestAll` and parallel-code's `find-tags` all call it.
 * `start` is passed only by a caller that already resolved it against
 * `codexFileKey(filePath)` (ingestAll's changed-file filter); otherwise it is
 * resolved here, and a rollout with nothing new is not read. `filePath` is
 * the path as listed, never the key: its basename is the session id.
 *
 * Identity (round-3 B17, and L2's keying): `session_id` is the file basename
 * without `.jsonl`, because a tail read starts past `session_meta` and the
 * fleet keys a Codex recipient's sessions by that basename. `session_meta`
 * only enriches the session row when it is read.
 */
export async function ingestCodexFile(
  filePath: string,
  db: Database.Database,
  start?: IngestStart,
): Promise<CodexIngestFileResult> {
  const base = path.basename(filePath);
  if (!ROLLOUT_NAME.test(base)) {
    throw new Error(`${filePath} is not a Codex rollout (expected rollout-*.jsonl)`);
  }
  const fileKey = codexFileKey(filePath);
  const sessionId = path.basename(base, '.jsonl');
  const result: CodexIngestFileResult = {
    messagesAdded: 0,
    blocksAdded: 0,
    malformedLines: 0,
    skippedLineTypes: {},
    skippedContentTypes: {},
  };

  // The file is stat'ed and read through the caller's spelling; the key is
  // only a row key. A case-sensitive directory on Windows (WSL-created, or
  // `fsutil setCaseSensitiveInfo`) has no file at the case-folded path.
  const from = start ?? (await resolveStart(db, filePath, fs.statSync(filePath).size, fileKey));
  if (from === null) return result;

  let linesConsumed = from.lines;
  let consumedOffset = from.offset;
  let sessionTouched = false;
  // The session's cwd for each message row: from session_meta when this pass
  // reads it, else the stored row (a tail read starts past the meta line).
  let cwd: string | null | undefined;

  for await (const { text: rawLine, endOffset } of readCompleteLines(filePath, from.offset)) {
    linesConsumed++;
    consumedOffset = endOffset;
    if (!rawLine.trim()) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(rawLine);
    } catch {
      // A newline-terminated line that does not parse is corruption: counted
      // and reported, and the offset still advances (as ingestFile does).
      result.malformedLines++;
      continue;
    }
    const line = (typeof parsed === 'object' && parsed !== null ? parsed : {}) as Record<string, unknown>;
    const lineType = typeof line['type'] === 'string' ? line['type'] : '(no type)';

    if (lineType === 'session_meta') {
      const meta = line as unknown as CodexSessionMetaLine;
      const payload = meta.payload ?? {};
      const metaCwd = stringOrNull(payload.cwd);
      const startedAt = toUtcIso(payload.timestamp) ?? toUtcIso(meta.timestamp);
      if (startedAt === null) {
        bump(result.skippedLineTypes, 'session_meta:no-timestamp');
        continue;
      }
      // Either separator: Codex records the cwd in its own platform's spelling.
      const cwdName = metaCwd?.replace(/[\\/]+$/, '').split(/[\\/]/).pop();
      const project = `codex:${cwdName ? cwdName : sessionId}`;
      const branch = stringOrNull(payload.git?.branch);
      const version = stringOrNull(payload.cli_version);
      upsertSession(db, sessionId, project, startedAt, branch, metaCwd, version, 'codex');
      applyCodexSessionMeta(db, sessionId, project, startedAt, branch, metaCwd, version);
      if (metaCwd !== null) cwd = metaCwd;
      sessionTouched = true;
      continue;
    }

    if (lineType !== 'response_item') {
      bump(result.skippedLineTypes, lineType);
      continue;
    }
    const item = line as unknown as CodexResponseItemLine;
    const payloadType = typeof item.payload?.type === 'string' ? item.payload.type : '(no type)';
    if (payloadType !== 'message') {
      bump(result.skippedLineTypes, `response_item:${payloadType}`);
      continue;
    }
    const role = item.payload.role;
    if (role !== 'user' && role !== 'assistant') {
      bump(result.skippedLineTypes, `response_item:message:${typeof role === 'string' ? role : '(no role)'}`);
      continue;
    }
    const timestamp = toUtcIso(item.timestamp);
    if (timestamp === null) {
      bump(result.skippedLineTypes, 'response_item:message:no-timestamp');
      continue;
    }

    if (cwd === undefined) cwd = getSessionCwd(db, sessionId);
    const uuid = codexMessageUuid(fileKey, linesConsumed);
    const messageRow: MessageRow = {
      uuid,
      session_id: sessionId,
      parent_uuid: null,
      type: role,
      role,
      timestamp,
      model: null,
      git_branch: null,
      cwd,
      request_id: null,
      is_sidechain: false,
      input_tokens: null,
      output_tokens: null,
      cache_read_tokens: null,
      cache_creation_tokens: null,
    };
    if (insertMessage(db, messageRow)) result.messagesAdded++;

    // A rollout whose meta line was never read still gets a row; its project
    // names the session, as ingestFile's does when no cwd was seen, until a
    // meta line names it.
    upsertSession(db, sessionId, `codex:${sessionId}`, timestamp, null, cwd, null, 'codex');
    sessionTouched = true;

    const content: CodexContentItem[] = Array.isArray(item.payload.content) ? item.payload.content : [];
    const textItem = TEXT_ITEM[role];
    for (let i = 0; i < content.length; i++) {
      const c = content[i]!;
      if (c?.type !== textItem.item || typeof c.text !== 'string') {
        bump(result.skippedContentTypes, typeof c?.type === 'string' ? c.type : '(no type)');
        continue;
      }
      const block: ContentBlockRow = {
        message_uuid: uuid,
        block_index: i,
        block_type: textItem.block,
        text_content: c.text,
        tool_name: null,
        tool_input: null,
      };
      insertContentBlock(db, block);
      result.blocksAdded++;
    }
  }

  if (sessionTouched) refreshSessionMessageCount(db, sessionId);

  // Same order and reasoning as ingestFile: the offset is the seek truth.
  upsertIngestOffset(db, fileKey, consumedOffset, linesConsumed);
  try {
    const stat = fs.statSync(filePath);
    upsertIngestLog(db, fileKey, linesConsumed, consumedOffset, stat.mtime);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    process.stderr.write(`flightlog: ingest_log not updated for ${base}: ${msg}\n`);
  }

  if (result.malformedLines > 0) {
    process.stderr.write(`flightlog: ${result.malformedLines} malformed line(s) skipped in ${base}\n`);
  }
  return result;
}
