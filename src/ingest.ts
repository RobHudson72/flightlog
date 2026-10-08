import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import type Database from 'better-sqlite3';
import type {
  JsonlLine,
  JsonlUserMessage,
  JsonlAssistantMessage,
  JsonlContentBlock,
  MessageRow,
  ContentBlockRow,
  IngestSummary,
  IngestProgress,
} from './types.js';
import {
  getDb,
  getIngestLog,
  getIngestOffset,
  insertMessage,
  insertContentBlock,
  refreshSessionMessageCount,
  upsertSession,
  upsertIngestLog,
  upsertIngestOffset,
} from './db.js';
import {
  CodexRolloutRefusedError,
  codexFileKey,
  discoverCodexRollouts,
  ingestCodexFile,
  isUnderCodexSessions,
  resolveCodexHome,
} from './ingest-codex.js';

// CAD-T-618: the per-file Codex entry point, and the capability flag that
// parallel-code's `find-tags` feature-detects on dist/ingest.js (CAD-T-614).
export { ingestCodexFile };
export const CODEX_INGEST = true;

// ── File discovery ──────────────────────────────────────────────

function getClaudeProjectsDir(): string {
  return path.join(os.homedir(), '.claude', 'projects');
}

export function discoverJsonlFiles(basePath?: string): string[] {
  const dir = basePath ?? getClaudeProjectsDir();
  if (!fs.existsSync(dir)) return [];

  const files: string[] = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      // Recurse into project directories
      const subEntries = fs.readdirSync(fullPath, { withFileTypes: true });
      for (const sub of subEntries) {
        if (sub.isFile() && sub.name.endsWith('.jsonl') && sub.name !== 'history.jsonl') {
          files.push(path.join(fullPath, sub.name));
        }
      }
    } else if (entry.isFile() && entry.name.endsWith('.jsonl') && entry.name !== 'history.jsonl') {
      files.push(fullPath);
    }
  }

  return files;
}

// ── Incremental filtering ───────────────────────────────────────
//
// Transcripts are append-only JSONL, so a changed file is tailed from the byte
// just past the last newline we consumed (`ingest_offsets.byte_offset`) rather
// than re-streamed from byte 0 and line-skipped. `start.offset === 0` means a
// full read. Offsets are BYTES, never string lengths (multibyte + CRLF safe).

export interface IngestStart {
  /** Byte offset to begin reading at; always just past a consumed `\n`, or 0. */
  offset: number;
  /** Lines consumed before `offset` (carried into the running total). */
  lines: number;
}

export interface FileToIngest {
  filePath: string;
  start: IngestStart;
}

const FULL_READ: IngestStart = { offset: 0, lines: 0 };

/** How many complete lines before a legacy position are re-read on bootstrap. */
const LEGACY_BACKUP_LINES = 2;
/** Largest tail window scanned for those lines; a longer last line → full read. */
const LEGACY_TAIL_WINDOW = 1024 * 1024;

/**
 * Picks a safe resume point for a file whose only recorded position is a
 * legacy `ingest_log` row. The old code took `file_size` from a stat() AFTER
 * its read loop, so that value can include a line it never parsed (appended
 * between EOF and stat) or a half-written line that `readline` counted and
 * `JSON.parse` rejected — and an older server still running on this DB keeps
 * writing such values. Rather than trust the position, back up to the start of
 * the last `LEGACY_BACKUP_LINES` complete lines before it and re-read them;
 * duplicates are absorbed by the unique constraints. Returns null (→ full
 * read) when the position is mid-line, the tail window holds no newline, or
 * the file is smaller than the recorded size. Older losses further back in a
 * file are not recoverable from here; `flightlog_rebuild` re-reads everything.
 */
async function legacyResumePoint(filePath: string, legacySize: number, fileSize: number): Promise<number | null> {
  const end = Math.min(legacySize, fileSize);
  if (end <= 0 || fileSize < legacySize) return null;
  const windowStart = Math.max(0, end - LEGACY_TAIL_WINDOW);

  const chunks: Buffer[] = [];
  const stream = fs.createReadStream(filePath, { start: windowStart, end: end - 1 });
  for await (const chunk of stream as AsyncIterable<Buffer>) chunks.push(chunk);
  const tail = Buffer.concat(chunks);
  if (tail.length === 0 || tail[tail.length - 1] !== 0x0a) return null; // mid-line

  // Walk back over LEGACY_BACKUP_LINES newlines beyond the terminating one.
  let pos = tail.length - 1;
  for (let i = 0; i < LEGACY_BACKUP_LINES; i++) {
    const prev = tail.lastIndexOf(0x0a, pos - 1);
    if (prev === -1) return windowStart === 0 ? 0 : null;
    pos = prev;
  }
  return windowStart + pos + 1;
}

/**
 * Decides where to start reading `filePath`, or null when nothing new exists.
 * Order of trust: `ingest_offsets` (written only by offset-aware code) → a
 * legacy `ingest_log` row, backed up by a couple of lines → full read. The
 * legacy branch never short-circuits on "same size": the old server's size
 * can cover a line it never read, so the bootstrap runs once per file.
 * `key` is the file's row key in `ingest_offsets`/`ingest_log`; it differs
 * from `filePath` only for a Codex rollout (CAD-T-618 `codexFileKey`), and the
 * file itself is always read through `filePath`, the caller's spelling.
 */
export async function resolveStart(
  db: Database.Database,
  filePath: string,
  fileSize: number,
  key: string = filePath,
): Promise<IngestStart | null> {
  const known = getIngestOffset(db, key);
  if (known) {
    if (fileSize === known.byte_offset) return null;
    if (fileSize < known.byte_offset) {
      process.stderr.write(
        `flightlog: ${path.basename(filePath)} shrank below its stored offset, re-reading from 0\n`,
      );
      return FULL_READ;
    }
    return { offset: known.byte_offset, lines: known.lines_consumed };
  }

  const legacy = getIngestLog(db, key);
  if (!legacy) return FULL_READ;

  const resume = await legacyResumePoint(filePath, legacy.file_size, fileSize);
  if (resume === null) {
    process.stderr.write(
      `flightlog: stored position for ${path.basename(filePath)} is not usable, re-reading from 0\n`,
    );
    return FULL_READ;
  }
  return { offset: resume, lines: Math.max(0, legacy.lines_ingested - LEGACY_BACKUP_LINES) };
}

export async function filterChangedFiles(
  files: string[],
  db: Database.Database,
): Promise<FileToIngest[]> {
  const toIngest: FileToIngest[] = [];

  for (const filePath of files) {
    const stat = fs.statSync(filePath);
    const start = await resolveStart(db, filePath, stat.size);
    if (start) toIngest.push({ filePath, start });
  }

  return toIngest;
}

// ── Byte-level line reader ──────────────────────────────────────

interface CompleteLine {
  text: string;
  /** Byte offset just past this line's terminating `\n`. */
  endOffset: number;
}

/**
 * Yields only `\n`-terminated lines from `start`, with exact byte positions.
 * Bytes after the last newline (a write in progress) are never yielded, so the
 * caller's recorded offset is always a line boundary. A trailing `\r` is
 * stripped so CRLF files parse; the offset still counts it.
 */
export async function* readCompleteLines(filePath: string, start: number): AsyncGenerator<CompleteLine> {
  const stream = fs.createReadStream(filePath, { start });
  const pending: Buffer[] = [];
  let chunkStart = start;

  for await (const chunk of stream as AsyncIterable<Buffer>) {
    let from = 0;
    let idx = chunk.indexOf(0x0a, from);
    while (idx !== -1) {
      const piece = chunk.subarray(from, idx);
      let lineBuf = pending.length > 0 ? Buffer.concat([...pending, piece]) : piece;
      pending.length = 0;
      if (lineBuf.length > 0 && lineBuf[lineBuf.length - 1] === 0x0d) {
        lineBuf = lineBuf.subarray(0, lineBuf.length - 1);
      }
      yield { text: lineBuf.toString('utf8'), endOffset: chunkStart + idx + 1 };
      from = idx + 1;
      idx = chunk.indexOf(0x0a, from);
    }
    if (from < chunk.length) pending.push(chunk.subarray(from));
    chunkStart += chunk.length;
  }
}

// ── Content block extraction ────────────────────────────────────

function extractContentBlocks(line: JsonlLine): ContentBlockRow[] {
  if (line.type === 'file-history-snapshot') return [];

  const blocks: ContentBlockRow[] = [];
  const messageUuid = line.uuid;

  if (line.type === 'user') {
    const userMsg = line as JsonlUserMessage;
    const content = userMsg.message.content;

    if (typeof content === 'string') {
      blocks.push({
        message_uuid: messageUuid,
        block_index: 0,
        block_type: 'user_text',
        text_content: content,
        tool_name: null,
        tool_input: null,
      });
    } else if (Array.isArray(content)) {
      for (let i = 0; i < content.length; i++) {
        const block = content[i];
        if (block.type === 'tool_result') {
          blocks.push({
            message_uuid: messageUuid,
            block_index: i,
            block_type: 'tool_result',
            text_content: typeof block.content === 'string' ? block.content : JSON.stringify(block.content),
            tool_name: null,
            tool_input: null,
          });
        } else if (block.type === 'text') {
          // CAD-T-618 (round-3 B1): a user turn written as an array keeps its
          // text, as the prompt it is; it used to be dropped, so no reader of
          // user_text (the fleet's submit receipts among them) could see it.
          blocks.push({
            message_uuid: messageUuid,
            block_index: i,
            block_type: 'user_text',
            text_content: block.text,
            tool_name: null,
            tool_input: null,
          });
        }
      }
    }
  } else if (line.type === 'assistant') {
    const assistantMsg = line as JsonlAssistantMessage;
    const contentArr = assistantMsg.message.content;

    if (Array.isArray(contentArr)) {
      for (let i = 0; i < contentArr.length; i++) {
        const block = contentArr[i] as JsonlContentBlock;
        switch (block.type) {
          case 'text':
            blocks.push({
              message_uuid: messageUuid,
              block_index: i,
              block_type: 'text',
              text_content: block.text,
              tool_name: null,
              tool_input: null,
            });
            break;
          case 'thinking':
            // Only index thinking blocks with actual content
            if (block.thinking && block.thinking.length > 0) {
              blocks.push({
                message_uuid: messageUuid,
                block_index: i,
                block_type: 'thinking',
                text_content: block.thinking,
                tool_name: null,
                tool_input: null,
              });
            }
            break;
          case 'tool_use':
            blocks.push({
              message_uuid: messageUuid,
              block_index: i,
              block_type: 'tool_use',
              text_content: JSON.stringify(block.input),
              tool_name: block.name,
              tool_input: JSON.stringify(block.input),
            });
            break;
          case 'tool_result':
            blocks.push({
              message_uuid: messageUuid,
              block_index: i,
              block_type: 'tool_result',
              text_content: typeof block.content === 'string' ? block.content : JSON.stringify(block.content),
              tool_name: null,
              tool_input: null,
            });
            break;
        }
      }
    }
  }

  return blocks;
}

// ── Queued prompts ──────────────────────────────────────────────

/**
 * CAD-T-668: a prompt submitted while the agent is mid-turn is not written as
 * a `user` line. Claude Code queues it and records an `attachment` line of
 * type `queued_command` carrying the prompt, which the agent then receives.
 * That IS a submitted user turn, so it is stored as one: without this, every
 * prompt delivered to a busy agent was invisible to readers of user messages
 * (the fleet's submit receipts confirmed none of them and re-pressed Enter).
 * Every other line passes through unchanged.
 */
function normalizeQueuedCommand(raw: unknown): unknown {
  if (typeof raw !== 'object' || raw === null) return raw;
  const line = raw as Record<string, unknown>;
  if (line['type'] !== 'attachment') return raw;
  const att = line['attachment'] as Record<string, unknown> | undefined;
  if (att === undefined || att['type'] !== 'queued_command') return raw;
  const prompt = att['prompt'];
  const content =
    typeof prompt === 'string'
      ? prompt
      : Array.isArray(prompt)
        ? prompt
            .filter((p): p is { type: string; text: string } => typeof p?.text === 'string')
            .map((p) => p.text)
            .join('\n')
        : null;
  if (content === null || typeof line['uuid'] !== 'string') return raw;
  return {
    type: 'user',
    uuid: line['uuid'],
    parentUuid: line['parentUuid'] ?? null,
    isSidechain: line['isSidechain'] === true,
    timestamp: line['timestamp'] ?? att['timestamp'],
    sessionId: line['sessionId'],
    cwd: line['cwd'] ?? null,
    gitBranch: line['gitBranch'],
    version: line['version'],
    message: { role: 'user', content },
  };
}

// ── Single file ingestion ───────────────────────────────────────

function toMessageRow(line: JsonlLine, sessionId: string): MessageRow | null {
  // CAD-T-654: a snapshot is not a message. Its `messageId` is the uuid of
  // the prompt it precedes, so a row keyed by it won the primary key and the
  // prompt itself was ignored: about 80% of prompts were stored with no role,
  // type or cwd. It carries no content (extractContentBlocks returns none),
  // and nothing reads it.
  if (line.type === 'file-history-snapshot') return null;

  if (line.type === 'user') {
    return {
      uuid: line.uuid,
      session_id: sessionId,
      parent_uuid: line.parentUuid,
      type: 'user',
      role: 'user',
      timestamp: line.timestamp,
      model: null,
      git_branch: line.gitBranch ?? null,
      cwd: line.cwd,
      request_id: null,
      is_sidechain: line.isSidechain,
      input_tokens: null,
      output_tokens: null,
      cache_read_tokens: null,
      cache_creation_tokens: null,
    };
  }

  if (line.type === 'assistant') {
    const usage = line.message.usage;
    return {
      uuid: line.uuid,
      session_id: sessionId,
      parent_uuid: line.parentUuid,
      type: 'assistant',
      role: 'assistant',
      timestamp: line.timestamp,
      model: line.message.model ?? null,
      git_branch: line.gitBranch ?? null,
      cwd: line.cwd,
      request_id: line.requestId ?? null,
      is_sidechain: line.isSidechain,
      input_tokens: usage?.input_tokens ?? null,
      output_tokens: usage?.output_tokens ?? null,
      cache_read_tokens: usage?.cache_read_input_tokens ?? null,
      cache_creation_tokens: usage?.cache_creation_input_tokens ?? null,
    };
  }

  return null;
}

const MALFORMED_LOG_LIMIT = 3;

export interface IngestFileResult {
  /** Message rows actually inserted (duplicates on re-read count 0). */
  messagesAdded: number;
  blocksAdded: number;
  /** Complete lines that failed to parse — a corruption signal, not noise. */
  malformedLines: number;
}

/**
 * Ingests `filePath` from `start.offset` (0 = whole file). Only complete lines
 * are consumed; the recorded offset is the byte just past the last `\n` read.
 * Every statement here is idempotent so a crash mid-file (offset not yet
 * advanced → the lines are re-read) cannot leave a message without its
 * session row or content blocks: session upsert and block inserts run on
 * every pass, and only `messagesAdded` depends on whether the insert was new.
 * `sessions.message_count` is derived from `messages` at the end of a pass.
 * A Codex rollout is refused by name (CAD-T-618 round-4 NF2): see
 * `CodexRolloutRefusedError`.
 */
export async function ingestFile(
  filePath: string,
  db: Database.Database,
  start: IngestStart = FULL_READ,
): Promise<IngestFileResult> {
  if (isUnderCodexSessions(filePath)) throw new CodexRolloutRefusedError(filePath);
  const sessionId = path.basename(filePath, '.jsonl');
  let project: string | null = null;
  let linesConsumed = start.lines;
  let consumedOffset = start.offset;
  let messagesAdded = 0;
  let blocksAdded = 0;
  let malformedLines = 0;
  let sessionTouched = false;

  for await (const { text: rawLine, endOffset } of readCompleteLines(filePath, start.offset)) {
    linesConsumed++;
    consumedOffset = endOffset;
    if (!rawLine.trim()) continue;

    let parsed: JsonlLine;
    try {
      parsed = normalizeQueuedCommand(JSON.parse(rawLine)) as JsonlLine;
    } catch {
      // The line is newline-terminated, so this is real corruption. Surface it
      // (the offset still advances: re-reading garbage would never succeed).
      malformedLines++;
      if (malformedLines <= MALFORMED_LOG_LIMIT) {
        process.stderr.write(
          `flightlog: skipping malformed line ending at byte ${endOffset} in ${path.basename(filePath)}\n`,
        );
      }
      continue;
    }

    // Extract project from first message with cwd
    if (!project && 'cwd' in parsed && parsed.cwd) {
      project = parsed.cwd;
    }

    const messageRow = toMessageRow(parsed, sessionId);
    if (!messageRow) continue;

    // Duplicates (a re-read, or another server that got here first) are
    // ignored by the uuid primary key; only the added-count depends on it.
    if (insertMessage(db, messageRow)) messagesAdded++;

    // Use the project we extracted, or fall back to session id
    const sessionProject = project ?? sessionId;

    // Upsert session on every pass (creates the row, advances last_message_at)
    upsertSession(
      db,
      sessionId,
      sessionProject,
      messageRow.timestamp,
      messageRow.git_branch,
      messageRow.cwd,
      'version' in parsed ? (parsed.version ?? null) : null,
      'claude',
    );
    sessionTouched = true;

    // Extract and insert content blocks (idempotent via the identity index)
    const blocks = extractContentBlocks(parsed);
    for (const block of blocks) {
      insertContentBlock(db, block);
      blocksAdded++;
    }
  }

  if (sessionTouched) refreshSessionMessageCount(db, sessionId);

  // Record the position first: `ingest_offsets` is the seek truth. `ingest_log`
  // is kept coherent for status/stats and for any older server sharing the DB;
  // its mtime column is informational, so a stat failure (file rotated away)
  // must not discard the offset just computed.
  upsertIngestOffset(db, filePath, consumedOffset, linesConsumed);
  try {
    const stat = fs.statSync(filePath);
    upsertIngestLog(db, filePath, linesConsumed, consumedOffset, stat.mtime);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    process.stderr.write(`flightlog: ingest_log not updated for ${path.basename(filePath)}: ${msg}\n`);
  }

  return { messagesAdded, blocksAdded, malformedLines };
}

// ── Progress tracking ───────────────────────────────────────────

const progress: IngestProgress = {
  status: 'idle',
  total_files: 0,
  files_ingested: 0,
  files_remaining: 0,
  percent_complete: 100,
  messages_added: 0,
  current_file: null,
  errors: [],
  queue_depth: 0,
  oldest_queued_since: null,
  queued_paths: [],
  watcher_active: false,
  fallback_polling: false,
};

// Optional hook for watcher to inject queue metrics into progress
type QueueMetricsProvider = () => {
  queue_depth: number;
  oldest_queued_since: string | null;
  queued_paths: string[];
  watcher_active: boolean;
  fallback_polling: boolean;
};

let queueMetricsProvider: QueueMetricsProvider | null = null;

export function setQueueMetricsProvider(provider: QueueMetricsProvider): void {
  queueMetricsProvider = provider;
}

export function getProgress(): IngestProgress {
  const base = { ...progress };
  if (queueMetricsProvider) {
    const metrics = queueMetricsProvider();
    base.queue_depth = metrics.queue_depth;
    base.oldest_queued_since = metrics.oldest_queued_since;
    base.queued_paths = metrics.queued_paths;
    base.watcher_active = metrics.watcher_active;
    base.fallback_polling = metrics.fallback_polling;
  }
  return base;
}

export function isIngestRunning(): boolean {
  return ingestRunning;
}

// ── Main ingest orchestrator ────────────────────────────────────

let ingestRunning = false;

/**
 * Triggers ingestion in the background. Returns immediately with current progress.
 * If an ingest is already running, returns existing progress without starting another.
 */
export function triggerIngest(ingestPath?: string): IngestProgress {
  if (!ingestRunning) {
    // Fire and forget — runs in background
    ingestAllInner(ingestPath, defaultCodexHome(ingestPath)).catch((e) => {
      const msg = e instanceof Error ? e.message : String(e);
      progress.errors.push(msg);
      progress.status = 'idle';
      ingestRunning = false;
    });
  }
  return getProgress();
}

/**
 * CAD-T-618: the Codex home `ingestAll` reads when its caller names none. The
 * whole-tree ingest (no path: the MCP servers, parallel-code's query worker)
 * reads the operator's Codex home; a caller that names its own path (a test,
 * `flightlog_ingest` with a path) ingests exactly that path, never the
 * operator's rollouts.
 */
function defaultCodexHome(ingestPath: string | undefined): string | null {
  return ingestPath === undefined ? resolveCodexHome() : null;
}

/**
 * Blocking version — waits for ingestion to complete. Used by auto-ingest timer.
 * Claude transcripts first, then Codex rollouts from `codexHome` (null: none).
 */
export async function ingestAll(
  ingestPath?: string,
  codexHome: string | null = defaultCodexHome(ingestPath),
): Promise<IngestSummary> {
  if (ingestRunning) {
    // Return a summary reflecting "nothing to do, already running"
    return {
      files_processed: 0,
      files_skipped: 0,
      messages_added: 0,
      content_blocks_added: 0,
      errors: ['Ingest already in progress'],
      codex_skipped_line_types: {},
    };
  }
  return ingestAllInner(ingestPath, codexHome);
}

const byMtimeDesc = (a: FileToIngest, b: FileToIngest): number =>
  fs.statSync(b.filePath).mtimeMs - fs.statSync(a.filePath).mtimeMs;

async function ingestAllInner(ingestPath: string | undefined, codexHome: string | null): Promise<IngestSummary> {
  ingestRunning = true;
  progress.status = 'running';
  progress.errors = [];
  progress.messages_added = 0;

  try {
    const db = getDb();

    // Discover files
    let files: string[];
    if (ingestPath) {
      const stat = fs.statSync(ingestPath);
      if (stat.isDirectory()) {
        files = discoverJsonlFiles(ingestPath);
      } else {
        files = [ingestPath];
      }
    } else {
      files = discoverJsonlFiles();
    }

    // Filter to only changed files
    const toIngest = await filterChangedFiles(files, db);

    // Sort by mtime descending — most recent conversations first
    toIngest.sort(byMtimeDesc);

    // CAD-T-618: Codex rollouts after Claude transcripts, so parallel-code's
    // query worker (which calls ingestAll per query) sees Codex with no
    // watcher running. The start is resolved under the key the rollout's
    // offset is stored by; the path itself is kept, since its basename is
    // the session id.
    const codexFiles = codexHome === null ? [] : discoverCodexRollouts(codexHome);
    const codexToIngest: FileToIngest[] = [];
    for (const filePath of codexFiles) {
      const start = await resolveStart(db, filePath, fs.statSync(filePath).size, codexFileKey(filePath));
      if (start) codexToIngest.push({ filePath, start });
    }
    codexToIngest.sort(byMtimeDesc);

    const totalFiles = files.length + codexFiles.length;
    const pending = toIngest.length + codexToIngest.length;

    // Update progress with totals
    progress.total_files = totalFiles;
    progress.files_ingested = totalFiles - pending;
    progress.files_remaining = pending;
    progress.percent_complete = totalFiles > 0
      ? Math.round((progress.files_ingested / totalFiles) * 100)
      : 100;

    const summary: IngestSummary = {
      files_processed: 0,
      files_skipped: totalFiles - pending,
      messages_added: 0,
      content_blocks_added: 0,
      errors: [],
      codex_skipped_line_types: {},
    };

    const queue = [
      ...toIngest.map(f => ({ ...f, codex: false })),
      ...codexToIngest.map(f => ({ ...f, codex: true })),
    ];

    for (const { filePath, start, codex } of queue) {
      progress.current_file = path.basename(filePath, '.jsonl');

      try {
        let result: IngestFileResult;
        if (codex) {
          const codexResult = await ingestCodexFile(filePath, db, start);
          for (const [type, n] of Object.entries(codexResult.skippedLineTypes)) {
            summary.codex_skipped_line_types[type] = (summary.codex_skipped_line_types[type] ?? 0) + n;
          }
          result = codexResult;
        } else {
          result = await ingestFile(filePath, db, start);
        }
        summary.files_processed++;
        summary.messages_added += result.messagesAdded;
        summary.content_blocks_added += result.blocksAdded;
        progress.messages_added += result.messagesAdded;
        if (result.malformedLines > 0) {
          const note = `${path.basename(filePath)}: ${result.malformedLines} malformed line(s) skipped`;
          summary.errors.push(note);
          progress.errors.push(note);
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        summary.errors.push(`${filePath}: ${msg}`);
        progress.errors.push(`${path.basename(filePath)}: ${msg}`);
      }

      // Update progress after each file
      progress.files_ingested++;
      progress.files_remaining--;
      progress.percent_complete = progress.total_files > 0
        ? Math.round((progress.files_ingested / progress.total_files) * 100)
        : 100;
    }

    progress.current_file = null;

    progress.status = 'complete';
    return summary;
  } finally {
    ingestRunning = false;
    if (progress.status === 'running') progress.status = 'idle';
  }
}
