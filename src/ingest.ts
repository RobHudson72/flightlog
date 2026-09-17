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
 */
export async function resolveStart(
  db: Database.Database,
  filePath: string,
  fileSize: number,
): Promise<IngestStart | null> {
  const known = getIngestOffset(db, filePath);
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

  const legacy = getIngestLog(db, filePath);
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
async function* readCompleteLines(filePath: string, start: number): AsyncGenerator<CompleteLine> {
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

// ── Single file ingestion ───────────────────────────────────────

function toMessageRow(line: JsonlLine, sessionId: string): MessageRow | null {
  if (line.type === 'file-history-snapshot') {
    return {
      uuid: line.messageId,
      session_id: sessionId,
      parent_uuid: null,
      type: 'file-history-snapshot',
      role: null,
      timestamp: line.snapshot.timestamp,
      model: null,
      git_branch: null,
      cwd: null,
      request_id: null,
      is_sidechain: false,
      input_tokens: null,
      output_tokens: null,
      cache_read_tokens: null,
      cache_creation_tokens: null,
    };
  }

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
 */
export async function ingestFile(
  filePath: string,
  db: Database.Database,
  start: IngestStart = FULL_READ,
): Promise<IngestFileResult> {
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
      parsed = JSON.parse(rawLine) as JsonlLine;
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
    ingestAllInner(ingestPath).catch((e) => {
      const msg = e instanceof Error ? e.message : String(e);
      progress.errors.push(msg);
      progress.status = 'idle';
      ingestRunning = false;
    });
  }
  return getProgress();
}

/**
 * Blocking version — waits for ingestion to complete. Used by auto-ingest timer.
 */
export async function ingestAll(ingestPath?: string): Promise<IngestSummary> {
  if (ingestRunning) {
    // Return a summary reflecting "nothing to do, already running"
    return {
      files_processed: 0,
      files_skipped: 0,
      messages_added: 0,
      content_blocks_added: 0,
      errors: ['Ingest already in progress'],
    };
  }
  return ingestAllInner(ingestPath);
}

async function ingestAllInner(ingestPath?: string): Promise<IngestSummary> {
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
    toIngest.sort((a, b) => {
      const mtimeA = fs.statSync(a.filePath).mtimeMs;
      const mtimeB = fs.statSync(b.filePath).mtimeMs;
      return mtimeB - mtimeA;
    });

    // Update progress with totals
    progress.total_files = files.length;
    progress.files_ingested = files.length - toIngest.length;
    progress.files_remaining = toIngest.length;
    progress.percent_complete = files.length > 0
      ? Math.round((progress.files_ingested / files.length) * 100)
      : 100;

    const summary: IngestSummary = {
      files_processed: 0,
      files_skipped: files.length - toIngest.length,
      messages_added: 0,
      content_blocks_added: 0,
      errors: [],
    };

    for (const { filePath, start } of toIngest) {
      progress.current_file = path.basename(filePath, '.jsonl');

      try {
        const result = await ingestFile(filePath, db, start);
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
