import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import readline from 'node:readline';
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
  insertMessage,
  insertContentBlock,
  upsertSession,
  upsertIngestLog,
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

export interface FileToIngest {
  filePath: string;
  skipLines: number;
}

export function filterChangedFiles(
  files: string[],
  db: Database.Database,
): FileToIngest[] {
  const toIngest: FileToIngest[] = [];

  for (const filePath of files) {
    const stat = fs.statSync(filePath);
    const existing = getIngestLog(db, filePath);

    if (!existing) {
      // New file — ingest from the start
      toIngest.push({ filePath, skipLines: 0 });
    } else if (stat.size > existing.file_size) {
      // File grew — append-only optimization: skip already-ingested lines
      toIngest.push({ filePath, skipLines: existing.lines_ingested });
    }
    // If same size or smaller, skip entirely
  }

  return toIngest;
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

export async function ingestFile(
  filePath: string,
  db: Database.Database,
  skipLines: number,
): Promise<{ messagesAdded: number; blocksAdded: number }> {
  const sessionId = path.basename(filePath, '.jsonl');
  let project: string | null = null;
  let lineNumber = 0;
  let messagesAdded = 0;
  let blocksAdded = 0;

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  for await (const rawLine of rl) {
    lineNumber++;
    if (lineNumber <= skipLines) continue;
    if (!rawLine.trim()) continue;

    let parsed: JsonlLine;
    try {
      parsed = JSON.parse(rawLine) as JsonlLine;
    } catch {
      continue; // skip malformed lines
    }

    // Extract project from first message with cwd
    if (!project && 'cwd' in parsed && parsed.cwd) {
      project = parsed.cwd;
    }

    const messageRow = toMessageRow(parsed, sessionId);
    if (!messageRow) continue;

    // Use the project we extracted, or fall back to session id
    const sessionProject = project ?? sessionId;

    // Upsert session with each message (updates last_message_at, increments count)
    upsertSession(
      db,
      sessionId,
      sessionProject,
      messageRow.timestamp,
      messageRow.git_branch,
      messageRow.cwd,
      'version' in parsed ? (parsed.version ?? null) : null,
    );

    insertMessage(db, messageRow);
    messagesAdded++;

    // Extract and insert content blocks
    const blocks = extractContentBlocks(parsed);
    for (const block of blocks) {
      insertContentBlock(db, block);
      blocksAdded++;
    }
  }

  // Update ingest log
  const stat = fs.statSync(filePath);
  upsertIngestLog(db, filePath, lineNumber, stat.size, stat.mtime);

  return { messagesAdded, blocksAdded };
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
    const toIngest = filterChangedFiles(files, db);

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

    for (const { filePath, skipLines } of toIngest) {
      progress.current_file = path.basename(filePath, '.jsonl');

      try {
        const result = await ingestFile(filePath, db, skipLines);
        summary.files_processed++;
        summary.messages_added += result.messagesAdded;
        summary.content_blocks_added += result.blocksAdded;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        summary.errors.push(`${filePath}: ${msg}`);
        progress.errors.push(`${path.basename(filePath)}: ${msg}`);
      }

      // Update progress after each file
      progress.files_ingested++;
      progress.files_remaining--;
      progress.messages_added += summary.messages_added;
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
