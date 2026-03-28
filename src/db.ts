import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import type {
  SearchFilters,
  SearchResult,
  SessionSummary,
  StatsResult,
  MessageRow,
  ContentBlockRow,
  IngestLogRow,
  TranscriptMessage,
  TranscriptBlock,
  TailResult,
  SyncState,
  SessionRow,
} from './types.js';

// ── Database connection management ──────────────────────────────
//
// SQLite with WAL mode supports concurrent readers + one writer,
// so a singleton connection is safe and efficient.

type RowData = Record<string, unknown>;

function getDbPath(): string {
  const envPath = process.env['FLIGHTLOG_DB_PATH'];
  if (envPath) return envPath;
  const dir = path.join(os.homedir(), '.flightlog');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'flightlog.db');
}

let singleton: Database.Database | null = null;
let schemaReady = false;

/**
 * Returns the singleton better-sqlite3 Database instance.
 * Creates it on first call, enables WAL mode, and ensures schema exists.
 */
export function getDb(): Database.Database {
  if (singleton) return singleton;

  const dbPath = getDbPath();
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  singleton = new Database(dbPath);
  singleton.pragma('journal_mode = WAL');
  singleton.pragma('foreign_keys = ON');

  if (!schemaReady) {
    ensureSchema(singleton);
    schemaReady = true;
  }

  return singleton;
}

/**
 * Close the singleton database connection and clear the cached reference.
 * Primarily used in tests to allow re-initialization with a different path.
 */
export function closeDb(): void {
  if (singleton) {
    singleton.close();
    singleton = null;
    schemaReady = false;
  }
}

// ── Schema DDL ──────────────────────────────────────────────────

function ensureSchema(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id    TEXT PRIMARY KEY,
      project       TEXT NOT NULL,
      started_at    TEXT NOT NULL,
      last_message_at TEXT NOT NULL,
      git_branch    TEXT,
      cwd           TEXT,
      message_count INTEGER DEFAULT 0,
      version       TEXT
    );

    CREATE TABLE IF NOT EXISTS messages (
      uuid          TEXT PRIMARY KEY,
      session_id    TEXT NOT NULL,
      parent_uuid   TEXT,
      type          TEXT NOT NULL,
      role          TEXT,
      timestamp     TEXT NOT NULL,
      model         TEXT,
      git_branch    TEXT,
      cwd           TEXT,
      request_id    TEXT,
      is_sidechain  INTEGER DEFAULT 0,
      input_tokens  INTEGER,
      output_tokens INTEGER,
      cache_read_tokens INTEGER,
      cache_creation_tokens INTEGER
    );

    CREATE TABLE IF NOT EXISTS content_blocks (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      message_uuid  TEXT NOT NULL,
      block_index   INTEGER NOT NULL,
      block_type    TEXT NOT NULL,
      text_content  TEXT,
      tool_name     TEXT,
      tool_input    TEXT
    );

    CREATE TABLE IF NOT EXISTS ingest_log (
      file_path       TEXT PRIMARY KEY,
      lines_ingested  INTEGER NOT NULL,
      file_size       INTEGER NOT NULL,
      last_modified   TEXT NOT NULL,
      ingested_at     TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sync_state (
      key                  TEXT PRIMARY KEY DEFAULT 'pg',
      last_synced_block_id INTEGER NOT NULL DEFAULT 0,
      last_sync_at         TEXT,
      last_sync_error      TEXT
    );

    -- Indexes for JOIN performance
    CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
    CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);
    CREATE INDEX IF NOT EXISTS idx_content_blocks_message_uuid ON content_blocks(message_uuid);
    CREATE INDEX IF NOT EXISTS idx_content_blocks_block_type ON content_blocks(block_type);
    CREATE INDEX IF NOT EXISTS idx_content_blocks_tool_name ON content_blocks(tool_name);
    CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON sessions(started_at);
    CREATE INDEX IF NOT EXISTS idx_sessions_git_branch ON sessions(git_branch);
  `);
}

// ── Insert helpers ──────────────────────────────────────────────

export function upsertSession(
  db: Database.Database,
  sessionId: string,
  project: string,
  timestamp: string,
  gitBranch: string | null,
  cwd: string | null,
  version: string | null,
): void {
  db.prepare(`
    INSERT INTO sessions (session_id, project, started_at, last_message_at, git_branch, cwd, message_count, version)
    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
    ON CONFLICT (session_id) DO UPDATE SET
      last_message_at = CASE WHEN excluded.last_message_at > sessions.last_message_at
                             THEN excluded.last_message_at ELSE sessions.last_message_at END,
      message_count = sessions.message_count + 1,
      git_branch = COALESCE(excluded.git_branch, sessions.git_branch),
      version = COALESCE(excluded.version, sessions.version)
  `).run(sessionId, project, timestamp, timestamp, gitBranch, cwd, version);
}

export function insertMessage(db: Database.Database, row: MessageRow): void {
  db.prepare(`
    INSERT OR IGNORE INTO messages (uuid, session_id, parent_uuid, type, role, timestamp, model,
      git_branch, cwd, request_id, is_sidechain, input_tokens, output_tokens,
      cache_read_tokens, cache_creation_tokens)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    row.uuid, row.session_id, row.parent_uuid, row.type, row.role, row.timestamp,
    row.model, row.git_branch, row.cwd, row.request_id, row.is_sidechain ? 1 : 0,
    row.input_tokens, row.output_tokens, row.cache_read_tokens,
    row.cache_creation_tokens,
  );
}

export function insertContentBlock(db: Database.Database, block: ContentBlockRow): void {
  db.prepare(`
    INSERT INTO content_blocks (message_uuid, block_index, block_type, text_content, tool_name, tool_input)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(block.message_uuid, block.block_index, block.block_type,
    block.text_content, block.tool_name, block.tool_input);
}

export function upsertIngestLog(
  db: Database.Database,
  filePath: string,
  linesIngested: number,
  fileSize: number,
  lastModified: Date,
): void {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO ingest_log (file_path, lines_ingested, file_size, last_modified, ingested_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (file_path) DO UPDATE SET
      lines_ingested = excluded.lines_ingested,
      file_size = excluded.file_size,
      last_modified = excluded.last_modified,
      ingested_at = excluded.ingested_at
  `).run(filePath, linesIngested, fileSize, lastModified.toISOString(), now);
}

// ── Query helpers ───────────────────────────────────────────────

export function getIngestLog(db: Database.Database, filePath: string): IngestLogRow | null {
  const row = db.prepare(`SELECT * FROM ingest_log WHERE file_path = ?`).get(filePath) as IngestLogRow | undefined;
  return row ?? null;
}

export function searchContentBlocks(
  db: Database.Database,
  query: string,
  filters: SearchFilters,
): SearchResult[] {
  const limit = filters.limit ?? 20;

  const whereClauses: string[] = [];
  const params: unknown[] = [];

  const terms = query.split(/\s+/).filter(t => t.length > 0);
  if (terms.length > 0) {
    for (const term of terms) {
      whereClauses.push(`cb.text_content LIKE ?`);
      params.push(`%${term}%`);
    }
  }

  if (filters.project) {
    whereClauses.push(`s.project LIKE ?`);
    params.push(`%${filters.project}%`);
  }
  if (filters.session_id) {
    whereClauses.push(`m.session_id = ?`);
    params.push(filters.session_id);
  }
  if (filters.date_from) {
    whereClauses.push(`m.timestamp >= ?`);
    params.push(filters.date_from);
  }
  if (filters.date_to) {
    whereClauses.push(`m.timestamp <= ?`);
    params.push(filters.date_to);
  }
  if (filters.role) {
    whereClauses.push(`m.role = ?`);
    params.push(filters.role);
  }
  if (filters.block_type) {
    whereClauses.push(`cb.block_type = ?`);
    params.push(filters.block_type);
  }
  if (filters.exclude_block_types && filters.exclude_block_types.length > 0) {
    const placeholders = filters.exclude_block_types.map(() => '?').join(', ');
    whereClauses.push(`cb.block_type NOT IN (${placeholders})`);
    params.push(...filters.exclude_block_types);
  }
  if (filters.tool_name) {
    whereClauses.push(`cb.tool_name = ?`);
    params.push(filters.tool_name);
  }

  const whereStr = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

  // Build optional columns
  const include = filters.include ?? [];
  const optionalCols: string[] = [];
  if (include.includes('token_counts')) {
    optionalCols.push('m.input_tokens', 'm.output_tokens', 'm.cache_read_tokens', 'm.cache_creation_tokens');
  }
  if (include.includes('version')) {
    optionalCols.push('s.version');
  }
  if (include.includes('uuid')) {
    optionalCols.push('m.uuid');
  }
  if (include.includes('cwd')) {
    optionalCols.push('m.cwd');
  }
  const extraCols = optionalCols.length > 0 ? ', ' + optionalCols.join(', ') : '';

  const sql = `
    SELECT
      m.session_id,
      s.project,
      m.timestamp,
      m.role,
      m.model,
      s.git_branch,
      cb.block_type,
      cb.tool_name,
      ${(filters.snippet_length ?? 300) === 0
        ? `cb.text_content AS snippet`
        : `CASE
        WHEN LENGTH(cb.text_content) > ${filters.snippet_length ?? 300} THEN SUBSTR(cb.text_content, 1, ${filters.snippet_length ?? 300}) || '...'
        ELSE cb.text_content
      END AS snippet`}
      ${extraCols}
    FROM content_blocks cb
    JOIN messages m ON m.uuid = cb.message_uuid
    JOIN sessions s ON s.session_id = m.session_id
    ${whereStr}
    ORDER BY m.timestamp DESC
    LIMIT ?
  `;

  params.push(limit);
  const rows = db.prepare(sql).all(...params) as RowData[];
  return rows.map(r => ({
    ...r,
    score: 0,
  })) as unknown as SearchResult[];
}

export function getSessionMessages(
  db: Database.Database,
  sessionId: string,
  includeToolIo: boolean,
): { session: RowData | null; messages: TranscriptMessage[] } {
  const sessionRow = db.prepare(`SELECT * FROM sessions WHERE session_id = ?`).get(sessionId) as RowData | undefined;
  if (!sessionRow) return { session: null, messages: [] };

  let blockTypeFilter = '';
  if (!includeToolIo) {
    blockTypeFilter = `AND cb.block_type NOT IN ('tool_use', 'tool_result')`;
  }

  const rows = db.prepare(`
    SELECT
      m.uuid, m.role, m.timestamp, m.model, m.type,
      cb.block_type, cb.text_content, cb.tool_name, cb.tool_input, cb.block_index
    FROM messages m
    LEFT JOIN content_blocks cb ON cb.message_uuid = m.uuid ${blockTypeFilter}
    WHERE m.session_id = ?
      AND m.type IN ('user', 'assistant')
    ORDER BY m.timestamp ASC, cb.block_index ASC
  `).all(sessionId) as RowData[];

  const messageMap = new Map<string, TranscriptMessage>();
  for (const row of rows) {
    const uuid = row['uuid'] as string;
    if (!messageMap.has(uuid)) {
      messageMap.set(uuid, {
        uuid,
        role: row['role'] as string | null,
        timestamp: String(row['timestamp']),
        model: row['model'] as string | null,
        content: [],
      });
    }
    if (row['block_type']) {
      messageMap.get(uuid)!.content.push({
        block_type: row['block_type'] as string,
        text: row['text_content'] as string | null,
        tool_name: row['tool_name'] as string | null,
        tool_input: row['tool_input'] as string | null,
      });
    }
  }

  return { session: sessionRow, messages: Array.from(messageMap.values()) };
}

export function listSessions(
  db: Database.Database,
  filters: {
    project?: string;
    date_from?: string;
    date_to?: string;
    git_branch?: string;
    limit?: number;
    offset?: number;
  },
): SessionSummary[] {
  const whereClauses: string[] = [];
  const params: unknown[] = [];

  if (filters.project) {
    whereClauses.push(`s.project LIKE ?`);
    params.push(`%${filters.project}%`);
  }
  if (filters.date_from) {
    whereClauses.push(`s.started_at >= ?`);
    params.push(filters.date_from);
  }
  if (filters.date_to) {
    whereClauses.push(`s.last_message_at <= ?`);
    params.push(filters.date_to);
  }
  if (filters.git_branch) {
    whereClauses.push(`s.git_branch = ?`);
    params.push(filters.git_branch);
  }

  const whereStr = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
  const limit = filters.limit ?? 25;
  const offset = filters.offset ?? 0;

  const rows = db.prepare(`
    SELECT
      s.session_id,
      s.project,
      s.started_at,
      s.last_message_at,
      s.git_branch,
      s.message_count,
      (
        SELECT SUBSTR(cb.text_content, 1, 200)
        FROM messages m
        JOIN content_blocks cb ON cb.message_uuid = m.uuid
        WHERE m.session_id = s.session_id
          AND m.role = 'user'
          AND cb.block_type = 'user_text'
        ORDER BY m.timestamp ASC
        LIMIT 1
      ) AS preview
    FROM sessions s
    ${whereStr}
    ORDER BY s.started_at DESC
    LIMIT ?
    OFFSET ?
  `).all(...params, limit, offset) as RowData[];

  return rows as unknown as SessionSummary[];
}

export function tailSession(
  db: Database.Database,
  sessionId: string,
  filters: {
    limit?: number;
    include_tool_io?: boolean;
    block_type?: string;
    snippet_length?: number;
  },
): TailResult[] {
  const limit = filters.limit ?? 20;
  const snippetLength = filters.snippet_length ?? 500;

  const whereClauses: string[] = ['m.session_id = ?'];
  const params: unknown[] = [sessionId];

  if (!filters.include_tool_io) {
    whereClauses.push(`cb.block_type NOT IN ('tool_use', 'tool_result')`);
  }
  if (filters.block_type) {
    whereClauses.push(`cb.block_type = ?`);
    params.push(filters.block_type);
  }

  const whereStr = whereClauses.join(' AND ');

  const sql = `
    SELECT
      m.session_id,
      m.timestamp,
      m.role,
      m.model,
      s.git_branch,
      cb.block_type,
      cb.tool_name,
      ${snippetLength === 0
        ? `cb.text_content AS snippet`
        : `CASE
        WHEN LENGTH(cb.text_content) > ${snippetLength} THEN SUBSTR(cb.text_content, 1, ${snippetLength}) || '...'
        ELSE cb.text_content
      END AS snippet`}
    FROM content_blocks cb
    JOIN messages m ON m.uuid = cb.message_uuid
    JOIN sessions s ON s.session_id = m.session_id
    WHERE ${whereStr}
    ORDER BY m.timestamp DESC, cb.block_index DESC
    LIMIT ?
  `;

  params.push(limit);
  return db.prepare(sql).all(...params) as unknown as TailResult[];
}

export function getStats(db: Database.Database): StatsResult {
  const dbPath = getDbPath();
  let dbSizeBytes = 0;
  try {
    const stat = fs.statSync(dbPath);
    dbSizeBytes = stat.size;
  } catch { /* db file may not exist yet */ }

  const counts = db.prepare(`
    SELECT
      (SELECT COUNT(*) FROM sessions) AS total_sessions,
      (SELECT COUNT(*) FROM messages) AS total_messages,
      (SELECT COUNT(*) FROM content_blocks) AS total_content_blocks
  `).get() as RowData;

  const projects = db.prepare(`
    SELECT project, COUNT(*) AS session_count
    FROM sessions
    GROUP BY project
    ORDER BY session_count DESC
  `).all() as RowData[];

  const dateRange = db.prepare(`
    SELECT
      MIN(started_at) AS earliest,
      MAX(last_message_at) AS latest
    FROM sessions
  `).get() as RowData;

  const rawSize = db.prepare(`SELECT COALESCE(SUM(file_size), 0) AS total_raw_bytes FROM ingest_log`).get() as RowData;
  const rawSizeBytes = Number(rawSize['total_raw_bytes']);

  return {
    total_sessions: Number(counts['total_sessions']),
    total_messages: Number(counts['total_messages']),
    total_content_blocks: Number(counts['total_content_blocks']),
    db_size_bytes: dbSizeBytes,
    raw_jsonl_bytes: rawSizeBytes,
    compression_ratio: rawSizeBytes > 0 ? Number((rawSizeBytes / dbSizeBytes).toFixed(1)) : null,
    projects: projects.map(r => ({
      project: String(r['project']),
      session_count: Number(r['session_count']),
    })),
    date_range: {
      earliest: dateRange['earliest'] ? String(dateRange['earliest']) : null,
      latest: dateRange['latest'] ? String(dateRange['latest']) : null,
    },
  };
}

// ── Data management ─────────────────────────────────────────────

export function deleteSessions(
  db: Database.Database,
  filters: { session_ids?: string[]; before_date?: string; project?: string },
): { sessions_deleted: number; messages_deleted: number; blocks_deleted: number } {
  const whereClauses: string[] = [];
  const params: unknown[] = [];

  if (filters.session_ids && filters.session_ids.length > 0) {
    const placeholders = filters.session_ids.map(() => '?').join(', ');
    whereClauses.push(`session_id IN (${placeholders})`);
    params.push(...filters.session_ids);
  }
  if (filters.before_date) {
    whereClauses.push(`last_message_at < ?`);
    params.push(filters.before_date);
  }
  if (filters.project) {
    whereClauses.push(`project LIKE ?`);
    params.push(`%${filters.project}%`);
  }

  if (whereClauses.length === 0) {
    throw new Error('At least one filter is required (session_ids, before_date, or project)');
  }

  const whereStr = whereClauses.join(' AND ');

  const sessions = db.prepare(`SELECT session_id FROM sessions WHERE ${whereStr}`).all(...params) as RowData[];
  if (sessions.length === 0) {
    return { sessions_deleted: 0, messages_deleted: 0, blocks_deleted: 0 };
  }

  const ids = sessions.map(s => String(s['session_id']));
  const idPlaceholders = ids.map(() => '?').join(', ');

  const msgCount = db.prepare(
    `SELECT COUNT(*) AS cnt FROM messages WHERE session_id IN (${idPlaceholders})`).get(...ids) as RowData;
  const blockCount = db.prepare(
    `SELECT COUNT(*) AS cnt FROM content_blocks WHERE message_uuid IN (SELECT uuid FROM messages WHERE session_id IN (${idPlaceholders}))`).get(...ids) as RowData;

  const deleteBlocks = db.prepare(
    `DELETE FROM content_blocks WHERE message_uuid IN (SELECT uuid FROM messages WHERE session_id IN (${idPlaceholders}))`);
  const deleteMessages = db.prepare(
    `DELETE FROM messages WHERE session_id IN (${idPlaceholders})`);
  const deleteSess = db.prepare(
    `DELETE FROM sessions WHERE session_id IN (${idPlaceholders})`);

  deleteBlocks.run(...ids);
  deleteMessages.run(...ids);
  deleteSess.run(...ids);

  for (const id of ids) {
    db.prepare(`DELETE FROM ingest_log WHERE file_path LIKE ?`).run(`%${id}%`);
  }

  return {
    sessions_deleted: ids.length,
    messages_deleted: Number(msgCount['cnt']),
    blocks_deleted: Number(blockCount['cnt']),
  };
}

export function getIngestStatus(db: Database.Database): {
  files_tracked: number;
  total_lines_ingested: number;
  last_ingest_at: string | null;
  files: { file_path: string; lines_ingested: number; file_size: number; ingested_at: string }[];
} {
  const files = db.prepare(`
    SELECT file_path, lines_ingested, file_size, ingested_at
    FROM ingest_log
    ORDER BY ingested_at DESC
  `).all() as RowData[];

  const agg = db.prepare(`
    SELECT
      COUNT(*) AS files_tracked,
      COALESCE(SUM(lines_ingested), 0) AS total_lines,
      MAX(ingested_at) AS last_ingest
    FROM ingest_log
  `).get() as RowData;

  return {
    files_tracked: Number(agg['files_tracked']),
    total_lines_ingested: Number(agg['total_lines']),
    last_ingest_at: agg['last_ingest'] ? String(agg['last_ingest']) : null,
    files: files.map(f => ({
      file_path: String(f['file_path']),
      lines_ingested: Number(f['lines_ingested']),
      file_size: Number(f['file_size']),
      ingested_at: String(f['ingested_at']),
    })),
  };
}

// ── Sync helpers ────────────────────────────────────────────────

export function getSyncState(db: Database.Database): SyncState {
  const row = db.prepare(`SELECT * FROM sync_state WHERE key = 'pg'`).get() as RowData | undefined;
  if (!row) {
    return { last_synced_block_id: 0, last_sync_at: null, last_sync_error: null };
  }
  return {
    last_synced_block_id: Number(row['last_synced_block_id']),
    last_sync_at: row['last_sync_at'] ? String(row['last_sync_at']) : null,
    last_sync_error: row['last_sync_error'] ? String(row['last_sync_error']) : null,
  };
}

export function updateSyncState(
  db: Database.Database,
  blockId: number,
  error?: string | null,
): void {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO sync_state (key, last_synced_block_id, last_sync_at, last_sync_error)
    VALUES ('pg', ?, ?, ?)
    ON CONFLICT (key) DO UPDATE SET
      last_synced_block_id = excluded.last_synced_block_id,
      last_sync_at = excluded.last_sync_at,
      last_sync_error = excluded.last_sync_error
  `).run(blockId, now, error ?? null);
}

export function getUnsyncedData(
  db: Database.Database,
  afterBlockId: number,
  activeOnly: boolean,
  limit: number,
): { sessions: SessionRow[]; messages: MessageRow[]; blocks: ContentBlockRow[] } {
  // Step 1: fetch new content blocks
  let blockSql = `
    SELECT cb.*
    FROM content_blocks cb
    JOIN messages m ON m.uuid = cb.message_uuid
  `;
  const blockParams: unknown[] = [];

  if (activeOnly) {
    blockSql += `
      JOIN sessions s ON s.session_id = m.session_id
      WHERE cb.id > ? AND s.last_message_at >= datetime('now', '-2 hours')
    `;
  } else {
    blockSql += `WHERE cb.id > ?`;
  }
  blockParams.push(afterBlockId);
  blockSql += ` ORDER BY cb.id ASC LIMIT ?`;
  blockParams.push(limit);

  const blocks = db.prepare(blockSql).all(...blockParams) as unknown as ContentBlockRow[];
  if (blocks.length === 0) {
    return { sessions: [], messages: [], blocks: [] };
  }

  // Step 2: collect distinct message UUIDs and fetch messages
  const messageUuids = [...new Set(blocks.map(b => b.message_uuid))];
  const msgPlaceholders = messageUuids.map(() => '?').join(', ');
  const messages = db.prepare(
    `SELECT * FROM messages WHERE uuid IN (${msgPlaceholders})`,
  ).all(...messageUuids) as unknown as MessageRow[];

  // Step 3: collect distinct session IDs and fetch sessions
  const sessionIds = [...new Set(messages.map(m => m.session_id))];
  const sessPlaceholders = sessionIds.map(() => '?').join(', ');
  const sessions = db.prepare(
    `SELECT * FROM sessions WHERE session_id IN (${sessPlaceholders})`,
  ).all(...sessionIds) as unknown as SessionRow[];

  return { sessions, messages, blocks };
}

export function resetDatabase(db: Database.Database): void {
  schemaReady = false;
  db.exec(`
    DROP TABLE IF EXISTS content_blocks;
    DROP TABLE IF EXISTS messages;
    DROP TABLE IF EXISTS sessions;
    DROP TABLE IF EXISTS ingest_log;
    DROP TABLE IF EXISTS sync_state;
  `);
  ensureSchema(db);
  schemaReady = true;
}
