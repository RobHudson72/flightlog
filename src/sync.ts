import type Database from 'better-sqlite3';
import type { Pool as PgPool } from 'pg';
import type { SyncResult, SessionRow, MessageRow, ContentBlockRow } from './types.js';
import { getSyncState, updateSyncState, getUnsyncedData } from './db.js';

const BATCH_SIZE = 5000;

// ── Configuration ───────────────────────────────────────────────

export function isSyncConfigured(): boolean {
  return Boolean(process.env['FLIGHTLOG_SYNC_URL']);
}

function getSyncInterval(): number {
  const sec = parseInt(process.env['FLIGHTLOG_SYNC_INTERVAL'] ?? '60', 10);
  return (Number.isFinite(sec) && sec > 0 ? sec : 60) * 1000;
}

function isActiveOnly(): boolean {
  const val = process.env['FLIGHTLOG_SYNC_ACTIVE_ONLY'];
  if (val === undefined) return true; // default
  return val !== '0' && val.toLowerCase() !== 'false';
}

// ── Postgres connection (lazy) ──────────────────────────────────

let pool: PgPool | null = null;
let schemaEnsured = false;

async function getPool(): Promise<PgPool> {
  if (pool) return pool;

  const url = process.env['FLIGHTLOG_SYNC_URL'];
  if (!url) throw new Error('FLIGHTLOG_SYNC_URL is not set');

  let pg: typeof import('pg');
  try {
    pg = await import('pg');
  } catch {
    throw new Error('pg module not found — run: npm install pg');
  }

  const PoolClass = pg.default?.Pool ?? pg.Pool;
  pool = new PoolClass({ connectionString: url });
  return pool;
}

async function ensurePgSchema(p: PgPool): Promise<void> {
  if (schemaEnsured) return;

  await p.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id        TEXT PRIMARY KEY,
      project           TEXT NOT NULL,
      started_at        TIMESTAMPTZ NOT NULL,
      last_message_at   TIMESTAMPTZ NOT NULL,
      git_branch        TEXT,
      cwd               TEXT,
      message_count     INTEGER DEFAULT 0,
      version           TEXT
    );

    CREATE TABLE IF NOT EXISTS messages (
      uuid                    TEXT PRIMARY KEY,
      session_id              TEXT NOT NULL REFERENCES sessions(session_id),
      parent_uuid             TEXT,
      type                    TEXT NOT NULL,
      role                    TEXT,
      timestamp               TIMESTAMPTZ NOT NULL,
      model                   TEXT,
      git_branch              TEXT,
      cwd                     TEXT,
      request_id              TEXT,
      is_sidechain            BOOLEAN DEFAULT FALSE,
      input_tokens            INTEGER,
      output_tokens           INTEGER,
      cache_read_tokens       INTEGER,
      cache_creation_tokens   INTEGER
    );

    CREATE TABLE IF NOT EXISTS content_blocks (
      id                INTEGER PRIMARY KEY,
      message_uuid      TEXT NOT NULL REFERENCES messages(uuid),
      block_index       INTEGER NOT NULL,
      block_type        TEXT NOT NULL,
      text_content      TEXT,
      tool_name         TEXT,
      tool_input        TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_pg_messages_session_id ON messages(session_id);
    CREATE INDEX IF NOT EXISTS idx_pg_messages_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_pg_content_blocks_message_uuid ON content_blocks(message_uuid);
    CREATE INDEX IF NOT EXISTS idx_pg_content_blocks_block_type ON content_blocks(block_type);
    CREATE INDEX IF NOT EXISTS idx_pg_sessions_started_at ON sessions(started_at);
  `);

  schemaEnsured = true;
}

// ── Batch upsert helpers ────────────────────────────────────────

async function upsertSessions(client: { query: PgPool['query'] }, sessions: SessionRow[]): Promise<void> {
  for (const s of sessions) {
    await client.query(
      `INSERT INTO sessions (session_id, project, started_at, last_message_at, git_branch, cwd, message_count, version)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (session_id) DO UPDATE SET
         last_message_at = GREATEST(sessions.last_message_at, EXCLUDED.last_message_at),
         message_count = GREATEST(sessions.message_count, EXCLUDED.message_count),
         git_branch = COALESCE(EXCLUDED.git_branch, sessions.git_branch),
         version = COALESCE(EXCLUDED.version, sessions.version)`,
      [s.session_id, s.project, s.started_at, s.last_message_at, s.git_branch, s.cwd, s.message_count, s.version],
    );
  }
}

async function insertMessages(client: { query: PgPool['query'] }, messages: MessageRow[]): Promise<void> {
  for (const m of messages) {
    await client.query(
      `INSERT INTO messages (uuid, session_id, parent_uuid, type, role, timestamp, model,
         git_branch, cwd, request_id, is_sidechain, input_tokens, output_tokens,
         cache_read_tokens, cache_creation_tokens)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       ON CONFLICT (uuid) DO NOTHING`,
      [m.uuid, m.session_id, m.parent_uuid, m.type, m.role, m.timestamp, m.model,
       m.git_branch, m.cwd, m.request_id, Boolean(m.is_sidechain), m.input_tokens,
       m.output_tokens, m.cache_read_tokens, m.cache_creation_tokens],
    );
  }
}

async function insertBlocks(client: { query: PgPool['query'] }, blocks: ContentBlockRow[]): Promise<void> {
  for (const b of blocks) {
    await client.query(
      `INSERT INTO content_blocks (id, message_uuid, block_index, block_type, text_content, tool_name, tool_input)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (id) DO NOTHING`,
      [b.id, b.message_uuid, b.block_index, b.block_type, b.text_content, b.tool_name, b.tool_input],
    );
  }
}

// ── Core sync cycle ─────────────────────────────────────────────

export async function runSyncCycle(db: Database.Database): Promise<SyncResult> {
  const start = performance.now();
  const totals = { sessions_synced: 0, messages_synced: 0, blocks_synced: 0 };
  let highWaterMark = 0;

  try {
    const p = await getPool();
    await ensurePgSchema(p);
    const activeOnly = isActiveOnly();

    let lastId = getSyncState(db).last_synced_block_id;

    // Batch loop — keep going while there's data
    while (true) {
      const { sessions, messages, blocks } = getUnsyncedData(db, lastId, activeOnly, BATCH_SIZE);
      if (blocks.length === 0) break;

      const client = await p.connect();
      try {
        await client.query('BEGIN');
        await upsertSessions(client, sessions);
        await insertMessages(client, messages);
        await insertBlocks(client, blocks);
        await client.query('COMMIT');
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }

      const maxBlockId = Math.max(...blocks.map(b => b.id!));
      updateSyncState(db, maxBlockId);
      lastId = maxBlockId;

      totals.sessions_synced += sessions.length;
      totals.messages_synced += messages.length;
      totals.blocks_synced += blocks.length;
      highWaterMark = maxBlockId;

      // If batch was not full, we're caught up
      if (blocks.length < BATCH_SIZE) break;
    }

    if (highWaterMark === 0) {
      highWaterMark = getSyncState(db).last_synced_block_id;
    }

    return {
      ...totals,
      high_water_mark: highWaterMark,
      duration_ms: Math.round(performance.now() - start),
      error: null,
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    const state = getSyncState(db);
    updateSyncState(db, state.last_synced_block_id, message);
    return {
      ...totals,
      high_water_mark: state.last_synced_block_id,
      duration_ms: Math.round(performance.now() - start),
      error: message,
    };
  }
}

// ── Background sync ─────────────────────────────────────────────

let syncTimer: ReturnType<typeof setInterval> | null = null;

export function startBackgroundSync(db: Database.Database): void {
  const intervalMs = getSyncInterval();

  async function trySyncCycle(): Promise<void> {
    try {
      const result = await runSyncCycle(db);
      if (result.blocks_synced > 0) {
        process.stderr.write(
          `flightlog: synced ${result.blocks_synced} blocks to PostgreSQL (${result.duration_ms}ms)\n`,
        );
      }
      if (result.error) {
        process.stderr.write(`flightlog: sync error: ${result.error}\n`);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      process.stderr.write(`flightlog: sync error: ${msg}\n`);
    }
  }

  // Delay first sync to let ingest populate data
  setTimeout(trySyncCycle, 3000);
  syncTimer = setInterval(trySyncCycle, intervalMs);
}

export function stopBackgroundSync(): void {
  if (syncTimer) {
    clearInterval(syncTimer);
    syncTimer = null;
  }
}
