import {
  getDb,
  searchContentBlocks,
  getSessionMessages,
  listSessions,
  tailSession,
  getStats,
  deleteSessions,
  getIngestStatus,
  resetDatabase,
} from './db.js';
import { ingestAll, triggerIngest, getProgress } from './ingest.js';
import type {
  SearchFilters,
  SearchResult,
  SessionDetail,
  SessionSummary,
  IngestSummary,
  StatsResult,
} from './types.js';

// ── Response helpers ────────────────────────────────────────────

function ok(data: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }] };
}

function err(message: string) {
  return { content: [{ type: 'text' as const, text: JSON.stringify({ error: message }) }], isError: true as const };
}

function errFromCatch(prefix: string, e: unknown) {
  const message = e instanceof Error ? e.message : String(e);
  return err(`${prefix}: ${message}`);
}

// ── Tool handlers ───────────────────────────────────────────────

export async function handleSearch(params: {
  query: string;
  project?: string;
  session_id?: string;
  date_from?: string;
  date_to?: string;
  role?: 'user' | 'assistant';
  block_type?: string;
  exclude_block_types?: string[];
  tool_name?: string;
  limit?: number;
  snippet_length?: number;
  include?: string[];
}) {
  try {
    const db = getDb();
    const filters: SearchFilters = {
      project: params.project,
      session_id: params.session_id,
      date_from: params.date_from,
      date_to: params.date_to,
      role: params.role,
      block_type: params.block_type,
      exclude_block_types: params.exclude_block_types,
      tool_name: params.tool_name,
      limit: params.limit,
      snippet_length: params.snippet_length,
      include: params.include,
    };
    const start = performance.now();
    const results = searchContentBlocks(db, params.query, filters);
    const queryMs = Math.round(performance.now() - start);

    if (results.length === 0) {
      return ok({ message: 'No results found', query: params.query, query_ms: queryMs, results: [] });
    }

    return ok({
      query: params.query,
      result_count: results.length,
      query_ms: queryMs,
      results: results.map(r => {
        const result: Record<string, unknown> = {
          session_id: r.session_id,
          project: r.project,
          timestamp: r.timestamp,
          role: r.role,
          model: r.model,
          git_branch: r.git_branch,
          block_type: r.block_type,
          tool_name: r.tool_name,
          snippet: r.snippet,
        };
        // Include opt-in fields only when present
        const include = params.include ?? [];
        if (include.includes('token_counts')) {
          result.input_tokens = r.input_tokens;
          result.output_tokens = r.output_tokens;
          result.cache_read_tokens = r.cache_read_tokens;
          result.cache_creation_tokens = r.cache_creation_tokens;
        }
        if (include.includes('version')) result.version = r.version;
        if (include.includes('uuid')) result.uuid = r.uuid;
        if (include.includes('cwd')) result.cwd = r.cwd;
        return result;
      }),
    });
  } catch (e) {
    return errFromCatch('flightlog_search', e);
  }
}

export async function handleGetSession(params: {
  session_id: string;
  include_tool_io?: boolean;
}) {
  try {
    const db = getDb();
    const { session, messages } = getSessionMessages(
      db,
      params.session_id,
      params.include_tool_io ?? false,
    );

    if (!session) {
      return err(`Session not found: ${params.session_id}`);
    }

    const detail: SessionDetail = {
      session_id: String(session['session_id']),
      project: String(session['project']),
      started_at: String(session['started_at']),
      last_message_at: String(session['last_message_at']),
      git_branch: session['git_branch'] ? String(session['git_branch']) : null,
      message_count: Number(session['message_count']),
      messages,
    };

    return ok(detail);
  } catch (e) {
    return errFromCatch('flightlog_get_session', e);
  }
}

export async function handleTail(params: {
  session_id: string;
  limit?: number;
  include_tool_io?: boolean;
  block_type?: string;
  snippet_length?: number;
}) {
  try {
    const db = getDb();
    const start = performance.now();
    const results = tailSession(db, params.session_id, {
      limit: params.limit,
      include_tool_io: params.include_tool_io,
      block_type: params.block_type,
      snippet_length: params.snippet_length,
    });
    const queryMs = Math.round(performance.now() - start);

    return ok({
      session_id: params.session_id,
      result_count: results.length,
      query_ms: queryMs,
      results,
    });
  } catch (e) {
    return errFromCatch('flightlog_tail', e);
  }
}

export async function handleListSessions(params: {
  project?: string;
  date_from?: string;
  date_to?: string;
  git_branch?: string;
  limit?: number;
  offset?: number;
}) {
  try {
    const db = getDb();
    const sessions = listSessions(db, params);
    return ok({
      session_count: sessions.length,
      sessions,
    });
  } catch (e) {
    return errFromCatch('flightlog_list_sessions', e);
  }
}

export async function handleIngest(params: { path?: string }) {
  try {
    const progress = triggerIngest(params.path);

    if (progress.status === 'running' && progress.files_remaining > 0) {
      return ok({
        message: `Ingestion is running in the background. Processing ${progress.total_files} conversation files, most recent first. ${progress.files_ingested} of ${progress.total_files} files done (${progress.percent_complete}%). Use flightlog_ingest_status to check progress.`,
        ...progress,
      });
    }

    if (progress.status === 'running') {
      return ok({
        message: 'Ingestion is already running. Use flightlog_ingest_status to check progress.',
        ...progress,
      });
    }

    return ok({
      message: 'All conversation files are already ingested and up to date. New conversations are automatically ingested every 60 seconds.',
      ...progress,
    });
  } catch (e) {
    return errFromCatch('flightlog_ingest', e);
  }
}

export async function handleStats() {
  try {
    const db = getDb();
    const stats = getStats(db);

    const formatSize = (bytes: number) => bytes < 1024 * 1024
      ? `${(bytes / 1024).toFixed(1)} KB`
      : `${(bytes / (1024 * 1024)).toFixed(1)} MB`;

    return ok({
      ...stats,
      db_size_human: formatSize(stats.db_size_bytes),
      raw_jsonl_human: formatSize(stats.raw_jsonl_bytes),
      compression_summary: stats.compression_ratio
        ? `${stats.compression_ratio}x (${formatSize(stats.raw_jsonl_bytes)} raw → ${formatSize(stats.db_size_bytes)} stored)`
        : null,
    });
  } catch (e) {
    return errFromCatch('flightlog_stats', e);
  }
}

export async function handleDeleteSessions(params: {
  session_ids?: string[];
  before_date?: string;
  project?: string;
}) {
  try {
    if (!params.session_ids && !params.before_date && !params.project) {
      return err('At least one filter is required: session_ids, before_date, or project');
    }
    const db = getDb();
    const result = deleteSessions(db, params);
    return ok(result);
  } catch (e) {
    return errFromCatch('flightlog_delete_sessions', e);
  }
}

export async function handleIngestStatus() {
  try {
    const progress = getProgress();
    const db = getDb();
    const dbStatus = getIngestStatus(db);

    const statusMessage = progress.status === 'running'
      ? `Ingestion in progress: ${progress.files_ingested}/${progress.total_files} files (${progress.percent_complete}%).${progress.current_file ? ` Currently processing: ${progress.current_file}` : ''}`
      : progress.status === 'complete'
        ? `Ingestion complete. ${dbStatus.files_tracked} files tracked, ${dbStatus.total_lines_ingested} total lines ingested.`
        : `Idle. ${dbStatus.files_tracked} files tracked. Auto-ingest runs every 60 seconds.`;

    return ok({
      message: statusMessage,
      progress,
      database: dbStatus,
    });
  } catch (e) {
    return errFromCatch('flightlog_ingest_status', e);
  }
}

export async function handleSync() {
  try {
    const { isSyncConfigured, runSyncCycle } = await import('./sync.js');
    if (!isSyncConfigured()) {
      return err('PostgreSQL sync is not configured. Set the FLIGHTLOG_SYNC_URL environment variable.');
    }
    const db = getDb();
    const result = await runSyncCycle(db);
    if (result.error) {
      return ok({ message: 'Sync completed with error', ...result });
    }
    return ok({ message: 'Sync completed successfully', ...result });
  } catch (e) {
    return errFromCatch('flightlog_sync', e);
  }
}

export async function handleRebuild() {
  try {
    const db = getDb();
    resetDatabase(db);
    const summary = await ingestAll();
    return ok({
      message: 'Database rebuilt from scratch',
      ...summary,
    });
  } catch (e) {
    return errFromCatch('flightlog_rebuild', e);
  }
}
