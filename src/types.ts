// ── JSONL line types (as stored by Claude Code) ─────────────────

export interface JsonlSnapshotMessage {
  type: 'file-history-snapshot';
  messageId: string;
  snapshot: {
    messageId: string;
    trackedFileBackups: Record<string, unknown>;
    timestamp: string;
  };
  isSnapshotUpdate: boolean;
}

export interface JsonlUserMessage {
  type: 'user';
  uuid: string;
  parentUuid: string | null;
  isSidechain: boolean;
  promptId?: string;
  message: {
    role: 'user';
    content: string | JsonlToolResultBlock[];
  };
  timestamp: string;
  sessionId: string;
  cwd: string;
  gitBranch?: string;
  version?: string;
  userType?: string;
  permissionMode?: string;
  toolUseResult?: string;
  sourceToolAssistantUUID?: string;
}

export interface JsonlAssistantMessage {
  type: 'assistant';
  uuid: string;
  parentUuid: string | null;
  isSidechain: boolean;
  requestId?: string;
  message: {
    model?: string;
    id?: string;
    type: 'message';
    role: 'assistant';
    content: JsonlContentBlock[];
    stop_reason: string | null;
    usage?: {
      input_tokens?: number;
      output_tokens?: number;
      cache_creation_input_tokens?: number;
      cache_read_input_tokens?: number;
    };
  };
  timestamp: string;
  sessionId: string;
  cwd: string;
  gitBranch?: string;
  version?: string;
  userType?: string;
}

export type JsonlContentBlock =
  | { type: 'text'; text: string }
  | { type: 'thinking'; thinking: string; signature?: string }
  | { type: 'tool_use'; id: string; name: string; input: Record<string, unknown>; caller?: unknown }
  | { type: 'tool_result'; content: string; is_error?: boolean; tool_use_id: string };

export type JsonlToolResultBlock = {
  type: 'tool_result';
  content: string;
  is_error?: boolean;
  tool_use_id: string;
};

export type JsonlLine = JsonlSnapshotMessage | JsonlUserMessage | JsonlAssistantMessage;

// ── DB row types ────────────────────────────────────────────────

export interface SessionRow {
  session_id: string;
  project: string;
  started_at: string;
  last_message_at: string;
  git_branch: string | null;
  cwd: string | null;
  message_count: number;
  version: string | null;
}

export interface MessageRow {
  uuid: string;
  session_id: string;
  parent_uuid: string | null;
  type: string;
  role: string | null;
  timestamp: string;
  model: string | null;
  git_branch: string | null;
  cwd: string | null;
  request_id: string | null;
  is_sidechain: boolean;
  input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  cache_creation_tokens: number | null;
}

export interface ContentBlockRow {
  id?: number;
  message_uuid: string;
  block_index: number;
  block_type: string;
  text_content: string | null;
  tool_name: string | null;
  tool_input: string | null;
}

export interface IngestLogRow {
  file_path: string;
  lines_ingested: number;
  file_size: number;
  last_modified: string;
  ingested_at: string;
}

// ── Tool return types ───────────────────────────────────────────

export interface SearchResult {
  session_id: string;
  project: string;
  timestamp: string;
  role: string | null;
  model: string | null;
  git_branch: string | null;
  block_type: string;
  tool_name: string | null;
  snippet: string;
  score: number;
  // opt-in fields via include parameter
  input_tokens?: number | null;
  output_tokens?: number | null;
  cache_read_tokens?: number | null;
  cache_creation_tokens?: number | null;
  version?: string | null;
  uuid?: string;
  cwd?: string | null;
}

export interface SessionDetail {
  session_id: string;
  project: string;
  started_at: string;
  last_message_at: string;
  git_branch: string | null;
  message_count: number;
  messages: TranscriptMessage[];
}

export interface TranscriptMessage {
  uuid: string;
  role: string | null;
  timestamp: string;
  model: string | null;
  content: TranscriptBlock[];
}

export interface TranscriptBlock {
  block_type: string;
  text: string | null;
  tool_name: string | null;
  tool_input: string | null;
}

export interface SessionSummary {
  session_id: string;
  project: string;
  started_at: string;
  last_message_at: string;
  git_branch: string | null;
  message_count: number;
  preview: string | null;
}

export interface IngestSummary {
  files_processed: number;
  files_skipped: number;
  messages_added: number;
  content_blocks_added: number;
  errors: string[];
}

export interface StatsResult {
  total_sessions: number;
  total_messages: number;
  total_content_blocks: number;
  db_size_bytes: number;
  raw_jsonl_bytes: number;
  compression_ratio: number | null;
  projects: { project: string; session_count: number }[];
  date_range: { earliest: string | null; latest: string | null };
}

export interface IngestProgress {
  status: 'idle' | 'running' | 'complete';
  total_files: number;
  files_ingested: number;
  files_remaining: number;
  percent_complete: number;
  messages_added: number;
  current_file: string | null;
  errors: string[];
}

export interface TailResult {
  session_id: string;
  timestamp: string;
  role: string | null;
  model: string | null;
  git_branch: string | null;
  block_type: string;
  tool_name: string | null;
  snippet: string;
}

export interface SyncState {
  last_synced_block_id: number;
  last_sync_at: string | null;
  last_sync_error: string | null;
}

export interface SyncResult {
  sessions_synced: number;
  messages_synced: number;
  blocks_synced: number;
  high_water_mark: number;
  duration_ms: number;
  error: string | null;
}

export interface SearchFilters {
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
}
