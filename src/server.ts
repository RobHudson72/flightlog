#!/usr/bin/env node

import 'dotenv/config';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import {
  handleSearch,
  handleGetSession,
  handleTail,
  handleListSessions,
  handleIngest,
  handleStats,
  handleDeleteSessions,
  handleIngestStatus,
  handleRebuild,
} from './tools.js';
import { ingestAll } from './ingest.js';

const INGEST_INTERVAL_MS = 5_000;

const server = new McpServer({
  name: 'flightlog',
  version: '0.2.0',
  description: 'Search and retrieve YOUR past Claude Code conversations. Flightlog indexes all conversation history from ~/.claude/projects/ into a searchable database. Use it to recall what you discussed, decided, planned, or built in previous sessions — especially useful after context compression loses details.',
});

// ── Tools ───────────────────────────────────────────────────────

server.tool(
  'flightlog_search',
  'Search YOUR past Claude Code conversations — every message, tool call, and thinking block from previous sessions. Use this to recall decisions, plans, code discussions, debugging sessions, or anything discussed in prior conversations that you no longer have in context. Returns matching snippets with session IDs and timestamps.',
  {
    query: z.string().describe('Search terms to find in past conversations (e.g. "auth migration", "wave monitor", "database schema")'),
    project: z.string().optional().describe('Filter by project path (substring match)'),
    session_id: z.string().optional().describe('Filter to a specific session'),
    date_from: z.string().optional().describe('ISO date string, inclusive lower bound'),
    date_to: z.string().optional().describe('ISO date string, inclusive upper bound'),
    role: z.enum(['user', 'assistant']).optional().describe('Filter by message role'),
    block_type: z.string().optional().describe('Filter to a specific block type: text, thinking, tool_use, tool_result, or user_text'),
    exclude_block_types: z.array(z.string()).optional().describe('Exclude block types from results (e.g. ["tool_result", "tool_use"] to focus on reasoning and discussion)'),
    tool_name: z.string().optional().describe('Filter to blocks from a specific tool (e.g. "Read", "Bash", "Edit")'),
    limit: z.number().optional().describe('Max results to return (default 20)'),
    snippet_length: z.number().optional().describe('Max characters per snippet (default 300). If a result ends with "..." it was truncated — retry with a larger value (e.g. 5000) or use flightlog_get_session to read the full transcript.'),
    include: z.array(z.string()).optional().describe('Array of extra field keys to add to each result. Valid keys: "token_counts" (adds input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens), "version" (Claude Code version string), "uuid" (message ID), "cwd" (working directory). Example: ["token_counts", "version"]. Default results already include session_id, project, timestamp, role, model, git_branch, block_type, tool_name, snippet — use this parameter only when you need fields beyond those.'),
  },
  async (params) => handleSearch(params),
);

server.tool(
  'flightlog_get_session',
  'Retrieve the full transcript of a past Claude Code conversation. Use after flightlog_search to read the complete context of a session — see exactly what the user asked, what you answered, what tools were called, and what decisions were made.',
  {
    session_id: z.string().describe('Session UUID to retrieve (from flightlog_search or flightlog_list_sessions results)'),
    include_tool_io: z.boolean().optional().describe('Include tool_use inputs and tool_result outputs in transcript (default false)'),
  },
  async (params) => handleGetSession(params),
);

server.tool(
  'flightlog_tail',
  'Get the last N messages from a Claude Code session, most recent first. Use this to check what an agent is currently doing without needing keywords. Much faster than flightlog_get_session for active sessions — returns only recent activity instead of the full transcript.',
  {
    session_id: z.string().describe('Session UUID to tail (from flightlog_list_sessions results)'),
    limit: z.number().optional().describe('Number of messages to return (default 20)'),
    include_tool_io: z.boolean().optional().describe('Include tool_use inputs and tool_result outputs (default false — excluded to reduce noise)'),
    block_type: z.string().optional().describe('Filter to a specific block type: text, thinking, tool_use, tool_result, or user_text'),
    snippet_length: z.number().optional().describe('Max characters per message snippet (default 500)'),
  },
  async (params) => handleTail(params),
);

server.tool(
  'flightlog_list_sessions',
  'Browse past Claude Code conversation sessions. Shows when each session happened, which project and git branch it was on, and a preview of the first user message. Use to find a specific past conversation.',
  {
    project: z.string().optional().describe('Filter by project path (substring match)'),
    date_from: z.string().optional().describe('ISO date string'),
    date_to: z.string().optional().describe('ISO date string'),
    git_branch: z.string().optional().describe('Filter by git branch'),
    limit: z.number().optional().describe('Max sessions to return (default 25)'),
    offset: z.number().optional().describe('Pagination offset (default 0)'),
  },
  async (params) => handleListSessions(params),
);

server.tool(
  'flightlog_ingest',
  'Trigger re-indexing of Claude Code conversation logs. Normally runs automatically every 5 seconds. Use this to force an immediate refresh if you need to search very recent conversations.',
  {
    path: z.string().optional().describe('Specific JSONL file or directory to ingest. Defaults to all of ~/.claude/projects/'),
  },
  async (params) => handleIngest(params),
);

server.tool(
  'flightlog_stats',
  'Show how many past conversations are indexed: total sessions, messages, content blocks, database size, compression ratio, and per-project breakdowns.',
  {},
  async () => handleStats(),
);

server.tool(
  'flightlog_delete_sessions',
  'Delete indexed conversation sessions and all associated data. At least one filter is required.',
  {
    session_ids: z.array(z.string()).optional().describe('Specific session UUIDs to delete'),
    before_date: z.string().optional().describe('Delete all sessions with last activity before this ISO date'),
    project: z.string().optional().describe('Delete sessions matching this project path (substring match)'),
  },
  async (params) => handleDeleteSessions(params),
);

server.tool(
  'flightlog_ingest_status',
  'Check if conversation indexing is in progress and how far along it is.',
  {},
  async () => handleIngestStatus(),
);

server.tool(
  'flightlog_rebuild',
  'Drop and recreate the entire conversation index from scratch. Use when the database is corrupted or schema has changed.',
  {},
  async () => handleRebuild(),
);

// ── Auto-ingest ─────────────────────────────────────────────────

async function tryIngest(): Promise<void> {
  try {
    const summary = await ingestAll();
    if (summary.messages_added > 0) {
      process.stderr.write(
        `flightlog: ingested ${summary.messages_added} messages from ${summary.files_processed} files\n`,
      );
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    process.stderr.write(`flightlog: ingest error: ${msg}\n`);
  }
}

// ── Start ───────────────────────────────────────────────────────

const transport = new StdioServerTransport();
await server.connect(transport);
process.stderr.write('flightlog MCP server running\n');

// Ingest on startup, then periodically.
tryIngest();
setInterval(tryIngest, INGEST_INTERVAL_MS);
