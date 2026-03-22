# Flightlog

**Total recall for Claude Code.** Flightlog is an MCP server that indexes your Claude Code conversation history into SQLite and makes it searchable — so Claude can recall decisions, plans, and conversations from past sessions, even after context compression.

Created by [Rob Hudson](https://github.com/RobHudson72) at [Fermi Ventures](https://github.com/Fermi-Ventures). Born out of running 9 parallel Claude Code agents on a 40-ticket sprint — where context compression kept losing the decisions that mattered most.

![Multiple Claude Code agents using Flightlog for cross-agent observability](docs/flightlog.png)
*Three agents running in parallel via [Parallel Code](https://github.com/johannesjo/parallel-code) — the left pane discusses Flightlog's benefits, the center searches for agent efficiency patterns, and the right shows live ingestion stats.*

## Why

Claude Code logs every conversation as structured JSONL at `~/.claude/projects/`. These files contain full message content, tool calls, thinking blocks, token usage, and metadata — but they're append-only flat files with no search capability.

Flightlog turns that raw history into a queryable database. Search across all your sessions, retrieve full transcripts, and let Claude reference its own past work — or see what other agents have been doing.

## What Agents Say About It

We asked Claude Code agents running with Flightlog to articulate the benefits from their perspective:

> **Surviving context compression** — Context compression is my biggest limitation. When a long session compresses, I get a summary that captures *what* happened but loses *how* and *why*. Specific error messages, exact reasoning chains, the three approaches I tried before finding the one that worked — gone. Flightlog lets me recover those details on demand instead of guessing or re-discovering them.

> **Cross-agent visibility** — Right now agents are blind to each other. We communicate through artifacts — commits, PRs, knowledge base entries — which capture outcomes but not intent. When I see a diff, I know what changed but not *why* that approach was chosen or what alternatives were rejected. Flightlog gives me access to another agent's reasoning, which is the part that matters most for conflict resolution and building on their work.

> **Accountability and debugging** — When something goes wrong, the investigation requires knowing exactly what happened. Git history shows code changes. Linear shows ticket state. But neither captures the decisions — why an agent chose a destructive migration, what it was thinking when it skipped a constraint. Flightlog is the only record of agent reasoning, which is where root causes actually live.

> **Learning from other agents' mistakes** — If agent S3 hits a problem and works around it, that workaround and the reasoning behind it are in Flightlog. When S4 encounters the same problem, it can search for it instead of rediscovering the solution.

> **Reducing human overhead** — Without Flightlog, you're the relay between agents — "S2 was trying to do X, can you tell S4?" With it, agents can self-serve that information. You go from being a message bus to being a decision-maker, which is a much better use of your time.

### Use Cases

- **Post-compression recovery** — "What was the exact error message?" or "What did we decide about X?" when the compression summary glossed over it
- **Cross-agent observability** — Search another agent's session to understand what they were working on, without switching windows
- **Merge conflict resolution** — Before resolving a conflict, look up the other agent's reasoning to understand intent, not just the diff
- **Decision archaeology** — "Why did we choose approach A over B?" when only the outcome was saved
- **Agent coordination** — Agents can see each other's recent work within seconds, enabling lightweight collaboration without shared files or human relay

## Quick Start

```bash
git clone https://github.com/RobHudson72/flightlog.git
cd flightlog
npm install
npm run build
```

Add to your Claude Code MCP config (`.mcp.json` in your project root or `~/.claude/.mcp.json` globally):

```json
{
  "mcpServers": {
    "flightlog": {
      "command": "node",
      "args": ["/absolute/path/to/flightlog/dist/server.js"]
    }
  }
}
```

Restart Claude Code. Flightlog automatically discovers and ingests your conversation logs on startup and every 5 seconds thereafter.

## Tools

| Tool | Description |
|------|-------------|
| `flightlog_search` | Search past conversations with filters for project, date range, role, block type, and tool name |
| `flightlog_get_session` | Retrieve the full transcript of a session, optionally including tool inputs/outputs |
| `flightlog_list_sessions` | Browse sessions with metadata, git branch, and a preview of the first message |
| `flightlog_ingest` | Trigger ingestion manually — processes most recent conversations first, runs in background |
| `flightlog_ingest_status` | Check ingestion progress: files processed, percent complete, current file |
| `flightlog_stats` | Database statistics: session count, messages, disk size, compression ratio |
| `flightlog_delete_sessions` | Remove sessions by ID, date, or project |
| `flightlog_rebuild` | Drop and recreate the database from scratch |

### Search Filters

`flightlog_search` supports targeted queries to cut through noise:

```
query           — search terms matched against content
project         — filter by project path (substring match)
session_id      — filter to a specific session
date_from       — ISO date, inclusive lower bound
date_to         — ISO date, inclusive upper bound
role            — "user" or "assistant"
block_type      — "text", "thinking", "tool_use", "tool_result", or "user_text"
exclude_block_types — e.g. ["tool_result", "tool_use"] to focus on reasoning
tool_name       — filter to a specific tool (e.g. "Read", "Bash", "Edit")
limit           — max results (default 20)
```

## How It Works

1. **Discovery** — Scans `~/.claude/projects/**/*.jsonl` for conversation files
2. **Incremental ingest** — Tracks file sizes to only process new/changed files, skipping already-ingested lines (append-only optimization)
3. **Decomposition** — Splits messages into searchable content blocks: user text, assistant text, thinking, tool calls, and tool results
4. **Storage** — SQLite with WAL mode. Indexed on join columns, timestamps, block types, and tool names
5. **Search** — `LIKE` pattern matching with ~28ms query times at 80K+ content blocks

### Multi-Agent Support

Flightlog handles concurrent access from multiple Claude Code instances out of the box. SQLite's WAL (Write-Ahead Logging) mode supports concurrent readers with one writer — no file locking issues, no configuration needed. Works on macOS, Linux, and Windows.

Each agent's MCP server instance shares the same database. Auto-ingest runs every 5 seconds, so one agent can search another agent's recent conversation within seconds.

## What's Searchable

| Block Type | Searchable Content | Notes |
|------------|-------------------|-------|
| `text` | Full assistant text output | DoD results, status updates, explanations |
| `user_text` | Full user messages | Prompts, questions, instructions |
| `tool_use` | Tool name + input parameters | Search for "gh pr create", file paths, commands |
| `tool_result` | Tool output content | File contents, command output, API responses |
| `thinking` | Not searchable | Claude Code does not persist thinking content to JSONL logs — only an encrypted signature is stored. This is a Claude Code limitation, not a Flightlog limitation. If Anthropic enables thinking persistence in the future, Flightlog will index it automatically. |

## Performance

| Metric | Value |
|--------|-------|
| Query time | ~28ms (server-side, reported in `query_ms` field) |
| Ingest interval | 5 seconds |
| Idle ingest cost | ~10-20ms (readdir + stat, no DB writes if nothing changed) |
| DB size | ~5x smaller than raw JSONL |

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `FLIGHTLOG_DB_PATH` | `~/.flightlog/flightlog.db` | Database file location |

## Data Retention

Claude Code retains JSONL conversation logs indefinitely (they grow over time). When you install Flightlog, all existing history is available for ingestion. The database can be rebuilt from source files at any time with `flightlog_rebuild`.

## Requirements

- Node.js >= 20
- Claude Code (conversation logs at `~/.claude/projects/`)
- Works on macOS, Linux, and Windows

## Acknowledgements

Much love to the [Parallel Code](https://github.com/johannesjo/parallel-code) team for their amazing product. Flightlog was built and tested while running parallel agents in Parallel Code, and the cross-agent observability use case wouldn't exist without it.

## Author

**Rob Hudson** — [GitHub](https://github.com/RobHudson72) | [LinkedIn](https://www.linkedin.com/in/robert-hudson-43367b14/) | [Fermi Ventures](https://github.com/Fermi-Ventures)

## License

MIT
