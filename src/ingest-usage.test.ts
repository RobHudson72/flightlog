import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { ingestAll, ingestFile, getProgress } from './ingest.js';
import { getDb, closeDb, resetDatabase } from './db.js';
import { makeTmpDir, writeJsonlLine, makeAssistantMessage } from './test-helpers.js';
import type Database from 'better-sqlite3';

/**
 * IL-T-1619: ingest reads a transcript line's usage through the meter's one
 * schema (parseUsageRecord). A line the schema refuses is still stored, with
 * NULL token columns, and is counted and reported, never silently NULLed.
 */

const SESSION = 'usage-session-001';
const FILE_NAME = `${SESSION}.jsonl`;

// Fixture constants: every expected value below comes from these.
const INPUT = 11;
const OUTPUT = 22;
const CACHE_READ = 33;
const CACHE_CREATION = 44;
const MODEL = 'claude-test-model';
const SYNTHETIC = '<synthetic>';
const TEXT = 'the searchable assistant text';
const NOTE =
  `${FILE_NAME}: 1 usage line(s) failed the usage schema; stored without token counts`;

type Usage = Record<string, unknown>;

function fullUsage(): Usage {
  return {
    input_tokens: INPUT,
    output_tokens: OUTPUT,
    cache_read_input_tokens: CACHE_READ,
    cache_creation_input_tokens: CACHE_CREATION,
  };
}

function assistantLine(uuid: string, opts: { usage?: Usage; model?: string; withRequestId?: boolean }): Record<string, unknown> {
  const line = makeAssistantMessage(uuid, TEXT, SESSION);
  const message = line['message'] as Record<string, unknown>;
  message['id'] = `msg-${uuid}`;
  message['model'] = opts.model ?? MODEL;
  if (opts.usage !== undefined) message['usage'] = opts.usage;
  if (opts.withRequestId !== false) line['requestId'] = `req-${uuid}`;
  return line;
}

interface UsageRow {
  input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  cache_creation_tokens: number | null;
}

describe('ingest reads usage through the meter schema', () => {
  let tmpDir: string;
  let jsonlPath: string;
  let db: Database.Database;
  let stderr: string[];

  beforeEach(() => {
    tmpDir = makeTmpDir();
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
    db = getDb();
    resetDatabase(db);
    jsonlPath = path.join(tmpDir, FILE_NAME);
    stderr = [];
    vi.spyOn(process.stderr, 'write').mockImplementation((chunk: string | Uint8Array): boolean => {
      stderr.push(String(chunk));
      return true;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  function rowOf(uuid: string): UsageRow {
    const rows = db
      .prepare(
        `SELECT input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens FROM messages WHERE uuid = ?`,
      )
      .all(uuid) as UsageRow[];
    expect(rows.length, `rows stored for ${uuid}`).toBe(1);
    return rows[0];
  }

  function textStored(uuid: string): number {
    const row = db
      .prepare(`SELECT COUNT(*) AS n FROM content_blocks WHERE message_uuid = ? AND text_content = ?`)
      .get(uuid, TEXT) as { n: number };
    return row.n;
  }

  const NULLS: UsageRow = {
    input_tokens: null,
    output_tokens: null,
    cache_read_tokens: null,
    cache_creation_tokens: null,
  };

  it('a valid assistant usage line stores its four counts and is not counted as refused', async () => {
    writeJsonlLine(jsonlPath, assistantLine('a1', { usage: fullUsage() }));
    const result = await ingestFile(jsonlPath, db);

    expect(result.messagesAdded, 'messages added').toBe(1);
    expect(rowOf('a1')).toEqual({
      input_tokens: INPUT,
      output_tokens: OUTPUT,
      cache_read_tokens: CACHE_READ,
      cache_creation_tokens: CACHE_CREATION,
    });
    expect(result.usageRefusedLines).toBe(0);
    expect(stderr.filter((s) => s.includes('usage schema')).length).toBe(0);
  });

  it('a usage line missing output_tokens is stored with its text and NULL counts, counted and named schema', async () => {
    const usage = fullUsage();
    delete usage['output_tokens'];
    writeJsonlLine(jsonlPath, assistantLine('a2', { usage }));
    const result = await ingestFile(jsonlPath, db);

    expect(result.messagesAdded, 'messages added').toBe(1);
    expect(rowOf('a2')).toEqual(NULLS);
    expect(textStored('a2'), 'text blocks stored for a2').toBe(1);
    expect(result.usageRefusedLines).toBe(1);
    const lines = stderr.filter((s) => s.includes('failed the usage schema'));
    expect(lines.length, 'stderr usage lines').toBe(1);
    const size = fs.statSync(jsonlPath).size;
    expect(lines[0]).toBe(
      `flightlog: usage line ending at byte ${size} in ${FILE_NAME} failed the usage schema (schema); stored without token counts\n`,
    );
  });

  it('a <synthetic> line with zero counts is stored with zeros and is not refused', async () => {
    const zeros: Usage = {
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0,
    };
    writeJsonlLine(jsonlPath, assistantLine('a3', { usage: zeros, model: SYNTHETIC, withRequestId: false }));
    const result = await ingestFile(jsonlPath, db);

    expect(result.messagesAdded, 'messages added').toBe(1);
    expect(rowOf('a3')).toEqual({
      input_tokens: 0,
      output_tokens: 0,
      cache_read_tokens: 0,
      cache_creation_tokens: 0,
    });
    expect(result.usageRefusedLines).toBe(0);
  });

  it('a <synthetic> line with a non-zero count is refused as synthetic-nonzero and stored with NULLs', async () => {
    writeJsonlLine(jsonlPath, assistantLine('a4', { usage: fullUsage(), model: SYNTHETIC, withRequestId: false }));
    const result = await ingestFile(jsonlPath, db);

    expect(result.messagesAdded, 'messages added').toBe(1);
    expect(rowOf('a4')).toEqual(NULLS);
    expect(textStored('a4'), 'text blocks stored for a4').toBe(1);
    expect(result.usageRefusedLines).toBe(1);
    const lines = stderr.filter((s) => s.includes('failed the usage schema'));
    expect(lines.length, 'stderr usage lines').toBe(1);
    expect(lines[0]).toContain('(synthetic-nonzero)');
  });

  it('an assistant line with no usage is stored with NULLs and is not counted', async () => {
    writeJsonlLine(jsonlPath, assistantLine('a5', {}));
    const result = await ingestFile(jsonlPath, db);

    expect(result.messagesAdded, 'messages added').toBe(1);
    expect(rowOf('a5')).toEqual(NULLS);
    expect(result.usageRefusedLines).toBe(0);
    expect(stderr.filter((s) => s.includes('failed the usage schema')).length).toBe(0);
  });

  it('the ingest-all summary and progress carry the exact note for a file with one refused usage line', async () => {
    const usage = fullUsage();
    delete usage['output_tokens'];
    writeJsonlLine(jsonlPath, assistantLine('a6', { usage }));
    const summary = await ingestAll(tmpDir, null);

    expect(summary.files_processed, 'files processed').toBe(1);
    expect(summary.errors).toEqual([NOTE]);
    expect(getProgress().errors).toEqual([NOTE]);
  });
});
