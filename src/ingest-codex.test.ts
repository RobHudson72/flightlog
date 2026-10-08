import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as ingestModule from './ingest.js';
import { ingestAll, ingestFile } from './ingest.js';
import {
  CodexRolloutRefusedError,
  codexFileKey,
  discoverCodexRollouts,
  ingestCodexFile,
  resolveCodexHome,
} from './ingest-codex.js';
import { getDb, closeDb, resetDatabase, getIngestOffset, addSessionsSourceColumn } from './db.js';
import { makeTmpDir, writeJsonlLine, makeAssistantMessage } from './test-helpers.js';

/**
 * CAD-T-618: Codex rollouts land in the tables Claude transcripts use, so
 * parallel-code's submit receipts (`find-tags`, CAD-T-614) can confirm a
 * delivery to a Codex agent. Every fixture lives in a temp dir; the real
 * ~/.codex is never read.
 */

const CONVERSATION = '0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';
const BASENAME = `rollout-2026-09-27T10-03-49-${CONVERSATION}`;
const CWD = 'E:\\repos\\fermi\\endorsed\\.worktrees\\task\\codex-ipc-1';
const TAG = '[e:k3x9q]';

/**
 * parallel-code's receipt query, copied verbatim from
 * E:/repos/fermi/parallel-code/electron/ipc/flightlog-query.cjs (FIND_TAGS_SQL,
 * CAD-T-614). If that query changes, this copy must change with it.
 */
const FIND_TAGS_SQL = `SELECT cb.text_content, m.session_id, m.timestamp
FROM messages m INDEXED BY idx_messages_timestamp
JOIN content_blocks cb ON cb.message_uuid = m.uuid
WHERE m.timestamp >= ? AND m.type = 'user' AND m.is_sidechain = 0
  AND +cb.block_type = 'user_text'`;

function sessionMeta(): Record<string, unknown> {
  return {
    timestamp: '2026-09-27T10:03:49.120Z',
    type: 'session_meta',
    payload: {
      id: CONVERSATION,
      timestamp: '2026-09-27T10:03:49.100Z',
      cwd: CWD,
      originator: 'codex_cli_rs',
      cli_version: '0.156.1',
      instructions: null,
      git: { branch: 'task/codex-ipc-1' },
    },
  };
}

function message(role: string, timestamp: string, content: Record<string, unknown>[]): Record<string, unknown> {
  return { timestamp, type: 'response_item', payload: { type: 'message', role, content } };
}

const userText = (text: string): Record<string, unknown> => ({ type: 'input_text', text });
const assistantText = (text: string): Record<string, unknown> => ({ type: 'output_text', text });

/** The line types a real rollout carries besides the two flightlog stores. */
function noiseLines(): Record<string, unknown>[] {
  return [
    { timestamp: '2026-09-27T10:03:49.200Z', type: 'turn_context', payload: { cwd: CWD, model: 'gpt-5-codex' } },
    message('developer', '2026-09-27T10:03:49.210Z', [userText('<permissions instructions>')]),
    { timestamp: '2026-09-27T10:03:50.000Z', type: 'event_msg', payload: { type: 'user_message', message: `${TAG} dup` } },
    { timestamp: '2026-09-27T10:03:51.000Z', type: 'response_item', payload: { type: 'reasoning', summary: [] } },
    { timestamp: '2026-09-27T10:03:52.000Z', type: 'response_item', payload: { type: 'function_call', name: 'shell' } },
    { timestamp: '2026-09-27T10:03:53.000Z', type: 'event_msg', payload: { type: 'token_count' } },
  ];
}

function storedRows(db: Database.Database): { type: string; role: string; block_type: string; text_content: string }[] {
  return db.prepare(`
    SELECT m.type, m.role, cb.block_type, cb.text_content
    FROM messages m JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE m.session_id = ? ORDER BY m.timestamp, cb.block_index
  `).all(BASENAME) as { type: string; role: string; block_type: string; text_content: string }[];
}

function count(db: Database.Database, table: string): number {
  return (db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n;
}

describe('Codex rollout ingest', () => {
  let tmpDir: string;
  let codexHome: string;
  let rolloutPath: string;
  let db: Database.Database;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    codexHome = path.join(tmpDir, 'codex-home');
    const dayDir = path.join(codexHome, 'sessions', '2026', '09', '27');
    fs.mkdirSync(dayDir, { recursive: true });
    rolloutPath = path.join(dayDir, `${BASENAME}.jsonl`);
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
    db = getDb();
    resetDatabase(db);
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  function writeFixture(): void {
    writeJsonlLine(rolloutPath, sessionMeta());
    for (const line of noiseLines()) writeJsonlLine(rolloutPath, line);
    writeJsonlLine(rolloutPath, message('user', '2026-09-27T10:04:00.000Z', [
      userText(`${TAG} [Cadence] [Message from DOC] msgId=abc`),
      { type: 'input_image', image_url: 'data:image/png;base64,AAAA' },
    ]));
    writeJsonlLine(rolloutPath, message('assistant', '2026-09-27T10:04:05.000Z', [assistantText('Reading the message now.')]));
  }

  it('ingests one session (source codex, cwd, version), its user and assistant messages and their text blocks', async () => {
    writeFixture();
    const r = await ingestCodexFile(rolloutPath, db);
    expect(r.messagesAdded).toBe(2);
    expect(r.blocksAdded).toBe(2);
    expect(r.malformedLines).toBe(0);

    expect(db.prepare(`SELECT * FROM sessions`).all()).toEqual([{
      session_id: BASENAME,
      project: 'codex:codex-ipc-1',
      started_at: '2026-09-27T10:03:49.100Z',
      last_message_at: '2026-09-27T10:04:05.000Z',
      git_branch: 'task/codex-ipc-1',
      cwd: CWD,
      message_count: 2,
      version: '0.156.1',
      source: 'codex',
    }]);
    expect(storedRows(db)).toEqual([
      { type: 'user', role: 'user', block_type: 'user_text', text_content: `${TAG} [Cadence] [Message from DOC] msgId=abc` },
      { type: 'assistant', role: 'assistant', block_type: 'text', text_content: 'Reading the message now.' },
    ]);
    const msg = db.prepare(`SELECT cwd, is_sidechain FROM messages WHERE type = 'user'`).get();
    expect(msg).toEqual({ cwd: CWD, is_sidechain: 0 });
  });

  it('skips and counts every line type it does not store, never guessing at one', async () => {
    writeFixture();
    const r = await ingestCodexFile(rolloutPath, db);
    expect(r.skippedLineTypes).toEqual({
      turn_context: 1,
      'response_item:message:developer': 1,
      event_msg: 2,
      'response_item:reasoning': 1,
      'response_item:function_call': 1,
    });
    expect(r.skippedContentTypes).toEqual({ input_image: 1 });
    // The event_msg copy of the prompt is not stored a second time.
    const tagged = db.prepare(`SELECT COUNT(*) AS n FROM content_blocks WHERE text_content LIKE ?`).get(`%${TAG}%`);
    expect(tagged).toEqual({ n: 1 });
  });

  it('finds a tag in a user input_text with a type = user query', async () => {
    writeFixture();
    await ingestCodexFile(rolloutPath, db);
    const rows = db.prepare(`
      SELECT m.session_id FROM messages m JOIN content_blocks cb ON cb.message_uuid = m.uuid
      WHERE m.type = 'user' AND cb.text_content LIKE ?
    `).all(`%${TAG}%`);
    expect(rows).toEqual([{ session_id: BASENAME }]);
  });

  it("is found by parallel-code's find-tags SQL, keyed by the rollout basename (cross-repo confirmation)", async () => {
    writeFixture();
    const r = await ingestModule.ingestCodexFile(rolloutPath, db);
    expect(r.messagesAdded).toBe(2);

    const since = '2026-09-27T10:00:00.000Z';
    const rows = db.prepare(FIND_TAGS_SQL).all(since) as { text_content: string; session_id: string; timestamp: string }[];
    // L2 keys a Codex recipient's sessionIds by the rollout file basename.
    const recipientSessionIds = [path.basename(rolloutPath, '.jsonl')];
    const hits = rows.filter(row => recipientSessionIds.includes(row.session_id) && row.text_content.includes(TAG));
    expect(hits).toEqual([{
      text_content: `${TAG} [Cadence] [Message from DOC] msgId=abc`,
      session_id: BASENAME,
      timestamp: '2026-09-27T10:04:00.000Z',
    }]);
    // A bound after the prompt finds nothing: the timestamps compare as strings.
    expect(db.prepare(FIND_TAGS_SQL).all('2026-09-27T10:04:00.001Z')).toEqual([]);
  });

  it('stores timestamps as UTC Z strings, normalizing a +hh:mm offset', async () => {
    writeJsonlLine(rolloutPath, sessionMeta());
    writeJsonlLine(rolloutPath, message('user', '2026-09-27T12:04:00.000+02:00', [userText('offset prompt')]));
    await ingestCodexFile(rolloutPath, db);
    expect(db.prepare(`SELECT timestamp FROM messages`).get()).toEqual({ timestamp: '2026-09-27T10:04:00.000Z' });
  });

  it('appending lines and re-ingesting adds only the new rows, from the stored byte offset', async () => {
    writeFixture();
    await ingestCodexFile(rolloutPath, db);
    const sizeBefore = fs.statSync(rolloutPath).size;
    expect(getIngestOffset(db, codexFileKey(rolloutPath))?.byte_offset).toBe(sizeBefore);
    const uuidsBefore = db.prepare(`SELECT uuid FROM messages ORDER BY uuid`).all();

    writeJsonlLine(rolloutPath, message('user', '2026-09-27T10:05:00.000Z', [userText('[e:zz7aa] second prompt')]));
    writeJsonlLine(rolloutPath, message('assistant', '2026-09-27T10:05:03.000Z', [assistantText('second reply')]));
    const r = await ingestCodexFile(rolloutPath, db);
    expect(r.messagesAdded).toBe(2);
    // The tail read started past session_meta and the noise lines.
    expect(r.skippedLineTypes).toEqual({});
    expect(count(db, 'messages')).toBe(4);
    expect(db.prepare(`SELECT message_count, cwd, source FROM sessions`).get()).toEqual({ message_count: 4, cwd: CWD, source: 'codex' });
    expect(db.prepare(`SELECT cwd FROM messages WHERE timestamp = '2026-09-27T10:05:00.000Z'`).get()).toEqual({ cwd: CWD });

    // Nothing new → nothing read
    expect((await ingestCodexFile(rolloutPath, db)).messagesAdded).toBe(0);

    // A full re-read maps every line to the same uuid: no duplicates
    db.prepare(`DELETE FROM ingest_offsets`).run();
    db.prepare(`DELETE FROM ingest_log`).run();
    const again = await ingestCodexFile(rolloutPath, db);
    expect(again.messagesAdded).toBe(0);
    expect(count(db, 'messages')).toBe(4);
    expect(count(db, 'content_blocks')).toBe(4);
    const uuidsAfter = db.prepare(`SELECT uuid FROM messages ORDER BY uuid`).all();
    expect(uuidsAfter).toEqual(expect.arrayContaining(uuidsBefore));
  });

  it('keys one rollout the same way whatever spelling of its path a caller hands in, reading it through that spelling', async () => {
    writeFixture();
    await ingestCodexFile(rolloutPath, db);
    const key = codexFileKey(rolloutPath);
    // An upper-cased drive letter and directory, forward slashes; the basename
    // (the session id) as readdir gives it.
    const otherSpelling = process.platform === 'win32'
      ? `${path.dirname(rolloutPath).toUpperCase().replace(/\\/g, '/')}/${path.basename(rolloutPath)}`
      : path.join(path.dirname(rolloutPath), '.', path.basename(rolloutPath));

    // Appended line: the second spelling must tail it from the one stored offset.
    writeJsonlLine(rolloutPath, message('user', '2026-09-27T10:06:00.000Z', [userText('third prompt')]));
    const statSpy = vi.spyOn(fs, 'statSync');
    const streamSpy = vi.spyOn(fs, 'createReadStream');
    try {
      const r = await ingestCodexFile(otherSpelling, db);
      expect(r.messagesAdded).toBe(1);
      // The file is stat'ed and read through the caller's spelling; the
      // case-folded key is a row key only (a case-sensitive directory has no
      // file at it).
      expect(statSpy.mock.calls.map(c => c[0])).toContain(otherSpelling);
      expect(streamSpy.mock.calls.map(c => c[0])).toEqual([otherSpelling]);
      if (key !== otherSpelling) {
        expect(statSpy.mock.calls.some(c => c[0] === key)).toBe(false);
        expect(streamSpy.mock.calls.some(c => c[0] === key)).toBe(false);
      }
    } finally {
      statSpy.mockRestore();
      streamSpy.mockRestore();
    }
    expect(db.prepare(`SELECT file_path FROM ingest_offsets`).all()).toEqual([{ file_path: key }]);
    expect(count(db, 'messages')).toBe(3);

    // A full re-read through the other spelling stores nothing twice.
    db.prepare(`DELETE FROM ingest_offsets`).run();
    db.prepare(`DELETE FROM ingest_log`).run();
    expect((await ingestCodexFile(otherSpelling, db)).messagesAdded).toBe(0);
    expect(count(db, 'messages')).toBe(3);
  });

  it('counts a malformed complete line instead of hiding it', async () => {
    writeJsonlLine(rolloutPath, sessionMeta());
    fs.appendFileSync(rolloutPath, '{"type":"response_item","payload":\n');
    writeJsonlLine(rolloutPath, message('user', '2026-09-27T10:04:00.000Z', [userText('after the break')]));
    const r = await ingestCodexFile(rolloutPath, db);
    expect(r.malformedLines).toBe(1);
    expect(r.messagesAdded).toBe(1);
  });

  it('refuses a file that is not named like a rollout', async () => {
    const other = path.join(path.dirname(rolloutPath), 'history.jsonl');
    writeJsonlLine(other, sessionMeta());
    await expect(ingestCodexFile(other, db)).rejects.toThrow(/not a Codex rollout/);
  });

  it('discovers rollouts at the YYYY/MM/DD depth only', () => {
    writeFixture();
    fs.writeFileSync(path.join(codexHome, 'sessions', 'rollout-at-root.jsonl'), '');
    fs.writeFileSync(path.join(codexHome, 'sessions', '2026', '09', '27', 'notes.jsonl'), '');
    expect(discoverCodexRollouts(codexHome)).toEqual([rolloutPath]);
  });

  it('a missing Codex home is a no-op, never an error', async () => {
    const missing = path.join(tmpDir, 'no-codex-here');
    expect(discoverCodexRollouts(missing)).toEqual([]);
    const projects = path.join(tmpDir, 'projects');
    fs.mkdirSync(projects);
    const summary = await ingestAll(projects, missing);
    expect(summary.errors).toEqual([]);
    expect(summary.files_processed).toBe(0);
    expect(fs.existsSync(missing)).toBe(false);
  });

  it('ingestAll ingests Codex rollouts after Claude transcripts and reports the skipped line types', async () => {
    writeFixture();
    const projects = path.join(tmpDir, 'projects');
    const projectDir = path.join(projects, 'p');
    fs.mkdirSync(projectDir, { recursive: true });
    writeJsonlLine(path.join(projectDir, 'claude-session-1.jsonl'), makeAssistantMessage('c1', 'claude reply', 'claude-session-1'));

    const summary = await ingestAll(projects, codexHome);
    expect(summary.errors).toEqual([]);
    expect(summary.files_processed).toBe(2);
    expect(summary.messages_added).toBe(3);
    expect(summary.codex_skipped_line_types).toMatchObject({ event_msg: 2, turn_context: 1 });
    expect(db.prepare(`SELECT session_id, source FROM sessions ORDER BY source`).all()).toEqual([
      { session_id: 'claude-session-1', source: 'claude' },
      { session_id: BASENAME, source: 'codex' },
    ]);

    // A second pass finds both unchanged
    const second = await ingestAll(projects, codexHome);
    expect(second.files_processed).toBe(0);
    expect(second.files_skipped).toBe(2);
  });

  it('exports CODEX_INGEST = true and ingestCodexFile from ingest (the find-tags feature-detect)', () => {
    expect(ingestModule.CODEX_INGEST).toBe(true);
    expect(typeof ingestModule.ingestCodexFile).toBe('function');
  });
});

describe('CODEX_HOME resolution', () => {
  it('is CODEX_HOME when set and non-empty, else ~/.codex (the codex-resume.ts rule)', () => {
    expect(resolveCodexHome({ CODEX_HOME: 'X:\\codex-home' })).toBe('X:\\codex-home');
    const fallback = resolveCodexHome({ CODEX_HOME: '' });
    expect(fallback.endsWith(`${path.sep}.codex`)).toBe(true);
    expect(resolveCodexHome({})).toBe(fallback);
  });
});

describe('the Claude ingest refuses a Codex rollout (round-4 NF2)', () => {
  let tmpDir: string;
  let savedCodexHome: string | undefined;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    savedCodexHome = process.env['CODEX_HOME'];
    process.env['CODEX_HOME'] = path.join(tmpDir, 'codex-home');
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    if (savedCodexHome === undefined) delete process.env['CODEX_HOME'];
    else process.env['CODEX_HOME'] = savedCodexHome;
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('throws CodexRolloutRefusedError and advances no offset', async () => {
    const db = getDb();
    resetDatabase(db);
    const dayDir = path.join(tmpDir, 'codex-home', 'sessions', '2026', '09', '27');
    fs.mkdirSync(dayDir, { recursive: true });
    const rollout = path.join(dayDir, `${BASENAME}.jsonl`);
    writeJsonlLine(rollout, sessionMeta());

    await expect(ingestFile(rollout, db)).rejects.toBeInstanceOf(CodexRolloutRefusedError);
    expect(count(db, 'ingest_offsets')).toBe(0);

    // The rollout is still fully ingestable through its own entry point.
    writeJsonlLine(rollout, message('user', '2026-09-27T10:04:00.000Z', [userText(TAG)]));
    expect((await ingestCodexFile(rollout, db)).messagesAdded).toBe(1);
  });
});

describe('a pre-L3 database migrates to sessions.source (round-3 B2)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'old.db');
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('adds the column, reads existing rows as claude, and ingests a Codex rollout into it', async () => {
    // The sessions table exactly as db.ts created it before CAD-T-618.
    const old = new Database(path.join(tmpDir, 'old.db'));
    old.exec(`
      CREATE TABLE sessions (
        session_id    TEXT PRIMARY KEY,
        project       TEXT NOT NULL,
        started_at    TEXT NOT NULL,
        last_message_at TEXT NOT NULL,
        git_branch    TEXT,
        cwd           TEXT,
        message_count INTEGER DEFAULT 0,
        version       TEXT
      );
    `);
    old.prepare(`INSERT INTO sessions (session_id, project, started_at, last_message_at) VALUES (?, ?, ?, ?)`)
      .run('old-claude-session', '/tmp/p', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z');
    old.close();

    const db = getDb();
    const columns = (db.prepare(`PRAGMA table_info(sessions)`).all() as { name: string }[]).map(c => c.name);
    expect(columns).toContain('source');
    expect(db.prepare(`SELECT source FROM sessions WHERE session_id = 'old-claude-session'`).get()).toEqual({ source: 'claude' });

    const dayDir = path.join(tmpDir, 'codex-home', 'sessions', '2026', '09', '27');
    fs.mkdirSync(dayDir, { recursive: true });
    const rollout = path.join(dayDir, `${BASENAME}.jsonl`);
    writeJsonlLine(rollout, sessionMeta());
    writeJsonlLine(rollout, message('user', '2026-09-27T10:04:00.000Z', [userText(TAG)]));
    expect((await ingestCodexFile(rollout, db)).messagesAdded).toBe(1);
    expect(db.prepare(`SELECT source FROM sessions WHERE session_id = ?`).get(BASENAME)).toEqual({ source: 'codex' });
  });

  it('tolerates the duplicate-column error a concurrent server causes', () => {
    const db = getDb(); // already migrated
    expect(() => addSessionsSourceColumn(db)).not.toThrow();
  });
});

describe('a Claude user turn written as an array keeps its text (round-3 B1)', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('stores a text item in array content as user_text, beside its tool_result', async () => {
    const db = getDb();
    resetDatabase(db);
    const jsonlPath = path.join(tmpDir, 'array-session.jsonl');
    writeJsonlLine(jsonlPath, {
      type: 'user',
      uuid: 'u1',
      parentUuid: null,
      isSidechain: false,
      message: {
        role: 'user',
        content: [
          { type: 'tool_result', tool_use_id: 't1', content: 'tool output' },
          { type: 'text', text: `${TAG} the prompt beside a tool result` },
        ],
      },
      timestamp: '2026-09-27T10:04:00.000Z',
      sessionId: 'array-session',
      cwd: '/tmp/test-project',
    });
    await ingestFile(jsonlPath, db);
    expect(db.prepare(`SELECT block_index, block_type, text_content FROM content_blocks ORDER BY block_index`).all()).toEqual([
      { block_index: 0, block_type: 'tool_result', text_content: 'tool output' },
      { block_index: 1, block_type: 'user_text', text_content: `${TAG} the prompt beside a tool result` },
    ]);
  });
});
