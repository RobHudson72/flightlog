import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { ingestFile } from './ingest.js';
import { getDb, closeDb, resetDatabase } from './db.js';
import { makeTmpDir, writeJsonlLine, makeUserMessage } from './test-helpers.js';
import type Database from 'better-sqlite3';

/**
 * CAD-T-654: Claude Code writes a `file-history-snapshot` line whose
 * `messageId` is the NEXT prompt's `uuid`, just before the prompt. Keyed by
 * that id, the snapshot used to win the uuid primary key, so the prompt was
 * stored as `type='file-history-snapshot', role=NULL, cwd=NULL` and every
 * reader filtering on user messages missed it (about 80% of fleet prompts).
 */

const SESSION = 'snapshot-session-001';

function snapshotLine(messageId: string): Record<string, unknown> {
  return {
    type: 'file-history-snapshot',
    messageId,
    snapshot: { messageId, trackedFileBackups: {}, timestamp: new Date().toISOString() },
    isSnapshotUpdate: false,
  };
}

describe('a file-history snapshot never displaces the prompt it precedes', () => {
  let tmpDir: string;
  let jsonlPath: string;
  let db: Database.Database;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
    db = getDb();
    resetDatabase(db);
    jsonlPath = path.join(tmpDir, `${SESSION}.jsonl`);
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('stores the prompt as a user message with its role, cwd and text, and no snapshot row', async () => {
    writeJsonlLine(jsonlPath, snapshotLine('p1'));
    writeJsonlLine(jsonlPath, makeUserMessage('p1', '[e:ab12c] Run /agent-dod W5 END-1', SESSION));
    await ingestFile(jsonlPath, db);

    const rows = db.prepare(`SELECT uuid, type, role, cwd, is_sidechain FROM messages WHERE session_id = ?`).all(SESSION);
    expect(rows).toEqual([{ uuid: 'p1', type: 'user', role: 'user', cwd: '/tmp/test-project', is_sidechain: 0 }]);
    const blocks = db
      .prepare(`SELECT block_type, text_content FROM content_blocks WHERE message_uuid = 'p1'`)
      .all();
    expect(blocks).toEqual([{ block_type: 'user_text', text_content: '[e:ab12c] Run /agent-dod W5 END-1' }]);
  });

  it('a snapshot update after the prompt changes nothing', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('p2', 'hello', SESSION));
    writeJsonlLine(jsonlPath, { ...snapshotLine('p2'), isSnapshotUpdate: true });
    await ingestFile(jsonlPath, db);

    const rows = db.prepare(`SELECT uuid, type FROM messages WHERE session_id = ?`).all(SESSION);
    expect(rows).toEqual([{ uuid: 'p2', type: 'user' }]);
  });
});

/**
 * CAD-T-668: a prompt submitted while the agent is mid-turn is recorded as an
 * `attachment` line of type `queued_command`, not a `user` line. It is a
 * submitted user turn and is stored as one.
 */
describe('a queued prompt is stored as the user message it is', () => {
  let tmpDir: string;
  let jsonlPath: string;
  let db: Database.Database;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
    db = getDb();
    resetDatabase(db);
    jsonlPath = path.join(tmpDir, `${SESSION}.jsonl`);
  });

  afterEach(() => {
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('stores a queued_command attachment as a user message with its prompt text', async () => {
    writeJsonlLine(jsonlPath, {
      type: 'attachment',
      uuid: 'q1',
      parentUuid: null,
      isSidechain: false,
      timestamp: new Date().toISOString(),
      sessionId: SESSION,
      cwd: '/tmp/test-project',
      attachment: {
        type: 'queued_command',
        prompt: '[Cadence] [Message from DOC] msgId=abc — use parallel_read(id="abc") to view [e:jpxhd]',
        commandMode: 'prompt',
      },
      // The system-reminder wrapper the agent saw must never be ingested as the text.
      rendered: [{ content: '<system-reminder>\nThe user sent a new message while you were working: …' }],
    });
    await ingestFile(jsonlPath, db);

    const rows = db.prepare(`SELECT uuid, type, role, cwd, is_sidechain FROM messages WHERE session_id = ?`).all(SESSION);
    expect(rows).toEqual([{ uuid: 'q1', type: 'user', role: 'user', cwd: '/tmp/test-project', is_sidechain: 0 }]);
    const blocks = db.prepare(`SELECT block_type, text_content FROM content_blocks WHERE message_uuid = 'q1'`).all();
    expect(blocks).toEqual([
      { block_type: 'user_text', text_content: '[Cadence] [Message from DOC] msgId=abc — use parallel_read(id="abc") to view [e:jpxhd]' },
    ]);
  });

  it('keeps a subagent's queued prompt (no commandMode) on the sidechain, out of prompt readers', async () => {
    writeJsonlLine(jsonlPath, {
      type: 'attachment',
      uuid: 's1',
      parentUuid: null,
      isSidechain: true,
      timestamp: new Date().toISOString(),
      sessionId: SESSION,
      attachment: { type: 'queued_command', prompt: 'to the subagent' },
    });
    await ingestFile(jsonlPath, db);
    expect(db.prepare(`SELECT type, is_sidechain FROM messages WHERE uuid = 's1'`).get()).toEqual({
      type: 'user',
      is_sidechain: 1,
    });
  });

    it('leaves every other attachment line alone', async () => {
    writeJsonlLine(jsonlPath, {
      type: 'attachment',
      uuid: 'a1',
      parentUuid: null,
      isSidechain: false,
      timestamp: new Date().toISOString(),
      sessionId: SESSION,
      attachment: { type: 'todo_reminder', content: [] },
    });
    await ingestFile(jsonlPath, db);
    expect(db.prepare(`SELECT COUNT(*) AS n FROM messages`).get()).toEqual({ n: 0 });
  });
});
