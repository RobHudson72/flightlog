import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { ingestFile, filterChangedFiles, resolveStart } from './ingest.js';
import {
  getDb,
  closeDb,
  resetDatabase,
  getIngestLog,
  getIngestOffset,
  upsertIngestLog,
  recomputeMessageCountsOnce,
} from './db.js';
import { makeTmpDir, writeJsonlLine, makeUserMessage, makeAssistantMessage } from './test-helpers.js';
import type Database from 'better-sqlite3';

const SESSION = 'tail-session-001';

function messageCount(db: Database.Database, sessionId: string): number {
  const row = db.prepare(`SELECT message_count FROM sessions WHERE session_id = ?`).get(sessionId) as
    | { message_count: number }
    | undefined;
  return row?.message_count ?? 0;
}

function storedMessages(db: Database.Database, sessionId: string): number {
  const row = db.prepare(`SELECT COUNT(*) AS n FROM messages WHERE session_id = ?`).get(sessionId) as { n: number };
  return row.n;
}

describe('byte-offset tail ingestion', () => {
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

  it('tails appended lines from the recorded byte offset', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    const first = await ingestFile(jsonlPath, db);
    expect(first.messagesAdded).toBe(2);
    const sizeAfterTwo = fs.statSync(jsonlPath).size;
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(sizeAfterTwo);

    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'third', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m4', 'fourth', SESSION));

    const changed = await filterChangedFiles([jsonlPath], db);
    expect(changed).toHaveLength(1);
    expect(changed[0]!.start).toEqual({ offset: sizeAfterTwo, lines: 2 });

    const second = await ingestFile(jsonlPath, db, changed[0]!.start);
    expect(second.messagesAdded).toBe(2);
    expect(getIngestOffset(db, jsonlPath)).toMatchObject({
      byte_offset: fs.statSync(jsonlPath).size,
      lines_consumed: 4,
    });
    expect(storedMessages(db, SESSION)).toBe(4);
    expect(messageCount(db, SESSION)).toBe(4);

    // Nothing new → nothing to ingest
    expect(await filterChangedFiles([jsonlPath], db)).toHaveLength(0);
  });

  it('consumes only newline-terminated lines and picks up the completed line later', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    await ingestFile(jsonlPath, db);

    const full = JSON.stringify(makeAssistantMessage('m2', 'complete', SESSION)) + '\n';
    const half = JSON.stringify(makeUserMessage('m3', 'in progress', SESSION));
    const cut = Math.floor(half.length / 2);
    fs.appendFileSync(jsonlPath, full + half.slice(0, cut));

    const r1 = await ingestFile(jsonlPath, db, (await filterChangedFiles([jsonlPath], db))[0]!.start);
    expect(r1.messagesAdded).toBe(1);
    const boundary = fs.statSync(jsonlPath).size - Buffer.byteLength(half.slice(0, cut));
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(boundary);
    expect(getIngestLog(db, jsonlPath)?.file_size).toBe(boundary);

    fs.appendFileSync(jsonlPath, half.slice(cut) + '\n');
    const r2 = await ingestFile(jsonlPath, db, (await filterChangedFiles([jsonlPath], db))[0]!.start);
    expect(r2.messagesAdded).toBe(1);
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(fs.statSync(jsonlPath).size);
    expect(storedMessages(db, SESSION)).toBe(3);
  });

  it('computes exact byte offsets with multibyte characters and CRLF terminators', async () => {
    const lines = [
      makeUserMessage('m1', 'box drawing ─── and café', SESSION),
      makeAssistantMessage('m2', 'emoji 🚀🔥 in the reply', SESSION),
      makeUserMessage('m3', '日本語のテキスト', SESSION),
    ];
    fs.writeFileSync(jsonlPath, lines.map(l => JSON.stringify(l)).join('\r\n') + '\r\n');
    const r1 = await ingestFile(jsonlPath, db);
    expect(r1.messagesAdded).toBe(3);
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(fs.statSync(jsonlPath).size);

    fs.appendFileSync(jsonlPath, JSON.stringify(makeAssistantMessage('m4', 'après 🎉', SESSION)) + '\r\n');
    const r2 = await ingestFile(jsonlPath, db, (await filterChangedFiles([jsonlPath], db))[0]!.start);
    expect(r2.messagesAdded).toBe(1);
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(fs.statSync(jsonlPath).size);
    expect(storedMessages(db, SESSION)).toBe(4);
  });

  it('re-reads from 0 when the file shrinks, without duplicating rows or inflating the counter', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'third', SESSION));
    await ingestFile(jsonlPath, db);
    expect(messageCount(db, SESSION)).toBe(3);

    // Rewrite shorter: keep m1 and m2 only, then append a new m4
    fs.writeFileSync(jsonlPath, '');
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    const start = await resolveStart(db, jsonlPath, fs.statSync(jsonlPath).size);
    expect(start).toEqual({ offset: 0, lines: 0 });

    const r = await ingestFile(jsonlPath, db, start!);
    expect(r.messagesAdded).toBe(0);
    expect(storedMessages(db, SESSION)).toBe(3);
    expect(messageCount(db, SESSION)).toBe(3);
    expect(getIngestOffset(db, jsonlPath)).toMatchObject({ lines_consumed: 2, byte_offset: fs.statSync(jsonlPath).size });
  });

  it('bootstraps a legacy ingest_log row by backing up two complete lines and seeking', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    const twoLines = fs.statSync(jsonlPath).size;
    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'third', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m4', 'fourth', SESSION));
    const legacySize = fs.statSync(jsonlPath).size;
    // Simulate the old code: rows in messages + a legacy ingest_log entry, no ingest_offsets
    await ingestFile(jsonlPath, db);
    db.prepare(`DELETE FROM ingest_offsets`).run();
    upsertIngestLog(db, jsonlPath, 4, legacySize, new Date());

    writeJsonlLine(jsonlPath, makeUserMessage('m5', 'fifth', SESSION));
    const start = await resolveStart(db, jsonlPath, fs.statSync(jsonlPath).size);
    expect(start).toEqual({ offset: twoLines, lines: 2 });

    const r = await ingestFile(jsonlPath, db, start!);
    expect(r.messagesAdded).toBe(1); // m3/m4 re-read as duplicates, m5 new
    expect(getIngestOffset(db, jsonlPath)).toMatchObject({ lines_consumed: 5, byte_offset: fs.statSync(jsonlPath).size });
    expect(messageCount(db, SESSION)).toBe(5);
  });

  it('recovers a line the old server counted while it was half-written (partial-then-completed)', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    await ingestFile(jsonlPath, db);
    db.prepare(`DELETE FROM ingest_offsets`).run();
    // Old server: readline yielded the partial third line (parse failed, never inserted),
    // counted it, then stat() ran after the writer completed it.
    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'completed later', SESSION));
    upsertIngestLog(db, jsonlPath, 3, fs.statSync(jsonlPath).size, new Date());
    expect(storedMessages(db, SESSION)).toBe(2);

    const start = await resolveStart(db, jsonlPath, fs.statSync(jsonlPath).size);
    expect(start).not.toBeNull();
    const r = await ingestFile(jsonlPath, db, start!);
    expect(r.messagesAdded).toBe(1);
    expect(storedMessages(db, SESSION)).toBe(3);
  });

  it('recovers a final line the old server never read even though the size matches', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    await ingestFile(jsonlPath, db);
    db.prepare(`DELETE FROM ingest_offsets`).run();
    // Old server: read 2 lines, then a complete m3 landed before its stat(); session ended.
    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'last message of the session', SESSION));
    upsertIngestLog(db, jsonlPath, 2, fs.statSync(jsonlPath).size, new Date());

    const changed = await filterChangedFiles([jsonlPath], db);
    expect(changed).toHaveLength(1); // never short-circuits on equal size for a legacy row
    const r = await ingestFile(jsonlPath, db, changed[0]!.start);
    expect(r.messagesAdded).toBe(1);
    expect(storedMessages(db, SESSION)).toBe(3);
    // Bootstrapped: from now on ingest_offsets is the truth and equal size means nothing to do
    expect(await filterChangedFiles([jsonlPath], db)).toHaveLength(0);
  });

  it('re-reads from 0 when a legacy file_size sits mid-line (old server stat race)', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    await ingestFile(jsonlPath, db);
    db.prepare(`DELETE FROM ingest_offsets`).run();
    const midLine = fs.statSync(jsonlPath).size - 7; // inside m2's line
    upsertIngestLog(db, jsonlPath, 2, midLine, new Date());

    writeJsonlLine(jsonlPath, makeUserMessage('m3', 'third', SESSION));
    const start = await resolveStart(db, jsonlPath, fs.statSync(jsonlPath).size);
    expect(start).toEqual({ offset: 0, lines: 0 });

    const r = await ingestFile(jsonlPath, db, start!);
    expect(r.messagesAdded).toBe(1); // m1, m2 already present; only m3 is new
    expect(storedMessages(db, SESSION)).toBe(3);
    expect(messageCount(db, SESSION)).toBe(3);
  });

  it('backfills content blocks and the session row for a message inserted by an interrupted pass', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'orphan MARKER_ORPHAN', SESSION));
    // Simulate a crash after insertMessage but before session/blocks were written
    db.prepare(`INSERT INTO messages (uuid, session_id, type, role, timestamp) VALUES (?, ?, ?, ?, ?)`)
      .run('m1', SESSION, 'user', 'user', new Date().toISOString());

    const r = await ingestFile(jsonlPath, db);
    expect(r.messagesAdded).toBe(0);
    const blocks = db.prepare(`SELECT COUNT(*) AS n FROM content_blocks WHERE message_uuid = 'm1'`).get() as { n: number };
    expect(blocks.n).toBe(1);
    expect(messageCount(db, SESSION)).toBe(1);
    expect(db.prepare(`SELECT project FROM sessions WHERE session_id = ?`).get(SESSION)).toBeTruthy();
  });

  it('handles lines spanning several stream chunks, including a CRLF split at the chunk edge', async () => {
    const CHUNK = 64 * 1024; // fs.createReadStream default highWaterMark
    // Line 1 is padded so its '\r' is byte 65535 and its '\n' byte 65536.
    const probe = makeUserMessage('m1', '', SESSION);
    const overhead = Buffer.byteLength(JSON.stringify(probe));
    const padLen = CHUNK - 1 - overhead;
    const line1 = JSON.stringify(makeUserMessage('m1', 'x'.repeat(padLen), SESSION));
    expect(Buffer.byteLength(line1)).toBe(CHUNK - 1);
    // Line 2 is ~210 KB of multibyte text so it spans four chunks.
    const bigText = 'é─🚀'.repeat(30_000);
    const line2 = JSON.stringify(makeAssistantMessage('m2', bigText, SESSION));
    fs.writeFileSync(jsonlPath, line1 + '\r\n' + line2 + '\r\n');

    const r = await ingestFile(jsonlPath, db);
    expect(r.messagesAdded).toBe(2);
    expect(r.malformedLines).toBe(0);
    expect(getIngestOffset(db, jsonlPath)?.byte_offset).toBe(fs.statSync(jsonlPath).size);
    const stored = db.prepare(`SELECT text_content FROM content_blocks WHERE message_uuid = 'm2'`).get() as { text_content: string };
    expect(stored.text_content).toBe(bigText);
  });

  it('reports a malformed complete line instead of hiding it', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    fs.appendFileSync(jsonlPath, '{"type":"user","uuid":"broken"\n');
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    const r = await ingestFile(jsonlPath, db);
    expect(r.messagesAdded).toBe(2);
    expect(r.malformedLines).toBe(1);
    expect(getIngestOffset(db, jsonlPath)).toMatchObject({ lines_consumed: 3, byte_offset: fs.statSync(jsonlPath).size });
  });

  it('does not inflate message_count when already-indexed lines are re-ingested', async () => {
    writeJsonlLine(jsonlPath, makeUserMessage('m1', 'first', SESSION));
    writeJsonlLine(jsonlPath, makeAssistantMessage('m2', 'second', SESSION));
    await ingestFile(jsonlPath, db);
    await ingestFile(jsonlPath, db); // a second server doing the same work
    await ingestFile(jsonlPath, db);
    expect(messageCount(db, SESSION)).toBe(2);
  });

  it('repairs inflated message_count exactly once', () => {
    db.prepare(`INSERT INTO sessions (session_id, project, started_at, last_message_at, message_count) VALUES (?, ?, ?, ?, ?)`)
      .run(SESSION, 'p', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 26);
    db.prepare(`INSERT INTO messages (uuid, session_id, type, timestamp) VALUES (?, ?, ?, ?)`)
      .run('m1', SESSION, 'user', '2026-01-01T00:00:00Z');
    db.prepare(`INSERT INTO messages (uuid, session_id, type, timestamp) VALUES (?, ?, ?, ?)`)
      .run('m2', SESSION, 'assistant', '2026-01-01T00:00:01Z');

    expect(recomputeMessageCountsOnce(db)).toBe(true);
    expect(messageCount(db, SESSION)).toBe(2);

    db.prepare(`UPDATE sessions SET message_count = 99`).run();
    expect(recomputeMessageCountsOnce(db)).toBe(false);
    expect(messageCount(db, SESSION)).toBe(99);
  });
});
