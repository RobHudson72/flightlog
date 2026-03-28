import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { startWatcher, stopWatcher, getQueueMetrics } from './watcher.js';
import { getDb, closeDb, resetDatabase, searchContentBlocks } from './db.js';
import {
  makeTmpDir,
  writeJsonlLine,
  makeUserMessage,
  makeAssistantMessage,
  waitFor,
} from './test-helpers.js';

describe('watcher', () => {
  let tmpDir: string;
  let projectDir: string;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    projectDir = path.join(tmpDir, 'projects');
    fs.mkdirSync(projectDir, { recursive: true });
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
  });

  afterEach(async () => {
    await stopWatcher();
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  it('should ingest a new JSONL file when it appears', async () => {
    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'test-project');
    fs.mkdirSync(subDir);

    const sessionId = 'test-session-001';
    const jsonlPath = path.join(subDir, `${sessionId}.jsonl`);

    writeJsonlLine(jsonlPath, makeUserMessage('msg-1', 'hello world MARKER_ALPHA', sessionId));
    writeJsonlLine(jsonlPath, makeAssistantMessage('msg-2', 'response to MARKER_ALPHA', sessionId));

    await waitFor(() => searchContentBlocks(db, 'MARKER_ALPHA', {}).length >= 2);

    const results = searchContentBlocks(db, 'MARKER_ALPHA', {});
    expect(results.length).toBe(2);
    expect(results.some(r => r.snippet.includes('hello world MARKER_ALPHA'))).toBe(true);
    expect(results.some(r => r.snippet.includes('response to MARKER_ALPHA'))).toBe(true);
  });

  it('should incrementally ingest appended lines', async () => {
    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'test-project');
    fs.mkdirSync(subDir);

    const sessionId = 'test-session-002';
    const jsonlPath = path.join(subDir, `${sessionId}.jsonl`);

    writeJsonlLine(jsonlPath, makeUserMessage('msg-1', 'first message MARKER_BETA', sessionId));
    await waitFor(() => searchContentBlocks(db, 'MARKER_BETA', {}).length >= 1);

    writeJsonlLine(jsonlPath, makeAssistantMessage('msg-2', 'second message MARKER_GAMMA', sessionId));
    await waitFor(() => searchContentBlocks(db, 'MARKER_GAMMA', {}).length >= 1);

    expect(searchContentBlocks(db, 'MARKER_BETA', {}).length).toBe(1);
    expect(searchContentBlocks(db, 'MARKER_GAMMA', {}).length).toBe(1);
  });

  it('should ignore history.jsonl files', async () => {
    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'test-project');
    fs.mkdirSync(subDir);

    writeJsonlLine(path.join(subDir, 'history.jsonl'), makeUserMessage('msg-1', 'MARKER_HISTORY_IGNORE', 'history'));

    const sessionId = 'test-session-003';
    writeJsonlLine(path.join(subDir, `${sessionId}.jsonl`), makeUserMessage('msg-2', 'MARKER_NORMAL_FILE', sessionId));

    await waitFor(() => searchContentBlocks(db, 'MARKER_NORMAL_FILE', {}).length >= 1);
    await new Promise((r) => setTimeout(r, 300));

    expect(searchContentBlocks(db, 'MARKER_HISTORY_IGNORE', {}).length).toBe(0);
  });

  it('should report queue metrics correctly', async () => {
    await startWatcher(projectDir);

    const metrics = getQueueMetrics();
    expect(metrics.queue_depth).toBe(0);
    expect(metrics.oldest_queued_since).toBeNull();
    expect(metrics.queued_paths).toEqual([]);
    expect(metrics.watcher_active).toBe(true);
    expect(metrics.fallback_polling).toBe(false);
  });
});
