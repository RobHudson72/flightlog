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

/**
 * CAD-T-618: the watcher also watches `<codexHome>/sessions`, where Codex
 * creates a YYYY/MM/DD directory per day; a rollout appearing at that depth
 * after the watch starts is ingested live.
 */
describe('watcher on Codex rollouts', () => {
  let tmpDir: string;
  let projectDir: string;
  let codexHome: string;

  beforeEach(() => {
    tmpDir = makeTmpDir();
    projectDir = path.join(tmpDir, 'projects');
    codexHome = path.join(tmpDir, 'codex-home');
    fs.mkdirSync(projectDir, { recursive: true });
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmpDir, 'test.db');
  });

  afterEach(async () => {
    await stopWatcher();
    closeDb();
    delete process.env['FLIGHTLOG_DB_PATH'];
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* Windows file locks */ }
  });

  function codexMessage(role: string, text: string): Record<string, unknown> {
    return {
      timestamp: new Date().toISOString(),
      type: 'response_item',
      payload: { type: 'message', role, content: [{ type: role === 'user' ? 'input_text' : 'output_text', text }] },
    };
  }

  it('ingests a rollout that appears in a date directory created after the watch started, then its appended lines', async () => {
    const db = getDb();
    resetDatabase(db);
    fs.mkdirSync(path.join(codexHome, 'sessions'), { recursive: true });
    expect(await startWatcher(projectDir, codexHome)).toBe(true);

    const dayDir = path.join(codexHome, 'sessions', '2026', '10', '07');
    fs.mkdirSync(dayDir, { recursive: true });
    const basename = 'rollout-2026-10-07T09-00-00-0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';
    const rollout = path.join(dayDir, `${basename}.jsonl`);
    writeJsonlLine(rollout, {
      timestamp: new Date().toISOString(),
      type: 'session_meta',
      payload: { id: '0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b', cwd: '/w/codex-ipc-2', cli_version: '0.156.1' },
    });
    writeJsonlLine(rollout, codexMessage('user', '[e:live1] MARKER_CODEX_LIVE'));

    await waitFor(() => searchContentBlocks(db, 'MARKER_CODEX_LIVE', {}).length >= 1);
    const hit = searchContentBlocks(db, 'MARKER_CODEX_LIVE', {})[0]!;
    expect(hit).toMatchObject({ session_id: basename, project: 'codex:codex-ipc-2', role: 'user', block_type: 'user_text' });

    writeJsonlLine(rollout, codexMessage('assistant', 'MARKER_CODEX_REPLY'));
    await waitFor(() => searchContentBlocks(db, 'MARKER_CODEX_REPLY', {}).length >= 1);
    expect(searchContentBlocks(db, 'MARKER_CODEX_LIVE', {}).length).toBe(1);
  });

  it('starts, and still watches Claude transcripts, when the Codex home is missing', async () => {
    const db = getDb();
    resetDatabase(db);
    expect(await startWatcher(projectDir, path.join(tmpDir, 'no-codex-here'))).toBe(true);
    expect(getQueueMetrics().watcher_active).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, 'no-codex-here'))).toBe(false);

    const subDir = path.join(projectDir, 'test-project');
    fs.mkdirSync(subDir);
    writeJsonlLine(path.join(subDir, 'claude-1.jsonl'), makeUserMessage('m1', 'MARKER_CLAUDE_STILL', 'claude-1'));
    await waitFor(() => searchContentBlocks(db, 'MARKER_CLAUDE_STILL', {}).length >= 1);
  });
});

describe('watcher fallback recovery', () => {
  it('clears fallback_polling when a later start succeeds', async () => {
    const tmp = fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'flightlog-retry-'));
    process.env['FLIGHTLOG_DB_PATH'] = path.join(tmp, 'test.db');
    try {
      // A path with an illegal character cannot be created -> failed start sets the fallback flag
      const bad = path.join(tmp, 'bad<dir>');
      expect(await startWatcher(bad)).toBe(false);
      expect(getQueueMetrics().fallback_polling).toBe(true);

      const good = path.join(tmp, 'projects');
      fs.mkdirSync(good, { recursive: true });
      expect(await startWatcher(good)).toBe(true);
      expect(getQueueMetrics().fallback_polling).toBe(false);
      expect(getQueueMetrics().watcher_active).toBe(true);
    } finally {
      await stopWatcher();
      closeDb();
      delete process.env['FLIGHTLOG_DB_PATH'];
      try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows file locks */ }
    }
  });
});
