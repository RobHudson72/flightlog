import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { startWatcher, stopWatcher, getQueueMetrics } from './watcher.js';
import { getDb, closeDb, resetDatabase, searchContentBlocks } from './db.js';
import type Database from 'better-sqlite3';
import {
  makeTmpDir,
  writeJsonlLine,
  makeUserMessage,
  makeAssistantMessage,
  waitFor,
} from './test-helpers.js';

// ── Stress-test helpers ────────────────────────────────────────

function countContentBlocks(db: Database.Database): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM content_blocks').get() as { count: number };
  return row.count;
}

function countMessages(db: Database.Database): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM messages').get() as { count: number };
  return row.count;
}

/** Sample queue depth at a regular interval, return all samples. */
function sampleQueueDepth(intervalMs: number): { samples: { t: number; depth: number }[]; stop: () => void } {
  const samples: { t: number; depth: number }[] = [];
  const start = Date.now();
  const timer = setInterval(() => {
    const metrics = getQueueMetrics();
    samples.push({ t: Date.now() - start, depth: metrics.queue_depth });
  }, intervalMs);

  return {
    samples,
    stop: () => clearInterval(timer),
  };
}

// ── Tests ──────────────────────────────────────────────────────

describe('watcher stress tests', () => {
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

  it('rapid appends to a single file — simulates one fast agent', async () => {
    const MESSAGE_COUNT = 200;
    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'fast-agent');
    fs.mkdirSync(subDir);
    const sessionId = 'stress-single';
    const jsonlPath = path.join(subDir, `${sessionId}.jsonl`);

    const sampler = sampleQueueDepth(20);
    const writeStart = Date.now();

    // Blast messages as fast as possible
    for (let i = 0; i < MESSAGE_COUNT; i++) {
      const msg = i % 2 === 0
        ? makeUserMessage(`msg-${i}`, `stress single MARKER_S${i}`, sessionId)
        : makeAssistantMessage(`msg-${i}`, `response MARKER_S${i}`, sessionId);
      writeJsonlLine(jsonlPath, msg);
    }
    const writeDuration = Date.now() - writeStart;

    // Wait for all messages to be ingested
    await waitFor(() => countMessages(db) >= MESSAGE_COUNT, 30_000, 100);
    const totalDuration = Date.now() - writeStart;

    sampler.stop();

    const peakDepth = Math.max(...sampler.samples.map(s => s.depth));
    const msgsPerSec = Math.round((MESSAGE_COUNT / totalDuration) * 1000);

    // Report
    process.stderr.write('\n── Stress: single file rapid append ──\n');
    process.stderr.write(`  Messages:      ${MESSAGE_COUNT}\n`);
    process.stderr.write(`  Write time:    ${writeDuration}ms\n`);
    process.stderr.write(`  Total time:    ${totalDuration}ms (write + ingest)\n`);
    process.stderr.write(`  Throughput:    ${msgsPerSec} msgs/sec\n`);
    process.stderr.write(`  Peak queue:    ${peakDepth}\n`);
    process.stderr.write(`  Queue samples: ${sampler.samples.length}\n`);
    process.stderr.write(`  Final blocks:  ${countContentBlocks(db)}\n`);

    expect(countMessages(db)).toBe(MESSAGE_COUNT);
    // Queue should be fully drained
    expect(getQueueMetrics().queue_depth).toBe(0);
  }, 60_000);

  it('many concurrent files — simulates multiple agents writing simultaneously', async () => {
    const AGENT_COUNT = 20;
    const MESSAGES_PER_AGENT = 20;
    const TOTAL_MESSAGES = AGENT_COUNT * MESSAGES_PER_AGENT;

    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'multi-agent');
    fs.mkdirSync(subDir);

    const sampler = sampleQueueDepth(20);
    const writeStart = Date.now();

    // Create all agent files and write messages in round-robin
    const paths: string[] = [];
    for (let a = 0; a < AGENT_COUNT; a++) {
      const sessionId = `agent-${a.toString().padStart(3, '0')}`;
      paths.push(path.join(subDir, `${sessionId}.jsonl`));
    }

    // Round-robin: each round writes one message to each agent file
    for (let round = 0; round < MESSAGES_PER_AGENT; round++) {
      for (let a = 0; a < AGENT_COUNT; a++) {
        const sessionId = `agent-${a.toString().padStart(3, '0')}`;
        const msg = round % 2 === 0
          ? makeUserMessage(`msg-${a}-${round}`, `multi MARKER_M${a}_${round}`, sessionId)
          : makeAssistantMessage(`msg-${a}-${round}`, `reply MARKER_M${a}_${round}`, sessionId);
        writeJsonlLine(paths[a], msg);
      }
    }
    const writeDuration = Date.now() - writeStart;

    // Wait for all messages to be ingested
    await waitFor(() => countMessages(db) >= TOTAL_MESSAGES, 30_000, 100);
    const totalDuration = Date.now() - writeStart;

    sampler.stop();

    const peakDepth = Math.max(...sampler.samples.map(s => s.depth));
    const msgsPerSec = Math.round((TOTAL_MESSAGES / totalDuration) * 1000);

    process.stderr.write('\n── Stress: multi-agent concurrent writes ──\n');
    process.stderr.write(`  Agents:        ${AGENT_COUNT}\n`);
    process.stderr.write(`  Msgs/agent:    ${MESSAGES_PER_AGENT}\n`);
    process.stderr.write(`  Total msgs:    ${TOTAL_MESSAGES}\n`);
    process.stderr.write(`  Write time:    ${writeDuration}ms\n`);
    process.stderr.write(`  Total time:    ${totalDuration}ms\n`);
    process.stderr.write(`  Throughput:    ${msgsPerSec} msgs/sec\n`);
    process.stderr.write(`  Peak queue:    ${peakDepth}\n`);
    process.stderr.write(`  Queue samples: ${sampler.samples.length}\n`);

    expect(countMessages(db)).toBe(TOTAL_MESSAGES);
    expect(getQueueMetrics().queue_depth).toBe(0);
  }, 60_000);

  it('sustained trickle — simulates IPC message exchange over time', async () => {
    const EXCHANGE_COUNT = 30;
    const DELAY_BETWEEN_MS = 50; // one message pair every 50ms

    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const subDir = path.join(projectDir, 'ipc-sim');
    fs.mkdirSync(subDir);
    const sessionId = 'ipc-session';
    const jsonlPath = path.join(subDir, `${sessionId}.jsonl`);

    const sampler = sampleQueueDepth(10);
    const latencies: number[] = [];
    const writeStart = Date.now();

    for (let i = 0; i < EXCHANGE_COUNT; i++) {
      const marker = `IPC_MSG_${i.toString().padStart(4, '0')}`;
      const sentAt = Date.now();
      writeJsonlLine(jsonlPath, makeUserMessage(`ipc-${i}`, marker, sessionId));

      // Wait for this specific message to become searchable
      await waitFor(() => searchContentBlocks(db, marker, {}).length >= 1, 5000, 10);
      latencies.push(Date.now() - sentAt);

      if (DELAY_BETWEEN_MS > 0) {
        await new Promise(r => setTimeout(r, DELAY_BETWEEN_MS));
      }
    }

    const totalDuration = Date.now() - writeStart;
    sampler.stop();

    latencies.sort((a, b) => a - b);
    const p50 = latencies[Math.floor(latencies.length * 0.5)];
    const p90 = latencies[Math.floor(latencies.length * 0.9)];
    const p99 = latencies[Math.floor(latencies.length * 0.99)];
    const avg = Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length);
    const max = latencies[latencies.length - 1];

    process.stderr.write('\n── Stress: IPC trickle latency ──\n');
    process.stderr.write(`  Exchanges:     ${EXCHANGE_COUNT}\n`);
    process.stderr.write(`  Total time:    ${totalDuration}ms\n`);
    process.stderr.write(`  Latency avg:   ${avg}ms\n`);
    process.stderr.write(`  Latency p50:   ${p50}ms\n`);
    process.stderr.write(`  Latency p90:   ${p90}ms\n`);
    process.stderr.write(`  Latency p99:   ${p99}ms\n`);
    process.stderr.write(`  Latency max:   ${max}ms\n`);
    process.stderr.write(`  Peak queue:    ${Math.max(...sampler.samples.map(s => s.depth))}\n`);

    expect(countMessages(db)).toBe(EXCHANGE_COUNT);
    expect(getQueueMetrics().queue_depth).toBe(0);

    // IPC latency SLA: p90 should be under 500ms
    expect(p90).toBeLessThan(500);
  }, 60_000);

  it('queue depth is observable during burst writes', async () => {
    const db = getDb();
    resetDatabase(db);
    await startWatcher(projectDir);

    const BURST_FILES = 50;
    const subDir = path.join(projectDir, 'burst');
    fs.mkdirSync(subDir);

    // Write many files as fast as possible — queue should build up
    for (let i = 0; i < BURST_FILES; i++) {
      const sessionId = `burst-${i.toString().padStart(3, '0')}`;
      const jsonlPath = path.join(subDir, `${sessionId}.jsonl`);
      writeJsonlLine(jsonlPath, makeUserMessage(`msg-${i}`, `burst content ${i}`, sessionId));
    }

    // Sample queue depth immediately — should be non-zero during burst
    // (the debounce is 30ms so we wait just past it)
    await new Promise(r => setTimeout(r, 60));
    const midBurstMetrics = getQueueMetrics();

    process.stderr.write('\n── Stress: queue depth observability ──\n');
    process.stderr.write(`  Burst files:      ${BURST_FILES}\n`);
    process.stderr.write(`  Queue at 150ms:   ${midBurstMetrics.queue_depth}\n`);
    process.stderr.write(`  Queued paths:     ${midBurstMetrics.queued_paths.length}\n`);
    process.stderr.write(`  Oldest pending:   ${midBurstMetrics.oldest_queued_since}\n`);

    // Queue should have items in it (the drain can't process all 50 files in 150ms)
    expect(midBurstMetrics.queue_depth).toBeGreaterThan(0);
    expect(midBurstMetrics.oldest_queued_since).not.toBeNull();
    expect(midBurstMetrics.queued_paths.length).toBeGreaterThan(0);

    // Wait for full drain
    await waitFor(() => countMessages(db) >= BURST_FILES, 30_000, 100);

    const finalMetrics = getQueueMetrics();
    expect(finalMetrics.queue_depth).toBe(0);
    expect(finalMetrics.oldest_queued_since).toBeNull();

    process.stderr.write(`  Final queue:      ${finalMetrics.queue_depth}\n`);
    process.stderr.write(`  Total ingested:   ${countMessages(db)}\n`);
  }, 60_000);
});
