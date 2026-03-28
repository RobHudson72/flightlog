import chokidar, { type FSWatcher } from 'chokidar';
import path from 'node:path';
import os from 'node:os';
import fs from 'node:fs';
import { getDb } from './db.js';
import { ingestFile, filterChangedFiles, isIngestRunning, setQueueMetricsProvider } from './ingest.js';

// ── Configuration ──────────────────���───────────────────────────

const DEBOUNCE_MS = 30;
const DRAIN_RETRY_MS = 200;
const DRAIN_MAX_RETRIES = 50; // 50 × 200ms = 10s max wait for ingest lock

function getClaudeProjectsDir(): string {
  return path.join(os.homedir(), '.claude', 'projects');
}

// ── Queue state ────────────────────────────────────────────────

const pendingSet = new Set<string>();
const pendingQueue: string[] = [];
const queuedAt = new Map<string, number>();
const debounceMap = new Map<string, ReturnType<typeof setTimeout>>();
let draining = false;
let drainPromise: Promise<void> | null = null;
let watcher: FSWatcher | null = null;
let watcherActive = false;
let fallbackPolling = false;

// ── Queue metrics ──────────────────────────────────────────────

export interface QueueMetrics {
  queue_depth: number;
  oldest_queued_since: string | null;
  queued_paths: string[];
  watcher_active: boolean;
  fallback_polling: boolean;
}

export function getQueueMetrics(): QueueMetrics {
  let oldest: number | null = null;
  for (const ts of queuedAt.values()) {
    if (oldest === null || ts < oldest) oldest = ts;
  }

  return {
    queue_depth: pendingQueue.length,
    oldest_queued_since: oldest ? new Date(oldest).toISOString() : null,
    queued_paths: pendingQueue.map(p => path.basename(p, '.jsonl')),
    watcher_active: watcherActive,
    fallback_polling: fallbackPolling,
  };
}

// ─��� Enqueue / debounce ───────────���─────────────────────────────

function enqueueFile(filePath: string): void {
  const normalized = path.resolve(filePath);

  // Clear any existing debounce timer for this file
  const existing = debounceMap.get(normalized);
  if (existing) clearTimeout(existing);

  // Set a new debounce timer
  const timer = setTimeout(() => {
    debounceMap.delete(normalized);

    // Only enqueue if not already in the queue (O(1) check via Set)
    if (!pendingSet.has(normalized)) {
      pendingSet.add(normalized);
      pendingQueue.push(normalized);
      queuedAt.set(normalized, Date.now());
    }

    // Kick the drain loop
    if (!draining) {
      drainPromise = drain().catch(e => {
        const msg = e instanceof Error ? e.message : String(e);
        process.stderr.write(`flightlog: watcher drain error: ${msg}\n`);
      });
    }
  }, DEBOUNCE_MS);

  debounceMap.set(normalized, timer);
}

// ── Drain loop ──────────────────���──────────────────────────────

async function drain(): Promise<void> {
  if (draining) return;
  draining = true;

  try {
    // Wait for any full-scan ingest to finish before draining
    let retries = 0;
    while (isIngestRunning()) {
      retries++;
      if (retries > DRAIN_MAX_RETRIES) {
        process.stderr.write(
          `flightlog: watcher gave up waiting for ingest lock after ${DRAIN_MAX_RETRIES} retries, ${pendingQueue.length} file(s) still queued\n`,
        );
        return;
      }
      await new Promise(r => setTimeout(r, DRAIN_RETRY_MS));
    }

    while (pendingQueue.length > 0) {
      // Yield to ingest lock if a full scan started mid-drain
      if (isIngestRunning()) {
        let retries2 = 0;
        while (isIngestRunning()) {
          retries2++;
          if (retries2 > DRAIN_MAX_RETRIES) {
            process.stderr.write(
              `flightlog: watcher gave up waiting for ingest lock mid-drain, ${pendingQueue.length} file(s) still queued\n`,
            );
            return;
          }
          await new Promise(r => setTimeout(r, DRAIN_RETRY_MS));
        }
      }

      const filePath = pendingQueue.shift()!;
      pendingSet.delete(filePath);
      queuedAt.delete(filePath);

      try {
        if (!fs.existsSync(filePath)) {
          process.stderr.write(`flightlog: watcher skipping deleted file: ${path.basename(filePath)}\n`);
          continue;
        }

        const db = getDb();

        // Determine how many lines to skip (incremental)
        const changed = filterChangedFiles([filePath], db);
        if (changed.length === 0) {
          // File exists but has no new content — this is normal for duplicate events
          continue;
        }

        const { skipLines } = changed[0];
        const result = await ingestFile(filePath, db, skipLines);

        if (result.messagesAdded > 0) {
          process.stderr.write(
            `flightlog: watcher ingested ${result.messagesAdded} messages from ${path.basename(filePath)}\n`,
          );
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        process.stderr.write(`flightlog: watcher error on ${path.basename(filePath)}: ${msg}\n`);
      }
    }
  } finally {
    draining = false;
    drainPromise = null;
  }
}

// ── Watcher lifecycle ���─────────────────────────────────────────

export async function startWatcher(watchPath?: string): Promise<boolean> {
  const projectsDir = watchPath ?? getClaudeProjectsDir();

  // Register queue metrics provider with ingest module
  setQueueMetricsProvider(getQueueMetrics);

  if (!fs.existsSync(projectsDir)) {
    // Create the directory so we can watch it
    try {
      fs.mkdirSync(projectsDir, { recursive: true });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      process.stderr.write(`flightlog: cannot create ${projectsDir}: ${msg}, falling back to polling\n`);
      fallbackPolling = true;
      return false;
    }
  }

  try {
    // Watch the directory, not a glob — chokidar globs miss dynamically-created
    // subdirectories on some platforms (notably Windows).
    watcher = chokidar.watch(projectsDir, {
      ignoreInitial: true,
      persistent: true,
      awaitWriteFinish: false,
    });

    const isRelevant = (filePath: string): boolean => {
      const base = path.basename(filePath);
      return base.endsWith('.jsonl') && base !== 'history.jsonl';
    };

    watcher.on('add', (filePath: string) => {
      if (isRelevant(filePath)) enqueueFile(filePath);
    });

    watcher.on('change', (filePath: string) => {
      if (isRelevant(filePath)) enqueueFile(filePath);
    });

    watcher.on('error', (error: unknown) => {
      const msg = error instanceof Error ? error.message : String(error);
      process.stderr.write(`flightlog: watcher error: ${msg}\n`);
    });

    // Wait for the watcher to be ready, with a timeout
    await new Promise<void>((resolve, reject) => {
      const timeoutId = setTimeout(() => reject(new Error('Watcher initialization timed out')), 10_000);
      watcher!.on('ready', () => {
        clearTimeout(timeoutId);
        resolve();
      });
    });

    watcherActive = true;
    process.stderr.write(`flightlog: watching ${projectsDir}\n`);
    return true;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    process.stderr.write(`flightlog: watcher failed to start: ${msg}\n`);
    fallbackPolling = true;

    // Clean up partial watcher
    if (watcher) {
      await watcher.close().catch((closeErr: unknown) => {
        const closeMsg = closeErr instanceof Error ? closeErr.message : String(closeErr);
        process.stderr.write(`flightlog: error closing failed watcher: ${closeMsg}\n`);
      });
      watcher = null;
    }

    return false;
  }
}

export async function stopWatcher(): Promise<void> {
  // Clear all debounce timers
  for (const timer of debounceMap.values()) {
    clearTimeout(timer);
  }
  debounceMap.clear();

  // Wait for any in-flight drain to complete
  if (drainPromise) {
    await drainPromise.catch(() => {});
  }

  // Clear queue state
  pendingQueue.length = 0;
  pendingSet.clear();
  queuedAt.clear();

  if (watcher) {
    await watcher.close();
    watcher = null;
  }
  watcherActive = false;
  fallbackPolling = false;
}
