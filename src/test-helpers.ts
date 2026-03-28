import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

export function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'flightlog-test-'));
}

export function writeJsonlLine(filePath: string, line: Record<string, unknown>): void {
  fs.appendFileSync(filePath, JSON.stringify(line) + '\n');
}

export function makeUserMessage(uuid: string, text: string, sessionId: string): Record<string, unknown> {
  return {
    type: 'user',
    uuid,
    parentUuid: null,
    isSidechain: false,
    message: { role: 'user', content: text },
    timestamp: new Date().toISOString(),
    sessionId,
    cwd: '/tmp/test-project',
  };
}

export function makeAssistantMessage(uuid: string, text: string, sessionId: string): Record<string, unknown> {
  return {
    type: 'assistant',
    uuid,
    parentUuid: null,
    isSidechain: false,
    message: {
      type: 'message',
      role: 'assistant',
      content: [{ type: 'text', text }],
      stop_reason: 'end_turn',
    },
    timestamp: new Date().toISOString(),
    sessionId,
    cwd: '/tmp/test-project',
  };
}

/** Wait until a condition is true, polling every `intervalMs`. */
export async function waitFor(
  condition: () => boolean,
  timeoutMs = 5000,
  intervalMs = 50,
): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
