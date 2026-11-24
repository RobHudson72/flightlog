import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runMeter } from './meter.js';

// Expected values below are summed by hand from the constants the fixture
// files under src/__fixtures__/meter/ were written with. They are never
// captured from a run of the meter.

const FIXTURES = path.join(path.dirname(fileURLToPath(import.meta.url)), '__fixtures__', 'meter');

interface U {
  i: number;
  o: number;
  cr: number;
  cc: number;
}
const u = (i: number, o: number, cr: number, cc: number): U => ({ i, o, cr, cc });
const sum = (...us: U[]): U => ({
  i: us.reduce((n, x) => n + x.i, 0),
  o: us.reduce((n, x) => n + x.o, 0),
  cr: us.reduce((n, x) => n + x.cr, 0),
  cc: us.reduce((n, x) => n + x.cc, 0),
});

// --- fixture constants (the LAST line of each response is what counts) ---
// proj-a/s-main.jsonl
const A1 = u(10, 5, 100, 20); // one line
const A2 = u(20, 40, 200, 30); // two lines, output 1 then 40
const A3 = u(30, 60, 300, 40); // five lines, output 2, 5, 9, 9, 60
const S_MAIN = sum(A1, A2, A3);
// proj-a/s-main/subagents/agent-sub1.jsonl
const B1 = u(7, 3, 70, 10); // two lines, output 1 then 3
const B2 = u(2, 2, 2, 2);
const SUB1 = sum(B1, B2);
// proj-a/s-main/subagents/agent-resumed.jsonl
const R1 = u(1, 1, 1, 1);
const R2 = u(2, 2, 2, 2);
const R3 = u(3, 4, 3, 3); // two lines, output 3 then 4
const RESUMED = sum(R1, R2, R3);
// share-x.jsonl, share-y.jsonl, share-x/subagents/agent-z.jsonl
const P1 = u(4, 6, 40, 8); // in x, y and z, equal usage
const P2 = u(5, 7, 50, 9); // in x and y, equal usage
const QX = u(1, 2, 3, 4); // in x
const QY = u(1, 9, 3, 4); // in y, two lines (output 5 then 9): differs from QX
const X1 = u(2, 2, 2, 2);
const Y1 = u(3, 3, 3, 3);
const Z1 = u(6, 6, 6, 6);
const SHARE_X = sum(P1, P2, QX, X1);
const SHARE_Y = sum(P1, P2, QY, Y1);
const SHARE_Z = sum(P1, Z1);

const excl = (x: U): number => x.i + x.o + x.cc;
const incl = (x: U): number => x.i + x.o + x.cr + x.cc;

interface Entry {
  kind: string;
  id: string;
  path: string;
  models: string[];
  input_tokens: number;
  output_tokens: number;
  cache_read_input_tokens: number;
  cache_creation_input_tokens: number;
  tokens_excluding_cache_reads: number;
  tokens_including_cache_reads: number;
  responses: number;
  synthetic_lines: number;
  lines_other: number;
  first_timestamp: string | null;
  last_timestamp: string | null;
  wall_seconds: number | null;
}
interface Doc {
  meter_version: number;
  ids: Entry[];
  total: Record<string, number>;
  unmeasured: { kind: string; id: string; reason: string }[];
  refusals: { file: string; line: number; reason: string }[];
  complete: boolean;
  dollars: string;
}

let projects: string;

beforeAll(() => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flightlog-meter-test-'));
  projects = path.join(tmp, 'projects');
  fs.cpSync(FIXTURES, projects, { recursive: true });
});
afterAll(() => {
  fs.rmSync(path.dirname(projects), { recursive: true, force: true });
});

function run(...args: string[]): { exitCode: number; doc: Doc; stdout: string; stderr: string } {
  const r = runMeter(['--projects', projects, ...args]);
  return { exitCode: r.exitCode, doc: JSON.parse(r.stdout) as Doc, stdout: r.stdout, stderr: r.stderr };
}

function expectCounts(e: Entry, x: U, responses: number): void {
  expect(e.input_tokens).toBe(x.i);
  expect(e.output_tokens).toBe(x.o);
  expect(e.cache_read_input_tokens).toBe(x.cr);
  expect(e.cache_creation_input_tokens).toBe(x.cc);
  expect(e.tokens_excluding_cache_reads).toBe(excl(x));
  expect(e.tokens_including_cache_reads).toBe(incl(x));
  expect(e.responses).toBe(responses);
}

describe('flightlog-meter', () => {
  describe('one main session, responses over 1, 2 and 5 lines', () => {
    it('counts each response once at its last line, in both definitions', () => {
      const { exitCode, doc } = run('--session', 's-main');
      expect(exitCode).toBe(0);
      expect(doc.ids.length, 'ids measured').toBe(1);
      const e = doc.ids[0];
      expect(e.kind).toBe('session');
      expect(e.id).toBe('s-main');
      expect(e.path).toBe(path.join(projects, 'proj-a', 's-main.jsonl'));
      expectCounts(e, S_MAIN, 3);
      // hand sums: 60 + 105 + 90 = 255, and + 600 cache reads = 855
      expect(e.tokens_excluding_cache_reads).toBe(255);
      expect(e.tokens_including_cache_reads).toBe(855);
      expect(e.models).toEqual(['claude-x']);
      expect(doc.complete).toBe(true);
      expect(doc.dollars).toBe('not measured');
      expect(doc.meter_version).toBe(1);
      expect(doc.unmeasured).toEqual([]);
      expect(doc.refusals).toEqual([]);
    });

    it('counts the synthetic line apart, the other-type lines apart, and reads time from timestamped lines only', () => {
      const e = run('--session', 's-main').doc.ids[0];
      expect(e.synthetic_lines).toBe(1); // the one <synthetic> line
      expect(e.lines_other).toBe(3); // two user lines and the summary line
      // earliest is the synthetic line (09:59:59), latest the last user line
      expect(e.first_timestamp).toBe('2026-11-01T09:59:59.000Z');
      expect(e.last_timestamp).toBe('2026-11-01T10:01:00.000Z');
      expect(e.wall_seconds).toBe(61);
    });

    it('prints no message content', () => {
      const { stdout } = run('--session', 's-main');
      expect(stdout.length).toBeGreaterThan(0);
      expect(stdout.includes('SECRET-CONTENT-MARKER')).toBe(false);
    });

    it('carries no timestamps or wall_seconds in total', () => {
      const t = run('--session', 's-main').doc.total;
      expect(Object.keys(t).sort()).toEqual(
        [
          'cache_creation_input_tokens',
          'cache_read_input_tokens',
          'input_tokens',
          'output_tokens',
          'responses',
          'responses_shared_across_ids',
          'tokens_excluding_cache_reads',
          'tokens_including_cache_reads',
        ].sort(),
      );
    });
  });

  describe('a subagent transcript', () => {
    it('is found under <session>/subagents/ and measured', () => {
      const { exitCode, doc } = run('--agent', 'sub1');
      expect(exitCode).toBe(0);
      expect(doc.ids.length).toBe(1);
      const e = doc.ids[0];
      expect(e.kind).toBe('agent');
      expect(e.path).toBe(path.join(projects, 'proj-a', 's-main', 'subagents', 'agent-sub1.jsonl'));
      expectCounts(e, SUB1, 2);
      expect(e.models).toEqual(['claude-y']);
      expect(e.first_timestamp).toBe('2026-11-01T11:00:00.000Z');
      expect(e.last_timestamp).toBe('2026-11-01T11:00:20.000Z');
      expect(e.wall_seconds).toBe(20);
    });

    it('measures a session and a subagent together, in command-line order, with nothing shared', () => {
      const { exitCode, doc } = run('--agent', 'sub1', '--session', 's-main');
      expect(exitCode).toBe(0);
      expect(doc.ids.map((e) => e.id)).toEqual(['sub1', 's-main']);
      const all = sum(S_MAIN, SUB1);
      expect(doc.total.input_tokens).toBe(all.i);
      expect(doc.total.output_tokens).toBe(all.o);
      expect(doc.total.cache_read_input_tokens).toBe(all.cr);
      expect(doc.total.cache_creation_input_tokens).toBe(all.cc);
      expect(doc.total.tokens_excluding_cache_reads).toBe(excl(all));
      expect(doc.total.tokens_including_cache_reads).toBe(incl(all));
      expect(doc.total.responses).toBe(5);
      expect(doc.total.responses_shared_across_ids).toBe(0);
    });
  });

  describe('a resumed subagent whose file holds two runs', () => {
    it('spans both runs in wall_seconds, idle time included', () => {
      const e = run('--agent', 'resumed').doc.ids[0];
      expectCounts(e, RESUMED, 3);
      expect(e.first_timestamp).toBe('2026-11-01T12:00:00.000Z');
      expect(e.last_timestamp).toBe('2026-11-01T12:30:05.000Z');
      expect(e.wall_seconds).toBe(30 * 60 + 5);
    });
  });

  describe('a response in more than one id file', () => {
    it('counts once in total when the usage is equal, and each id still counts it', () => {
      const { exitCode, doc } = run('--session', 'share-x', '--agent', 'z');
      expect(exitCode).toBe(0);
      expect(doc.ids.length).toBe(2);
      expectCounts(doc.ids[0], SHARE_X, 4);
      expectCounts(doc.ids[1], SHARE_Z, 2);
      const t = sum(P1, P2, QX, X1, Z1); // P1 is in both files, counted once
      expect(doc.total.input_tokens).toBe(t.i);
      expect(doc.total.output_tokens).toBe(t.o);
      expect(doc.total.cache_read_input_tokens).toBe(t.cr);
      expect(doc.total.cache_creation_input_tokens).toBe(t.cc);
      expect(doc.total.tokens_excluding_cache_reads).toBe(excl(t));
      expect(doc.total.tokens_including_cache_reads).toBe(incl(t));
      expect(doc.total.responses).toBe(5);
      expect(doc.total.responses_shared_across_ids).toBe(1);
      expect(doc.refusals).toEqual([]);
    });

    it('refuses a pair whose usage differs, counts the earlier id, and handles a pair shared by three ids', () => {
      const { exitCode, doc } = run('--session', 'share-x', '--session', 'share-y', '--agent', 'z');
      expect(exitCode).toBe(4);
      expect(doc.complete).toBe(false);
      expect(doc.ids.map((e) => e.id)).toEqual(['share-x', 'share-y', 'z']);
      // share-y's refused Q is left out of its own figures; share-x keeps Q
      expectCounts(doc.ids[0], SHARE_X, 4);
      expectCounts(doc.ids[1], sum(P1, P2, Y1), 3);
      expectCounts(doc.ids[2], SHARE_Z, 2);
      // total: P1 (three files), P2 (two), Q at the earlier id's usage, X1, Y1, Z1
      const t = sum(P1, P2, QX, X1, Y1, Z1);
      expect(doc.total.input_tokens).toBe(t.i);
      expect(doc.total.output_tokens).toBe(t.o);
      expect(doc.total.cache_read_input_tokens).toBe(t.cr);
      expect(doc.total.cache_creation_input_tokens).toBe(t.cc);
      expect(doc.total.tokens_excluding_cache_reads).toBe(excl(t));
      expect(doc.total.tokens_including_cache_reads).toBe(incl(t));
      expect(doc.total.responses).toBe(6);
      expect(doc.total.responses_shared_across_ids).toBe(3); // P1, P2 and Q
      // the refusal names the pair's LAST line (line 4) in the later id's file
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'share-y.jsonl'), line: 4, reason: 'usage-mismatch' },
      ]);
    });
  });

  describe('refused lines', () => {
    it('refuses a pair whose later line changes input_tokens, once per pair, and does not count it', () => {
      const { exitCode, doc } = run('--session', 'bad-input');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'bad-input.jsonl'), line: 2, reason: 'usage-mismatch' },
      ]);
      expect(doc.ids.length).toBe(1); // the refused id stays in ids
      expectCounts(doc.ids[0], u(1, 1, 1, 1), 1); // only msg_m2 counted
      // time still reads every timestamped line, the refused ones included
      expect(doc.ids[0].first_timestamp).toBe('2026-11-01T14:00:00.000Z');
      expect(doc.ids[0].wall_seconds).toBe(3);
      expect(doc.total.responses).toBe(1);
      expect(doc.complete).toBe(false);
    });

    it('refuses a pair whose later line lowers output_tokens', () => {
      const { exitCode, doc } = run('--session', 'bad-output');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'bad-output.jsonl'), line: 2, reason: 'usage-mismatch' },
      ]);
      expectCounts(doc.ids[0], u(1, 1, 1, 1), 1);
    });

    it('refuses a line of another type that carries message.usage', () => {
      const { exitCode, doc } = run('--session', 'other-usage');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'other-usage.jsonl'), line: 2, reason: 'usage-on-non-assistant' },
      ]);
      expectCounts(doc.ids[0], sum(u(1, 1, 1, 1), u(2, 2, 2, 2)), 2);
      expect(doc.ids[0].lines_other).toBe(0); // the refused line is not "other"
    });

    it('refuses a usage line missing a count and a non-synthetic usage line with no requestId', () => {
      const { exitCode, doc } = run('--session', 'schema');
      expect(exitCode).toBe(4);
      const file = path.join(projects, 'proj-a', 'schema.jsonl');
      expect(doc.refusals).toEqual([
        { file, line: 2, reason: 'schema' },
        { file, line: 3, reason: 'schema' },
      ]);
      expectCounts(doc.ids[0], sum(u(1, 1, 1, 1), u(2, 2, 2, 2)), 2);
    });

    it('refuses an invalid JSON line with its file and line number', () => {
      const { exitCode, doc } = run('--session', 'bad-json');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'bad-json.jsonl'), line: 2, reason: 'invalid-json' },
      ]);
      expectCounts(doc.ids[0], sum(u(1, 1, 1, 1), u(2, 2, 2, 2)), 2);
    });

    it('counts a zero <synthetic> line apart and refuses a non-zero one', () => {
      const { exitCode, doc } = run('--session', 'synthetic');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'synthetic.jsonl'), line: 2, reason: 'synthetic-nonzero' },
      ]);
      const e = doc.ids[0];
      expect(e.synthetic_lines).toBe(1); // line 1 only
      expectCounts(e, u(2, 2, 2, 2), 1);
      expect(e.models).toEqual(['claude-x']); // <synthetic> is never a model
    });

    it('refuses a timestamp that is not an ISO-8601 instant, on any line', () => {
      const { exitCode, doc } = run('--session', 'bad-time');
      expect(exitCode).toBe(4);
      const file = path.join(projects, 'proj-a', 'bad-time.jsonl');
      expect(doc.refusals).toEqual([
        { file, line: 2, reason: 'bad-timestamp' },
        { file, line: 3, reason: 'bad-timestamp' },
        { file, line: 4, reason: 'bad-timestamp' },
      ]);
      const e = doc.ids[0];
      expectCounts(e, sum(u(1, 1, 1, 1), u(2, 2, 2, 2)), 2);
      expect(e.first_timestamp).toBe('2026-11-01T15:00:00.000Z');
      expect(e.last_timestamp).toBe('2026-11-01T15:00:04.000Z');
      expect(e.wall_seconds).toBe(4);
    });
  });

  describe('time is read from every line whose envelope parsed', () => {
    it('reads a refused line, but not a line whose own timestamp is bad', () => {
      const { exitCode, doc } = run('--session', 'late-refused');
      expect(exitCode).toBe(4);
      const file = path.join(projects, 'proj-a', 'late-refused.jsonl');
      expect(doc.refusals).toEqual([
        { file, line: 2, reason: 'schema' },
        { file, line: 3, reason: 'usage-on-non-assistant' },
        { file, line: 4, reason: 'synthetic-nonzero' },
        { file, line: 5, reason: 'bad-timestamp' },
      ]);
      const e = doc.ids[0];
      expectCounts(e, u(1, 1, 1, 1), 1);
      // the latest line is the refused synthetic one at 10:07:00; the bad-timestamp line (10:30:00) is not read
      expect(e.first_timestamp).toBe('2026-11-01T10:00:00.000Z');
      expect(e.last_timestamp).toBe('2026-11-01T10:07:00.000Z');
      expect(e.wall_seconds).toBe(7 * 60);
    });
  });

  describe('a pair refused in any file', () => {
    // K = msg_k/req_k: refused inside refuse-a (its second line changes input_tokens), valid in refuse-b
    const KB = u(3, 1, 0, 0);
    const KA1 = u(1, 1, 1, 1);
    const KB1 = u(2, 2, 2, 2);

    it('adds nothing to total, whichever id comes first, and is not shared', () => {
      for (const argv of [
        ['--session', 'refuse-a', '--session', 'refuse-b'],
        ['--session', 'refuse-b', '--session', 'refuse-a'],
      ]) {
        const { exitCode, doc } = run(...argv);
        expect(exitCode, argv.join(' ')).toBe(4);
        expect(doc.refusals).toEqual([
          { file: path.join(projects, 'proj-a', 'refuse-a.jsonl'), line: 2, reason: 'usage-mismatch' },
        ]);
        const t = sum(KA1, KB1); // K is left out
        expect(doc.total.input_tokens, argv.join(' ')).toBe(t.i);
        expect(doc.total.output_tokens).toBe(t.o);
        expect(doc.total.cache_read_input_tokens).toBe(t.cr);
        expect(doc.total.cache_creation_input_tokens).toBe(t.cc);
        expect(doc.total.tokens_excluding_cache_reads).toBe(excl(t));
        expect(doc.total.tokens_including_cache_reads).toBe(incl(t));
        expect(doc.total.responses).toBe(2);
        expect(doc.total.responses_shared_across_ids).toBe(0);
        expect(doc.ids.length, argv.join(' ')).toBe(2);
        const byId = Object.fromEntries(doc.ids.map((e) => [e.id, e]));
        expectCounts(byId['refuse-a'], KA1, 1); // refuse-a's own figures skip K
        expectCounts(byId['refuse-b'], sum(KB, KB1), 2); // refuse-b's own figures count its valid K
      }
    });
  });

  describe('an id whose file has no timestamped line', () => {
    it('is measured with null time and zero counts, and its lines are counted as other', () => {
      const { exitCode, doc } = run('--session', 'no-time');
      expect(exitCode).toBe(0);
      expect(doc.ids.length).toBe(1);
      const e = doc.ids[0];
      expect(e.first_timestamp).toBeNull();
      expect(e.last_timestamp).toBeNull();
      expect(e.wall_seconds).toBeNull();
      expect(e.lines_other).toBe(2);
      expectCounts(e, u(0, 0, 0, 0), 0);
    });
  });

  describe('finding an id', () => {
    it('lists an id with no transcript as unmeasured not-found, never as zero', () => {
      const { exitCode, doc } = run('--session', 'ghost');
      expect(exitCode).toBe(3);
      expect(doc.ids).toEqual([]);
      expect(doc.unmeasured.length).toBe(1);
      expect(doc.unmeasured[0]).toMatchObject({ kind: 'session', id: 'ghost', reason: 'not-found' });
      expect(doc.complete).toBe(false);
    });

    it('lists an id whose file name occurs under two projects as ambiguous', () => {
      const { exitCode, doc } = run('--session', 'dup-sess');
      expect(exitCode).toBe(3);
      expect(doc.ids).toEqual([]);
      expect(doc.unmeasured.length).toBe(1);
      expect(doc.unmeasured[0]).toMatchObject({ kind: 'session', id: 'dup-sess', reason: 'ambiguous' });
    });

    it('matches --session only against session files and --agent only against subagent files', () => {
      // sub1 exists only as an agent, s-main only as a session
      const a = run('--session', 'sub1');
      expect(a.doc.unmeasured[0]).toMatchObject({ id: 'sub1', reason: 'not-found' });
      const b = run('--agent', 's-main');
      expect(b.doc.unmeasured[0]).toMatchObject({ id: 's-main', reason: 'not-found' });
      expect(a.exitCode).toBe(3);
      expect(b.exitCode).toBe(3);
    });

    it('treats the same string as --session and --agent as two lookups', () => {
      const { exitCode, doc } = run('--session', 'twin', '--agent', 'twin');
      expect(exitCode).toBe(0);
      expect(doc.ids.map((e) => `${e.kind}:${e.id}`)).toEqual(['session:twin', 'agent:twin']);
      expectCounts(doc.ids[0], u(1, 1, 1, 1), 1);
      expectCounts(doc.ids[1], u(2, 2, 2, 2), 1);
      expect(doc.total.responses).toBe(2);
    });

    it('measures the same id given twice under one flag once, at its first position', () => {
      const { exitCode, doc } = run('--session', 'twin', '--agent', 'sub1', '--session', 'twin');
      expect(exitCode).toBe(0);
      expect(doc.ids.map((e) => `${e.kind}:${e.id}`)).toEqual(['session:twin', 'agent:sub1']);
      expect(doc.total.responses).toBe(1 + 2);
    });
  });

  describe('exit codes', () => {
    it('is 4 with an unresolved id and a refused line together, both lists filled', () => {
      const { exitCode, doc } = run('--session', 'bad-input', '--session', 'ghost');
      expect(exitCode).toBe(4);
      expect(doc.refusals).toEqual([
        { file: path.join(projects, 'proj-a', 'bad-input.jsonl'), line: 2, reason: 'usage-mismatch' },
      ]);
      expect(doc.unmeasured).toEqual([{ kind: 'session', id: 'ghost', reason: 'not-found' }]);
      expect(doc.ids.map((e) => e.id)).toEqual(['bad-input']);
      expect(doc.complete).toBe(false);
    });

    it('exits 2 with usage on stderr and no JSON when no id is given', () => {
      const r = runMeter(['--projects', projects]);
      expect(r.exitCode).toBe(2);
      expect(r.stdout).toBe('');
      expect(r.stderr.length).toBeGreaterThan(0);
    });

    it('exits 2 with no JSON on an unknown flag', () => {
      const r = runMeter(['--projects', projects, '--session', 's-main', '--bogus']);
      expect(r.exitCode).toBe(2);
      expect(r.stdout).toBe('');
      expect(r.stderr.length).toBeGreaterThan(0);
    });

    it('exits 2 with no JSON on a flag with no value', () => {
      for (const argv of [
        ['--projects', projects, '--session'],
        ['--projects', projects, '--session', '--agent', 'sub1'],
        ['--projects', projects, '--agent', 'sub1', '--session'],
        ['--session', 's-main', '--projects'],
      ]) {
        const r = runMeter(argv);
        expect(r.exitCode, argv.join(' ')).toBe(2);
        expect(r.stdout).toBe('');
        expect(r.stderr.length).toBeGreaterThan(0);
      }
    });
  });
});
