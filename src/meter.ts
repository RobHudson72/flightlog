import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { z } from 'zod';

// flightlog-meter: measures a ticket's tokens and session time from the
// transcripts of the sessions and subagents that worked on it. Reads local
// files, writes nothing, prices nothing.

/** The version of the transcript-record schema below and of the document's shape. */
const METER_VERSION = 1;

// ---------------------------------------------------------------------------
// The one parse point (M1). Nothing else in flightlog parses usage. Version
// METER_VERSION reads exactly: type, timestamp, requestId, message.id,
// message.model and the four message.usage counts. No message content is read.
// ---------------------------------------------------------------------------

const ISO_INSTANT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
const instant = z.string().refine((s) => ISO_INSTANT.test(s) && !Number.isNaN(Date.parse(s)));
const count = z.number().int().nonnegative();
const SYNTHETIC_MODEL = '<synthetic>';

const envelopeSchema = z.object({
  type: z.string(),
  timestamp: instant.optional(),
  // whether message.usage is present; the usage itself goes through recordSchema
  message: z.unknown().transform(
    (m) =>
      typeof m === 'object' &&
      m !== null &&
      !Array.isArray(m) &&
      (m as Record<string, unknown>).usage !== undefined,
  ),
});

const recordSchema = z
  .object({
    timestamp: instant,
    requestId: z.string().min(1).optional(),
    message: z.object({
      id: z.string().min(1),
      model: z.string().min(1),
      usage: z.object({
        input_tokens: count,
        output_tokens: count,
        cache_read_input_tokens: count,
        cache_creation_input_tokens: count,
      }),
    }),
  })
  .superRefine((r, ctx) => {
    if (r.message.model !== SYNTHETIC_MODEL && r.requestId === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['requestId'], message: 'requestId is required' });
    }
  });

interface Usage {
  input: number;
  output: number;
  cacheRead: number;
  cacheCreation: number;
}

type ParsedLine =
  | { kind: 'refused'; reason: string }
  | { kind: 'other'; timestamp: string | undefined }
  | { kind: 'synthetic'; timestamp: string }
  | { kind: 'response'; timestamp: string; model: string; pair: string; usage: Usage };

function parseLine(raw: string): ParsedLine {
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch {
    return { kind: 'refused', reason: 'invalid-json' };
  }
  const env = envelopeSchema.safeParse(json);
  if (!env.success) {
    const onTimestamp = env.error.issues.some((i) => i.path[0] === 'timestamp');
    return { kind: 'refused', reason: onTimestamp ? 'bad-timestamp' : 'schema' };
  }
  const { type, timestamp, message: carriesUsage } = env.data;
  if (type !== 'assistant' || !carriesUsage) {
    if (carriesUsage) return { kind: 'refused', reason: 'usage-on-non-assistant' };
    return { kind: 'other', timestamp };
  }
  const rec = recordSchema.safeParse(json);
  if (!rec.success) return { kind: 'refused', reason: 'schema' };
  const { message, requestId } = rec.data;
  const usage: Usage = {
    input: message.usage.input_tokens,
    output: message.usage.output_tokens,
    cacheRead: message.usage.cache_read_input_tokens,
    cacheCreation: message.usage.cache_creation_input_tokens,
  };
  if (message.model === SYNTHETIC_MODEL) {
    if (usage.input + usage.output + usage.cacheRead + usage.cacheCreation !== 0) {
      return { kind: 'refused', reason: 'synthetic-nonzero' };
    }
    return { kind: 'synthetic', timestamp: rec.data.timestamp };
  }
  // recordSchema guarantees requestId for a non-synthetic line
  return {
    kind: 'response',
    timestamp: rec.data.timestamp,
    model: message.model,
    pair: JSON.stringify([message.id, requestId]),
    usage,
  };
}

// ---------------------------------------------------------------------------
// Measuring one file
// ---------------------------------------------------------------------------

interface Refusal {
  file: string;
  line: number;
  reason: string;
}

interface PairState {
  usage: Usage;
  line: number;
  refused: boolean;
}

interface FileMeasure {
  entry: Record<string, unknown>;
  refusals: Refusal[];
  /** Every counted pair of the file: its last line's usage and line number. */
  pairs: Map<string, { usage: Usage; line: number }>;
}

function sameInputs(a: Usage, b: Usage): boolean {
  return a.input === b.input && a.cacheRead === b.cacheRead && a.cacheCreation === b.cacheCreation;
}

function sameUsage(a: Usage, b: Usage): boolean {
  return sameInputs(a, b) && a.output === b.output;
}

interface Totals {
  input: number;
  output: number;
  cacheRead: number;
  cacheCreation: number;
}

function addUsage(t: Totals, u: Usage): void {
  t.input += u.input;
  t.output += u.output;
  t.cacheRead += u.cacheRead;
  t.cacheCreation += u.cacheCreation;
}

function countFields(t: Totals): Record<string, number> {
  return {
    input_tokens: t.input,
    output_tokens: t.output,
    cache_read_input_tokens: t.cacheRead,
    cache_creation_input_tokens: t.cacheCreation,
    tokens_excluding_cache_reads: t.input + t.output + t.cacheCreation,
    tokens_including_cache_reads: t.input + t.output + t.cacheRead + t.cacheCreation,
  };
}

function measureFile(kind: string, id: string, file: string): FileMeasure {
  const lines = fs.readFileSync(file, 'utf8').split('\n');
  if (lines[lines.length - 1] === '') lines.pop();

  const refusals: Refusal[] = [];
  const pairStates = new Map<string, PairState>();
  const models = new Set<string>();
  let syntheticLines = 0;
  let linesOther = 0;
  let first: { ms: number; text: string } | undefined;
  let last: { ms: number; text: string } | undefined;

  const readTime = (text: string | undefined): void => {
    if (text === undefined) return;
    const ms = Date.parse(text);
    if (first === undefined || ms < first.ms) first = { ms, text };
    if (last === undefined || ms > last.ms) last = { ms, text };
  };

  lines.forEach((raw, index) => {
    const lineNo = index + 1;
    const parsed = parseLine(raw);
    if (parsed.kind === 'refused') {
      refusals.push({ file, line: lineNo, reason: parsed.reason });
      return;
    }
    if (parsed.kind === 'other') {
      linesOther += 1;
      readTime(parsed.timestamp);
      return;
    }
    readTime(parsed.timestamp);
    if (parsed.kind === 'synthetic') {
      syntheticLines += 1;
      return;
    }
    models.add(parsed.model);
    const prev = pairStates.get(parsed.pair);
    if (prev === undefined) {
      pairStates.set(parsed.pair, { usage: parsed.usage, line: lineNo, refused: false });
      return;
    }
    if (prev.refused) return;
    if (!sameInputs(prev.usage, parsed.usage) || parsed.usage.output < prev.usage.output) {
      prev.refused = true;
      refusals.push({ file, line: lineNo, reason: 'usage-mismatch' });
      return;
    }
    prev.usage = parsed.usage;
    prev.line = lineNo;
  });

  const totals: Totals = { input: 0, output: 0, cacheRead: 0, cacheCreation: 0 };
  const pairs = new Map<string, { usage: Usage; line: number }>();
  for (const [key, state] of pairStates) {
    if (state.refused) continue;
    addUsage(totals, state.usage);
    pairs.set(key, { usage: state.usage, line: state.line });
  }

  const entry: Record<string, unknown> = {
    kind,
    id,
    path: file,
    models: [...models],
    ...countFields(totals),
    responses: pairs.size,
    synthetic_lines: syntheticLines,
    lines_other: linesOther,
    first_timestamp: first === undefined ? null : first.text,
    last_timestamp: last === undefined ? null : last.text,
    wall_seconds: first === undefined || last === undefined ? null : (last.ms - first.ms) / 1000,
  };
  return { entry, refusals, pairs };
}

// ---------------------------------------------------------------------------
// Finding an id
// ---------------------------------------------------------------------------

function readDirIfPresent(dir: string): fs.Dirent[] {
  try {
    return fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ENOENT' || code === 'ENOTDIR') return [];
    throw err;
  }
}

interface TranscriptIndex {
  sessions: Map<string, string[]>;
  agents: Map<string, string[]>;
}

function addTo(map: Map<string, string[]>, key: string, file: string): void {
  const files = map.get(key);
  if (files === undefined) map.set(key, [file]);
  else files.push(file);
}

/** Session files at <projects>/<project>/<id>.jsonl; subagent files at <projects>/<project>/<session>/subagents/agent-<id>.jsonl. */
function indexTranscripts(projects: string): TranscriptIndex {
  const index: TranscriptIndex = { sessions: new Map(), agents: new Map() };
  const byName = (a: fs.Dirent, b: fs.Dirent): number => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  for (const project of readDirIfPresent(projects).filter((d) => d.isDirectory()).sort(byName)) {
    const projectDir = path.join(projects, project.name);
    for (const entry of readDirIfPresent(projectDir).sort(byName)) {
      if (entry.isFile() && entry.name.endsWith('.jsonl')) {
        addTo(index.sessions, entry.name.slice(0, -'.jsonl'.length), path.join(projectDir, entry.name));
      } else if (entry.isDirectory()) {
        const subDir = path.join(projectDir, entry.name, 'subagents');
        for (const sub of readDirIfPresent(subDir).sort(byName)) {
          if (sub.isFile() && sub.name.startsWith('agent-') && sub.name.endsWith('.jsonl')) {
            addTo(index.agents, sub.name.slice('agent-'.length, -'.jsonl'.length), path.join(subDir, sub.name));
          }
        }
      }
    }
  }
  return index;
}

// ---------------------------------------------------------------------------
// The command
// ---------------------------------------------------------------------------

const USAGE =
  'usage: flightlog-meter [--session <sessionId>]... [--agent <agentId>]... [--projects <dir>]\n' +
  '  at least one --session or --agent is required; --projects defaults to ~/.claude/projects\n';

interface Wanted {
  kind: 'session' | 'agent';
  id: string;
}

type Args = { projects: string; wanted: Wanted[] } | { error: string };

function parseArgs(argv: string[]): Args {
  const wanted: Wanted[] = [];
  const seen = new Set<string>();
  let projects: string | undefined;
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i];
    if (flag !== '--session' && flag !== '--agent' && flag !== '--projects') {
      return { error: `unknown argument: ${flag}` };
    }
    const value = argv[i + 1];
    if (value === undefined || value === '' || value.startsWith('--')) {
      return { error: `${flag} needs a value` };
    }
    if (flag === '--projects') {
      if (projects !== undefined) return { error: '--projects given twice' };
      projects = value;
      continue;
    }
    const kind = flag === '--session' ? 'session' : 'agent';
    if (seen.has(`${kind}:${value}`)) continue;
    seen.add(`${kind}:${value}`);
    wanted.push({ kind, id: value });
  }
  if (wanted.length === 0) return { error: 'no --session or --agent given' };
  return { projects: projects === undefined ? path.join(os.homedir(), '.claude', 'projects') : projects, wanted };
}

export interface MeterResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export function runMeter(argv: string[]): MeterResult {
  const args = parseArgs(argv);
  if ('error' in args) {
    return { exitCode: 2, stdout: '', stderr: `flightlog-meter: ${args.error}\n${USAGE}` };
  }

  const index = indexTranscripts(args.projects);
  const ids: Record<string, unknown>[] = [];
  const unmeasured: Record<string, unknown>[] = [];
  const refusals: Refusal[] = [];
  const total: Totals = { input: 0, output: 0, cacheRead: 0, cacheCreation: 0 };
  // every counted pair across ids: the first id's usage, and how many files hold it
  const seenPairs = new Map<string, { usage: Usage; files: number }>();

  for (const want of args.wanted) {
    const files = (want.kind === 'session' ? index.sessions : index.agents).get(want.id);
    if (files === undefined) {
      unmeasured.push({ kind: want.kind, id: want.id, reason: 'not-found' });
      continue;
    }
    if (files.length > 1) {
      unmeasured.push({ kind: want.kind, id: want.id, reason: 'ambiguous', paths: files });
      continue;
    }
    const measured = measureFile(want.kind, want.id, files[0]);
    ids.push(measured.entry);
    refusals.push(...measured.refusals);
    const crossRefusals: Refusal[] = [];
    for (const [key, pair] of measured.pairs) {
      const earlier = seenPairs.get(key);
      if (earlier === undefined) {
        seenPairs.set(key, { usage: pair.usage, files: 1 });
        addUsage(total, pair.usage);
        continue;
      }
      earlier.files += 1;
      if (!sameUsage(earlier.usage, pair.usage)) {
        crossRefusals.push({ file: files[0], line: pair.line, reason: 'usage-mismatch' });
      }
    }
    crossRefusals.sort((a, b) => a.line - b.line);
    refusals.push(...crossRefusals);
  }

  let shared = 0;
  for (const p of seenPairs.values()) if (p.files >= 2) shared += 1;

  const exitCode = refusals.length > 0 ? 4 : unmeasured.length > 0 ? 3 : 0;
  const doc = {
    meter_version: METER_VERSION,
    ids,
    total: { ...countFields(total), responses: seenPairs.size, responses_shared_across_ids: shared },
    unmeasured,
    refusals,
    complete: exitCode === 0,
    dollars: 'not measured',
  };
  return { exitCode, stdout: JSON.stringify(doc, null, 2) + '\n', stderr: '' };
}
