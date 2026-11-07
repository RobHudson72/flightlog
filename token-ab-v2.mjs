#!/usr/bin/env node
// Scope-controlled A/B + tokens/turn KPI + failure-mode incidence.
// Extends token-ab.mjs. Methodology: methodology_token_burn_analysis.md

import Database from 'better-sqlite3';
const DB = 'E:/data/fermi/flightlog/flightlog.db';
const db = new Database(DB, { readonly: true });

function fmt(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return String(Math.round(n));
}

// ----- scope-matched pairs -----
// Infra/service/plumbing scope only. Schema-phase tickets excluded.
const PAIRS = [
  { before: 'END-3063', beforeScope: 'service (NetworkService)',     after: 'END-3207', afterScope: 'service (PublicProfileService)' },
  { before: 'END-3154', beforeScope: 'service (MilestoneGeneration)', after: 'END-3151', afterScope: 'service (ApexCandidate + actions/map)' },
  { before: 'END-3155', beforeScope: 'auth (Clerk session enrichment)', after: 'END-3201', afterScope: 'auth (proxy/sign-out/Clerk)' },
  { before: 'END-3075', beforeScope: 'lib plumbing (error-messages, phases)', after: 'END-3048', afterScope: 'lib plumbing (map/discovery-*)' },
  { before: 'END-3063', beforeScope: 'service (NetworkService)',     after: 'END-3108', afterScope: 'UI plumbing (dashboard/onboarding pages)' },
];

// Pull per-ticket aggregate (main-thread only).
// Restrict to canonical `ticket/END-N` branch — older `feat/END-N` or
// `task/end-N-*` branches pre-date D22-D28 and would contaminate AFTER numbers.
function ticketAgg(ticket) {
  const row = db.prepare(`
    SELECT
      COUNT(m.uuid) AS msgs,
      COALESCE(SUM(m.input_tokens),0) + COALESCE(SUM(m.output_tokens),0)
        + COALESCE(SUM(m.cache_creation_tokens),0) + COALESCE(SUM(m.cache_read_tokens),0) AS total,
      COUNT(DISTINCT m.session_id) AS sessions
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    WHERE s.git_branch = ('ticket/' || ?) AND m.role = 'assistant'
      AND s.session_id NOT LIKE 'agent-%'
  `).get(ticket);
  return row;
}

console.log('# Part 1 — Scope-controlled A/B');
console.log('');
console.log('| BEFORE (scope) | AFTER (scope) | B tokens | A tokens | B msgs | A msgs | Δ tokens |');
console.log('|---|---|---|---|---|---|---|');
for (const p of PAIRS) {
  const b = ticketAgg(p.before);
  const a = ticketAgg(p.after);
  const delta = a.total - b.total;
  const sign = delta >= 0 ? '+' : '';
  console.log(`| ${p.before} (${p.beforeScope}) | ${p.after} (${p.afterScope}) | ${fmt(b.total)} | ${fmt(a.total)} | ${b.msgs} | ${a.msgs} | ${sign}${fmt(delta)} |`);
}

// ===== Part 2: tokens/turn and first-half vs second-half =====
function turnStats(ticket) {
  const rows = db.prepare(`
    SELECT m.session_id, m.timestamp,
      COALESCE(m.input_tokens,0) + COALESCE(m.output_tokens,0)
       + COALESCE(m.cache_creation_tokens,0) + COALESCE(m.cache_read_tokens,0) AS tok
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    WHERE s.git_branch = ('ticket/' || ?) AND m.role = 'assistant'
      AND s.session_id NOT LIKE 'agent-%'
    ORDER BY m.session_id, m.timestamp
  `).all(ticket);
  // split each session in half
  const bySess = new Map();
  for (const r of rows) {
    if (!bySess.has(r.session_id)) bySess.set(r.session_id, []);
    bySess.get(r.session_id).push(r.tok);
  }
  const firstHalf = [], secondHalf = [];
  for (const arr of bySess.values()) {
    const mid = Math.floor(arr.length / 2);
    firstHalf.push(...arr.slice(0, mid));
    secondHalf.push(...arr.slice(mid));
  }
  const all = rows.map(r => r.tok);
  function s(a) {
    if (!a.length) return { n: 0, mean: 0, median: 0 };
    const sorted = [...a].sort((x,y)=>x-y);
    const sum = a.reduce((x,y)=>x+y,0);
    return { n: a.length, mean: sum/a.length, median: sorted[Math.floor(sorted.length/2)] };
  }
  return { all: s(all), firstHalf: s(firstHalf), secondHalf: s(secondHalf) };
}

function groupTurnStats(tickets) {
  const all = [], fh = [], sh = [];
  for (const t of tickets) {
    const rows = db.prepare(`
      SELECT m.session_id, m.timestamp,
        COALESCE(m.input_tokens,0) + COALESCE(m.output_tokens,0)
         + COALESCE(m.cache_creation_tokens,0) + COALESCE(m.cache_read_tokens,0) AS tok
      FROM sessions s
      JOIN messages m ON m.session_id = s.session_id
      WHERE s.git_branch = ('ticket/' || ?) AND m.role = 'assistant'
        AND s.session_id NOT LIKE 'agent-%'
      ORDER BY m.session_id, m.timestamp
    `).all(t);
    const bySess = new Map();
    for (const r of rows) {
      if (!bySess.has(r.session_id)) bySess.set(r.session_id, []);
      bySess.get(r.session_id).push(r.tok);
    }
    for (const arr of bySess.values()) {
      const mid = Math.floor(arr.length / 2);
      fh.push(...arr.slice(0, mid));
      sh.push(...arr.slice(mid));
    }
    all.push(...rows.map(r=>r.tok));
  }
  function s(a) {
    if (!a.length) return { n: 0, mean: 0, median: 0 };
    const sorted = [...a].sort((x,y)=>x-y);
    const sum = a.reduce((x,y)=>x+y,0);
    return { n: a.length, mean: sum/a.length, median: sorted[Math.floor(sorted.length/2)] };
  }
  return { all: s(all), firstHalf: s(fh), secondHalf: s(sh) };
}

const BEFORE_TICKETS = ['END-3063','END-3075','END-3154','END-3155'];
// Use only scope-matched AFTER for fairness
const AFTER_TICKETS_SCOPED = ['END-3207','END-3151','END-3201','END-3048','END-3108'];

const bT = groupTurnStats(BEFORE_TICKETS);
const aT = groupTurnStats(AFTER_TICKETS_SCOPED);

console.log('\n# Part 2 — Tokens per assistant message (main-thread only)');
console.log('');
console.log('| Group | N msgs | Mean/msg | Median/msg | 1st-half Mean | 2nd-half Mean | Trend |');
console.log('|---|---|---|---|---|---|---|');
function trend(fh, sh) {
  if (!fh || !sh) return '—';
  const pct = ((sh - fh) / fh * 100).toFixed(1);
  return `${pct >= 0 ? '+' : ''}${pct}%`;
}
console.log(`| BEFORE (scope-matched) | ${bT.all.n} | ${fmt(bT.all.mean)} | ${fmt(bT.all.median)} | ${fmt(bT.firstHalf.mean)} | ${fmt(bT.secondHalf.mean)} | ${trend(bT.firstHalf.mean,bT.secondHalf.mean)} |`);
console.log(`| AFTER (scope-matched)  | ${aT.all.n} | ${fmt(aT.all.mean)} | ${fmt(aT.all.median)} | ${fmt(aT.firstHalf.mean)} | ${fmt(aT.secondHalf.mean)} | ${trend(aT.firstHalf.mean,aT.secondHalf.mean)} |`);

// ===== Part 3: failure-mode incidence =====
// Each pattern → SQL LIKE / regex checks on text_content (and tool_input for Bash commands).

function countMatches(tickets, where, params = []) {
  const placeholders = tickets.map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT COUNT(*) AS n, COUNT(DISTINCT m.session_id) AS sessions
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE s.git_branch LIKE 'ticket/%' AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
      AND ${where}
  `).get(...tickets, ...params);
  return rows;
}

// Gather tool_input for Bash calls separately — easier than string-matching.
function bashMatches(tickets, regex) {
  const tSet = new Set(tickets.map(t => t.toLowerCase()));
  const rows = db.prepare(`
    SELECT s.git_branch, cb.tool_input
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE cb.block_type = 'tool_use' AND cb.tool_name = 'Bash'
      AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
  `).all(...tickets);
  let n = 0;
  for (const r of rows) {
    try {
      const ti = JSON.parse(r.tool_input);
      const cmd = ti.command || '';
      if (regex.test(cmd)) n++;
    } catch (e) {}
  }
  return n;
}

// Gather tool_input for ANY tool call with specific tool_name + JSON key match.
function toolInputMatches(tickets, toolName, predicate) {
  const rows = db.prepare(`
    SELECT s.git_branch, cb.tool_input, cb.tool_name
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE cb.block_type = 'tool_use' AND cb.tool_name = ?
      AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
  `).all(toolName, ...tickets);
  let n = 0;
  for (const r of rows) {
    try {
      const ti = JSON.parse(r.tool_input);
      if (predicate(ti, r)) n++;
    } catch (e) {}
  }
  return n;
}

// --- D22: run_in_background:true on cadence-* OR `until gh pr view` OR Monitor of cadence ---
function countD22(tickets) {
  // (a) Bash run_in_background=true calling cadence-*
  const rIb = toolInputMatches(tickets, 'Bash', (ti) =>
    ti.run_in_background === true && /cadence-/.test(ti.command || '')
  );
  // (b) `until gh pr view` in any Bash
  const rUntilPR = bashMatches(tickets, /until\s+gh\s+pr\s+view/);
  // (c) Monitor tool on cadence output
  const rMon = toolInputMatches(tickets, 'Monitor', (ti) => {
    const s = JSON.stringify(ti);
    return /cadence/i.test(s);
  });
  return { total: rIb + rUntilPR + rMon, a: rIb, b: rUntilPR, c: rMon };
}

// --- D23: `gates.ok:false` when `13/13 passed` appears in SAME session ---
function countD23(tickets) {
  // sessions containing "13/13 passed"
  const sessIds13 = db.prepare(`
    SELECT DISTINCT m.session_id
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE cb.text_content LIKE '%13/13 passed%'
      AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
  `).all(...tickets).map(r => r.session_id);
  if (!sessIds13.length) return 0;
  // in those sessions, any block with gates.ok:false (in tool_input or text)
  const placeholders = sessIds13.map(()=>'?').join(',');
  const r = db.prepare(`
    SELECT COUNT(*) AS n
    FROM content_blocks cb
    JOIN messages m ON m.uuid = cb.message_uuid
    WHERE m.session_id IN (${placeholders})
      AND (cb.text_content LIKE '%gates.ok%false%'
        OR cb.text_content LIKE '%"ok": false%'
        OR cb.text_content LIKE '%"ok":false%'
        OR cb.tool_input LIKE '%"ok":false%')
  `).get(...sessIds13);
  return r.n;
}

// --- D24: parallel_inbox called >2x within 5 min after a parallel_signal or parallel_send ---
function countD24(tickets) {
  // Fetch per session timeline of parallel_* calls
  const rows = db.prepare(`
    SELECT m.session_id, m.timestamp, cb.tool_name
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE cb.block_type = 'tool_use'
      AND cb.tool_name IN ('mcp__parallel__parallel_inbox','mcp__parallel__parallel_signal','mcp__parallel__parallel_send')
      AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
    ORDER BY m.session_id, m.timestamp
  `).all(...tickets);
  const bySess = new Map();
  for (const r of rows) {
    if (!bySess.has(r.session_id)) bySess.set(r.session_id, []);
    bySess.get(r.session_id).push(r);
  }
  let incidents = 0;
  for (const arr of bySess.values()) {
    for (let i = 0; i < arr.length; i++) {
      const ev = arr[i];
      if (ev.tool_name === 'mcp__parallel__parallel_signal' || ev.tool_name === 'mcp__parallel__parallel_send') {
        // count parallel_inbox calls in next 5min
        const t0 = Date.parse(ev.timestamp);
        let count = 0;
        for (let j = i + 1; j < arr.length; j++) {
          const dt = Date.parse(arr[j].timestamp) - t0;
          if (dt > 5 * 60 * 1000) break;
          if (arr[j].tool_name === 'mcp__parallel__parallel_inbox') count++;
        }
        if (count > 2) incidents++;
      }
    }
  }
  return incidents;
}

// --- D27: DOC session classifying agent as rate-limited/unresponsive ---
// Approximate: occurrences of "rate-limited" or "unresponsive" in tickets.
function countD27(tickets) {
  const r = db.prepare(`
    SELECT COUNT(*) AS n
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE (cb.text_content LIKE '%rate-limited%' OR cb.text_content LIKE '%unresponsive%')
      AND s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
  `).get(...tickets);
  return r.n;
}

// --- D28: `until ... sleep ... gh pr view` OR `gh pr view` >2 within 5 min after cadence-ship timeout ---
function countD28(tickets) {
  // (a) until ... sleep ... gh pr view
  const rA = bashMatches(tickets, /until[\s\S]{0,80}sleep[\s\S]{0,80}gh\s+pr\s+view/i);
  // (b) ship timeout followed by >2 gh pr view in 5 min
  const rows = db.prepare(`
    SELECT m.session_id, m.timestamp, cb.tool_name, cb.tool_input, cb.text_content
    FROM sessions s
    JOIN messages m ON m.session_id = s.session_id
    JOIN content_blocks cb ON cb.message_uuid = m.uuid
    WHERE s.session_id NOT LIKE 'agent-%'
      AND (${tickets.map(() => "s.git_branch = ('ticket/' || ?)").join(' OR ')})
      AND (
        (cb.block_type='tool_use' AND cb.tool_name='Bash')
        OR cb.text_content LIKE '%cadence-ship%timeout%'
        OR cb.text_content LIKE '%cadence-ship%timed out%'
      )
    ORDER BY m.session_id, m.timestamp
  `).all(...tickets);
  const bySess = new Map();
  for (const r of rows) {
    if (!bySess.has(r.session_id)) bySess.set(r.session_id, []);
    bySess.get(r.session_id).push(r);
  }
  let rB = 0;
  for (const arr of bySess.values()) {
    for (let i = 0; i < arr.length; i++) {
      const ev = arr[i];
      const txt = (ev.text_content || '') + ' ' + (ev.tool_input || '');
      if (/cadence-ship[\s\S]{0,60}(timeout|timed out)/i.test(txt)) {
        const t0 = Date.parse(ev.timestamp);
        let ghViews = 0;
        for (let j = i + 1; j < arr.length; j++) {
          const dt = Date.parse(arr[j].timestamp) - t0;
          if (dt > 5 * 60 * 1000) break;
          if (arr[j].tool_name === 'Bash') {
            try {
              const ti = JSON.parse(arr[j].tool_input);
              if (/gh\s+pr\s+view/.test(ti.command || '')) ghViews++;
            } catch(e) {}
          }
        }
        if (ghViews > 2) rB++;
      }
    }
  }
  return { total: rA + rB, a: rA, b: rB };
}

console.log('\n# Part 3 — Failure-mode incidence');
console.log('');
const ALL_BEFORE = BEFORE_TICKETS;
const ALL_AFTER_SCOPED = AFTER_TICKETS_SCOPED;
const d22B = countD22(ALL_BEFORE), d22A = countD22(ALL_AFTER_SCOPED);
const d23B = countD23(ALL_BEFORE), d23A = countD23(ALL_AFTER_SCOPED);
const d24B = countD24(ALL_BEFORE), d24A = countD24(ALL_AFTER_SCOPED);
const d27B = countD27(ALL_BEFORE), d27A = countD27(ALL_AFTER_SCOPED);
const d28B = countD28(ALL_BEFORE), d28A = countD28(ALL_AFTER_SCOPED);
console.log('| Pattern | BEFORE | AFTER |');
console.log('|---|---|---|');
console.log(`| D22 (bg cadence / until gh pr view / Monitor cadence) | ${d22B.total} (a=${d22B.a} b=${d22B.b} c=${d22B.c}) | ${d22A.total} (a=${d22A.a} b=${d22A.b} c=${d22A.c}) |`);
console.log(`| D23 (gates.ok:false with 13/13 passed in same session) | ${d23B} | ${d23A} |`);
console.log(`| D24 (parallel_inbox >2× in 5 min after signal/send) | ${d24B} | ${d24A} |`);
console.log(`| D27 (rate-limited/unresponsive mentions) | ${d27B} | ${d27A} |`);
console.log(`| D28 (until/sleep/gh pr view OR gh pr view >2× after ship timeout) | ${d28B.total} (a=${d28B.a} b=${d28B.b}) | ${d28A.total} (a=${d28A.a} b=${d28A.b}) |`);

// Also run Part 3 over the FULL AFTER set (all ticket/END-* tickets post-cutoff, main-thread only)
// for broader validation.
const allAfterRows = db.prepare(`
  SELECT DISTINCT s.git_branch
  FROM sessions s
  WHERE s.git_branch LIKE 'ticket/END-%'
    AND s.started_at >= '2026-04-19T15:00:00Z'
    AND s.session_id NOT LIKE 'agent-%'
`).all();
const ALL_AFTER_FULL = allAfterRows.map(r => r.git_branch.replace('ticket/','')).filter(t => !BEFORE_TICKETS.includes(t));

const d22Af = countD22(ALL_AFTER_FULL);
const d23Af = countD23(ALL_AFTER_FULL);
const d24Af = countD24(ALL_AFTER_FULL);
const d27Af = countD27(ALL_AFTER_FULL);
const d28Af = countD28(ALL_AFTER_FULL);
console.log('\n(Supplement: full AFTER set, N=' + ALL_AFTER_FULL.length + ' tickets — broader validation)');
console.log('| Pattern | AFTER (all) |');
console.log('|---|---|');
console.log(`| D22 | ${d22Af.total} (a=${d22Af.a} b=${d22Af.b} c=${d22Af.c}) |`);
console.log(`| D23 | ${d23Af} |`);
console.log(`| D24 | ${d24Af} |`);
console.log(`| D27 | ${d27Af} |`);
console.log(`| D28 | ${d28Af.total} |`);

db.close();
