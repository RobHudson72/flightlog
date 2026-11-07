#!/usr/bin/env node
// Token-burn A/B analysis — SQL-direct against flightlog DB.
// Methodology: memory/methodology_token_burn_analysis.md

import Database from 'better-sqlite3';
const DB = 'E:/data/fermi/flightlog/flightlog.db';
const db = new Database(DB, { readonly: true });

// Pull all sessions on ticket/END-* branches with aggregated token totals.
// Assistant messages only (user/tool_result rows are not billed API calls).
const rows = db.prepare(`
  SELECT
    s.session_id,
    s.git_branch,
    s.started_at,
    s.last_message_at,
    s.project,
    s.message_count,
    COUNT(m.uuid) AS assistant_msgs,
    COALESCE(SUM(m.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(m.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(m.cache_creation_tokens), 0) AS cache_creation,
    COALESCE(SUM(m.cache_read_tokens), 0) AS cache_read
  FROM sessions s
  LEFT JOIN messages m
    ON m.session_id = s.session_id AND m.role = 'assistant'
  WHERE s.git_branch LIKE 'ticket/END-%'
  GROUP BY s.session_id
  ORDER BY s.started_at ASC
`).all();

function extractTicket(branch) {
  const m = branch.match(/ticket\/(END-\d+)/);
  return m ? m[1] : branch;
}

// Aggregate by ticket. Exclude Task subagent sessions (session_id starts with
// "agent-") — those are Explore/research delegations, not main-thread skill work.
const byTicket = new Map();
for (const r of rows) {
  if (r.session_id.startsWith('agent-')) continue;
  const ticket = extractTicket(r.git_branch);
  if (!byTicket.has(ticket)) byTicket.set(ticket, []);
  const total = r.input_tokens + r.output_tokens + r.cache_creation + r.cache_read;
  byTicket.get(ticket).push({ ...r, total, ticket });
}

// Classify before/after by date (D22-D28 landed ~2026-04-19)
const AFTER_CUTOFF = '2026-04-19T15:00:00Z';
const BEFORE_TICKETS = new Set(['END-3063', 'END-3155', 'END-3154', 'END-3075']);

function fmtTokens(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return String(n);
}

const summary = [];
for (const [ticket, sessions] of [...byTicket.entries()].sort()) {
  const total = sessions.reduce((a, s) => a + s.total, 0);
  const msgs = sessions.reduce((a, s) => a + s.assistant_msgs, 0);
  const firstStart = sessions[0].started_at;
  const lastEnd = sessions[sessions.length - 1].last_message_at;
  const isBefore = BEFORE_TICKETS.has(ticket);
  const isAfter = !isBefore && firstStart >= AFTER_CUTOFF;
  const group = isBefore ? 'BEFORE' : (isAfter ? 'AFTER' : 'MID');
  summary.push({
    ticket, group, sessions: sessions.length, assistant_msgs: msgs,
    total, first: firstStart, last: lastEnd, session_detail: sessions,
  });
}

// Print per-ticket table
console.log('# Per-ticket totals (all phases combined)');
console.log('');
console.log('| Ticket | Group | Sessions | A-msgs | Total tokens | First | Last |');
console.log('|---|---|---|---|---|---|---|');
for (const s of summary) {
  console.log(`| ${s.ticket} | ${s.group} | ${s.sessions} | ${s.assistant_msgs} | ${fmtTokens(s.total)} | ${s.first.slice(0,16)} | ${s.last.slice(0,16)} |`);
}

// Aggregate group stats
function stats(arr) {
  if (!arr.length) return { n: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  const sum = arr.reduce((a, b) => a + b, 0);
  return {
    n: arr.length,
    mean: Math.round(sum / arr.length),
    median: sorted[Math.floor(sorted.length / 2)],
    max: sorted[sorted.length - 1],
    min: sorted[0],
  };
}

const before = summary.filter(s => s.group === 'BEFORE').map(s => s.total);
const after = summary.filter(s => s.group === 'AFTER').map(s => s.total);

console.log('\n# Aggregate by group (per-ticket totals)');
console.log('');
console.log('| Group | N | Mean | Median | Min | Max |');
console.log('|---|---|---|---|---|---|');
const bs = stats(before);
const as_ = stats(after);
console.log(`| BEFORE | ${bs.n} | ${fmtTokens(bs.mean||0)} | ${fmtTokens(bs.median||0)} | ${fmtTokens(bs.min||0)} | ${fmtTokens(bs.max||0)} |`);
console.log(`| AFTER  | ${as_.n} | ${fmtTokens(as_.mean||0)} | ${fmtTokens(as_.median||0)} | ${fmtTokens(as_.min||0)} | ${fmtTokens(as_.max||0)} |`);

if (bs.median && as_.median) {
  const pct = ((as_.median - bs.median) / bs.median * 100).toFixed(1);
  console.log(`\nMedian change (all): ${pct}%`);
}
if (bs.mean && as_.mean) {
  const pct = ((as_.mean - bs.mean) / bs.mean * 100).toFixed(1);
  console.log(`Mean change   (all): ${pct}%`);
}

// SINGLE-SESSION SUB-COMPARISON — the cleanest A/B because it isolates the
// happy path: no CCR, no rework loop, one clean pass through the skill. If
// D22-D28 are effective at all, they should show up most clearly here.
console.log('\n# Single-session tickets only (no CCR, no rework)');
console.log('');
const beforeSingle = summary.filter(s => s.group === 'BEFORE' && s.sessions === 1).map(s => s.total);
const afterSingle  = summary.filter(s => s.group === 'AFTER'  && s.sessions === 1).map(s => s.total);
const bSS = stats(beforeSingle);
const aSS = stats(afterSingle);
console.log('| Group | N | Mean | Median | Min | Max |');
console.log('|---|---|---|---|---|---|');
console.log(`| BEFORE | ${bSS.n} | ${fmtTokens(bSS.mean||0)} | ${fmtTokens(bSS.median||0)} | ${fmtTokens(bSS.min||0)} | ${fmtTokens(bSS.max||0)} |`);
console.log(`| AFTER  | ${aSS.n} | ${fmtTokens(aSS.mean||0)} | ${fmtTokens(aSS.median||0)} | ${fmtTokens(aSS.min||0)} | ${fmtTokens(aSS.max||0)} |`);

if (bSS.median && aSS.median) {
  const pct = ((aSS.median - bSS.median) / bSS.median * 100).toFixed(1);
  console.log(`\nSingle-session median change: ${pct}%`);
}
if (bSS.mean && aSS.mean) {
  const pct = ((aSS.mean - bSS.mean) / bSS.mean * 100).toFixed(1);
  console.log(`Single-session mean change:   ${pct}%`);
}

// Also show: per-assistant-message cost (normalizes for scope variance)
console.log('\n# Per-assistant-message cost (tokens/msg)');
function perMsg(items) {
  return items.map(s => s.total / Math.max(1, s.assistant_msgs));
}
const bPM = stats(perMsg(summary.filter(s => s.group === 'BEFORE')));
const aPM = stats(perMsg(summary.filter(s => s.group === 'AFTER')));
console.log('');
console.log('| Group | Mean | Median |');
console.log('|---|---|---|');
console.log(`| BEFORE | ${fmtTokens(bPM.mean||0)}/msg | ${fmtTokens(bPM.median||0)}/msg |`);
console.log(`| AFTER  | ${fmtTokens(aPM.mean||0)}/msg | ${fmtTokens(aPM.median||0)}/msg |`);

// Detail: per-session for each ticket
console.log('\n# Per-session detail');
for (const s of summary) {
  if (s.group === 'MID') continue;
  console.log(`\n## ${s.ticket} [${s.group}]`);
  for (const sess of s.session_detail) {
    console.log(`  ${sess.started_at.slice(0,16)}  sess=${sess.session_id.slice(0,8)}  a-msgs=${sess.assistant_msgs}  tokens=${fmtTokens(sess.total)} (in=${fmtTokens(sess.input_tokens)} out=${fmtTokens(sess.output_tokens)} cc=${fmtTokens(sess.cache_creation)} cr=${fmtTokens(sess.cache_read)})`);
  }
}

db.close();
