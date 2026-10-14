// The Global Aggregate — The Wire ingestion (primary-source policy/legal items)
//
// SEPARATE from ingest.js / ingest-rss.js on purpose: it writes to wire_items,
// never to articles, and does NOT require ingest.js (which process.exit()s at
// require-time when unrelated API keys are missing).
//
// Week 1 sources:
//   - Federal Register API (no key): rules, proposed rules, notices, presidential docs
//   - CISA RSS feeds: cybersecurity advisories / alerts
//
// Design rules (see PRD in project knowledge):
//   - Cache-first: users only ever query wire_items; upstream limits never matter to users.
//   - One source failing must not stop the others. Exit non-zero only if EVERY source failed.
//   - Idempotent: unique (source, source_id); existing rows are left untouched
//     (ignoreDuplicates) so later AI summaries are never overwritten by a re-ingest.
//   - Excerpts only (<=500 chars); the link goes to the primary source.

const { createClient } = require('@supabase/supabase-js');
const Parser = require('rss-parser');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const missing = ['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'].filter((k) => !process.env[k]);
if (missing.length > 0) {
  console.error(`Missing required secrets: ${missing.join(', ')}. Check your GitHub secrets.`);
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const UA = 'Mozilla/5.0 (compatible; TheGlobalAggregateWire/1.0; +https://globalaggregate.org)';
const LOOKBACK_DAYS = Number(process.env.WIRE_LOOKBACK_DAYS) || 3;
const EXCERPT_MAX = 500;

// CISA feed URLs are UNVERIFIED from the build sandbox (egress blocked there).
// The first real run's log is the verification: any feed that logs FAILED
// needs its URL corrected, nothing else.
const CISA_FEEDS = [
  { name: 'cisa-advisories', url: 'https://www.cisa.gov/cybersecurity-advisories/all.xml' },
];

const parser = new Parser({ timeout: 15000, headers: { 'User-Agent': UA } });

// ---------- helpers ----------

function cap(text, n = EXCERPT_MAX) {
  if (!text) return null;
  const clean = String(text).replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
  if (!clean) return null;
  return clean.length > n ? clean.slice(0, n - 1).trimEnd() + '…' : clean;
}

function safeDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

// Simple, transparent keyword tagging. Deliberately coarse for v1; the filter
// UI needs *some* topic signal now, and this is easy to inspect and extend.
const TOPIC_RULES = [
  ['privacy', /\b(privacy|personal (data|information)|data protection|gdpr|ccpa|coppa|data broker|biometric)\b/i],
  ['cybersecurity', /\b(cyber|ransomware|malware|vulnerabilit|CVE-\d|exploit|breach|incident reporting|CIRCIA|zero[- ]day|cisa)\b/i],
  ['ai', /\b(artificial intelligence|machine learning|\bAI\b|algorithm)/i],
  ['telecom', /\b(telecommunication|broadband|spectrum|FCC)\b/i],
  ['finance', /\b(securities|banking|SEC |treasury|financial (institution|stability)|anti-money)/i],
  ['health', /\b(HIPAA|health (care|information)|medicare|medicaid|FDA)\b/i],
  ['energy', /\b(energy|pipeline|electric grid|nuclear|NERC)\b/i],
  ['trade', /\b(tariff|export control|sanction|OFAC|customs)\b/i],
];

function tagTopics(...parts) {
  const hay = parts.filter(Boolean).join(' ');
  return TOPIC_RULES.filter(([, re]) => re.test(hay)).map(([t]) => t);
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { 'User-Agent': UA, Accept: 'application/json' }, signal: AbortSignal.timeout(30000) });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function upsertItems(label, rows) {
  if (rows.length === 0) return { inserted: 0 };
  let inserted = 0;
  for (let i = 0; i < rows.length; i += 200) {
    const chunk = rows.slice(i, i + 200);
    const { data, error } = await supabase
      .from('wire_items')
      .upsert(chunk, { onConflict: 'source,source_id', ignoreDuplicates: true })
      .select('id');
    if (error) throw new Error(`[${label}] upsert failed: ${error.message}`);
    inserted += data ? data.length : 0;
  }
  return { inserted };
}

// ---------- Federal Register ----------

const FR_TYPE_MAP = {
  Rule: 'rule_notice',
  'Proposed Rule': 'rule_notice',
  Notice: 'rule_notice',
  'Presidential Document': 'rule_notice',
};

async function ingestFederalRegister() {
  const since = new Date(Date.now() - LOOKBACK_DAYS * 86400000).toISOString().slice(0, 10);
  const fields = ['document_number', 'title', 'abstract', 'html_url', 'publication_date', 'type', 'agency_names'];
  const params = new URLSearchParams({
    per_page: '200',
    order: 'newest',
    'conditions[publication_date][gte]': since,
  });
  fields.forEach((f) => params.append('fields[]', f));

  let url = `https://www.federalregister.gov/api/v1/documents.json?${params.toString()}`;
  const rows = [];
  let pages = 0;
  let reportedCount = null;

  while (url && pages < 10) {
    const json = await fetchJson(url);
    if (reportedCount === null) reportedCount = json.count;
    for (const d of json.results || []) {
      if (!d.document_number || !d.title || !d.html_url) continue;
      const agencies = Array.isArray(d.agency_names) ? d.agency_names : [];
      rows.push({
        source: 'federal_register',
        source_id: d.document_number,
        source_type: FR_TYPE_MAP[d.type] || 'rule_notice',
        jurisdiction: 'US-FED',
        title: d.title,
        url: d.html_url,
        published_at: safeDate(d.publication_date),
        excerpt: cap(d.abstract),
        topics: tagTopics(d.title, d.abstract),
        entities: agencies,
        language: 'en',
      });
    }
    url = json.next_page_url || null;
    pages++;
  }

  console.log(`[federal_register] API reports ${reportedCount} docs since ${since}; fetched ${rows.length} across ${pages} page(s)`);
  if (reportedCount !== null && rows.length < Math.min(reportedCount, 2000)) {
    console.warn(`[federal_register] WARNING: fetched ${rows.length} < reported ${reportedCount} (page cap or skipped rows); raise page cap if this persists`);
  }
  const { inserted } = await upsertItems('federal_register', rows);
  console.log(`[federal_register] new rows inserted: ${inserted} (rest already present)`);
  return { fetched: rows.length, inserted };
}

// ---------- CISA ----------

async function ingestCisa() {
  let fetched = 0;
  let inserted = 0;
  let feedsOk = 0;
  for (const feed of CISA_FEEDS) {
    try {
      const parsed = await parser.parseURL(feed.url);
      const rows = [];
      for (const it of parsed.items || []) {
        const link = it.link;
        const id = it.guid || link;
        if (!id || !it.title || !link) continue;
        rows.push({
          source: feed.name,
          source_id: String(id).slice(0, 500),
          source_type: 'agency_advisory',
          jurisdiction: 'US-FED',
          title: it.title.trim(),
          url: link,
          published_at: safeDate(it.isoDate || it.pubDate),
          excerpt: cap(it.contentSnippet || it.content || it.summary),
          topics: Array.from(new Set(['cybersecurity', ...tagTopics(it.title, it.contentSnippet)])),
          entities: ['CISA'],
          language: 'en',
        });
      }
      const r = await upsertItems(feed.name, rows);
      console.log(`[${feed.name}] OK: ${rows.length} items parsed, ${r.inserted} new`);
      fetched += rows.length;
      inserted += r.inserted;
      feedsOk++;
    } catch (err) {
      console.error(`[${feed.name}] FAILED (${feed.url}): ${err.message}`);
    }
  }
  if (feedsOk === 0) throw new Error('all CISA feeds failed');
  return { fetched, inserted };
}

// ---------- main ----------

(async () => {
  const started = Date.now();
  const sources = [
    ['federal_register', ingestFederalRegister],
    ['cisa', ingestCisa],
  ];
  let ok = 0;
  for (const [name, fn] of sources) {
    try {
      await fn();
      ok++;
    } catch (err) {
      console.error(`[${name}] SOURCE FAILED: ${err.message}`);
    }
  }
  console.log(`Wire ingestion done in ${((Date.now() - started) / 1000).toFixed(1)}s; ${ok}/${sources.length} sources succeeded`);
  if (ok === 0) process.exit(1);
})();
