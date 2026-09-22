/**
 * FIX: Google News redirect-URL dedup gap + recurring boilerplate/aggregator pages
 * -----------------------------------------------------------------------------
 * ROOT CAUSE (confirmed against live data, 2026-09-22):
 *   news.google.com mints a NEW redirect URL for the same underlying story on
 *   every RSS poll cycle. Your existing dedup is keyed on `url` (DB unique
 *   constraint + generated `url_key`), so the same story re-inserts as a "new"
 *   row 3-6x. Confirmed live examples: "Russia Ukraine War - GazetteXtra" (4x),
 *   "Mideast Wars Yemen - <partner>" (4-6x across partner-name variants),
 *   plus recurring non-article boilerplate: "The Weather" (brecorder.com),
 *   "Haiti - News : Zapping..." (haitilibre.com), "<Outlet> ePAPER <date>".
 *
 * FIX SHAPE: two independent checks, both run BEFORE the existing URL-based
 * upsert, so they add pre-filtering rather than replacing anything.
 *
 * INTEGRATION (you'll need to adapt names to your actual ingest.js structure —
 * I don't have live repo access this session, so these are drop-in functions,
 * not a verified diff):
 *
 *   1. Import both functions into ingest.js:
 *        const { isAggregatorBoilerplate, isDuplicateGoogleNewsStory } = require('./google-news-dedup-fix');
 *
 *   2. In your per-row filtering loop, BEFORE the existing getJunkReason(row)
 *      call (or as an additional branch inside it), add:
 *
 *        if (isAggregatorBoilerplate(row.title, row.source)) {
 *          logJunk(row, 'aggregator_boilerplate_page');   // use your existing logging pattern
 *          continue; // skip this row entirely
 *        }
 *
 *   3. Immediately before your existing upsert-on-url step, for rows where
 *      row.source === 'news.google.com', check against recently-seen titles
 *      for that country (this run's batch AND a short DB lookback — Google
 *      re-polls the same story across multiple ingest runs, not just within
 *      one run, so an in-memory-only check isn't enough):
 *
 *        const seenThisRun = new Set(); // declare once per ingest run, before the row loop
 *        ...
 *        if (row.source === 'news.google.com') {
 *          const dupKey = googleNewsDedupKey(row.title, row.country);
 *          if (seenThisRun.has(dupKey)) {
 *            logJunk(row, 'gnews_duplicate_this_run');
 *            continue;
 *          }
 *          seenThisRun.add(dupKey);
 *
 *          const existing = await isDuplicateGoogleNewsStory(supabase, row.title, row.country);
 *          if (existing) {
 *            logJunk(row, 'gnews_duplicate_recent_db');
 *            continue;
 *          }
 *        }
 *
 *   4. No schema change needed — this fix operates entirely at the
 *      pre-insert filtering stage, same layer as your existing getJunkReason().
 */

// ---- Part 1: recurring boilerplate / topic-aggregator pages ----
// These are not individual articles — they're generic index/roundup pages that
// Google News (or the outlet's own RSS) re-emits under a fresh URL every cycle.
// Extend this list as you find more via the same log-driven process you already use.
const BOILERPLATE_PATTERNS = [
  // Google News topic-aggregator pages: "<Generic Topic> - <Partner Outlet>"
  // with no actual headline content — confirmed on Mideast Wars / Russia Ukraine War
  { sourceMatch: 'news.google.com', titleRegex: /^(Mideast Wars|Russia Ukraine War|Israel.{0,20}War)\s*-\s*/i },
  // Recurring generic section pages, confirmed per-outlet
  { sourceMatch: 'brecorder.com', titleRegex: /^The Weather$/i },
  { sourceMatch: 'haitilibre.com', titleRegex: /^Haiti - News\s*:\s*Zapping/i },
  // ePaper index listings (any outlet) — not article content
  { sourceMatch: null, titleRegex: /\bePAPER\b/i },
];

function isAggregatorBoilerplate(title, source) {
  if (!title) return false;
  return BOILERPLATE_PATTERNS.some(({ sourceMatch, titleRegex }) => {
    if (sourceMatch && source !== sourceMatch) return false;
    return titleRegex.test(title);
  });
}

// ---- Part 2: Google News redirect-URL dedup ----
// Normalize a title so trivial punctuation/whitespace differences between
// polling cycles don't defeat the match.
function normalizeTitle(title) {
  return (title || '')
    .toLowerCase()
    .replace(/[’‘]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function googleNewsDedupKey(title, country) {
  return `${country || ''}::${normalizeTitle(title)}`;
}

// Checks the DB for a same-title, same-country news.google.com article
// ingested within the lookback window (default 4 days — long enough to
// catch late re-polls of an aging story, short enough not to block a
// genuinely new story that happens to share a title years later).
async function isDuplicateGoogleNewsStory(supabase, title, country, lookbackDays = 4) {
  if (!title || !country) return false;
  const since = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from('articles')
    .select('id')
    .eq('source', 'news.google.com')
    .eq('country', country)
    .eq('title', title) // exact match; DB doesn't have a normalized-title column,
                          // so keep normalization consistent at write time if you
                          // want fuzzier matching later
    .gte('created_at', since)
    .limit(1);

  if (error) {
    // Fail open (don't block ingestion on a diagnostic query failure) but log it
    console.error('[gnews-dedup] lookup failed, allowing row through:', error.message);
    return false;
  }
  return Array.isArray(data) && data.length > 0;
}

module.exports = {
  isAggregatorBoilerplate,
  googleNewsDedupKey,
  isDuplicateGoogleNewsStory,
  normalizeTitle, // exported in case you want to reuse it in getJunkReason()
};
