// The Global Aggregate — RSS ingestion script (Stage 2: real-time backbone)
//
// This is a SEPARATE ingestion path from ingest.js, not a replacement for it.
// The 3 commercial APIs (NewsData, GNews, Currents) keep running on their
// existing 3-hour cadence via ingest.js -- they provide broad discovery and
// clean structured data. This script polls RSS feeds directly from outlets
// already confirmed real through the allowlist process, and is
// designed to run far more often (e.g. every 15 minutes) since RSS feeds
// have no per-request "credits" system, unlike the 3 APIs.
//
// PILOT BATCH: the FEED_URLS_BY_COUNTRY list below is a starting batch, not
// an exhaustive one. Some entries were fetch-verified this session (BBC,
// Al Jazeera, the India feeds); others are well-documented standard feed
// URLs that haven't been individually fetch-tested yet. This is deliberate,
// matching the same process already validated for the API-based allowlist:
// don't try to pre-verify everything by hand -- turn it on, let the first
// run's logging tell you which feed URLs actually resolve (200 + valid XML)
// vs which need correcting (404, moved, wrong path), and fix only the ones
// that turn out broken. Expect to prune/expand this list after the first
// few real runs, not to get it perfect on the first try.

const { createClient } = require('@supabase/supabase-js');
const Parser = require('rss-parser');
const {
  getJunkReason,
  capDescription,
  mapTopic,
  normalizeTitle,
} = require('./ingest.js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const missing = ['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'].filter((k) => !process.env[k]);
if (missing.length > 0) {
  console.error(`Missing required secrets: ${missing.join(', ')}. Check your GitHub secrets.`);
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
// nation.africa returned a 403 with the default (generic/missing) User-Agent
// rss-parser sends -- a common form of basic bot-blocking some sites apply.
// A realistic browser User-Agent is a standard, low-risk workaround.
const parser = new Parser({
  timeout: 10000,
  headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function domainFromUrl(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return 'unknown';
  }
}

// Pilot batch. Each entry: country ISO-2 code -> array of {source, feedUrl}.
// "WORLD" is used for wire-service feeds not tied to one specific country --
// these get tagged per-country later based on which countries' runs they're
// checked against, same as ingest.js's wire-relevance logic.
const FEED_URLS_BY_COUNTRY = {
  // Wires -- fetch-verified this session, genuinely real and live
  WORLD: [
    { source: 'bbc.com', feedUrl: 'https://feeds.bbci.co.uk/news/rss.xml' },
    { source: 'aljazeera.com', feedUrl: 'https://www.aljazeera.com/xml/rss/all.xml' },
  ],
  // India -- fetch-verified via search this session (real, current feed URLs)
  IN: [
    { source: 'timesofindia.indiatimes.com', feedUrl: 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms' },
    { source: 'ndtv.com', feedUrl: 'http://feeds.feedburner.com/ndtvnews-top-stories' },
    { source: 'indianexpress.com', feedUrl: 'https://indianexpress.com/feed/' },
  ],
  // The following are well-documented standard feed URLs for outlets already
  // on the API-based allowlist, NOT individually fetch-tested this session --
  // exactly the kind of entry the diagnostic logging below is meant to
  // validate or correct on the first real run.
  GB: [
    { source: 'theguardian.com', feedUrl: 'https://www.theguardian.com/uk/rss' },
    { source: 'bbc.co.uk', feedUrl: 'https://feeds.bbci.co.uk/news/uk/rss.xml' },
  ],
  US: [
    { source: 'nytimes.com', feedUrl: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml' },
  ],
  TR: [
    { source: 'dailysabah.com', feedUrl: 'https://www.dailysabah.com/rssFeed/10000' },
  ],
  NG: [
    { source: 'punchng.com', feedUrl: 'https://punchng.com/feed/' },
  ],
  KE: [
    { source: 'nation.africa', feedUrl: 'https://nation.africa/kenya/rss' },
  ],
  // Fetch-verified via search this session (real, current feed URLs)
  PK: [
    { source: 'dawn.com', feedUrl: 'https://www.dawn.com/feeds/home' },
  ],
  TH: [
    { source: 'bangkokpost.com', feedUrl: 'https://www.bangkokpost.com/rss/data/topstories.xml' },
  ],
  GH: [
    { source: 'myjoyonline.com', feedUrl: 'https://www.myjoyonline.com/feed/' },
  ],
  // NEW: added for the 7 countries confirmed to throw real Currents API
  // errors (not just empty results) -- Currents documents covering ~70
  // countries total, so these are very likely just outside that supported
  // set, not a fixable allowlist problem. RSS has no such coverage ceiling.
  // Fetch-verified this session (live content confirmed directly):
  MA: [
    { source: 'moroccoworldnews.com', feedUrl: 'https://www.moroccoworldnews.com/feed/' },
  ],
  LK: [
    { source: 'dailymirror.lk', feedUrl: 'https://www.dailymirror.lk/rss' },
  ],
  // Verified via a real feed-listing source this session, not directly
  // fetch-tested. allafrica.com's English feed avoids the problem that
  // Senegal's major domestic outlets (Seneweb, Le Soleil, APS) are all
  // French-only and would likely be caught by the non-English filter.
  SN: [
    { source: 'allafrica.com', feedUrl: 'http://allafrica.com/tools/headlines/rdf/senegal/headlines.rdf' },
  ],
  FJ: [
    { source: 'fbcnews.com.fj', feedUrl: 'https://www.fbcnews.com.fj/feed/' },
  ],
  // NOT fetch-tested and NOT verified via a feed listing -- pattern-matched
  // from the same Nation Media Group platform as Kenya's nation.africa
  // entry above. Exactly the kind of entry the diagnostic logging is meant
  // to validate or correct on the first real run.
  UG: [
    { source: 'monitor.co.ug', feedUrl: 'https://www.monitor.co.ug/uganda/rss' },
  ],
  // NOT fetch-tested -- standard WordPress /feed/ convention guessed from
  // the domain, no feed listing found to confirm. Same caveat as above.
  PG: [
    { source: 'postcourier.com.pg', feedUrl: 'https://postcourier.com.pg/feed/' },
  ],
  // NEW: added for the 5 countries still confirmed at zero articles across
  // BOTH the API pipeline (empty/error results in ingest.js logs) and RSS
  // (never had a feed at all until now).
  NP: [
    { source: 'onlinekhabar.com', feedUrl: 'https://www.onlinekhabar.com/feed' },
  ],
  GR: [
    { source: 'greekreporter.com', feedUrl: 'https://greekreporter.com/greece/feed' },
  ],
  // Verified via an rssing.com archive listing showing the live RSS URL
  // directly, plus current (July 2026) live content confirmed on the site.
  ZW: [
    { source: 'newsday.co.zw', feedUrl: 'https://www.newsday.co.zw/feed' },
  ],
  // Pattern-matched from MercoPress's per-country RSS structure, confirmed
  // working for 4 sibling countries (Uruguay, Argentina, Venezuela,
  // Paraguay) via the same en.mercopress.com/rss/{country} URL shape --
  // not individually fetch-tested for these two specific slugs.
  EC: [
    { source: 'en.mercopress.com', feedUrl: 'https://en.mercopress.com/rss/ecuador' },
  ],
  BO: [
    { source: 'en.mercopress.com', feedUrl: 'https://en.mercopress.com/rss/bolivia' },
  ],
};

async function loadExistingTitles() {
  const { data, error } = await supabase.from('articles').select('title');
  if (error) {
    console.error('Could not load existing titles for dedup, continuing without it:', error.message);
    return new Set();
  }
  return new Set(data.map((row) => normalizeTitle(row.title)));
}

async function fetchFeed(feedUrl) {
  // Hard backstop independent of rss-parser's own `timeout` option (see
  // module-level comment on the Parser config) -- confirmed necessary after
  // a real hang (run #11, 2026-07-13) sat "In progress" for 3h47m+ instead
  // of erroring out at the configured 10s. If the library's internal timeout
  // doesn't fire for some reason, this one still will.
  const HARD_TIMEOUT_MS = 15000;
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error(`Hard timeout after ${HARD_TIMEOUT_MS}ms (rss-parser's own timeout did not fire)`)), HARD_TIMEOUT_MS)
  );
  const feed = await Promise.race([parser.parseURL(feedUrl), timeout]);
  return feed.items || [];
}

function buildRow(item, country, source) {
  return {
    source,
    country,
    topic: mapTopic(item.categories),
    title: item.title ? item.title.trim() : null,
    description: capDescription(item.contentSnippet || item.content || item.summary || null),
    url: item.link,
    published_at: item.isoDate ? new Date(item.isoDate).toISOString() : (item.pubDate ? new Date(item.pubDate).toISOString() : null),
  };
}

async function processFeed(country, feedEntry, seenTitles, seenUrls) {
  const label = `${country} via RSS (${feedEntry.source})`;
  let items;
  try {
    items = await fetchFeed(feedEntry.feedUrl);
  } catch (err) {
    console.error(`[${label}] Feed fetch failed: ${err.message} -- URL: ${feedEntry.feedUrl}`);
    return { label, inserted: 0, error: err.message };
  }

  if (items.length === 0) {
    console.warn(`[${label}] Feed returned zero items.`);
    return { label, inserted: 0 };
  }

  const rows = items
    .filter((item) => item.title && item.link)
    .map((item) => buildRow(item, country, feedEntry.source));

  const reasonCounts = {};
  const blockedSources = new Set();
  const clean = [];
  for (const row of rows) {
    if (seenUrls.has(row.url)) {
      reasonCounts['already_seen_url'] = (reasonCounts['already_seen_url'] || 0) + 1;
      continue;
    }
    const reason = getJunkReason(row);
    if (reason === null) {
      const key = normalizeTitle(row.title);
      if (seenTitles.has(key)) {
        reasonCounts['duplicate_title'] = (reasonCounts['duplicate_title'] || 0) + 1;
        continue;
      }
      seenTitles.add(key);
      seenUrls.add(row.url);
      clean.push(row);
    } else {
      reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
      if (reason === 'not_relevant_to_country' && row.source) blockedSources.add(row.source);
    }
  }

  const filteredCount = rows.length - clean.length;
  if (filteredCount > 0) {
    const breakdown = Object.entries(reasonCounts).map(([r, c]) => `${r}: ${c}`).join(', ');
    console.log(`[${label}] Filtered out ${filteredCount} item(s) -- ${breakdown}.`);
  }

  if (clean.length === 0) {
    console.warn(`[${label}] No new articles to insert.`);
    return { label, inserted: 0 };
  }

  const { error } = await supabase.from('articles').upsert(clean, { onConflict: 'url', ignoreDuplicates: true });
  if (error) {
    console.error(`[${label}] Supabase insert error: ${error.message}`);
    return { label, inserted: 0, error: error.message };
  }
  console.log(`[${label}] Upserted ${clean.length} articles.`);
  return { label, inserted: clean.length };
}

async function main() {
  const countryCodes = Object.keys(FEED_URLS_BY_COUNTRY);
  const totalFeeds = countryCodes.reduce((sum, c) => sum + FEED_URLS_BY_COUNTRY[c].length, 0);
  console.log(`Starting RSS ingestion: ${totalFeeds} feed(s) across ${countryCodes.length} country group(s)...\n`);

  const seenTitles = await loadExistingTitles();
  const { data: existingUrls } = await supabase.from('articles').select('url');
  const seenUrls = new Set((existingUrls || []).map((r) => r.url));
  console.log(`Loaded ${seenTitles.size} existing titles / ${seenUrls.size} existing URLs for dedup.\n`);

  const results = [];
  for (const country of countryCodes) {
    // WORLD (wire) feeds get checked against every real country, since their
    // relevance depends on content (does it mention Kenya, Poland, etc.),
    // not which feed group they were fetched under -- same principle as the
    // wire-relevance check in ingest.js.
    const targetCountries = country === 'WORLD' ? countryCodes.filter((c) => c !== 'WORLD') : [country];

    for (const feedEntry of FEED_URLS_BY_COUNTRY[country]) {
      if (country === 'WORLD') {
        // Fetch once, but the row's country needs to be a real tag for
        // filtering/display purposes. Wire items get evaluated against each
        // real target country in turn; a story only gets inserted for a
        // country it's actually relevant to.
        let items;
        try {
          items = await fetchFeed(feedEntry.feedUrl);
        } catch (err) {
          console.error(`[WORLD via RSS (${feedEntry.source})] Feed fetch failed: ${err.message}`);
          await sleep(1000);
          continue;
        }
        for (const targetCountry of targetCountries) {
          const rows = items
            .filter((item) => item.title && item.link)
            .map((item) => buildRow(item, targetCountry, feedEntry.source));
          const clean = rows.filter((row) => {
            if (seenUrls.has(row.url)) return false;
            const reason = getJunkReason(row);
            if (reason !== null) return false;
            const key = normalizeTitle(row.title);
            if (seenTitles.has(key)) return false;
            seenTitles.add(key);
            seenUrls.add(row.url);
            return true;
          });
          if (clean.length > 0) {
            const { error } = await supabase.from('articles').upsert(clean, { onConflict: 'url', ignoreDuplicates: true });
            if (!error) {
              console.log(`[${targetCountry} via RSS (${feedEntry.source})] Upserted ${clean.length} relevant article(s).`);
              results.push({ label: `${targetCountry} via ${feedEntry.source}`, inserted: clean.length });
            }
          }
        }
      } else {
        const result = await processFeed(country, feedEntry, seenTitles, seenUrls);
        results.push(result);
      }
      await sleep(1000); // be a polite RSS consumer even though there's no hard rate limit
    }
  }

  const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
  console.log(`\nDone. ${totalInserted} articles processed across ${totalFeeds} feed(s).`);

  console.log('\nClustering related stories across countries...');
  // RSS runs every 15 minutes, not every 3 hours like the API pipeline --
  // the function's default process_window_hours (6h) was sized for that
  // slower cadence. At RSS's frequency, a 6h window means re-scanning the
  // same growing backlog ~24 times within that window, which is what caused
  // a real timeout (confirmed: 478 of 483 articles in the last 6h were still
  // unclustered at the time it failed). 1 hour comfortably covers several
  // missed cycles' worth of buffer without re-scanning that much backlog.
  const { error: clusterError } = await supabase.rpc('cluster_related_articles', { process_window_hours: 1 });
  if (clusterError) {
    console.error('Clustering failed (non-fatal):', clusterError.message);
  } else {
    console.log('Clustering complete.');
  }
}

main().catch((err) => {
  console.error('RSS ingestion failed:', err);
  process.exit(1);
});
