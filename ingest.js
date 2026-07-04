// The Global Aggregate — RSS ingestion script (MVP)
//
// Fetches an English-language Google News feed for each pilot country,
// parses the articles, and upserts them into the Supabase `articles` table.
//
// Run manually with: npm run ingest
// (Automated scheduling comes in a later step — this is fine to run by hand for now.)

require('dotenv').config();
const Parser = require('rss-parser');
const { createClient } = require('@supabase/supabase-js');
const countries = require('./countries.json');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error(
    'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. Copy .env.example to .env and fill both in.'
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const parser = new Parser({
  headers: {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  },
});

// English-language Google News feed, scoped to a given country.
// Some country/language combos aren't officially supported by Google News —
// if a country logs 0 articles below, that feed may need a manual check.
function feedUrlFor(countryCode) {
  return `https://news.google.com/rss?hl=en-US&gl=${countryCode}&ceid=${countryCode}:en`;
}

async function ingestCountry(country) {
  const url = feedUrlFor(country.code);
  let feed;
  try {
    feed = await parser.parseURL(url);
  } catch (err) {
    console.error(`[${country.name}] Failed to fetch/parse feed: ${err.message}`);
    return { country: country.name, inserted: 0, error: err.message };
  }

  const rows = (feed.items || []).map((item) => ({
    source: item.creator || feed.title || 'Google News',
    country: country.code,
    topic: 'World', // MVP: no classification yet, see PRD section 3.4
    title: item.title,
    url: item.link,
    published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
  }));

  if (rows.length === 0) {
    console.warn(`[${country.name}] No articles found — check feed manually: ${url}`);
    return { country: country.name, inserted: 0 };
  }

  // Upsert on url so re-running this script never creates duplicates
  const { error } = await supabase
    .from('articles')
    .upsert(rows, { onConflict: 'url', ignoreDuplicates: true });

  if (error) {
    console.error(`[${country.name}] Supabase insert error: ${error.message}`);
    return { country: country.name, inserted: 0, error: error.message };
  }

  console.log(`[${country.name}] Upserted ${rows.length} articles.`);
  return { country: country.name, inserted: rows.length };
}

async function main() {
  console.log(`Starting ingestion for ${countries.length} countries...\n`);

  const results = [];
  // Run sequentially (not in parallel) to avoid hammering Google News all at once
  for (const country of countries) {
    const result = await ingestCountry(country);
    results.push(result);
  }

  const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
  const failed = results.filter((r) => r.error || r.inserted === 0);

  console.log(`\nDone. ${totalInserted} articles processed across ${countries.length} countries.`);
  if (failed.length > 0) {
    console.log(`\nCountries with issues (0 articles or errors):`);
    failed.forEach((r) => console.log(`  - ${r.country}${r.error ? `: ${r.error}` : ' (empty feed)'}`));
  }
}

main().catch((err) => {
  console.error('Ingestion failed:', err);
  process.exit(1);
});
