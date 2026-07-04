// The Global Aggregate — ingestion script (Phase 2: multi-source)
//
// Splits the 30 pilot countries evenly across three free news APIs so no
// single provider's daily cap becomes the bottleneck:
//   - NewsData.io   (~200 req/day free)  -> 10 countries
//   - GNews         (~100 req/day free)  -> 10 countries
//   - Currents API  (~600-1000 req/day free) -> 10 countries
//
// At 10 requests per source per run, running every 3 hours (8 runs/day)
// uses ~80 requests/day per source — safely under every provider's cap,
// with margin left for manual test runs.

const { createClient } = require('@supabase/supabase-js');
const countries = require('./countries.json');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const NEWSDATA_API_KEY = process.env.NEWSDATA_API_KEY;
const GNEWS_API_KEY = process.env.GNEWS_API_KEY;
const CURRENTS_API_KEY = process.env.CURRENTS_API_KEY;

const required = {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  NEWSDATA_API_KEY,
  GNEWS_API_KEY,
  CURRENTS_API_KEY,
};
const missing = Object.entries(required)
  .filter(([, v]) => !v)
  .map(([k]) => k);
if (missing.length > 0) {
  console.error(`Missing required secrets: ${missing.join(', ')}. Check your GitHub secrets.`);
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Normalizes various category label formats onto our fixed MVP topic list.
// Anything unrecognized falls back to "World".
function mapTopic(rawCategory) {
  if (!rawCategory) return 'World';
  const cats = Array.isArray(rawCategory) ? rawCategory : [rawCategory];
  const first = String(cats[0] || '').toLowerCase();

  if (first.includes('polit')) return 'Politics';
  if (first.includes('business') || first.includes('economy') || first.includes('finance')) return 'Business';
  if (first.includes('tech')) return 'Tech';
  if (first.includes('sport')) return 'Sports';
  if (first.includes('health')) return 'Health';
  return 'World';
}

async function upsertRows(countryName, rows) {
  if (rows.length === 0) {
    console.warn(`[${countryName}] No articles returned.`);
    return { country: countryName, inserted: 0 };
  }
  const { error } = await supabase
    .from('articles')
    .upsert(rows, { onConflict: 'url', ignoreDuplicates: true });

  if (error) {
    console.error(`[${countryName}] Supabase insert error: ${error.message}`);
    return { country: countryName, inserted: 0, error: error.message };
  }
  console.log(`[${countryName}] Upserted ${rows.length} articles.`);
  return { country: countryName, inserted: rows.length };
}

async function fetchNewsData(country) {
  const url = `https://newsdata.io/api/1/latest?apikey=${NEWSDATA_API_KEY}&country=${country.code.toLowerCase()}&language=en`;
  const res = await fetch(url);
  const data = await res.json();
  if (data.status !== 'success') throw new Error(data.message || 'NewsData.io error');

  const rows = (data.results || [])
    .filter((item) => item.title && item.link)
    .map((item) => ({
      source: item.source_id || 'NewsData.io',
      country: country.code,
      topic: mapTopic(item.category),
      title: item.title,
      description: item.description || null,
      url: item.link,
      published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
    }));
  return rows;
}

async function fetchGNews(country) {
  const url = `https://gnews.io/api/v4/top-headlines?country=${country.code.toLowerCase()}&lang=en&max=10&apikey=${GNEWS_API_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!data.articles) throw new Error(data.errors ? JSON.stringify(data.errors) : 'GNews error');

  // GNews top-headlines doesn't return a per-article category, so these default to "World".
  const rows = data.articles
    .filter((item) => item.title && item.url)
    .map((item) => ({
      source: (item.source && item.source.name) || 'GNews',
      country: country.code,
      topic: 'World',
      title: item.title,
      description: item.description || null,
      url: item.url,
      published_at: item.publishedAt ? new Date(item.publishedAt).toISOString() : null,
    }));
  return rows;
}

async function fetchCurrents(country) {
  const url = `https://api.currentsapi.services/v1/latest-news?language=en&country=${country.code}&apiKey=${CURRENTS_API_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if (data.status !== 'ok') throw new Error(data.message || 'Currents API error');

  const rows = (data.news || [])
    .filter((item) => item.title && item.url)
    .map((item) => ({
      source: 'Currents',
      country: country.code,
      topic: mapTopic(item.category),
      title: item.title,
      description: item.description || null,
      url: item.url,
      published_at: item.published ? new Date(item.published).toISOString() : null,
    }));
  return rows;
}

// Round-robin assignment: every 3rd country goes to the same source,
// splitting the 30 countries into 10/10/10 across the three providers.
const SOURCES = [
  { name: 'NewsData.io', fetcher: fetchNewsData },
  { name: 'GNews', fetcher: fetchGNews },
  { name: 'Currents', fetcher: fetchCurrents },
];

async function main() {
  console.log(`Starting ingestion for ${countries.length} countries across ${SOURCES.length} sources...\n`);

  const results = [];
  for (let i = 0; i < countries.length; i++) {
    const country = countries[i];
    const source = SOURCES[i % SOURCES.length];

    try {
      const rows = await source.fetcher(country);
      const result = await upsertRows(`${country.name} via ${source.name}`, rows);
      results.push(result);
    } catch (err) {
      console.error(`[${country.name} via ${source.name}] Failed: ${err.message}`);
      results.push({ country: country.name, inserted: 0, error: err.message });
    }

    await sleep(1500); // stay comfortably under each source's short-window rate limit
  }

  const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
  const failed = results.filter((r) => r.error || r.inserted === 0);

  console.log(`\nDone. ${totalInserted} articles processed across ${countries.length} countries.`);
  if (failed.length > 0) {
    console.log(`\nCountries with issues (0 articles or errors):`);
    failed.forEach((r) => console.log(`  - ${r.country}${r.error ? `: ${r.error}` : ' (empty result)'}`));
  }
}

main().catch((err) => {
  console.error('Ingestion failed:', err);
  process.exit(1);
});
