// The Global Aggregate — ingestion script (MVP v2)
//
// Switched from Google News RSS to NewsData.io: Google News blocks/rate-limits
// requests from cloud/CI IP ranges (like GitHub Actions runners), which is why
// the first version returned 0 articles for every country. NewsData.io is a
// real API built for exactly this kind of automated access.
//
// Free tier: 200 requests/day. This script uses 1 request per country (30
// total per run), so it's scheduled every 4 hours to stay well under that cap.

const { createClient } = require('@supabase/supabase-js');
const countries = require('./countries.json');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const NEWSDATA_API_KEY = process.env.NEWSDATA_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !NEWSDATA_API_KEY) {
  console.error(
    'Missing one of: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, NEWSDATA_API_KEY. Check your GitHub secrets.'
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Maps NewsData.io's category values onto our fixed MVP topic list.
// Anything not in this map falls back to "World".
const TOPIC_MAP = {
  politics: 'Politics',
  business: 'Business',
  technology: 'Tech',
  sports: 'Sports',
  health: 'Health',
  world: 'World',
};

function mapTopic(categories) {
  if (!Array.isArray(categories) || categories.length === 0) return 'World';
  const first = String(categories[0]).toLowerCase();
  return TOPIC_MAP[first] || 'World';
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ingestCountry(country) {
  const url = `https://newsdata.io/api/1/latest?apikey=${NEWSDATA_API_KEY}&country=${country.code.toLowerCase()}&language=en`;

  let data;
  try {
    const response = await fetch(url);
    data = await response.json();

    if (data.status !== 'success') {
      console.error(`[${country.name}] API error: ${data.message || JSON.stringify(data)}`);
      return { country: country.name, inserted: 0, error: data.message };
    }
  } catch (err) {
    console.error(`[${country.name}] Fetch failed: ${err.message}`);
    return { country: country.name, inserted: 0, error: err.message };
  }

  const results = data.results || [];

  if (results.length === 0) {
    console.warn(`[${country.name}] No articles returned.`);
    return { country: country.name, inserted: 0 };
  }

  const rows = results
    .filter((item) => item.title && item.link)
    .map((item) => ({
      source: item.source_id || 'NewsData.io',
      country: country.code,
      topic: mapTopic(item.category),
      title: item.title,
      url: item.link,
      published_at: item.pubDate ? new Date(item.pubDate).toISOString() : null,
    }));

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
  for (const country of countries) {
    const result = await ingestCountry(country);
    results.push(result);
    await sleep(1500); // stay well under NewsData.io's short-window rate limit
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
