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

// Filters out obvious non-news content (classified listings, SEO spam pages,
// betting/promo content, listicles, etc.) that free-tier news APIs sometimes
// mis-categorize as news. Grouped by category so it's easier to extend later.
const JUNK_PATTERNS = [
  // Classifieds / listings
  /for sale near/i,
  /used .* for sale/i,
  /autos on [\w.]+\.com/i,
  /real estate listings?/i,
  /homes? for sale/i,
  /jobs? near you/i,
  /hiring near/i,
  /\bclassifieds?\b/i,

  // Betting / gambling promos
  /promo code/i,
  /bonus code/i,
  /free bet/i,
  /risk-free bet/i,
  /odds boost/i,
  /betting offer/i,
  /\bsportsbook\b/i,
  /welcome bonus/i,
  /sign-?up bonus/i,
  /deposit bonus/i,
  /\bparlay\b/i,
  /bet \$?\d+/i,
  /get \$?\d+ (for|when|on)/i,
  /\b(bet|betting) (bonus|offer|promo)\b/i,
  /\b(dabble|draftkings|fanduel|betmgm|caesars sportsbook|pointsbet|bet365)\b/i,

  // "How to watch" / streaming guides (almost never real news)
  /how to watch .*(for free|live|online|stream)/i,
  /where to watch/i,
  /live stream(ing)? (guide|free|online)?/i,
  /watch .* online free/i,

  // Coupons / deals / shopping
  /\bcoupons?\b/i,
  /discount code/i,
  /\d+% off\b/i,
  /deal of the day/i,
  /best deals?\b/i,
  /price drop/i,
  /\bshop the sale\b/i,

  // Horoscope / astrology
  /\bhoroscope\b/i,
  /\bzodiac\b/i,
  /\bastrology\b/i,

  // Listicle / quiz / engagement-bait
  /you won.?t believe/i,
  /\bquiz\b/i,
  /which .* are you\??$/i,
  /\btop \d+\b.*(things|reasons|ways) (you|to)/i,
  // NEW: buying-guide/roundup listicles (e.g. "Best used hybrid cars under $50,000")
  /\bbest .{0,40}(under|over) \$\d/i,

  // Lottery / sweepstakes / giveaways
  /lottery numbers/i,
  /\bpowerball\b/i,
  /mega millions/i,
  /winning numbers/i,
  /\bsweepstakes\b/i,
  /\bgiveaway\b/i,

  // Sponsored / ad markers
  /\bsponsored\b/i,
  /\badvertisement\b/i,
  /\(ad\)/i,
  /paid partnership/i,

  // Non-news content mis-tagged by aggregators (e.g. GitHub repos showing up as "Tech" news)
  /^github - /i,
  // NEW: product/tool listing pages (e.g. "Desunofier - Make Suno AI Songs Sound More Human | InstaSong")
  /^[\w\s]+ - make .{0,60}\|/i,

  // Obituaries / memorial notices (not national news)
  /\bobituar(y|ies)\b/i,
  /in loving memory of/i,
  /ways to support the family/i,

  // Hyperlocal government/community news (not national news)
  /\bHOA\b/i,
  /homeowners? association/i,
  /\bcity council\b/i,
  /\bzoning\b/i,
  /\bschool board\b/i,
  /\bcounty commission(er)?\b/i,
  /\btownship\b/i,
  /\bplanning commission\b/i,
  /\bboard of education\b/i,
  // NEW: hyperlocal human-interest / nonprofit PR (e.g. "Single mother moves from homelessness to new Carson apartment")
  /\bhomeless(ness)? .{0,40}(apartment|nonprofit|shelter)\b/i,

  // NEW: dev-blog / tutorial content mis-tagged as Tech news
  /\b(deprecated|WP-CLI|npm install|git commit|stack trace)\b/i,

  // NEW: human-interest clickbait framing
  /\bfinally achiev\w+/i,
  /heartbreaking (setback|journey)/i,
  /against all odds/i,

  // NEW: Reddit-formatted titles/threads mis-tagged as news
  /\[link\]\s*\[comments\]/i,
  /^\/u\/\w+/i,
];

// Specific domains known to be content farms, press-release wires, or
// non-news sites that keep showing up mis-tagged as news by the APIs above.
const BLOCKED_SOURCE_DOMAINS = [
  'github.com',
  'legacy.com',
  'openpr.com',
  'chinanationalnews.com',
  'pekingpress.com',
  'shanghaisun.com',
  'beijingbulletin.com',
  'bignewsnetwork.com',
  'tickerreport.com',
  'dailypolitical.com',
  // NEW: Reddit threads getting ingested as if they were news articles
  'reddit.com',
  'old.reddit.com',
];

// International/pan-regional wire services (Reuters, France24, Euronews,
// AfricaNews, MENAFN, Channel News Asia, Al-Monitor) are intentionally NOT
// blocked. They're legitimate journalism -- they just aren't native to any
// single country, so country-specific filters will occasionally surface one
// with no real connection to the requested country (e.g. a Reuters story
// showing up under a "Turkey" filter). Accepted tradeoff: keep the real
// international coverage rather than lose it to fix country-purity. The
// actual fix, if this becomes a problem, is the planned national-outlet
// allowlist (Stage 1), not a wire-service blocklist.
function isBlockedSource(source) {
  if (!source) return false;
  const normalized = source.toLowerCase();
  return BLOCKED_SOURCE_DOMAINS.some((domain) => normalized.includes(domain));
}

// Sports coverage from these APIs skews heavily toward minor/local sports news.
// Rather than blocking specific junk, this allowlists major leagues/tournaments —
// a Sports-tagged article must mention one of these to be kept. This is a real
// tradeoff: it will occasionally cut a legitimate major story that happens not
// to use one of these terms, and it can't catch every major event by design.
const MAJOR_SPORTS_PATTERNS = [
  /\bNFL\b/i, /\bNBA\b/i, /\bMLB\b/i, /\bNHL\b/i, /\bMLS\b/i,
  /premier league/i, /la liga/i, /serie a/i, /bundesliga/i, /ligue 1/i,
  /champions league/i, /europa league/i, /\bFA cup\b/i,
  /copa am[ée]rica/i, /copa libertadores/i,
  /\bIPL\b/i, /\bICC\b/i, /\bT20\b/i, /\bASHES\b/i,
  /six nations/i, /rugby world cup/i, /super rugby/i,
  /wimbledon/i, /french open/i, /australian open/i, /\bATP\b/i, /\bWTA\b/i, /grand slam/i,
  /\bmasters\b/i, /\bPGA\b/i, /ryder cup/i,
  /formula (1|one)\b/i, /\bF1\b/i, /motogp/i, /\bNASCAR\b/i,
  /\bUFC\b/i, /world title/i, /heavyweight/i,
  /\bolympics?\b/i, /paralympics/i, /commonwealth games/i,
  /world cup/i, /\bFIFA\b/i, /\bUEFA\b/i, /\bCONMEBOL\b/i, /\bAFCON\b/i,
  /national team/i, /world championships?/i, /super bowl/i, /stanley cup/i, /world series/i,
];

function isObscureSports(row) {
  if (row.topic !== 'Sports') return false;
  return !MAJOR_SPORTS_PATTERNS.some((pattern) => pattern.test(row.title));
}

// International/pan-regional wire services -- legitimate journalism, not
// tied to any single country, so they're exempt from the per-country
// allowlist check below rather than required to match one specific country.
const WIRE_SERVICE_DOMAINS = [
  'reuters.com', 'france24.com', 'euronews.com', 'africanews.com',
  'menafn.com', 'channelnewsasia.com', 'almonitor.com',
];

// Major national outlets per country. A country-tagged article whose source
// domain isn't in its list (and isn't a wire service above) gets filtered as
// too-obscure/off-topic for that country -- this is the real fix for
// "national news only": pattern-matching titles can't tell hyperlocal from
// national, but a known-outlets list can.
// NOTE: this is necessarily incomplete -- built from what's actually
// appearing in the data plus general knowledge, not exhaustive research into
// every regional/minority-language outlet per country. Expect to add entries
// over time rather than treating this as final.
const ALLOWLIST_BY_COUNTRY = {
  AR: ['batimes.com.ar', 'clarin.com', 'lanacion.com.ar', 'infobae.com', 'canal26.com', 'buenosairesherald.com'],
  AU: ['smh.com.au', 'theage.com.au', 'abc.net.au', 'news.com.au', 'theaustralian.com.au', 'heraldsun.com.au', '7news.com.au', '9news.com.au', 'thewest.com.au', 'perthnow.com.au', 'ntnews.com.au', 'drive.com.au'],
  BD: ['thedailystar.net', 'dhakatribune.com', 'tbsnews.net', 'prothomalo.com', 'daily-sun.com'],
  BR: ['g1.globo.com', 'folha.uol.com.br', 'estadao.com.br', 'riotimesonline.com'],
  CA: ['cbc.ca', 'ctvnews.ca', 'globalnews.ca', 'theglobeandmail.com', 'nationalpost.com', 'thestar.com'],
  CD: ['radiookapi.net', 'actualite.cd'],
  CN: ['xinhuanet.com', 'chinadaily.com.cn', 'cgtn.com', 'scmp.com', 'globaltimes.cn', 'ecns.cn'],
  CO: ['elespectador.com', 'eltiempo.com', 'semana.com', 'vanguardia.com'],
  DE: ['dw.com', 'spiegel.de', 'faz.net', 'sueddeutsche.de', 'thelocal.de', 'zeit.de', 'tagesschau.de'],
  EG: ['egypttoday.com', 'egyptindependent.com', 'ahram.org.eg', 'dailynewsegypt.com'],
  ES: ['elpais.com', 'elmundo.es', 'abc.es', 'thelocal.es'],
  ET: ['ena.et', 'thereporterethiopia.com', 'addisstandard.com'],
  FR: ['lefigaro.fr', 'lemonde.fr', 'francetvinfo.fr', 'thelocal.fr', 'leparisien.fr', 'lesechos.fr'],
  GB: ['bbc.co.uk', 'theguardian.com', 'thetimes.co.uk', 'telegraph.co.uk', 'independent.co.uk', 'skynews.com', 'itv.com'],
  ID: ['thejakartapost.com', 'tempo.co', 'kompas.com', 'antaranews.com', 'jakartaglobe.id'],
  IN: ['timesofindia.com', 'hindustantimes.com', 'ndtv.com', 'thehindu.com', 'indianexpress.com', 'news18.com'],
  IR: ['en.irna.ir', 'mehrnews.com', 'presstv.ir', 'tehrantimes.com', 'irna.ir'],
  IT: ['ansa.it', 'corriere.it', 'repubblica.it', 'tgcom24.mediaset.it', 'ilsole24ore.com'],
  JP: ['japantimes.co.jp', 'japantoday.com', 'asahi.com', 'mainichi.jp', 'nhk.or.jp', 'kyodonews.net'],
  KE: ['nation.africa', 'standardmedia.co.ke', 'capitalfm.co.ke', 'citizen.digital', 'the-star.co.ke'],
  MX: ['eluniversal.com.mx', 'milenio.com', 'jornada.com.mx', 'mexiconewsdaily.com', 'elsoldemexico.com.mx'],
  NG: ['vanguardngr.com', 'punchng.com', 'thenationonlineng.net', 'premiumtimesng.com', 'dailypost.ng', 'businessday.ng'],
  PH: ['inquirer.net', 'philstar.com', 'manilatimes.net', 'rappler.com', 'gmanetwork.com', 'abs-cbn.com', 'sunstar.com.ph', 'mb.com.ph', 'tribune.net.ph'],
  PK: ['dawn.com', 'thenews.com.pk', 'tribune.com.pk', 'geo.tv', 'arynews.tv', 'dunyanews.tv', 'pakobserver.net', 'dailytimes.com.pk', 'brecorder.com', 'nation.com.pk'],
  RU: ['tass.com', 'rt.com', 'themoscowtimes.com', 'interfax.com'],
  TR: ['aa.com.tr', 'dailysabah.com', 'hurriyetdailynews.com', 'trtworld.com', 'birgun.net'],
  TZ: ['thecitizen.co.tz', 'dailynews.co.tz', 'ippmedia.com'],
  US: ['usatoday.com', 'nypost.com', 'nytimes.com', 'washingtonpost.com', 'cnn.com', 'apnews.com', 'npr.org', 'foxnews.com'],
  VN: ['vietnamnews.vn', 'vietnamplus.vn', 'tuoitrenews.vn', 'vnexpress.net', 'sggpnews.org.vn'],
  ZA: ['iol.co.za', 'timeslive.co.za', 'news24.com', 'sabcnews.com', 'ewn.co.za', 'citizen.co.za', 'sowetanlive.co.za'],
};

function isWireService(source) {
  if (!source) return false;
  const normalized = source.toLowerCase();
  return WIRE_SERVICE_DOMAINS.some((domain) => normalized.includes(domain));
}

function failsNationalAllowlist(row) {
  const list = ALLOWLIST_BY_COUNTRY[row.country];
  if (!list) return false; // no allowlist yet for this country -- don't filter, fall back to pattern-based junk rules only
  if (isWireService(row.source)) return false;
  if (!row.source) return true;
  const normalized = row.source.toLowerCase();
  return !list.some((domain) => normalized.includes(domain));
}

function isJunk(row) {
  if (!row.title) return true;
  if (isBlockedSource(row.source)) return true;
  if (isObscureSports(row)) return true;
  if (failsNationalAllowlist(row)) return true;
  return JUNK_PATTERNS.some((pattern) => pattern.test(row.title));
}

// Same story often gets republished verbatim across sister publications
// (different URL each time, so the DB's unique-URL constraint won't catch it).
// This tracks titles we've already seen — both already in the database and
// within this run — so syndicated repeats get skipped instead of piling up.
function normalizeTitle(title) {
  return (title || '').trim().toLowerCase();
}

async function loadExistingTitles() {
  const { data, error } = await supabase.from('articles').select('title');
  if (error) {
    console.error('Could not load existing titles for dedup, continuing without it:', error.message);
    return new Set();
  }
  return new Set(data.map((row) => normalizeTitle(row.title)));
}

async function upsertRows(countryName, rows, seenTitles) {
  const noJunk = rows.filter((row) => !isJunk(row));
  const junkSkipped = rows.length - noJunk.length;

  const deduped = [];
  let dupeSkipped = 0;
  for (const row of noJunk) {
    const key = normalizeTitle(row.title);
    if (seenTitles.has(key)) {
      dupeSkipped++;
      continue;
    }
    seenTitles.add(key);
    deduped.push(row);
  }

  if (junkSkipped > 0) console.log(`[${countryName}] Filtered out ${junkSkipped} junk/non-news item(s).`);
  if (dupeSkipped > 0) console.log(`[${countryName}] Skipped ${dupeSkipped} duplicate/syndicated title(s).`);

  if (deduped.length === 0) {
    console.warn(`[${countryName}] No new articles to insert.`);
    return { country: countryName, inserted: 0 };
  }
  const { error } = await supabase
    .from('articles')
    .upsert(deduped, { onConflict: 'url', ignoreDuplicates: true });

  if (error) {
    console.error(`[${countryName}] Supabase insert error: ${error.message}`);
    return { country: countryName, inserted: 0, error: error.message };
  }
  console.log(`[${countryName}] Upserted ${deduped.length} articles.`);
  return { country: countryName, inserted: deduped.length };
}

// Extracts a readable domain from a URL to use as the source label.
// All three APIs return source info in a different format (NewsData: a slug
// like "aa_tr", GNews: a display name, Currents: nothing usable at all), so
// deriving the domain from the article's own URL is the one thing that's
// consistent across all three — needed so blocklist/allowlist matching
// actually works uniformly instead of silently missing entries whose slug
// happened not to match (this is why openpr/bignewsnetwork/chinanationalnews
// were still leaking through the existing blocklist despite being listed).
function domainFromUrl(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return 'unknown';
  }
}

async function fetchNewsData(country) {
  const url = `https://newsdata.io/api/1/latest?apikey=${NEWSDATA_API_KEY}&country=${country.code.toLowerCase()}&language=en`;
  const res = await fetch(url);
  const data = await res.json();
  if (data.status !== 'success') throw new Error(data.message || 'NewsData.io error');

  const rows = (data.results || [])
    .filter((item) => item.title && item.link)
    .map((item) => ({
      source: domainFromUrl(item.link),
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
      source: domainFromUrl(item.url),
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
      source: domainFromUrl(item.url), // real domain, not a hardcoded label — needed so source-blocklist filtering actually works
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

  const seenTitles = await loadExistingTitles();
  console.log(`Loaded ${seenTitles.size} existing titles for dedup.\n`);

  const results = [];
  for (let i = 0; i < countries.length; i++) {
    const country = countries[i];
    const source = SOURCES[i % SOURCES.length];

    try {
      const rows = await source.fetcher(country);
      const result = await upsertRows(`${country.name} via ${source.name}`, rows, seenTitles);
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

  console.log('\nClustering related stories across countries...');
  const { error: clusterError } = await supabase.rpc('cluster_related_articles');
  if (clusterError) {
    console.error('Clustering failed (non-fatal):', clusterError.message);
  } else {
    console.log('Clustering complete.');
  }
}

main().catch((err) => {
  console.error('Ingestion failed:', err);
  process.exit(1);
});
