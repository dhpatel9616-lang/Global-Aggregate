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
    // Moved here from broken per-country EC/BO guesses (both 404'd --
    // MercoPress doesn't appear to have dedicated Ecuador/Bolivia RSS
    // sub-feeds, consistent with them still being in the process of
    // joining Mercosur rather than full members). This confirmed general
    // Latin America feed gets evaluated against every South/Central
    // American country instead, a strict improvement over the narrower
    // per-country attempt.
    { source: 'en.mercopress.com', feedUrl: 'https://en.mercopress.com/rss/latin-america' },
  ],
  // India -- fetch-verified via search this session (real, current feed URLs)
  IN: [
    { source: 'timesofindia.indiatimes.com', feedUrl: 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms' },
    { source: 'ndtv.com', feedUrl: 'http://feeds.feedburner.com/ndtvnews-top-stories' },
    // Switched from the generic /feed/ (all sections) to the India-only
    // section feed. A random sample of 25 articles from the generic feed
    // (2026-07-16) showed roughly half were hyperlocal High Court rulings,
    // celebrity gossip, live-blog/digest posts, and state-government
    // funding announcements -- none of which belong in a national-
    // headlines aggregator. This section feed excludes Entertainment,
    // Lifestyle, Cities, and Opinion sections entirely.
    { source: 'indianexpress.com', feedUrl: 'https://indianexpress.com/section/india/feed/' },
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
  // greekreporter.com/greece/feed 403'd (likely IP-reputation blocking,
  // same category as Kenya/Morocco/Sri Lanka/Uganda -- a UA header alone
  // doesn't fix this class of block). Switched to a different outlet.
  GR: [
    { source: 'thenationalherald.com', feedUrl: 'https://www.thenationalherald.com/feed/' },
  ],
  // herald.co.zw 403'd (same IP-reputation pattern as Kenya/Morocco/Sri
  // Lanka/Uganda). Switched to zimlive.com -- confirmed live current
  // content when checked.
  ZW: [
    { source: 'zimlive.com', feedUrl: 'https://www.zimlive.com/feed/' },
  ],
  // jamaica-star.com threw "unable to verify the first certificate" -- a
  // real TLS cert chain issue on their end (likely a missing intermediate
  // cert), not a blocking issue. Trying plain http:// as a low-risk
  // workaround: RSS content isn't sensitive, and many older regional sites
  // still serve http even when their https cert chain is broken.
  JM: [
    { source: 'jamaica-star.com', feedUrl: 'http://jamaica-star.com/feed/news.xml' },
  ],
  // jordannews.jo's feed was malformed XML (unquoted attribute value --
  // broken on their end, not fixable client-side). Switched to Ammon News,
  // an established bilingual (Arabic/English) Jordanian outlet -- exact
  // English RSS path not independently fetch-verified, moderate confidence.
  JO: [
    { source: 'ammonnews.net', feedUrl: 'https://en.ammonnews.net/rss.php' },
  ],
  // dohanews.co 403'd. Switched to thepeninsulaqatar.com -- already proven
  // as a real, active outlet (it appeared as a legitimate Currents-sourced
  // article for DR Congo earlier this session), though that doesn't
  // guarantee its own RSS feed won't hit the same IP-reputation blocking
  // that's affected several other feeds -- worth checking the next log.
  QA: [
    { source: 'thepeninsulaqatar.com', feedUrl: 'https://thepeninsulaqatar.com/feed' },
  ],

  // --- Batch 3: countries with NO working API path at all (not on GNews's
  // 10-country cap, not on NewsData's cap, and not in Currents' real
  // supported-region list -- confirmed 2026-07-22 via a live call to
  // Currents' /v1/available/regions). RSS is the only remaining option for
  // these. Same discipline as batch 2: standard/documented feed paths,
  // NOT individually fetch-tested -- the next real run's diagnostic
  // logging (403s, malformed XML, non_english, etc.) is what actually
  // validates or corrects these, not pre-research. Expect a meaningfully
  // higher break rate here than batch 1/2 -- these are smaller, less
  // resourced outlets than BBC/Guardian/Times of India.
  AL: [{ source: 'albaniandailynews.com', feedUrl: 'https://albaniandailynews.com/index.php?feed=rss2' }],
  AM: [{ source: 'armenpress.am', feedUrl: 'https://armenpress.am/eng/rss/' }],
  AO: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/angola/headlines.rdf' }],
  AZ: [{ source: 'trend.az', feedUrl: 'https://en.trend.az/rss/' }],
  BG: [{ source: 'sofiaglobe.com', feedUrl: 'https://sofiaglobe.com/feed/' }],
  BH: [{ source: 'bna.bh', feedUrl: 'https://www.bna.bh/en/rss.aspx' }],
  BJ: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/benin/headlines.rdf' }],
  BN: [{ source: 'borneobulletin.com.bn', feedUrl: 'https://borneobulletin.com.bn/feed/' }],
  BW: [{ source: 'mmegi.bw', feedUrl: 'https://www.mmegi.bw/feed' }],
  CI: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/cotedivoire/headlines.rdf' }],
  CM: [{ source: 'journalducameroun.com', feedUrl: 'https://www.journalducameroun.com/en/feed/' }],
  CR: [{ source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/feed' }],
  CU: [{ source: 'havanatimes.org', feedUrl: 'https://havanatimes.org/feed/' }],
  CY: [{ source: 'in-cyprus.philenews.com', feedUrl: 'https://in-cyprus.philenews.com/feed/' }],
  DO: [{ source: 'dominicantoday.com', feedUrl: 'https://dominicantoday.com/feed/' }],
  DZ: [{ source: 'aps.dz', feedUrl: 'https://www.aps.dz/en/rss' }],
  GE: [{ source: 'agenda.ge', feedUrl: 'https://agenda.ge/en/rss' }],
  GY: [{ source: 'stabroeknews.com', feedUrl: 'https://www.stabroeknews.com/feed/' }],
  HR: [{ source: 'total-croatia-news.com', feedUrl: 'https://www.total-croatia-news.com/feed' }],
  HT: [{ source: 'haitiantimes.com', feedUrl: 'https://haitiantimes.com/feed/' }],
  IS: [{ source: 'icelandreview.com', feedUrl: 'https://www.icelandreview.com/feed/' }],
  KG: [{ source: '24.kg', feedUrl: 'https://24.kg/rss/' }],
  KZ: [{ source: 'astanatimes.com', feedUrl: 'https://astanatimes.com/feed/' }],
  LA: [{ source: 'laotiantimes.com', feedUrl: 'https://laotiantimes.com/feed/' }],
  LT: [{ source: 'lrt.lt', feedUrl: 'https://www.lrt.lt/en/rss' }],
  LY: [{ source: 'libyaobserver.ly', feedUrl: 'https://www.libyaobserver.ly/rss.xml' }],
  MD: [{ source: 'agora.md', feedUrl: 'https://agora.md/rss' }],
  MG: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/madagascar/headlines.rdf' }],
  MK: [{ source: 'mia.mk', feedUrl: 'https://mia.mk/feed/' }],
  ML: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/mali/headlines.rdf' }],
  MN: [{ source: 'montsame.mn', feedUrl: 'https://montsame.mn/en/rss' }],
  MW: [{ source: 'nyasatimes.com', feedUrl: 'https://www.nyasatimes.com/feed/' }],
  MZ: [{ source: 'clubofmozambique.com', feedUrl: 'https://clubofmozambique.com/feed/' }],
  NA: [{ source: 'namibian.com.na', feedUrl: 'https://www.namibian.com.na/feed/' }],
  OM: [{ source: 'omanobserver.om', feedUrl: 'https://www.omanobserver.om/rss' }],
  RW: [{ source: 'newtimes.co.rw', feedUrl: 'https://www.newtimes.co.rw/rss.xml' }],
  SD: [{ source: 'sudantribune.com', feedUrl: 'https://sudantribune.com/feed/' }],
  SK: [{ source: 'spectator.sme.sk', feedUrl: 'https://spectator.sme.sk/rss' }],
  SO: [{ source: 'garoweonline.com', feedUrl: 'https://www.garoweonline.com/en/feed' }],
  SY: [{ source: 'syrianobserver.com', feedUrl: 'https://syrianobserver.com/feed' }],
  TN: [{ source: 'africanmanager.com', feedUrl: 'https://africanmanager.com/feed/' }],
  TT: [{ source: 'newsday.co.tt', feedUrl: 'https://newsday.co.tt/feed/' }],
  UZ: [{ source: 'daryo.uz', feedUrl: 'https://daryo.uz/en/feed' }],
  ZM: [{ source: 'lusakatimes.com', feedUrl: 'https://www.lusakatimes.com/feed/' }],
  // NOT YET FOUND -- no plausible, reliably-updated English-language outlet
  // located for these in this pass. Real research needed, not a guess:
  // BF (Burkina Faso), GT (Guatemala), NI (Nicaragua), SV (El Salvador),
  // YE (Yemen, though it likely relies on wire coverage -- BBC/AJ/Reuters
  // mentioning it by name via the WORLD feeds -- rather than a domestic
  // outlet, since one may not reliably exist).

  // --- Batch 4: the 64 UN member/observer states added 2026-07-22 to reach
  // full 195-country coverage. Same discipline as batches 2/3 -- real
  // candidate outlets, standard feed paths, NOT individually fetch-tested.
  // Confidence varies a lot more here than earlier batches: several of
  // these countries have genuinely thin or state-controlled press, so
  // expect a higher break/skip rate than usual on the first real run.
  AG: [{ source: 'antiguaobserver.com', feedUrl: 'https://antiguaobserver.com/feed/' }],
  BS: [{ source: 'tribune242.com', feedUrl: 'https://www.tribune242.com/rss/news/' }],
  BB: [{ source: 'barbadostoday.bb', feedUrl: 'https://barbadostoday.bb/feed/' }],
  BZ: [{ source: 'breakingbelizenews.com', feedUrl: 'https://www.breakingbelizenews.com/feed' }],
  BT: [{ source: 'kuenselonline.com', feedUrl: 'https://kuenselonline.com/feed/' }],
  CG: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/republicofcongo/headlines.rdf' }],
  DM: [{ source: 'dominicanewsonline.com', feedUrl: 'https://dominicanewsonline.com/news/feed/' }],
  EE: [{ source: 'news.err.ee', feedUrl: 'https://news.err.ee/rss' }],
  SZ: [{ source: 'times.co.sz', feedUrl: 'https://times.co.sz/feed/' }],
  GA: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/gabon/headlines.rdf' }],
  GM: [{ source: 'thepoint.gm', feedUrl: 'https://thepoint.gm/posts/rss/xml' }], // confirmed real feed URL from page source (rel=alternate link tag) -- not a guess
  GD: [{ source: 'nowgrenada.com', feedUrl: 'https://nowgrenada.com/feed/' }],
  GN: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/guinea/headlines.rdf' }],
  VA: [{ source: 'vaticannews.va', feedUrl: 'https://www.vaticannews.va/en.rss.xml' }],
  LV: [{ source: 'eng.lsm.lv', feedUrl: 'https://eng.lsm.lv/rss/' }],
  LS: [{ source: 'lestimes.com', feedUrl: 'https://lestimes.com/feed/' }], // feed itself has an unescaped "&" in it (invalid XML on their end) -- not fixable by changing the URL, same class of issue as the Jordan/Zimbabwe feeds already accepted as known-broken
  LR: [{ source: 'fpa.news', feedUrl: 'https://fpa.news/feed/' }], // confirmed real feed URL from page source -- FrontPage Africa actually serves its feed from a completely different domain (fpa.news), not frontpageafricaonline.com
  LU: [{ source: 'luxtimes.lu', feedUrl: 'https://www.luxtimes.lu/rss' }],
  // MV (Maldives) confirmed NO RSS feed exists at all -- checked page source directly, no rel=alternate rss+xml tag anywhere (Nuxt SPA site). Not a path problem, genuinely not offered.
  MT: [{ source: 'timesofmalta.com', feedUrl: 'https://timesofmalta.com/rss.xml' }], // now resolves but returns 403 -- same IP-reputation/bot-blocking pattern as Kenya/Uganda/Morocco, not a path problem, no further URL guessing will help
  MR: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/mauritania/headlines.rdf' }],
  MU: [{ source: 'defimedia.info', feedUrl: 'https://defimedia.info/?feed=rss2' }], // feed exists at this path now (no more 404) but has malformed XML of its own -- source-side bug, same class as LS/TJ below
  MC: [{ source: 'monacotribune.com', feedUrl: 'https://www.monacotribune.com/feed/' }], // cert mismatch is a real misconfiguration on their shared host (cert covers a different domain entirely) -- not fixable by changing the URL path, leaving as-is; will keep failing harmlessly until they fix their TLS setup
  ME: [{ source: 'total-montenegro-news.com', feedUrl: 'https://total-montenegro-news.com/feed/' }],
  NE: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/niger/headlines.rdf' }],
  // PS (Palestine/WAFA) confirmed NO real RSS feed -- an RSS icon exists on the site but its href is just "#" (a dead placeholder), not an actual feed link. Not a path problem, genuinely not offered.
  // KN (St Kitts/sknvibes.com) confirmed NO RSS feed exists at all -- checked page source directly, no rss+xml link tag anywhere. Not a path problem, genuinely not offered.
  LC: [{ source: 'stluciatimes.com', feedUrl: 'https://stluciatimes.com/feed' }],
  VC: [{ source: 'iwnsvg.com', feedUrl: 'https://www.iwnsvg.com/feed/' }],
  // WS (Samoa Observer) confirmed NO RSS feed exists at all -- checked page source directly, no rss+xml link tag anywhere. Not a path problem, genuinely not offered.
  SC: [{ source: 'seychellesnewsagency.com', feedUrl: 'https://www.seychellesnewsagency.com/rss' }],
  SL: [{ source: 'thesierraleonetelegraph.com', feedUrl: 'https://www.thesierraleonetelegraph.com/feed/' }],
  SI: [{ source: 'sloveniatimes.com', feedUrl: 'https://sloveniatimes.com/feed' }],
  SB: [{ source: 'solomonstarnews.com', feedUrl: 'https://www.solomonstarnews.com/feed/' }],
  SS: [{ source: 'radiotamazuj.org', feedUrl: 'https://radiotamazuj.org/en/feed' }], // /en/rss.xml 404'd -- retrying /en/feed
  TJ: [{ source: 'asiaplustj.info', feedUrl: 'https://asiaplustj.info/en/rss/news' }], // same unescaped-"&" issue as LS above -- source-side XML bug, not fixable by URL changes
  TL: [{ source: 'en.tatoli.tl', feedUrl: 'https://en.tatoli.tl/feed/' }],
  TG: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/togo/headlines.rdf' }],
  TO: [{ source: 'matangitonga.to', feedUrl: 'https://matangitonga.to/feed/' }], // 404 without trailing slash -- retrying with one
  VU: [{ source: 'dailypost.vu', feedUrl: 'https://dailypost.vu/feed' }], // path is correct now (429 rate-limited, not 404) -- may succeed on a later run without any change needed
  // NOT FOUND -- no plausible independent English-language outlet located
  // in this pass, or (for BY/ER/KP/TM) the press is state-controlled to a
  // degree that a "national outlet" RSS feed isn't a meaningful concept --
  // these realistically depend on wire-service mentions (WORLD feed) or
  // exile/diaspora media that would need deliberate individual research:
  // AD (Andorra), BY (Belarus), BI (Burundi), CV (Cabo Verde),
  // CF (Central African Republic), TD (Chad), KM (Comoros),
  // DJ (Djibouti), GQ (Equatorial Guinea), ER (Eritrea),
  // GW (Guinea-Bissau), HN (Honduras), KI (Kiribati), LI (Liechtenstein),
  // MH (Marshall Islands), FM (Micronesia), NR (Nauru), KP (North Korea),
  // PW (Palau), SM (San Marino), ST (Sao Tome and Principe),
  // SR (Suriname), TM (Turkmenistan), TV (Tuvalu).
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
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Hard timeout after ${HARD_TIMEOUT_MS}ms (rss-parser's own timeout did not fire)`)), HARD_TIMEOUT_MS);
  });
  try {
    const feed = await Promise.race([parser.parseURL(feedUrl), timeout]);
    return feed.items || [];
  } finally {
    // Clear the timer regardless of which side of the race won -- left
    // dangling before, which kept the event loop alive for up to 15 more
    // seconds per feed in the overwhelming majority (successful) case.
    clearTimeout(timer);
  }
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
    _rawCategory: item.categories,
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

  const { error } = await supabase.from('articles').upsert(
    clean.map(({ _rawCategory, ...cleanRow }) => cleanRow),
    { onConflict: 'url', ignoreDuplicates: true }
  );
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
            const { error } = await supabase.from('articles').upsert(
    clean.map(({ _rawCategory, ...cleanRow }) => cleanRow),
    { onConflict: 'url', ignoreDuplicates: true }
  );
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
  const CLUSTER_HARD_TIMEOUT_MS = 60000;
  const clusterTimeout = new Promise((resolve) =>
    setTimeout(() => resolve({ error: { message: `Hard timeout after ${CLUSTER_HARD_TIMEOUT_MS}ms -- clustering RPC did not respond in time` } }), CLUSTER_HARD_TIMEOUT_MS)
  );
  const { error: clusterError } = await Promise.race([
    supabase.rpc('cluster_related_articles', { process_window_hours: 1, max_batch_size: 30 }),
    clusterTimeout,
  ]);
  if (clusterError) {
    console.error('Clustering failed (non-fatal):', clusterError.message);
  } else {
    console.log('Clustering complete.');
  }
}

main()
  .then(() => {
    // Force a clean exit. Without this, Node waits for the event loop to
    // empty naturally -- and either the fetchFeed() hard-timeout timers
    // (never cleared when a fetch wins the race) or supabase-js's
    // keep-alive HTTP connections can keep the process alive indefinitely
    // after all real work is done, with no further output, until GitHub
    // Actions kills the job at the 10-minute mark. Confirmed via a real
    // run (2026-07-15) that completed everything successfully -- "Done...
    // 647 articles... Clustering complete." -- then still got cancelled.
    process.exit(0);
  })
  .catch((err) => {
    console.error('RSS ingestion failed:', err);
    process.exit(1);
  });
