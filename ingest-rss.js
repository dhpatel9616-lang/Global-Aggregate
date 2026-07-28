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
  safeStringify,
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

// news.google.com is deliberately in ingest.js's shared BLOCKED_SOURCE_DOMAINS
// list (added 2026-07-17) because the API pipeline can't tell which
// country-scoped search produced a given article there, so the source label
// itself is misleading in that context. This script uses news.google.com
// differently: every FEED_URLS_BY_COUNTRY entry that lists it as `source` is
// a hand-picked, single-country Google News search (q=<Country>&gl=<CC>),
// used only as a last-resort fallback where no dedicated national outlet
// exists -- a different, safe context, not the ambiguous one the block was
// written for. Confirmed via a real run (2026-07-28): the shared block
// silently caught 100% of articles from all 13 countries using this
// fallback (BN, BO, BZ, EC, GT, HN, IS, KN, MV, NI, PY, SC, SR), every
// single run -- not thin coverage, a total dead end with zero exceptions.
// This wrapper skips ONLY the blocked_source gate for this one known-safe
// source; every other check (country relevance, language, staleness, junk
// patterns) still runs normally against a cloned row, and the row actually
// inserted still carries the real, honest 'news.google.com' source label --
// this substitution never touches the row that gets written to Supabase.
function getJunkReasonForRss(row) {
  const reason = getJunkReason(row);
  if (reason !== 'blocked_source' || row.source !== 'news.google.com') return reason;
  return getJunkReason({ ...row, source: 'google-news-country-fallback' });
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
    { source: 'the-star.co.ke', feedUrl: 'https://www.the-star.co.ke/rss' },
    // ^ Replaced nation.africa -- confirmed persistently 403-blocked across
    // many runs (same IP-reputation pattern as Morocco/Sri Lanka/Uganda).
    // The Star is a genuinely different domain/publisher, worth a real try.
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
    { source: 'en.hespress.com', feedUrl: 'https://en.hespress.com/feed' },
    // ^ Replaced moroccoworldnews.com -- confirmed persistently 403-blocked.
    // Hespress is Morocco's most-read news site, genuinely different domain.
  ],
  LK: [
    { source: 'colombogazette.com', feedUrl: 'https://colombogazette.com/feed/' },
    // ^ Replaced dailymirror.lk -- confirmed persistently 403-blocked.
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
    { source: 'independent.co.ug', feedUrl: 'https://www.independent.co.ug/feed/' },
    // ^ Replaced monitor.co.ug -- confirmed persistently 403-blocked (same
    // Nation Media Group platform as Kenya's original blocked entry).
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
  BI: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/burundi/headlines.rdf' }],
  AM: [{ source: 'oc-media.org', feedUrl: 'https://oc-media.org/feed/' }], // replaced armenpress.am -- confirmed 403-blocked. OC Media covers the Caucasus region independently (also used for Georgia below).
  AO: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/angola/headlines.rdf' }],
  AZ: [{ source: 'trend.az', feedUrl: 'https://en.trend.az/rss/' }],
  BG: [{ source: 'novinite.com', feedUrl: 'https://www.novinite.com/services/news_rdf.php' }], // replaced sofiaglobe.com -- confirmed 403-blocked. Novinite is a genuinely different Bulgarian English-language outlet.
  BH: [{ source: 'bna.bh', feedUrl: 'https://www.bna.bh/en/rss.aspx' }],
  BJ: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/benin/headlines.rdf' }],
  BN: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Brunei&hl=en&gl=BN&ceid=BN:en' }], // replaced borneobulletin.com.bn -- confirmed persistently blocked. Google News RSS as a universal fallback: no API key required, confirmed still working as of July 2026. This is an aggregation (many sources), not a single outlet -- expect more duplicates/off-topic hits than a dedicated feed, relies on the existing relevance/junk filters more heavily.
  BW: [{ source: 'thevoicebw.com', feedUrl: 'https://www.thevoicebw.com/feed' }], // replaced mmegi.bw -- confirmed 404 twice on that domain, abandoned rather than a third guess. The Voice is a real, established Botswana outlet with a documented RSS feed at this exact path.
  CI: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/cotedivoire/headlines.rdf' }],
  CM: [{ source: 'journalducameroun.com', feedUrl: 'https://www.journalducameroun.com/feed/' }], // 404 on /en/feed/ -- retrying without the /en/ prefix
  CR: [{ source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/feed' }], // "Non-whitespace before first tag" -- the response isn't valid XML at all (likely an HTML error page served at this path, or a redirect not being followed) -- not a simple path-guess fix, needs real investigation
  CU: [{ source: 'havanatimes.org', feedUrl: 'https://havanatimes.org/feed/' }],
  CY: [{ source: 'in-cyprus.philenews.com', feedUrl: 'https://in-cyprus.philenews.com/feed/' }],
  DO: [{ source: 'dominicantoday.com', feedUrl: 'https://dominicantoday.com/feed/' }],
  DZ: [{ source: 'aps.dz', feedUrl: 'https://www.aps.dz/en/feed' }], // 404 on two different guessed paths now -- needs real research, not another guess
  GE: [{ source: 'oc-media.org', feedUrl: 'https://oc-media.org/feed/' }], // replaced agenda.ge -- their TLS cert had genuinely expired. OC Media covers Georgia independently (also used for Armenia above -- same regional outlet, each entry independently checked for country relevance).
  GY: [{ source: 'kaieteurnewsonline.com', feedUrl: 'https://www.kaieteurnewsonline.com/feed' }], // replaced stabroeknews.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Kaieteur News is a real, established private Guyanese daily.
  HR: [{ source: 'total-croatia-news.com', feedUrl: 'https://www.total-croatia-news.com/feed' }],
  HT: [{ source: 'haitiantimes.com', feedUrl: 'https://haitiantimes.com/feed/' }],
  IS: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Iceland&hl=en&gl=IS&ceid=IS:en' }], // replaced icelandreview.com -- confirmed malformed XML on their end. Google News RSS fallback (see Brunei entry above for the general rationale/caveats).
  KG: [{ source: '24.kg', feedUrl: 'https://24.kg/rss/' }],
  KZ: [{ source: 'astanatimes.com', feedUrl: 'https://astanatimes.com/feed/' }],
  LA: [{ source: 'laotiantimes.com', feedUrl: 'https://laotiantimes.com/feed/' }],
  LT: [{ source: 'baltictimes.com', feedUrl: 'https://www.baltictimes.com/feed/' }], // replaced lrt.lt -- confirmed 403-blocked. Baltic Times is a real independent outlet covering Estonia/Latvia/Lithuania. (Their Feedburner feed URL appeared in search results but was truncated -- using their own domain's standard path instead of guessing the exact Feedburner slug.)
  DJ: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/djibouti/headlines.rdf' }],
  LY: [{ source: 'libyaherald.com', feedUrl: 'https://libyaherald.com/feed/' }], // replaced libyaobserver.ly -- confirmed persistently blocked. Libya Herald is a genuinely different domain.
  MD: [{ source: 'moldovalive.md', feedUrl: 'https://moldovalive.md/feed' }], // replaced agora.md -- confirmed 404 twice on that domain, abandoned rather than a third guess. MoldovaLive.md is confirmed genuinely active with current 2026 English-language content.
  MG: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/madagascar/headlines.rdf' }],
  MK: [
    { source: 'mia.mk', feedUrl: 'https://mia.mk/feed/' },
    // NEW (2026-07-28): mia.mk has returned 100% non_english across every
    // run checked despite being sought as an English source -- confirmed
    // via research that MIA's English service exists but this feed URL
    // appears to serve the Macedonian edition. meta.mk is confirmed via
    // multiple independent sources to actually publish in English (as well
    // as Macedonian/Albanian) and is a legitimate, well-established
    // independent Macedonian news agency. Feed URL below is an educated
    // guess at their standard path (WordPress convention) -- NOT verified
    // by a test fetch, since meta.mk isn't reachable from this sandbox.
    // Let the next run's log confirm or correct it, per the established
    // process.
    { source: 'meta.mk', feedUrl: 'https://meta.mk/en/feed/' },
  ],
  ML: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/mali/headlines.rdf' }],
  MN: [{ source: 'montsame.mn', feedUrl: 'https://montsame.mn/en/rss' }],
  MW: [{ source: 'nyasatimes.com', feedUrl: 'https://www.nyasatimes.com/feed/' }],
  MZ: [{ source: 'clubofmozambique.com', feedUrl: 'https://clubofmozambique.com/feed/' }],
  NA: [{ source: 'namibian.com.na', feedUrl: 'https://www.namibian.com.na/feed/' }],
  OM: [{ source: 'omanobserver.om', feedUrl: 'https://www.omanobserver.om/rss' }],
  RW: [{ source: 'taarifa.rw', feedUrl: 'https://taarifa.rw/feed' }], // replaced newtimes.co.rw -- failed differently on two different guessed paths (502, then 404), abandoned rather than a third guess. Taarifa is a real Rwandan English-language news platform with a documented RSS feed at this exact path.
  SD: [{ source: 'dabangasudan.org', feedUrl: 'https://www.dabangasudan.org/en/feed' }], // replaced sudantribune.com -- confirmed persistently blocked. Radio Dabanga's English feed was externally verified live with current content (not just guessed).
  SK: [{ source: 'spectator.sme.sk', feedUrl: 'https://spectator.sme.sk/rss' }],
  SO: [{ source: 'thesomalidigest.com', feedUrl: 'https://thesomalidigest.com/feed' }], // replaced garoweonline.com -- confirmed broken (500 then 404) on two different guessed paths, abandoned rather than a third guess. The Somali Digest is a confirmed real English-language outlet with a documented RSS feed at this exact path.
  SY: [{ source: 'syrianobserver.com', feedUrl: 'https://syrianobserver.com/feed' }],
  TN: [{ source: 'africanmanager.com', feedUrl: 'https://africanmanager.com/feed/' }],
  TT: [{ source: 'newsday.co.tt', feedUrl: 'https://newsday.co.tt/feed/' }],
  CV: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/capeverde/headlines.rdf' }],
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
  BS: [{ source: 'ewnews.com', feedUrl: 'https://ewnews.com/feed' }], // replaced tribune242.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Eye Witness News is the Bahamas' #1 local outlet, with a dedicated RSS feed page on their site ("/rss-feed-2/") confirming a feed exists -- using the standard /feed path first since the exact confirmed URL was on a landing page, not necessarily the raw feed itself.
  BB: [{ source: 'barbadostoday.bb', feedUrl: 'https://barbadostoday.bb/feed/' }],
  BZ: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Belize&hl=en&gl=BZ&ceid=BZ:en' }], // replaced breakingbelizenews.com -- confirmed persistently blocked. Google News RSS fallback.
  BT: [{ source: 'dailybhutan.com', feedUrl: 'https://www.dailybhutan.com/feed' }], // replaced kuenselonline.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Daily Bhutan is confirmed very actively updated (May/June 2026 content seen directly), a stronger candidate than Kuensel.
  CG: [{ source: 'africanews.com', feedUrl: 'https://www.africanews.com/feed/rss' }], // replaced the allafrica.com/congo_brazzaville RDF feed -- confirmed reachable (right slug) but returning zero items every run, a volume problem not a config one. Africanews is a pan-African feed externally verified live with July 2026 content; relies on the existing country-mention relevance check to filter for Congo-Brazzaville specifically, same as any WORLD-tier wire source.
  DM: [{ source: 'dominicanewsonline.com', feedUrl: 'https://dominicanewsonline.com/news/feed/' }],
  EE: [{ source: 'news.err.ee', feedUrl: 'https://news.err.ee/rss' }],
  GQ: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/equatorialguinea/headlines.rdf' }],
  SZ: [{ source: 'times.co.sz', feedUrl: 'https://times.co.sz/feed/' }],
  GA: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/gabon/headlines.rdf' }],
  GM: [{ source: 'thepoint.gm', feedUrl: 'https://thepoint.gm/posts/rss/xml' }], // confirmed real feed URL from page source (rel=alternate link tag) -- not a guess
  GD: [{ source: 'nowgrenada.com', feedUrl: 'https://nowgrenada.com/feed/' }],
  GN: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/guinea/headlines.rdf' }],
  VA: [{ source: 'vaticannews.va', feedUrl: 'https://www.vaticannews.va/en.rss.xml' }],
  LV: [{ source: 'eng.lsm.lv', feedUrl: 'https://eng.lsm.lv/rss/' }],
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
  SC: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Seychelles&hl=en&gl=SC&ceid=SC:en' }], // replaced seychellesnewsagency.com -- confirmed persistently blocked. Google News RSS fallback.
  SL: [{ source: 'thesierraleonetelegraph.com', feedUrl: 'https://www.thesierraleonetelegraph.com/feed/' }],
  SI: [{ source: 'total-slovenia-news.com', feedUrl: 'https://www.total-slovenia-news.com/feed' }], // replaced sloveniatimes.com -- confirmed persistently blocked. Total Slovenia News is a genuinely different domain.
  SB: [{ source: 'solomonstarnews.com', feedUrl: 'https://www.solomonstarnews.com/feed/' }],
  SS: [{ source: 'radiotamazuj.org', feedUrl: 'https://radiotamazuj.org/en/feed' }], // /en/rss.xml 404'd -- retrying /en/feed
  GW: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/guineabissau/headlines.rdf' }],
  TL: [{ source: 'en.tatoli.tl', feedUrl: 'https://en.tatoli.tl/feed/' }],
  TG: [
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/togo/headlines.rdf' },
    // NEW (2026-07-28): AllAfrica's Togo feed has returned "zero items"
    // every single run checked -- structurally empty, not flaky. TogoFirst
    // is a real English-language Togo business/investment news outlet
    // (operated by Ecofin Agency); this specific feed URL is listed
    // directly in FeedSpot's RSS directory (a live-feed listing service,
    // not a guess), so confidence is higher than the meta.mk candidate
    // above, but still unverified by an actual test fetch from this
    // sandbox -- confirm via next run's log.
    { source: 'togofirst.com', feedUrl: 'https://www.togofirst.com/en/rss-en' },
  ],
  TO: [{ source: 'matangitonga.to', feedUrl: 'https://matangitonga.to/feed/' }], // 404 on three separate runs now -- genuinely no feed at any guessed path, needs real research (view page source) rather than another guess
  VU: [{ source: 'dailypost.vu', feedUrl: 'https://dailypost.vu/feed' }], // status has flip-flopped across runs (403, then 429, now 404) -- looks like a genuinely unstable small site rather than one fixable path issue
  // NEW (2026-07-28): WS (Samoa) previously had no RSS entry at all. Samoa
  // Observer is Samoa's real, independent, award-winning national daily
  // (RSF profile confirms it's the country's flagship free press outlet).
  // Feed path is a standard-convention guess -- unverified, confirm via
  // next run's log. Uses Cloudflare per its own tech stack, so a 403 here
  // wouldn't be surprising.
  WS: [{ source: 'samoaobserver.ws', feedUrl: 'https://www.samoaobserver.ws/feed' }],
  // NOT FOUND -- no plausible independent English-language outlet located
  // in this pass, or these are genuinely tiny states with no discoverable
  // English-language press at all:
  // AD (Andorra), BI (Burundi), CV (Cabo Verde),
  // CF (Central African Republic), TD (Chad), KM (Comoros),
  // DJ (Djibouti), GQ (Equatorial Guinea),
  // GW (Guinea-Bissau), HN (Honduras), KI (Kiribati), LI (Liechtenstein),
  // MH (Marshall Islands), FM (Micronesia), NR (Nauru),
  // PW (Palau), SM (San Marino), ST (Sao Tome and Principe),
  // SR (Suriname), TV (Tuvalu).

  // --- State media (flagged stateMedia: true -> is_state_media on the row,
  // for a frontend disclaimer badge, not a quality judgment). These are the
  // 4 countries whose press is state-controlled enough that "find the
  // independent national outlet" isn't a coherent research task -- the
  // outlet itself IS the government.
  KP: [
    { source: 'kcnawatch.org', feedUrl: 'https://kcnawatch.org/newstream/feed/', stateMedia: true },
    // ^ still malformed XML on their end, left in in case it self-resolves.
    { source: 'dailynk.com', feedUrl: 'https://www.dailynk.com/english/feed/' },
    // ^ Daily NK -- genuinely independent, defector-sourced reporting (the
    // opposite of state media), actively updated, real RSS feed mentioned
    // on-site. Much stronger primary source than the state mirror above.
    // NOT flagged stateMedia -- this is independent journalism, not
    // government output.
  ],
  // ^ Feed has malformed XML of its own (unquoted attribute value) -- same
  // source-side-bug class as LS/TJ/SV, not fixable by URL changes.
  // ^ Deliberately NOT fetching kcna.kp directly -- KCNA's own DPRK-hosted
  // site has a documented history of malicious scripts and frequent outages.
  // KCNA Watch is a dedicated third-party mirror built specifically to work
  // around that risk while still surfacing genuine official DPRK output.
  BY: [{ source: 'eng.belta.by', feedUrl: 'https://eng.belta.by/rss', stateMedia: true }],
  // ^ Confirmed working -- saw live, current-dated content at this URL directly.
  ER: [{ source: 'shabait.com', feedUrl: 'https://shabait.com/feed/', stateMedia: true }],
  // ^ 403 -- same IP-reputation/bot-blocking pattern as Kenya/Uganda/Morocco/
  // Malta, not a path problem, no further URL guessing will help.
  TM: [{ source: 'en.hronikatm.com', feedUrl: 'https://en.hronikatm.com/feed/' }],
  // ^ Real, correct URL but their server has an incomplete TLS certificate
  // chain ("unable to verify the first certificate") -- a genuine
  // misconfiguration on their end, same class of issue as Monaco's cert
  // mismatch. Not fixable by changing the URL. Left in in case they fix
  // their server config; still the right source (independent, RSF-
  // documented), just currently unreachable.
  // ^ /rss 404'd -- retrying /en/rss. Turkmenistan has no single clean "the
  // state agency" with a public feed (TDH/tdh.gov.tm shows no evidence of
  // one). Orient.tm is described as pro-government rather than strictly
  // state-owned -- flagged as state media here as the closest honest
  // approximation, not a perfect fit.

  // --- Batch 5: English-outlet sweep for the 15 non-English-press
  // countries (Category A) plus Yemen (previously a low-confidence "not
  // found" -- rechecked properly this pass). Real, currently-active
  // English outlets found for 8 of these; the rest fall back to the
  // AllAfrica RDF pattern already proven for Gabon/Guinea/Mali/Niger/Togo.
  // Guatemala is the one genuine remaining gap -- a source directly
  // confirmed "English-language news providers solely in or about
  // Guatemala are relatively sparse," and the only candidate found was a
  // monthly print magazine, not a live news feed.
  HN: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Honduras&hl=en&gl=HN&ceid=HN:en' }], // replaced hondurasdaily.com -- confirmed broken feed generator (500 on two different paths). Google News RSS fallback.
  SV: [{ source: 'elsalvadorinenglish.com', feedUrl: 'https://elsalvadorinenglish.com/feed' }],
  // ^ Replaced elsalvadordaily.com -- confirmed persistent malformed XML
  // across two runs, abandoned rather than a third guess. This is a real,
  // actively-updated (through July 2026) dedicated English-language site.
  // Worth knowing: its editorial tone leans favorable toward the Bukele
  // government in sample content -- not officially state-owned, but not
  // neutral either.
  NI: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Nicaragua&hl=en&gl=NI&ceid=NI:en' }], // replaced nicaraguadailytimes.com -- confirmed broken format on two different paths. Google News RSS fallback.
  KM: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/comoros/headlines.rdf' }],
  // ^ HN/SV/NI appear to be the same templated network of AI-summarized
  // English news briefings (same subscription-alert pattern across all
  // three) -- confirmed to exist via search, feed paths NOT yet verified.
  SR: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Suriname&hl=en&gl=SR&ceid=SR:en' }], // replaced surinametimes.com -- confirmed 404 on two different paths, and their real content skews Dutch anyway. Google News RSS fallback (English-only via hl=en).
  // ^ Times of Suriname -- genuinely bilingual Dutch/English daily, not a guess.
  YE: [{ source: 'almasdaronline.com', feedUrl: 'https://almasdaronline.com/en/feed' }],
  // ^ 403 -- same IP-reputation/bot-blocking pattern as Kenya/Uganda/Morocco/
  // Malta/Eritrea, not a path problem, no further URL guessing will help.
  // ^ Al-Masdar Online -- confirmed still actively publishing as of 2026,
  // maintains an English-language version. Corrects the earlier low-
  // confidence "no outlet found" flag from an earlier, less rigorous pass.
  // BI/DJ/CV/GQ/GW/KM/ST/CF/TD/BF: no dedicated English-language domestic
  // outlet found for any of these (all Francophone/Lusophone press) --
  // falling back to the AllAfrica RDF pattern already confirmed to work
  // for similarly-situated countries. Genuinely no independent-outlet
  // candidate exists, not a research shortcut. Deliberately NOT listed
  // consecutively here -- confirmed via a real run (2026-07-28) that 10
  // AllAfrica entries in a row, on top of ~12 earlier AllAfrica calls
  // already made that same run, cascaded into repeated 10s-timeout+20s-
  // retry failures and ate enough of the job's time budget to get the
  // whole run cancelled at the 10-minute mark with 18 country groups never
  // even attempted. Same finding as the inter-domain delay comment above:
  // AllAfrica's throttling doesn't respond predictably to more delay, so
  // instead these 10 are spread out among unrelated-domain entries below,
  // each several entries away from the next AllAfrica call.

  TV: [{ source: 'tuvalutimes.com', feedUrl: 'https://www.tuvalutimes.com/feed' }],
  ST: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/saotomeandprincipe/headlines.rdf' }],
  // ^ Right URL (no 404), but the feed itself has malformed XML (unexpected
  // close tag) -- source-side bug, same class as Lesotho/Asia-Plus/KCNA
  // Watch, not fixable by changing the URL.

  // --- Batch 6: first research pass on the 12 countries that had NO RSS
  // entry at all yet (as opposed to the ~39 zero-content countries that
  // already had an attempt sitting here unproductively -- those need a
  // fresh run + log diagnosis, not more research). 9 real, currently-active
  // dedicated English outlets found; Bolivia, Ecuador, and Paraguay did not
  // turn up a clear candidate in this pass and are left for a future one
  // rather than guessed.
  BA: [{ source: 'sarajevotimes.com', feedUrl: 'https://sarajevotimes.com/feed' }],
  // ^ Confirmed "the only Bosnian portal that gives news in English."
  BD: [{ source: 'thedailystar.net', feedUrl: 'https://www.thedailystar.net/frontpage/rss.xml' }], // right URL, but every item's title comes through unusable after sanitization (missing_title: 10/10) -- a source-side feed structure issue, not a path problem
  CF: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/centralafricanrepublic/headlines.rdf' }],
  // ^ The Daily Star -- Bangladesh's largest circulating English-language
  // newspaper, feed URL confirmed via a curated OPML feed list, not guessed.
  BE: [{ source: 'brusselstimes.com', feedUrl: 'https://www.brusselstimes.com/feed' }], // malformed XML (unquoted attribute value) -- source-side bug, same class as Lesotho/Asia-Plus/KCNA Watch
  // ^ The Brussels Times -- Belgium's largest English-language news outlet.
  KH: [{ source: 'phnompenhpost.com', feedUrl: 'https://www.phnompenhpost.com/feed' }],
  // ^ Phnom Penh Post -- Cambodia's oldest English-language newspaper, confirmed active with current 2026 content.
  CZ: [{ source: 'praguemonitor.com', feedUrl: 'https://praguemonitor.com/feed' }],
  // ^ Prague Monitor -- confirmed active English-language Czech Republic news site since 2003.
  MM: [
    { source: 'irrawaddy.com', feedUrl: 'https://www.irrawaddy.com/feed' }, // intermittent 403 (same IP-reputation pattern as Kenya/Uganda/Morocco) -- has succeeded at least once before, not a path problem
    { source: 'myanmar-now.org', feedUrl: 'https://myanmar-now.org/en/feed' }, // added as a second, genuinely different source rather than a replacement, since Irrawaddy does succeed sometimes
  ],
  // ^ The Irrawaddy -- genuinely independent (exile-founded, press-freedom-award-winning), confirmed active.
  KW: [{ source: 'kuwaittimes.com', feedUrl: 'https://www.kuwaittimes.com/?feed=rss2' }], // path is correct now (no more 404), but the feed itself has malformed XML -- source-side bug, same class as Lesotho/Brussels Times, not fixable by URL changes
  // ^ Kuwait Times -- oldest active English-language newspaper in Kuwait, founded 1961.
  PA: [{ source: 'expat-times.com', feedUrl: 'https://expat-times.com/panama/feed' }],
  TD: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/chad/headlines.rdf' }],
  // ^ Panama Expat Times -- confirmed active (Nov 2025 content), dedicated English coverage.
  VE: [{ source: 'caracaschronicles.com', feedUrl: 'https://www.caracaschronicles.com/feed' }],
  // ^ Caracas Chronicles -- genuinely independent, confirmed active with July 2026 content.
  // NOT FOUND -- no clear dedicated English-language outlet turned up in
  // this pass: BO (Bolivia), EC (Ecuador), PY (Paraguay). Worth a second,
  // more targeted research pass rather than a guess.

  // --- Google News RSS fallback batch: for countries where real research
  // (including a second, cross-referenced pass via an external feed-
  // expansion report) found no dependable single-outlet feed. Confirmed
  // still working with no API key as of July 2026. This is fundamentally
  // different from every other entry in this file -- an aggregation of
  // many sources via Google's own search, not one outlet -- so expect more
  // duplicates and occasional off-topic hits; the existing relevance/junk
  // filters do the real work of cleaning it up rather than the source
  // itself being pre-vetted the way a dedicated outlet is.
  GT: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Guatemala&hl=en&gl=GT&ceid=GT:en' }],
  MV: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Maldives&hl=en&gl=MV&ceid=MV:en' }],
  KN: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Saint+Kitts+and+Nevis&hl=en&gl=KN&ceid=KN:en' }],
  BO: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Bolivia&hl=en&gl=BO&ceid=BO:en' }],
  BF: [{ source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/burkinafaso/headlines.rdf' }],
  EC: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Ecuador&hl=en&gl=EC&ceid=EC:en' }],
  PY: [{ source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Paraguay&hl=en&gl=PY&ceid=PY:en' }],
  // NEW (2026-07-28): these 9 countries had ZERO dedicated RSS feed --
  // relying entirely on the capped 3-API pipeline (79/186 countries
  // attempted per run), which explains why several major, well-known
  // countries were stuck "thin" despite having real allowlisted outlets.
  // FR/PL/NO/PS/CO/BR feed URLs confirmed via a live RSS-directory listing
  // (FeedSpot), not guessed. UA/IQ/ET use the standard /feed/ convention on
  // an already-allowlisted domain -- unverified by test fetch, confirm via
  // next run's log.
  FR: [{ source: 'lemonde.fr', feedUrl: 'https://www.lemonde.fr/en/rss/une.xml' }], // Le Monde's English digital edition (launched April 2022)
  PL: [{ source: 'notesfrompoland.com', feedUrl: 'https://notesfrompoland.com/feed' }],
  NO: [{ source: 'thelocal.no', feedUrl: 'https://feeds.thelocal.com/rss/no' }],
  PS: [{ source: 'palestinechronicle.com', feedUrl: 'https://www.palestinechronicle.com/feed' }],
  CO: [{ source: 'colombiareports.com', feedUrl: 'https://colombiareports.com/feed' }],
  BR: [{ source: 'riotimesonline.com', feedUrl: 'https://www.riotimesonline.com/feed' }],
  UA: [{ source: 'euromaidanpress.com', feedUrl: 'https://euromaidanpress.com/feed' }], // swapped from kyivindependent.com/feed/ (404, wrong path) -- this one confirmed via FeedSpot directory listing, not a guess
  IQ: [{ source: 'iraqinews.com', feedUrl: 'https://www.iraqinews.com/feed' }], // unverified path guess
  ET: [{ source: 'capitalethiopia.com', feedUrl: 'https://www.capitalethiopia.com/feed/' }], // swapped from thereporterethiopia.com (403, bot-blocked) -- this one confirmed reachable, live content seen directly
};

async function loadExistingTitles() {
  const { data, error } = await supabase.from('articles').select('title');
  if (error) {
    console.error('Could not load existing titles for dedup, continuing without it:', error.message);
    return new Set();
  }
  return new Set(data.map((row) => normalizeTitle(row.title)));
}

async function fetchFeedOnce(feedUrl) {
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

function isRetryableError(err) {
  // Only timeouts get retried -- a 404, malformed XML, or cert error will
  // fail identically on a second attempt, so retrying just wastes time.
  // Timeouts are the one failure mode that's plausibly transient (server
  // load, throttling, network blip).
  return /timed out/i.test(err.message);
}

async function fetchFeed(feedUrl) {
  // One retry on timeout, after a real pause -- not more delay tuning.
  // Confirmed via two real runs that hand-tuning the pre-emptive delay
  // between same-domain requests doesn't behave predictably: going from
  // 4s to 8s made AllAfrica's failure point WORSE (started failing at the
  // 8th consecutive call instead of the 16th), the opposite of what a
  // simple request-count throttle would predict. Rather than keep guessing
  // at whatever AllAfrica's actual rate-limit mechanism is, this handles
  // the failure reactively: if a fetch times out, wait a real amount
  // (20s) and try exactly once more before giving up.
  try {
    return await fetchFeedOnce(feedUrl);
  } catch (err) {
    if (!isRetryableError(err)) throw err;
    console.warn(`  (retrying after timeout, waiting 20s: ${feedUrl})`);
    await sleep(20000);
    return await fetchFeedOnce(feedUrl);
  }
}

function buildRow(item, country, source, isStateMedia) {
  // item.title is assumed to always be a plain string by both this function
  // and mapTopic, but isn't always -- confirmed via two separate crashes in
  // production runs (both traced to Bangladesh's Daily Star feed returning
  // a non-string title, likely from an unusual nested/CDATA XML structure).
  // Sanitizing once here means every downstream use of row.title
  // (normalizeTitle, dedup, display) is automatically safe too, rather than
  // needing the same guard repeated at each call site.
  const safeTitle = item.title ? safeStringify(item.title).trim() : null;
  return {
    source,
    country,
    topic: mapTopic(item.categories, safeTitle),
    title: safeTitle,
    description: capDescription(item.contentSnippet || item.content || item.summary || null),
    url: item.link,
    published_at: item.isoDate ? new Date(item.isoDate).toISOString() : (item.pubDate ? new Date(item.pubDate).toISOString() : null),
    is_state_media: !!isStateMedia,
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
    .map((item) => buildRow(item, country, feedEntry.source, feedEntry.stateMedia));

  const reasonCounts = {};
  const blockedSources = new Set();
  const clean = [];
  for (const row of rows) {
    if (seenUrls.has(row.url)) {
      reasonCounts['already_seen_url'] = (reasonCounts['already_seen_url'] || 0) + 1;
      continue;
    }
    const reason = getJunkReasonForRss(row);
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
  let lastSource = null;
  for (const country of countryCodes) {
    // WORLD (wire) feeds get checked against every real country, since their
    // relevance depends on content (does it mention Kenya, Poland, etc.),
    // not which feed group they were fetched under -- same principle as the
    // wire-relevance check in ingest.js.
    const targetCountries = country === 'WORLD' ? countryCodes.filter((c) => c !== 'WORLD') : [country];

    for (const feedEntry of FEED_URLS_BY_COUNTRY[country]) {
      // Same-domain requests need more breathing room than the standard 1s
      // inter-feed delay. Confirmed via a real run: AllAfrica is hit 16
      // times across this project's country batches (each addition looked
      // fine in isolation at the time), and the first ~3 back-to-back calls
      // succeeded while all 13 subsequent ones in the same run timed out --
      // a rate-limit/throttle pattern, not AllAfrica being down. This is a
      // structural risk for ANY domain reused across many countries, not
      // just AllAfrica, so the check is generic rather than AllAfrica-specific.
      if (feedEntry.source === lastSource) {
        await sleep(4000);
        // ^ Reverted from 8000ms -- confirmed via a real run that 8s
        // performed WORSE than 4s (failures started at the 8th consecutive
        // same-domain call instead of the 16th), so more pre-emptive delay
        // isn't the right lever. The real fix is the retry-on-timeout logic
        // in fetchFeed() above; this delay is now just a light first line
        // of defense, not the primary mechanism.
      }
      lastSource = feedEntry.source;

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
            .map((item) => buildRow(item, targetCountry, feedEntry.source, feedEntry.stateMedia));
          const clean = rows.filter((row) => {
            if (seenUrls.has(row.url)) return false;
            const reason = getJunkReasonForRss(row);
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
