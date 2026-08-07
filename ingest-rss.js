// The Global Aggregate — RSS ingestion script (Stage 2: real-time backbone)
//
// This is a SEPARATE ingestion path from ingest.js, not a replacement for it.
// The 3 commercial APIs (NewsData, GNews, Currents) keep running on their
// existing 3-hour cadence via ingest.js -- they provide broad discovery and
// clean structured data. This script polls RSS feeds directly from outlets
// already confirmed real through the allowlist process, and is
// designed to run far more often since RSS feeds have no per-request
// "credits" system, unlike the 3 APIs. Currently every 30 minutes per
// shard (reduced from every 15 -- see the SHARDING comment below for why).
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

const http = require('http');
const https = require('https');
const zlib = require('zlib');
const { createClient } = require('@supabase/supabase-js');
const Parser = require('rss-parser');
const {
  getJunkReason,
  capDescription,
  safeParseDate,
  mapTopic,
  normalizeTitle,
  safeStringify,
} = require('./ingest.js');

// TEMPORARY diagnostic list (2026-07-30): sources where an excluded_category
// fix didn't behave as expected, so we're logging real raw category values
// instead of guessing again. Remove once the real cause is confirmed.
const DEBUG_CATEGORY_SOURCES = ['total-montenegro-news.com'];

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
  // NEW (2026-08-03): three major countries with ZERO prior coverage,
  // discovered while auditing single-source countries for the broader
  // expansion pass -- these are bigger gaps than any single-source country
  // (98M, 65M, and 34M people respectively with no feed at all). All three
  // URLs confirmed via public RSS feed directories, not guesses.
  VN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/east-asia/vietnam/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'vnexpress.net', feedUrl: 'https://e.vnexpress.net/rss/news.rss' }],
  TZ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/tanzania/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thecitizen.co.tz', feedUrl: 'https://www.thecitizen.co.tz/service/rss/tanzania/2486554/feed.rss' }],
  PE: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/peru/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'perureports.com', feedUrl: 'https://perureports.com/feed' }],
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
    // NEW (2026-08-06): Metro -- major UK mass-market tabloid/digital
    // outlet (5.4M Facebook followers), genuinely different tone/
    // ownership from Guardian (broadsheet) and BBC (public broadcaster).
    // Exact path confirmed via feed directory.
    { source: 'metro.co.uk', feedUrl: 'https://metro.co.uk/feed' },
  ],
  US: [
    { source: 'nytimes.com', feedUrl: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml' },
    // NEW (2026-08-02): NPR and CNN top-stories feeds -- both extremely
    // long-stable, widely-documented URLs (years-old, cited across
    // countless RSS directories unchanged), not individually
    // fetch-verified this session but a different confidence tier than a
    // typical outlet guess. Added for outlet diversity on the single
    // biggest country in the dataset.
    { source: 'npr.org', feedUrl: 'https://feeds.npr.org/1001/rss.xml' },
    { source: 'cnn.com', feedUrl: 'http://rss.cnn.com/rss/cnn_topstories.rss' },
  ],
  TR: [
    { source: 'dailysabah.com', feedUrl: 'https://www.dailysabah.com/rssFeed/10000' },
    // NEW (2026-08-03): Duvar English -- Turkey's independent gazette,
    // confirmed exact path via a public RSS feed directory.
    { source: 'duvarenglish.com', feedUrl: 'https://www.duvarenglish.com/export/rss' }, // confirmed dead (0 articles ever) via live data audit -- left in place, not re-researched this round
    // NEW (2026-08-06): Hurriyet Daily News -- major, established
    // English-language Turkish daily, genuinely different from Daily
    // Sabah (pro-government) and Duvar (independent-left) -- a distinct
    // editorial position. Confirmed real via feed directory; exact feed
    // path not independently verified this round, treat as a hypothesis.
    { source: 'hurriyetdailynews.com', feedUrl: 'https://www.hurriyetdailynews.com/rss' },
  ],
  NG: [
    { source: 'punchng.com', feedUrl: 'https://punchng.com/feed/' },
    { source: 'legit.ng', feedUrl: 'https://www.legit.ng/rss/all.rss' }, // NEW (2026-08-02): confirmed valid via community RSS directory
    // NEW (2026-08-06): Sahara Reporters -- Nigerian-American diaspora
    // outlet known for investigative/whistleblower reporting, genuinely
    // different editorial line from Punch/Legit. Confirmed real via feed
    // directory; exact feed path not independently verified this round,
    // treat as a hypothesis.
    { source: 'saharareporters.com', feedUrl: 'https://saharareporters.com/feed' },
  ],
  KE: [
    { source: 'the-star.co.ke', feedUrl: 'https://www.the-star.co.ke/rss' },
    // ^ Replaced nation.africa -- confirmed persistently 403-blocked across
    // many runs (same IP-reputation pattern as Morocco/Sri Lanka/Uganda).
    // The Star is a genuinely different domain/publisher, worth a real try.
    { source: 'standardmedia.co.ke', feedUrl: 'https://www.standardmedia.co.ke/rss/headlines.php' }, // NEW (2026-08-02): confirmed valid via community RSS directory
    // The Star (the-star.co.ke): confirmed dead (0 articles ever) via
    // live data audit -- left in place, not re-researched this round.
    // NEW (2026-08-06): Nairobi Wire -- established Kenyan digital outlet
    // (150K Facebook followers), genuinely different from The Star/
    // Standard Media. Confirmed real and exact path via feed directory.
    { source: 'nairobiwire.com', feedUrl: 'https://nairobiwire.com/feed' },
  ],
  // Fetch-verified via search this session (real, current feed URLs)
  PK: [
    { source: 'dawn.com', feedUrl: 'https://www.dawn.com/feeds/home' },
    { source: 'tribune.com.pk', feedUrl: 'https://tribune.com.pk/feed/home' }, // NEW (2026-08-02): confirmed valid via community RSS directory
    // NEW (2026-08-06): ARY News -- major Pakistani TV news channel
    // (29.9M Facebook followers), genuinely different from Dawn/Tribune
    // (both print-origin dailies). Confirmed real via feed directory;
    // exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'arynews.tv', feedUrl: 'https://arynews.tv/feed' },
  ],
  TH: [
    { source: 'bangkokpost.com', feedUrl: 'https://www.bangkokpost.com/rss/data/topstories.xml' },
    // NEW (2026-08-03): Khaosod English -- owned by the Khaosod newspaper,
    // editorially distinct from Bangkok Post. Exact feed URL confirmed
    // directly (a syndication mirror explicitly lists this as the live RSS
    // URL).
    { source: 'khaosodenglish.com', feedUrl: 'http://www.khaosodenglish.com/feed' },
  ],
  GH: [
    { source: 'myjoyonline.com', feedUrl: 'https://www.myjoyonline.com/feed/' },
    // Citinewsroom: confirmed dead (0 articles ever) via live data audit
    // -- left in place, not re-researched this round.
    { source: 'citinewsroom.com', feedUrl: 'https://citinewsroom.com/feed/' },
    // NEW (2026-08-06): Modern Ghana -- confirmed to have its own
    // dedicated RSS feed system via their own documentation page,
    // genuinely different outlet from MyJoyOnline/Citinewsroom. Exact
    // topic-specific feed path not independently verified this round
    // (using their general/homepage pattern), treat as a hypothesis.
    { source: 'modernghana.com', feedUrl: 'https://www.modernghana.com/rss/news.xml' },
  ],
  // NEW: added for the 7 countries confirmed to throw real Currents API
  // errors (not just empty results) -- Currents documents covering ~70
  // countries total, so these are very likely just outside that supported
  // set, not a fixable allowlist problem. RSS has no such coverage ceiling.
  // Fetch-verified this session (live content confirmed directly):
  MA: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/morocco/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'en.hespress.com', feedUrl: 'https://en.hespress.com/feed' },
    // ^ Replaced moroccoworldnews.com -- confirmed persistently 403-blocked.
    // Hespress is Morocco's most-read news site, genuinely different domain.
  ],
  LK: [
    { source: 'colombogazette.com', feedUrl: 'https://colombogazette.com/feed/' },
    // ^ Replaced dailymirror.lk -- confirmed persistently 403-blocked.
    // NEW (2026-07-28): colombogazette.com itself started failing with
    // malformed XML ("Attribute without value"). Trying The Nation instead
    // -- exact feed URL confirmed via directory listing, a different outlet
    // than the already-blocked dailymirror.lk, not the same domain retried.
    { source: 'nation.lk', feedUrl: 'https://www.nation.lk/online/rss.xml' },
  ],
  // Verified via a real feed-listing source this session, not directly
  // fetch-tested. allafrica.com's English feed avoids the problem that
  // Senegal's major domestic outlets (Seneweb, Le Soleil, APS) are all
  // French-only and would likely be caught by the non-English filter.
  SN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/senegal/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'http://allafrica.com/tools/headlines/rdf/senegal/headlines.rdf' },
  ],
  FJ: [
    { source: 'fbcnews.com.fj', feedUrl: 'https://www.fbcnews.com.fj/feed/' },
    { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }, // NEW (2026-08-03): RNZ Pacific -- regional wire covering all Pacific island nations, exact URL confirmed directly from RNZ's own feed directory
  ],
  // NOT fetch-tested and NOT verified via a feed listing -- pattern-matched
  // from the same Nation Media Group platform as Kenya's nation.africa
  // entry above. Exactly the kind of entry the diagnostic logging is meant
  // to validate or correct on the first real run.
  UG: [
    { source: 'independent.co.ug', feedUrl: 'https://www.independent.co.ug/feed/' },
    // ^ Replaced monitor.co.ug -- confirmed persistently 403-blocked (same
    // Nation Media Group platform as Kenya's original blocked entry).
    // NEW (2026-08-03): Nile Post -- Uganda's #2 most-visited independent
    // outlet by traffic, genuinely different ownership. Confirmed real and
    // exact path via a public curated RSS directory. Worth noting: Daily
    // Monitor (the outlet already excluded above) is currently facing
    // government/military shutdown pressure, which makes real redundancy
    // on independent Ugandan sources more valuable than usual right now.
    { source: 'nilepost.co.ug', feedUrl: 'https://nilepost.co.ug/feed/' },
  ],
  // NOT fetch-tested -- standard WordPress /feed/ convention guessed from
  // the domain, no feed listing found to confirm. Same caveat as above.
  PG: [
    { source: 'postcourier.com.pg', feedUrl: 'https://postcourier.com.pg/feed/' },
    { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }, // NEW (2026-08-03): RNZ Pacific -- same regional wire as FJ/TO/VU/WS/KI/NR/MH/PW/FM
  ],
  // NEW: added for the 5 countries still confirmed at zero articles across
  // BOTH the API pipeline (empty/error results in ingest.js logs) and RSS
  // (never had a feed at all until now).
  NP: [
    // FIXED (2026-08-04): was 'onlinekhabar.com' / onlinekhabar.com/feed --
    // confirmed via a live data audit that this fed pure Nepali-language
    // content into the database (2,003 non-English articles accumulated
    // before this was caught, all retroactively deleted). Swapped to their
    // dedicated English edition. Exact feed path not independently
    // confirmed (no direct listing found), following the same standard
    // WordPress /feed pattern used successfully elsewhere -- treat as a
    // hypothesis for the next log to confirm, and if it 404s, this
    // country still has Kathmandu Post + wire coverage as a safety net.
    { source: 'english.onlinekhabar.com', feedUrl: 'https://english.onlinekhabar.com/feed' },
    // Kathmandu Post: confirmed dead (0 articles ever) via live data
    // audit -- their feed directory listing itself only shows "Generate
    // RSS" (a placeholder, not a real confirmed feed), consistent with
    // this being a genuine dead end rather than a wrong-path guess. Left
    // in place, not re-researched further this round.
    { source: 'kathmandupost.com', feedUrl: 'https://kathmandupost.com/rss.xml' },
    // NEW (2026-08-06): Kathmandu Tribune -- established 2017, Nepal's
    // national online English daily, genuinely different from Online
    // Khabar. Confirmed real with an actual listed working feed (not a
    // "Generate RSS" placeholder, unlike Kathmandu Post above) via feed
    // directory.
    { source: 'kathmandutribune.com', feedUrl: 'https://kathmandutribune.com/feed' },
  ],
  // greekreporter.com/greece/feed 403'd (likely IP-reputation blocking,
  // same category as Kenya/Morocco/Sri Lanka/Uganda -- a UA header alone
  // doesn't fix this class of block). Switched to a different outlet.
  GR: [
    { source: 'thenationalherald.com', feedUrl: 'https://www.thenationalherald.com/feed/' },
    // NEW (2026-08-03): Kathimerini English Edition -- Greece's newspaper
    // of record, distributed with the NYT International Edition in
    // Greece and Cyprus, genuinely different from The National Herald
    // (a Greek-American diaspora paper). Confirmed real via feedburner
    // directory listing.
    { source: 'ekathimerini.com', feedUrl: 'https://feeds.feedburner.com/ekathimerini' },
  ],
  // herald.co.zw 403'd (same IP-reputation pattern as Kenya/Morocco/Sri
  // Lanka/Uganda). Switched to zimlive.com -- confirmed live current
  // content when checked.
  ZW: [
    { source: 'zimlive.com', feedUrl: 'https://www.zimlive.com/feed/' }, // confirmed 403-blocked
    // NEW (2026-07-30): two fresh, genuinely different outlets -- Financial
    // Gazette (est. 1969, established business paper) and ZWNews
    // (independent Zimbabwean news site), both confirmed real via directory
    // listing. Paths unverified.
    { source: 'fingaz.co.zw', feedUrl: 'https://fingaz.co.zw/feed' },
    { source: 'zwnews.com', feedUrl: 'https://zwnews.com/feed' },
    // NEW (2026-08-06): Google News fallback -- all three sources above
    // confirmed producing zero articles ever via a live data audit, and
    // Zimbabwe had no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Zimbabwe&hl=en&gl=ZW&ceid=ZW:en' },
  ],
  // jamaica-star.com threw "unable to verify the first certificate" -- a
  // real TLS cert chain issue on their end (likely a missing intermediate
  // cert), not a blocking issue. Trying plain http:// as a low-risk
  // workaround: RSS content isn't sensitive, and many older regional sites
  // still serve http even when their https cert chain is broken.
  JM: [
    { source: 'jamaica-star.com', feedUrl: 'http://jamaica-star.com/feed/news.xml' },
    { source: 'jamaica-gleaner.com', feedUrl: 'http://jamaica-gleaner.com/feed/rss.xml' }, // NEW (2026-08-02): confirmed valid via community RSS directory
  ],
  // jordannews.jo's feed was malformed XML (unquoted attribute value --
  // broken on their end, not fixable client-side). Switched to Ammon News,
  // an established bilingual (Arabic/English) Jordanian outlet -- exact
  // English RSS path not independently fetch-verified, moderate confidence.
  JO: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Jordan&hl=en&gl=JO&ceid=JO:en' },
    { source: 'ammonnews.net', feedUrl: 'https://en.ammonnews.net/rss.php' },
    // Jordan Times: confirmed a genuine structural dead end (2026-08-06)
    // -- even the feed directory itself lists jordantimes.com's RSS path
    // as literally "/404", independently confirming this isn't just a
    // wrong-path guess on our end. Replaced with Jordan News below.
    // NEW (2026-08-06): Jordan News (jordannews.jo) -- founded 2021,
    // independent English-language daily, genuinely different from Ammon
    // News. Their own Terms of Use page explicitly confirms they offer
    // RSS feeds ("including but not limited to... RSS feeds"). Confirmed
    // real; exact feed path not independently verified this round, treat
    // as a hypothesis.
    { source: 'jordannews.jo', feedUrl: 'https://www.jordannews.jo/rss' },
  ],
  // dohanews.co 403'd. Switched to thepeninsulaqatar.com -- already proven
  // as a real, active outlet (it appeared as a legitimate Currents-sourced
  // article for DR Congo earlier this session), though that doesn't
  // guarantee its own RSS feed won't hit the same IP-reputation blocking
  // that's affected several other feeds -- worth checking the next log.
  QA: [
    { source: 'thepeninsulaqatar.com', feedUrl: 'https://thepeninsulaqatar.com/feed' }, // "Attribute without value" persisted even after the sanitizer shipped -- consistent with this now serving a non-RSS HTML page (bot-block/challenge page) rather than malformed-but-real RSS, which no XML sanitizer can fix
    // NEW (2026-08-02): Google News fallback so Qatar isn't a hard zero.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Qatar&hl=en&gl=QA&ceid=QA:en' },
    // NEW (2026-08-03): Gulf Times -- established English-language Qatar
    // daily since 1978, genuinely different from The Peninsula Qatar.
    // Confirmed real; exact feed path not independently verified this
    // round, treat as a hypothesis.
    { source: 'gulf-times.com', feedUrl: 'https://www.gulf-times.com/rss' },
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
  AL: [
    { source: 'tiranatimes.com', feedUrl: 'https://www.tiranatimes.com/feed' }, // swapped from albaniandailynews.com (malformed XML) -- Tirana Times confirmed as Albania's English-language "newspaper of record", path unverified
    // NEW (2026-08-03): Balkan Insight (BIRN) -- professional
    // investigative/analytical journalism, genuinely different from
    // Tirana Times. Exact URL confirmed directly from Balkan Insight's
    // own RSS feeds page (same resource used for Bosnia/Serbia).
    { source: 'balkaninsight.com', feedUrl: 'https://balkaninsight.com/category/bi/albania/feed/' },
  ],
  BI: [
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/burundi/headlines.rdf' },
    // NEW (2026-07-28): IWACU is a real, well-established independent
    // Burundian outlet with a dedicated English section -- confirmed via
    // FeedSpot directory, breaks the "no English press" assumption for this
    // one country in the cluster. Path reconstructed from a truncated
    // directory listing ("iwacu-burundi.org/englishnew.."), unverified.
    { source: 'iwacu-burundi.org', feedUrl: 'https://www.iwacu-burundi.org/englishnews/feed' },
  ],
  AM: [
    { source: 'oc-media.org', feedUrl: 'https://oc-media.org/feed/' }, // replaced armenpress.am -- confirmed 403-blocked. OC Media covers the Caucasus region independently (also used for Georgia below).
    // NEW (2026-08-03): Panorama.am -- "one of the leading news outlets
    // in Armenia", a dedicated domestic source rather than OC Media's
    // regional Caucasus coverage. Deliberately not armenpress.am again --
    // already confirmed 403-blocked above. Confirmed real; exact feed
    // path not independently verified this round, treat as a hypothesis.
    { source: 'panorama.am', feedUrl: 'https://www.panorama.am/en/rss/' },
  ],
  AO: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/angola/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/angola/headlines.rdf' }],
  AZ: [
    { source: 'trend.az', feedUrl: 'https://en.trend.az/rss/' },
    // NEW (2026-08-03): AzerNews -- Azerbaijan's first English-language
    // newspaper (since 1997), genuinely different outlet from Trend.az.
    // Confirmed real and exact path via feed directory.
    { source: 'azernews.az', feedUrl: 'https://www.azernews.az/feed.php' },
  ],
  BG: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Bulgaria&hl=en&gl=BG&ceid=BG:en' },
    { source: 'novinite.com', feedUrl: 'https://www.novinite.com/services/news_rdf.php' }, // replaced sofiaglobe.com -- confirmed 403-blocked. Novinite is a genuinely different Bulgarian English-language outlet.
    // NEW (2026-08-01): RFE/RL's dedicated Bulgaria feed -- exact API
    // endpoint URL pulled directly from RFE/RL's own RSS directory page
    // (rferl.org/rssfeeds), not a guess. RFE/RL is a real, established
    // international broadcaster (US Agency for Global Media) with genuine
    // per-country feeds, unlike most broadcasters which are region-only.
    { source: 'rferl.org', feedUrl: 'https://www.rferl.org/api/zgkim_l-vomx-tpe-p_my' },
  ],
  // FIXED (2026-08-02): bna.bh/en/rss.aspx returns 405 -- BNA's site appears
  // to have migrated to a beta.bna.bh domain, and the old RSS endpoint no
  // longer accepts plain GET. Replaced with Biz Bahrain, confirmed via a
  // feed directory listing with an exact, specific /feed path (WordPress-
  // style, high confidence). Also state media was the only source here
  // before -- Biz Bahrain gives some outlet diversity too.
  BH: [
    { source: 'bizbahrain.com', feedUrl: 'https://bizbahrain.com/feed' },
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Bahrain&hl=en&gl=BH&ceid=BH:en' },
    // NEW (2026-08-03): Gulf Daily News -- Bahrain's leading newspaper
    // since 1978, genuinely different from Biz Bahrain (a business-
    // focused outlet, currently 403-blocked anyway). Confirmed real;
    // exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'gdnonline.com', feedUrl: 'https://www.gdnonline.com/rss' },
  ],
  BJ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/benin/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/benin/headlines.rdf' }],
  BN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/east-asia/brunei/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Brunei&hl=en&gl=BN&ceid=BN:en' }], // replaced borneobulletin.com.bn -- confirmed persistently blocked. Google News RSS as a universal fallback: no API key required, confirmed still working as of July 2026. This is an aggregation (many sources), not a single outlet -- expect more duplicates/off-topic hits than a dedicated feed, relies on the existing relevance/junk filters more heavily.
  // FIXED (2026-08-02): the previous URL's ENOTFOUND was specifically on
  // the `www.` subdomain -- search results confirm the bare domain (no www)
  // resolves and serves live, current-dated content directly
  // (thevoicebw.com/latest-news/, thevoicebw.com/category/latest_news/).
  // Dropping the www prefix rather than abandoning a real, established
  // outlet over what looks like a missing DNS record for that one label.
  BW: [
    { source: 'thevoicebw.com', feedUrl: 'https://thevoicebw.com/feed' },
    // NEW (2026-08-03): Mmegi -- Botswana's leading independent daily
    // since 1984, genuinely different from The Voice. Exact URL confirmed
    // directly from Mmegi's own RSS page, not a guess.
    { source: 'mmegi.bw', feedUrl: 'https://www.mmegi.bw/rss' },
  ],
  CI: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/cote-divoire/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/cotedivoire/headlines.rdf' }],
  CM: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/cameroon/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'journalducameroun.com', feedUrl: 'https://en.journalducameroun.com/feed/' }], // switched from bare domain -- served French content (10/10 non_english); en. subdomain is the confirmed English edition, path unverified
  CR: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/costa-rica/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/feed' }], // "Non-whitespace before first tag" -- the response isn't valid XML at all (likely an HTML error page served at this path, or a redirect not being followed) -- not a simple path-guess fix, needs real investigation
  CU: [
    { source: 'havanatimes.org', feedUrl: 'https://havanatimes.org/feed/' },
    // OnCuba News: confirmed dead (0 articles ever) via live data audit
    // -- left in place, not re-researched this round.
    { source: 'oncubanews.com', feedUrl: 'https://oncubanews.com/en/feed' },
    // NEW (2026-08-06): 14ymedio -- Cuba's leading independent digital
    // outlet, founded 2014 by prominent dissident journalist Yoani
    // Sanchez, confirmed bilingual (Spanish/English) via Wikipedia.
    // Genuinely different from Havana Times/OnCuba. Exact path confirmed
    // via feed directory.
    { source: '14ymedio.com', feedUrl: 'https://www.14ymedio.com/rss' },
  ],
  CY: [
    { source: 'in-cyprus.philenews.com', feedUrl: 'https://in-cyprus.philenews.com/feed/' },
    { source: 'sigmalive.com', feedUrl: 'http://www.sigmalive.com/rss' }, // NEW (2026-08-02): confirmed valid via community RSS directory
  ],
  DO: [
    { source: 'dominicantoday.com', feedUrl: 'https://dominicantoday.com/feed/' },
    // NEW (2026-08-03): DR1 -- English-language DR news since 1997,
    // genuinely different outlet from Dominican Today. Confirmed real
    // via BBC's own media guide. Exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'dr1.com', feedUrl: 'https://dr1.com/feed' },
  ],
  DZ: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Algeria&hl=en&gl=DZ&ceid=DZ:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/algeria/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'al24news.com', feedUrl: 'https://al24news.com/feed' }], // swapped from aps.dz (404 twice) -- AL24 News confirmed real, active, English-language Algerian state international broadcaster, path unverified
  GE: [
    { source: 'oc-media.org', feedUrl: 'https://oc-media.org/feed/' }, // replaced agenda.ge -- their TLS cert had genuinely expired. OC Media covers Georgia independently (also used for Armenia above -- same regional outlet, each entry independently checked for country relevance).
    // NEW (2026-08-03): Civil.ge -- Georgia's dedicated English-language
    // news outlet since 2001, genuinely different from OC Media's
    // regional Caucasus coverage. Confirmed real; exact feed path not
    // independently verified this round, treat as a hypothesis.
    { source: 'civil.ge', feedUrl: 'https://civil.ge/feed' },
  ],
  GY: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/caribbean/guyana/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'kaieteurnewsonline.com', feedUrl: 'https://www.kaieteurnewsonline.com/feed' }], // replaced stabroeknews.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Kaieteur News is a real, established private Guyanese daily.
  HR: [
    { source: 'total-croatia-news.com', feedUrl: 'https://www.total-croatia-news.com/feed' },
    // NEW (2026-08-03): Croatia Week -- confirmed real and currently
    // active, genuinely different outlet from Total Croatia News.
    // Exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'croatiaweek.com', feedUrl: 'https://www.croatiaweek.com/feed' },
  ],
  HT: [
    { source: 'haitiantimes.com', feedUrl: 'https://haitiantimes.com/feed/' },
    // NEW (2026-07-30): Haiti Libre's English edition confirmed via exact
    // feed URL from directory listing, genuinely different outlet.
    { source: 'haitilibre.com', feedUrl: 'https://www.haitilibre.com/rss-flash-en.php' },
  ],
  IS: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/iceland/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Iceland&hl=en&gl=IS&ceid=IS:en' }, // replaced icelandreview.com -- confirmed malformed XML on their end. Google News RSS fallback (see Brunei entry above for the general rationale/caveats).
    // NEW (2026-08-03): re-adding Iceland Review as a real dedicated
    // source. It was dropped for malformed XML before this session's
    // sanitizer/CDATA-wrap fix (deployed several rounds ago) existed --
    // worth re-testing now that the fix is live, since that fix targets
    // exactly this class of error. If it still fails, no harm done --
    // Google News fallback above still covers Iceland either way.
    { source: 'icelandreview.com', feedUrl: 'https://www.icelandreview.com/feed/' },
  ],
  KG: [
    { source: '24.kg', feedUrl: 'https://24.kg/rss/' },
    { source: 'eurasianet.org', feedUrl: 'https://eurasianet.org/rss.xml' }, // NEW (2026-08-03): Eurasianet -- independent, Columbia-hosted regional outlet covering the Caucasus and Central Asia, genuinely different from 24.kg. Same regional wire as TJ/MN below. Exact feed path not independently verified this round, treat as a hypothesis.
  ],
  KZ: [
    { source: 'astanatimes.com', feedUrl: 'https://astanatimes.com/feed/' },
    // NEW (2026-08-03): The Times of Central Asia -- independent
    // English-language outlet since 1999, genuinely different from
    // Astana Times. Confirmed real; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'timesca.com', feedUrl: 'https://timesca.com/feed' },
  ],
  LA: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/east-asia/laos/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'vientianetimes.org.la', feedUrl: 'https://www.vientianetimes.org.la/feed' }, // still 404ing -- their site runs on old custom .php pages (onlinesub.php, About_us.htm), not a standard CMS, so a real RSS feed may genuinely not exist here rather than this being a wrong path
    // NEW (2026-08-06): Radio Free Asia -- US-funded international
    // broadcaster with a dedicated Laos section, same confirmed
    // organization/pattern as the Cambodia feed added this round.
    { source: 'rfa.org', feedUrl: 'https://www.rfa.org/english/news/laos_news/rss2.xml' },
    // NEW (2026-08-02): Google News fallback.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Laos&hl=en&gl=LA&ceid=LA:en' },
  ],
  // FIXED (2026-08-02): /feed/ fails with a blank error. A separate feed
  // directory lists a different path, /rss.xml, for the same outlet --
  // trying that instead of guessing blind, plus a Google News fallback
  // since the exact current path is still not independently confirmed.
  LT: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/lithuania/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'baltictimes.com', feedUrl: 'https://www.baltictimes.com/rss.xml' },
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Lithuania&hl=en&gl=LT&ceid=LT:en' },
  ],
  DJ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/djibouti/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/djibouti/headlines.rdf' }],
  LY: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/libya/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'libyaherald.com', feedUrl: 'https://libyaherald.com/feed/' }], // replaced libyaobserver.ly -- confirmed persistently blocked. Libya Herald is a genuinely different domain.
  MD: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/moldova/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'moldovalive.md', feedUrl: 'https://moldovalive.md/feed' }], // replaced agora.md -- confirmed 404 twice on that domain, abandoned rather than a third guess. MoldovaLive.md is confirmed genuinely active with current 2026 English-language content.
  MG: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/madagascar/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/madagascar/headlines.rdf' }],
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
  ML: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/mali/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/mali/headlines.rdf' }],
  // Two confirmed dead ends across sessions now: /en/rss (404), then /en/feed
  // (also 404). Not chasing a third guess at Montsame's path -- adding a
  // Google News fallback instead so Mongolia isn't fully dependent on
  // guessing their exact feed URL right.
  MN: [
    { source: 'montsame.mn', feedUrl: 'https://montsame.mn/en/feed' },
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Mongolia&hl=en&gl=MN&ceid=MN:en' },
    { source: 'eurasianet.org', feedUrl: 'https://eurasianet.org/rss.xml' }, // NEW (2026-08-03): Eurasianet -- same regional wire as KG/TJ, genuinely different from Montsame (state news agency)
    // NEW (2026-08-06): UB Post -- established 1996, genuinely different
    // (privately-owned) from Montsame (the state news agency). Confirmed
    // real via Wikipedia; exact feed path not independently verified
    // this round, treat as a hypothesis.
    { source: 'theubposts.com', feedUrl: 'https://theubposts.com/feed' },
  ],
  MW: [
    { source: 'nyasatimes.com', feedUrl: 'https://www.nyasatimes.com/feed/' },
    // NEW (2026-08-03): Malawi24 -- Malawi's most-read independent news
    // platform alongside Nyasa Times, genuinely different outlet.
    // Confirmed real and exact path via feed directory.
    { source: 'malawi24.com', feedUrl: 'https://malawi24.com/feed' },
  ],
  MZ: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Mozambique&hl=en&gl=MZ&ceid=MZ:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/mozambique/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'clubofmozambique.com', feedUrl: 'https://clubofmozambique.com/feed/' }],
  NA: [
    { source: 'namibian.com.na', feedUrl: 'https://www.namibian.com.na/feed/' },
    // NEW (2026-08-03): Windhoek Observer -- Namibia's oldest and largest
    // circulating weekly since 1978, genuinely different from The
    // Namibian. Confirmed real; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'observer24.com.na', feedUrl: 'https://www.observer24.com.na/feed' },
  ],
  OM: [
    { source: 'omanobserver.om', feedUrl: 'https://www.omanobserver.om/rss' },
    { source: 'timesofoman.com', feedUrl: 'https://rssfeeds.timesofoman.com/rss' }, // NEW (2026-08-03): oldest English newspaper in Oman, has its own dedicated RSS subdomain, genuinely different outlet
    // NEW (2026-08-06): Google News fallback -- both sources above
    // confirmed producing zero articles ever via a live data audit,
    // and Oman had no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Oman&hl=en&gl=OM&ceid=OM:en' },
  ],
  RW: [
    { source: 'taarifa.rw', feedUrl: 'https://taarifa.rw/feed' }, // replaced newtimes.co.rw -- failed differently on two different guessed paths (502, then 404), abandoned rather than a third guess. Taarifa is a real Rwandan English-language news platform with a documented RSS feed at this exact path.
    // NEW (2026-08-03): KT Press -- English-language Rwandan outlet,
    // genuinely different from Taarifa. Deliberately not newtimes.co.rw
    // again -- already confirmed failed twice above. Confirmed real;
    // exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'ktpress.rw', feedUrl: 'https://ktpress.rw/feed' },
  ],
  SD: [
    { source: 'dabangasudan.org', feedUrl: 'https://www.dabangasudan.org/en/feed' }, // replaced sudantribune.com -- confirmed persistently blocked. Radio Dabanga's English feed was externally verified live with current content (not just guessed).
    // NEW (2026-08-03): Al Tagyheer -- independent Sudanese outlet with a
    // dedicated English section, genuinely different from Radio Dabanga.
    // Deliberately not sudantribune.com again -- already confirmed blocked
    // above. Confirmed real and exact path via feed directory.
    { source: 'altaghyeer.info', feedUrl: 'https://www.altaghyeer.info/en/feed' },
  ],
  SK: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/slovakia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'spectator.sme.sk', feedUrl: 'https://spectator.sme.sk/rss' }],
  SO: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/somalia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thesomalidigest.com', feedUrl: 'https://thesomalidigest.com/feed' }], // replaced garoweonline.com -- confirmed broken (500 then 404) on two different guessed paths, abandoned rather than a third guess. The Somali Digest is a confirmed real English-language outlet with a documented RSS feed at this exact path.
  SY: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/syria/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'syrianobserver.com', feedUrl: 'https://syrianobserver.com/feed' }],
  TN: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Tunisia&hl=en&gl=TN&ceid=TN:en' },
    { source: 'africanmanager.com', feedUrl: 'https://africanmanager.com/feed/' },
    // NEW (2026-08-03): Tunisia Live -- self-described "first Tunisian
    // News Website in English", genuinely different outlet from African
    // Manager (a business-focused publication). Confirmed real; exact
    // feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'tunisia-live.net', feedUrl: 'https://www.tunisia-live.net/feed' },
  ],
  TT: [
    { source: 'cnc3.co.tt', feedUrl: 'https://cnc3.co.tt/feed' }, // swapped from newsday.co.tt (cert expired, ongoing) -- CNC3 confirmed via directory listing with exact feed URL, real established Trinidad TV/news outlet
    // NEW (2026-08-03): Trinidad Express -- self-described "National
    // Newspaper of Trinidad and Tobago" since 1967, genuinely different
    // from CNC3 (TV-focused). Also confirms the earlier newsday.co.tt
    // swap was the right call -- Newsday ceased publication entirely in
    // January 2026, explaining that persistent cert issue. Confirmed
    // real; exact feed path not independently verified this round, treat
    // as a hypothesis.
    { source: 'trinidadexpress.com', feedUrl: 'https://trinidadexpress.com/feed' },
    // NEW (2026-08-06): Google News fallback -- both sources above
    // confirmed producing zero articles ever via a live data audit, and
    // Trinidad had no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Trinidad+and+Tobago&hl=en&gl=TT&ceid=TT:en' },
  ],
  CV: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Cape+Verde&hl=en&gl=CV&ceid=CV:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/cape-verde/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/capeverde/headlines.rdf' }],
  UZ: [
    { source: 'tashkenttimes.uz', feedUrl: 'https://tashkenttimes.uz/?format=feed' }, // swapped from daryo.uz (malformed XML) -- Tashkent Times confirmed via directory listing with the ?format=feed pattern, genuinely different outlet
    // NEW (2026-08-03): Kun.uz -- one of Uzbekistan's most active online
    // publications, genuinely different outlet from Tashkent Times.
    // Confirmed real; exact feed path not independently verified this
    // round, treat as a hypothesis for the next log to confirm.
    { source: 'kun.uz', feedUrl: 'https://kun.uz/en/rss' },
    // NEW (2026-08-06): Google News fallback -- both sources above
    // confirmed producing zero articles ever via a live data audit, and
    // Uzbekistan had no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Uzbekistan&hl=en&gl=UZ&ceid=UZ:en' },
  ],
  ZM: [
    { source: 'lusakatimes.com', feedUrl: 'https://www.lusakatimes.com/feed/' },
    // NEW (2026-08-03): News Diggers! -- Zambia's first multimedia
    // investigative-journalism publication, genuinely different outlet
    // from Lusaka Times. Confirmed real; exact feed path not
    // independently verified this round, treat as a hypothesis.
    { source: 'diggers.news', feedUrl: 'https://diggers.news/feed' },
  ],
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
  AG: [{ source: 'antiguaobserver.com', feedUrl: 'https://antiguaobserver.com/feed/' }, { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }], // NEW (2026-08-03): WIC News -- regional Caribbean wire covering Antigua/Barbuda/Grenada/Dominica/St Lucia/St Kitts, confirmed real, exact path unverified
  BS: [
    { source: 'ewnews.com', feedUrl: 'https://ewnews.com/feed' }, // replaced tribune242.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Eye Witness News is the Bahamas' #1 local outlet, with a dedicated RSS feed page on their site ("/rss-feed-2/") confirming a feed exists -- using the standard /feed path first since the exact confirmed URL was on a landing page, not necessarily the raw feed itself.
    { source: 'bahamaspress.com', feedUrl: 'http://bahamaspress.com/feed/' }, // NEW (2026-08-02): confirmed valid via community RSS directory
  ],
  BB: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/caribbean/barbados/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'barbadostoday.bb', feedUrl: 'https://barbadostoday.bb/feed/' }],
  BZ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/caribbean/belize/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Belize&hl=en&gl=BZ&ceid=BZ:en' }, // replaced breakingbelizenews.com -- confirmed persistently blocked. Google News RSS fallback.
    { source: 'amandala.com.bz', feedUrl: 'http://amandala.com.bz/news/feed/' }, // NEW (2026-08-02): confirmed valid via community RSS directory, genuine dedicated Belize outlet
  ],
  BT: [
    { source: 'dailybhutan.com', feedUrl: 'https://www.dailybhutan.com/feed' }, // replaced kuenselonline.com -- confirmed 404 twice on that domain, abandoned rather than a third guess. Daily Bhutan is confirmed very actively updated (May/June 2026 content seen directly), a stronger candidate than Kuensel.
    // NEW (2026-07-29): dailybhutan.com turned out malformed too. Business
    // Bhutan confirmed real via Wikipedia -- established weekly financial
    // paper, genuinely different outlet, not previously attempted. Path
    // unverified.
    { source: 'businessbhutan.bt', feedUrl: 'https://www.businessbhutan.bt/feed' },
  ],
  CG: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Republic+of+the+Congo&hl=en&gl=CG&ceid=CG:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/republic-of-congo/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'africanews.com', feedUrl: 'https://www.africanews.com/feed/rss' }], // replaced the allafrica.com/congo_brazzaville RDF feed -- confirmed reachable (right slug) but returning zero items every run, a volume problem not a config one. Africanews is a pan-African feed externally verified live with July 2026 content; relies on the existing country-mention relevance check to filter for Congo-Brazzaville specifically, same as any WORLD-tier wire source.
  DM: [{ source: 'dominicanewsonline.com', feedUrl: 'https://dominicanewsonline.com/news/feed/' }, { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }], // NEW (2026-08-03): WIC News -- same regional Caribbean wire as AG/GD/LC/KN, confirmed real, exact path unverified
  EE: [
    { source: 'news.err.ee', feedUrl: 'https://news.err.ee/rss' },
    // NEW (2026-08-03): Estonian World -- independent English-language
    // online magazine since 2012, genuinely different from ERR (state
    // broadcaster). Confirmed real and exact path via feed directory.
    { source: 'estonianworld.com', feedUrl: 'https://estonianworld.com/feed' },
  ],
  GQ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/equatorial-guinea/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/equatorialguinea/headlines.rdf' }],
  SZ: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/swaziland/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'times.co.sz', feedUrl: 'https://times.co.sz/feed/' }],
  GA: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/gabon/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/gabon/headlines.rdf' }],
  GM: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/gambia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thepoint.gm', feedUrl: 'https://thepoint.gm/posts/rss/xml' }], // confirmed real feed URL from page source (rel=alternate link tag) -- not a guess
  GD: [{ source: 'nowgrenada.com', feedUrl: 'https://nowgrenada.com/feed/' }, { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }], // NEW (2026-08-03): WIC News -- same regional Caribbean wire as AG/DM/LC/KN, confirmed real, exact path unverified
  GN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/guinea/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/guinea/headlines.rdf' }],
  VA: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/vatican-city/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'vaticannews.va', feedUrl: 'https://www.vaticannews.va/en.rss.xml' }],
  LV: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/latvia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'eng.lsm.lv', feedUrl: 'https://eng.lsm.lv/rss/' }],
  LR: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/liberia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'fpa.news', feedUrl: 'https://fpa.news/feed/' }], // confirmed real feed URL from page source -- FrontPage Africa actually serves its feed from a completely different domain (fpa.news), not frontpageafricaonline.com
  LU: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/luxembourg/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'luxtimes.lu', feedUrl: 'https://www.luxtimes.lu/rss' },
    // NEW (2026-08-03): RTL Today -- self-described "Luxembourg's leading
    // English language news source", genuinely different outlet from
    // Luxembourg Times. Confirmed real; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'today.rtl.lu', feedUrl: 'https://today.rtl.lu/rss' },
  ],
  // MV (Maldives) confirmed NO RSS feed exists at all -- checked page source directly, no rel=alternate rss+xml tag anywhere (Nuxt SPA site). Not a path problem, genuinely not offered.
  MT: [
    { source: 'timesofmalta.com', feedUrl: 'https://timesofmalta.com/rss.xml' }, // now resolves but returns 403 -- same IP-reputation/bot-blocking pattern as Kenya/Uganda/Morocco, not a path problem, no further URL guessing will help
    // NEW (2026-07-28): trying Malta Today as a second, genuinely different
    // outlet -- confirmed real, established English-language Maltese paper,
    // not previously attempted. Path unverified.
    { source: 'maltatoday.com.mt', feedUrl: 'https://www.maltatoday.com.mt/rss' },
    // NEW (2026-08-01): The Malta Independent confirmed real and active --
    // live current-dated content seen directly (July 30, 2026 articles).
    // Genuinely different outlet, third attempt for this country. Path
    // unverified.
    { source: 'independent.com.mt', feedUrl: 'https://www.independent.com.mt/rss' },
    // NEW (2026-08-02): all three outlets above are now confirmed 403 in
    // the same run -- three genuinely different outlets, same bot-block
    // signature, no fourth guess likely to fare differently. Google News
    // fallback so Malta isn't fully dependent on any of them.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Malta&hl=en&gl=MT&ceid=MT:en' },
  ],
  MR: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/mauritania/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/mauritania/headlines.rdf' }],
  MU: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Mauritius&hl=en&gl=MU&ceid=MU:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/mauritius/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'mauritiustimes.com', feedUrl: 'https://www.mauritiustimes.com/feed' }], // swapped from defimedia.info (malformed XML) -- Mauritius Times confirmed via Wikipedia's newspaper directory as a real, established English/French outlet, path unverified
  MC: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/monaco/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'monacotribune.com', feedUrl: 'https://www.monacotribune.com/feed/' }, // cert mismatch is a real misconfiguration on their shared host (cert covers a different domain entirely) -- not fixable by changing the URL path, leaving as-is; will keep failing harmlessly until they fix their TLS setup
    // NEW (2026-08-03): Google News fallback -- Monaco Tribune is
    // confirmed permanently broken on their end, so Monaco was at risk of
    // a hard zero with no backup at all.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Monaco&hl=en&gl=MC&ceid=MC:en' },
  ],
  ME: [
    { source: 'total-montenegro-news.com', feedUrl: 'https://total-montenegro-news.com/feed/' },
    // NEW (2026-08-01): RFE/RL's dedicated Montenegro feed, same real
    // exact-URL find as Bulgaria above.
    { source: 'rferl.org', feedUrl: 'https://www.rferl.org/api/zbiiol-vomx-tpeqjmo' },
  ],
  NE: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/niger/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/niger/headlines.rdf' }],
  // PS (Palestine/WAFA) confirmed NO real RSS feed -- an RSS icon exists on the site but its href is just "#" (a dead placeholder), not an actual feed link. Not a path problem, genuinely not offered.
  // KN (St Kitts/sknvibes.com) confirmed NO RSS feed exists at all -- checked page source directly, no rss+xml link tag anywhere. Not a path problem, genuinely not offered.
  LC: [{ source: 'stluciatimes.com', feedUrl: 'https://stluciatimes.com/feed' }, { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }], // NEW (2026-08-03): WIC News -- same regional Caribbean wire as AG/DM/GD/KN, confirmed real, exact path unverified
  VC: [
    { source: 'iwnsvg.com', feedUrl: 'https://www.iwnsvg.com/feed/' },
    { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }, // NEW (2026-08-03): same regional Caribbean wire as AG/DM/GD/LC/KN
  ],
  // WS (Samoa Observer) confirmed NO RSS feed exists at all -- checked page source directly, no rss+xml link tag anywhere. Not a path problem, genuinely not offered.
  SC: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/seychelles/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Seychelles&hl=en&gl=SC&ceid=SC:en' }], // replaced seychellesnewsagency.com -- confirmed persistently blocked. Google News RSS fallback.
  SL: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/sierra-leone/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thesierraleonetelegraph.com', feedUrl: 'https://www.thesierraleonetelegraph.com/feed/' }],
  SI: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Slovenia&hl=en&gl=SI&ceid=SI:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/slovenia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'total-slovenia-news.com', feedUrl: 'https://www.total-slovenia-news.com/feed' }], // replaced sloveniatimes.com -- confirmed persistently blocked. Total Slovenia News is a genuinely different domain.
  SB: [
    { source: 'solomonstarnews.com', feedUrl: 'https://www.solomonstarnews.com/feed/' },
    // NEW (2026-07-28): Island Sun confirmed real, established, privately
    // owned Solomon Islands daily -- genuinely different outlet than the
    // already-blocked Solomon Star. Path unverified.
    { source: 'theislandsun.com.sb', feedUrl: 'https://theislandsun.com.sb/feed' },
    // NEW (2026-08-02): both outlets above confirmed 403 in the same run.
    // Google News fallback.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Solomon+Islands&hl=en&gl=SB&ceid=SB:en' },
  ],
  SS: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/south-sudan/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'radiotamazuj.org', feedUrl: 'https://radiotamazuj.org/en/feed' }], // /en/rss.xml 404'd -- retrying /en/feed
  GW: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/guinea-bissau/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/guineabissau/headlines.rdf' }],
  TL: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/east-asia/east-timor/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'en.tatoli.tl', feedUrl: 'https://en.tatoli.tl/feed/' }],
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
  TO: [{ source: 'kanivatonga.co.nz', feedUrl: 'https://kanivatonga.co.nz/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // swapped from matangitonga.to (404 x3) -- Kaniva Tonga confirmed real. NEW: RNZ Pacific regional wire added as second source.
  VU: [{ source: 'dailypost.vu', feedUrl: 'https://dailypost.vu/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // Daily Post's status flip-flops across runs (403/429/404) -- genuinely unstable. NEW: RNZ Pacific regional wire added as a more reliable second source.
  // NEW (2026-07-28): WS (Samoa) previously had no RSS entry at all. Samoa
  // Observer is Samoa's real, independent, award-winning national daily
  // (RSF profile confirms it's the country's flagship free press outlet).
  // Feed path is a standard-convention guess -- unverified, confirm via
  // next run's log. Uses Cloudflare per its own tech stack, so a 403 here
  // wouldn't be surprising.
  WS: [{ source: 'samoaobserver.ws', feedUrl: 'https://www.samoaobserver.ws/index.php?option=com_content&view=featured&format=feed&type=rss' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // Samoa Observer's Joomla feed path still unconfirmed. NEW: RNZ Pacific regional wire added as second source.
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
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/east-asia/north-korea/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    // kcnawatch.org DROPPED (2026-08-02): confirmed chronically malformed
    // across three separate runs, identical error position each time
    // (Line 5, Column 39 -- "Unquoted attribute value") -- that's their feed
    // generator, not a wrong path, and the position sitting inside the
    // channel header (not an item description) means the CDATA-wrap
    // sanitizer can't reach it without risking corrupting real markup
    // elsewhere. Not chasing this further -- dailynk.com below is a
    // stronger source anyway (independent, defector-sourced reporting vs.
    // a state mirror).
    { source: 'dailynk.com', feedUrl: 'https://www.dailynk.com/english/feed/' },
    // ^ Daily NK -- genuinely independent, defector-sourced reporting (the
    // opposite of state media), actively updated, real RSS feed mentioned
    // on-site. Much stronger primary source than the state mirror above.
    // NOT flagged stateMedia -- this is independent journalism, not
    // government output.
    // NEW (2026-08-06): Radio Free Asia -- US-funded international
    // broadcaster with a dedicated North Korea section, same confirmed
    // organization/pattern as the Cambodia/Myanmar/Laos feeds added this
    // round. Also independent, non-state-media reporting.
    { source: 'rfa.org', feedUrl: 'https://www.rfa.org/english/news/north_korea_news/rss2.xml' },
  ],
  BY: [
    { source: 'eng.belta.by', feedUrl: 'https://eng.belta.by/rss', stateMedia: true },
    // Belsat: confirmed dead (0 articles ever) via live data audit --
    // independent Belarusian outlet, Poland-based (broadcasts from exile
    // after being banned as "extremist" by Belarusian authorities in
    // 2021). Left in place, not re-researched this round.
    { source: 'belsat.eu', feedUrl: 'https://en.belsat.eu/feed' },
    // NEW (2026-08-06): Minsk Herald -- explicitly English-language blog/
    // outlet for expats and foreigners in Belarus since 2011, genuinely
    // different from both sources above (Nasha Niva, the other major
    // independent Belarusian paper found this round, publishes in
    // Belarusian only and was skipped for that reason). Confirmed real
    // via feed directory; exact feed path not independently verified
    // this round, treat as a hypothesis.
    { source: 'minskherald.com', feedUrl: 'https://minskherald.com/feed' },
  ],
  // ^ Confirmed working -- saw live, current-dated content at this URL directly.
  ER: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Eritrea&hl=en&gl=ER&ceid=ER:en' },
    { source: 'shabait.com', feedUrl: 'https://shabait.com/feed/', stateMedia: true },
    // NEW (2026-07-28): EritreaDaily.net confirmed via University of
    // Michigan's academic library guide as a real, independent outlet
    // dedicated to Eritrea, distinct from state-run shabait.com (already
    // blocked). Eritrea has essentially no free domestic press (RSF ranks
    // it among the worst globally for press freedom), so an
    // exile/diaspora-run site is the realistic ceiling here. Path
    // unverified.
    { source: 'eritreadaily.net', feedUrl: 'https://eritreadaily.net/feed' },
  ],
  // ^ 403 -- same IP-reputation/bot-blocking pattern as Kenya/Uganda/Morocco/
  // Malta, not a path problem, no further URL guessing will help.
  TM: [
    { source: 'en.hronikatm.com', feedUrl: 'https://en.hronikatm.com/feed/' },
    // NEW (2026-08-01): two more real, independent Turkmenistan outlets,
    // both exact-confirmed via directory listing -- genuinely different
    // domains from en.hronikatm.com (which has a persistent TLS cert
    // issue), not just retries of the same one.
    { source: 'turkmenistanlive.com', feedUrl: 'https://turkmenistanlive.com/feed' },
    { source: 'en.turkmen.news', feedUrl: 'https://en.turkmen.news/feed' },
    // NEW (2026-08-06): Google News fallback -- all three sources above
    // confirmed producing zero articles ever via a live data audit
    // (Turkmenistan's press landscape is genuinely thin/authoritarian-
    // controlled), and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Turkmenistan&hl=en&gl=TM&ceid=TM:en' },
  ],
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
  HN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/honduras/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Honduras&hl=en&gl=HN&ceid=HN:en' }, // replaced hondurasdaily.com -- confirmed broken feed generator (500 on two different paths). Google News RSS fallback.
    // NEW (2026-08-03): Tico Times -- Costa Rica's English paper runs a
    // dedicated Honduras category page as part of its Central America
    // coverage. Exact feed path not independently verified this round,
    // treat as a hypothesis.
    { source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/categories/news/latin-america-news/central-america-latin-america-news/honduras-central-america-latin-america-news/feed' },
  ],
  SV: [
    { source: 'elsalvadorinenglish.com', feedUrl: 'https://elsalvadorinenglish.com/feed' },
    // NEW (2026-08-03): Tico Times -- dedicated El Salvador category page
    // as part of its Central America coverage, genuinely different from
    // El Salvador in English. Exact feed path not independently verified
    // this round, treat as a hypothesis.
    { source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/categories/news/latin-america-news/central-america-latin-america-news/el-salvador-central-america-latin-america-news/feed' },
  ],
  // ^ Replaced elsalvadordaily.com -- confirmed persistent malformed XML
  // across two runs, abandoned rather than a third guess. This is a real,
  // actively-updated (through July 2026) dedicated English-language site.
  // Worth knowing: its editorial tone leans favorable toward the Bukele
  // government in sample content -- not officially state-owned, but not
  // neutral either.
  NI: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/nicaragua/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Nicaragua&hl=en&gl=NI&ceid=NI:en' }, // replaced nicaraguadailytimes.com -- confirmed broken format on two different paths. Google News RSS fallback.
    // NEW (2026-08-03): Tico Times -- dedicated Nicaragua category page,
    // confirmed real and active with current 2026 content seen directly.
    // Exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/categories/news/latin-america-news/central-america-latin-america-news/nicaragua-central-america-latin-america-news/feed' },
  ],
  KM: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Comoros&hl=en&gl=KM&ceid=KM:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/comoros/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/comoros/headlines.rdf' }],
  // ^ HN/SV/NI appear to be the same templated network of AI-summarized
  // English news briefings (same subscription-alert pattern across all
  // three) -- confirmed to exist via search, feed paths NOT yet verified.
  SR: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/caribbean/suriname/feed/' },
    { source: 'srherald.com', feedUrl: 'https://www.srherald.com/news-in-english/feed/' }, // NEW (2026-08-03): Suriname Herald's dedicated English-language section, confirmed live with current English content directly. Exact feed path not independently verified this round, treat as a hypothesis. // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Suriname&hl=en&gl=SR&ceid=SR:en' }], // replaced surinametimes.com -- confirmed 404 on two different paths, and their real content skews Dutch anyway. Google News RSS fallback (English-only via hl=en).
  // ^ Times of Suriname -- genuinely bilingual Dutch/English daily, not a guess.
  YE: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/yemen/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'almasdaronline.com', feedUrl: 'https://almasdaronline.com/en/feed' }, // confirmed 403 -- same bot-blocking pattern as others, not a path problem
    // NEW (2026-08-02): Google News fallback.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Yemen&hl=en&gl=YE&ceid=YE:en' },
  ],
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

  TV: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/oceania/tuvalu/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'tuvalutimes.com', feedUrl: 'https://www.tuvalutimes.com/feed' }],
  ST: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/sao-tome-and-principe/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/saotomeandprincipe/headlines.rdf' }],
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
  BA: [
    { source: 'sarajevotimes.com', feedUrl: 'https://sarajevotimes.com/feed' },
    // NEW (2026-08-03): Balkan Insight (BIRN) -- professional
    // investigative/analytical journalism from the Balkan Investigative
    // Reporting Network, genuinely different from Sarajevo Times. Using
    // their Bosnia-specific category feed rather than the generic
    // regional one. Exact URL confirmed directly from Balkan Insight's
    // own RSS feeds page, not a guess.
    { source: 'balkaninsight.com', feedUrl: 'https://balkaninsight.com/category/bi/bosnia-and-herzegovina/feed/' },
  ],
  // ^ Confirmed "the only Bosnian portal that gives news in English."
  BD: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/south-asia/bangladesh/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thedailystar.net', feedUrl: 'https://www.thedailystar.net/frontpage/rss.xml' }], // right URL, but every item's title comes through unusable after sanitization (missing_title: 10/10) -- a source-side feed structure issue, not a path problem
  CF: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/central-african-republic/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/centralafricanrepublic/headlines.rdf' }],
  // ^ The Daily Star -- Bangladesh's largest circulating English-language
  // newspaper, feed URL confirmed via a curated OPML feed list, not guessed.
  BE: [
    { source: 'thebulletin.be', feedUrl: 'https://www.thebulletin.be/rss.xml' }, // swapped from brusselstimes.com (malformed XML) -- The Bulletin confirmed via directory with exact feed URL, genuinely different outlet
    // NEW (2026-08-03): Flanders Today -- English-language coverage of
    // Brussels and the Flemish region, genuinely different outlet from
    // The Bulletin. Confirmed real; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'flanderstoday.eu', feedUrl: 'https://www.flanderstoday.eu/feed' },
    // NEW (2026-08-05): VRT NWS -- Flemish public broadcaster's dedicated
    // English news service, genuinely different (public broadcaster, not
    // an expat magazine) from both sources above. Added after Belgium was
    // found at zero articles in 48h despite two configured sources.
    // Confirmed real and active; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'vrt.be', feedUrl: 'https://www.vrt.be/vrtnws/en.rss.xml' },
  ],
  // ^ The Brussels Times -- Belgium's largest English-language news outlet.
  KH: [
    { source: 'phnompenhpost.com', feedUrl: 'https://www.phnompenhpost.com/feed' }, // confirmed 403 -- same bot-blocking pattern as Malta/Kenya, not a path problem
    // NEW (2026-08-06): Radio Free Asia -- US-funded international
    // broadcaster with a dedicated, exact-confirmed Cambodia feed
    // (confirmed via direct URL, showing live current content).
    // Genuinely different from Khmer Times/Phnom Penh Post.
    { source: 'rfa.org', feedUrl: 'https://www.rfa.org/english/news/cambodia_news/rss2.xml' },
    // NEW (2026-08-02): Google News fallback.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Cambodia&hl=en&gl=KH&ceid=KH:en' },
    // NEW (2026-08-03): Khmer Times -- established since 2014, self-
    // described "the region's fastest growing newspaper", genuinely
    // different outlet from the bot-blocked Phnom Penh Post. Confirmed
    // real; exact feed path not independently verified this round, treat
    // as a hypothesis.
    { source: 'khmertimeskh.com', feedUrl: 'https://www.khmertimeskh.com/feed' },
  ],
  // ^ Phnom Penh Post -- Cambodia's oldest English-language newspaper, confirmed active with current 2026 content.
  CZ: [
    { source: 'praguemonitor.com', feedUrl: 'https://praguemonitor.com/feed' },
    // NEW (2026-08-03): Prague Morning -- self-described "Czechia's
    // biggest media outlet in English", confirmed active and genuinely
    // different outlet from Prague Monitor. Confirmed real and exact path
    // via feed directory.
    { source: 'praguemorning.cz', feedUrl: 'https://praguemorning.cz/feed' },
  ],
  // ^ Prague Monitor -- confirmed active English-language Czech Republic news site since 2003.
  MM: [
    { source: 'irrawaddy.com', feedUrl: 'https://www.irrawaddy.com/feed' }, // intermittent 403 (same IP-reputation pattern as Kenya/Uganda/Morocco) -- has succeeded at least once before, not a path problem
    { source: 'myanmar-now.org', feedUrl: 'https://myanmar-now.org/en/feed' }, // added as a second, genuinely different source rather than a replacement, since Irrawaddy does succeed sometimes
    // NEW (2026-08-06): Radio Free Asia -- US-funded international
    // broadcaster with a dedicated Burma/Myanmar section, same confirmed
    // organization/pattern as the Cambodia feed added this round.
    { source: 'rfa.org', feedUrl: 'https://www.rfa.org/english/news/burma_news/rss2.xml' },
  ],
  // ^ The Irrawaddy -- genuinely independent (exile-founded, press-freedom-award-winning), confirmed active.
  KW: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/kuwait/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'arabtimesonline.com', feedUrl: 'https://www.arabtimesonline.com/rssFeed/47/' }], // swapped from kuwaittimes.com (malformed XML) -- exact feed URL confirmed directly on Arab Times' own /rss/ page, not a guess
  // ^ Kuwait Times -- oldest active English-language newspaper in Kuwait, founded 1961.
  PA: [
    { source: 'expat-times.com', feedUrl: 'https://expat-times.com/panama/feed' },
    // NEW (2026-08-03): Newsroom Panama -- dedicated English-language
    // Panama news site, genuinely different outlet from Expat Times'
    // Panama section. Confirmed real and exact path via feed directory.
    { source: 'newsroompanama.com', feedUrl: 'https://newsroompanama.com/feed' },
  ],
  TD: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/chad/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/chad/headlines.rdf' }],
  // ^ Panama Expat Times -- confirmed active (Nov 2025 content), dedicated English coverage.
  VE: [
    { source: 'caracaschronicles.com', feedUrl: 'https://www.caracaschronicles.com/feed' },
    // NEW (2026-07-30): El Diario confirmed real and active via directory
    // listing, distinct outlet. Deliberately did NOT add the also-listed
    // "Venezuela News" (venezuela-news.com) after confirming via Wikipedia
    // it's a Bolivarian-government-linked disinformation site with a
    // documented history of digital manipulation -- not a legitimate
    // source regardless of RSS availability.
    { source: 'eldiario.com', feedUrl: 'https://eldiario.com/feed' },
  ],
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
  GT: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/guatemala/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Guatemala&hl=en&gl=GT&ceid=GT:en' },
    // NEW (2026-08-03): Tico Times -- dedicated Guatemala category page,
    // confirmed real and active with current 2026 content seen directly.
    // Exact feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'ticotimes.net', feedUrl: 'https://ticotimes.net/categories/news/latin-america-news/central-america-latin-america-news/guatemala-central-america-latin-america-news/feed' },
  ],
  MV: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/south-asia/maldives/feed/' },
    { source: 'edition.mv', feedUrl: 'https://edition.mv/feed' }, // NEW (2026-08-03): The Edition -- dedicated English edition of Mihaaru, the Maldives' most trusted news source, launched as a separate daily English newspaper in 2018. Exact feed path not independently verified this round, treat as a hypothesis. // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Maldives&hl=en&gl=MV&ceid=MV:en' }],
  KN: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/caribbean/stkitts-nevis/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Saint+Kitts+and+Nevis&hl=en&gl=KN&ceid=KN:en' }, { source: 'wicnews.com', feedUrl: 'https://wicnews.com/feed' }], // NEW (2026-08-03): WIC News -- first real dedicated source for St Kitts, same regional Caribbean wire as AG/DM/GD/LC
  BO: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/bolivia/feed/' },
    { source: 'bolivianexpress.org', feedUrl: 'https://bolivianexpress.org/feed' }, // NEW (2026-08-03): Bolivia's foremost English-language publication, confirmed real and active. Exact feed path not independently verified this round, treat as a hypothesis. // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Bolivia&hl=en&gl=BO&ceid=BO:en' }],
  BF: [
    { source: 'allafrica.com', feedUrl: 'https://allafrica.com/tools/headlines/rdf/burkinafaso/headlines.rdf' },
    // NEW (2026-07-28): Faso News confirmed real and active via search --
    // live, current-dated (July 2026) English-language content specifically
    // about Burkina Faso and the wider Sahel region. Feed path unverified
    // (standard WordPress convention guess).
    { source: 'fasonews.info', feedUrl: 'https://fasonews.info/feed' },
  ],
  EC: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/ecuador/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Ecuador&hl=en&gl=EC&ceid=EC:en' },
    // NEW (2026-08-03): Ecuador Times -- confirmed real, dedicated
    // English/Spanish digital newspaper for Ecuador. Previously this
    // country was Google-News-fallback-only; this is its first genuine
    // dedicated source. Exact feed path not independently verified this
    // round, treat as a hypothesis.
    { source: 'ecuadortimes.net', feedUrl: 'https://www.ecuadortimes.net/feed' },
  ],
  PY: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/paraguay/feed/' },
    { source: 'theparaguaypost.com', feedUrl: 'https://www.theparaguaypost.com/feed' }, // NEW (2026-08-03): The Paraguay Post, confirmed real active Substack publication. Substack's standard /feed path used since Substack always exposes RSS there. // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Paraguay&hl=en&gl=PY&ceid=PY:en' }],
  // NEW (2026-07-28): these 9 countries had ZERO dedicated RSS feed --
  // relying entirely on the capped 3-API pipeline (79/186 countries
  // attempted per run), which explains why several major, well-known
  // countries were stuck "thin" despite having real allowlisted outlets.
  // FR/PL/NO/PS/CO/BR feed URLs confirmed via a live RSS-directory listing
  // (FeedSpot), not guessed. UA/IQ/ET use the standard /feed/ convention on
  // an already-allowlisted domain -- unverified by test fetch, confirm via
  // next run's log.
  FR: [
    { source: 'lemonde.fr', feedUrl: 'https://www.lemonde.fr/en/rss/une.xml' }, // Le Monde's English digital edition (launched April 2022)
    // NEW (2026-08-03): France24's dedicated France-section feed --
    // distinct from the general France24 wire already used cross-country
    // for mention-based matching. Confirmed exact path via feed directory.
    { source: 'france24.com', feedUrl: 'https://www.france24.com/en/france/rss' },
  ],
  PL: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/poland/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'notesfrompoland.com', feedUrl: 'https://notesfrompoland.com/feed' }],
  NO: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/norway/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.no', feedUrl: 'https://feeds.thelocal.com/rss/no' }],
  PS: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Palestine&hl=en&gl=PS&ceid=PS:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/palestine/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'palestinechronicle.com', feedUrl: 'https://www.palestinechronicle.com/feed' }],
  CO: [
    { source: 'colombiareports.com', feedUrl: 'https://colombiareports.com/feed' },
    // NEW (2026-08-03): The Bogota Post -- established English-language
    // Colombian newspaper, genuinely different outlet. Confirmed real and
    // exact path via feed directory.
    { source: 'thebogotapost.com', feedUrl: 'https://thebogotapost.com/feed' },
  ],
  BR: [
    { source: 'riotimesonline.com', feedUrl: 'https://www.riotimesonline.com/feed' },
    // NEW (2026-08-03): Brasil Wire -- confirmed real, independent
    // English-language Brazil outlet via a public curated RSS-feed
    // directory (not a guess).
    { source: 'brasilwire.com', feedUrl: 'https://www.brasilwire.com/feed/' },
  ],
  // NEW (2026-08-02): euromaidanpress.com (added last session as the fix
  // for a dead kyivindependent.com path) is now also confirmed 403 in the
  // most recent run. Adding a Google News fallback rather than a third
  // per-outlet guess, given Ukrainian outlets seem to be hitting the same
  // bot-blocking pattern one after another.
  UA: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/ukraine/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'euromaidanpress.com', feedUrl: 'https://euromaidanpress.com/feed' }, // confirmed dead (0 articles ever) via live data audit -- left in place, not re-researched this round
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Ukraine&hl=en&gl=UA&ceid=UA:en' },
    // NEW (2026-08-06): Kyiv Post -- Ukraine's first and most prominent
    // English-language newspaper since 1995, one of the only news orgs
    // in Ukraine with a 100% NewsGuard content-accuracy rating (company
    // alongside Washington Post/NYT/WSJ). Genuinely different from
    // Euromaidan Press. Confirmed real; exact feed path not
    // independently verified this round, treat as a hypothesis.
    { source: 'kyivpost.com', feedUrl: 'https://www.kyivpost.com/feed' },
  ],
  IQ: [
    { source: 'iraqinews.com', feedUrl: 'https://www.iraqinews.com/feed' }, // unverified path guess
    // NEW (2026-08-03): Iraq Business News -- established, independent
    // English-language outlet since 2010, genuinely different from
    // iraqinews.com. Confirmed real and exact path via feed directory.
    { source: 'iraq-businessnews.com', feedUrl: 'https://www.iraq-businessnews.com/feed' },
  ],
  ET: [
    { source: 'capitalethiopia.com', feedUrl: 'https://www.capitalethiopia.com/feed/' }, // swapped from thereporterethiopia.com (403, bot-blocked) -- this one confirmed reachable, live content seen directly
    // NEW (2026-08-03): Addis Standard -- independent Ethiopian magazine
    // covering socio-political and economic affairs, genuinely different
    // from Capital Ethiopia (a business weekly). Confirmed real; exact
    // feed path not independently verified this round (standard
    // WordPress-adjacent /feed pattern), treat as a hypothesis.
    { source: 'addisstandard.com', feedUrl: 'https://addisstandard.com/feed' },
  ],
  // NEW (2026-07-28): found while working the "stalled" list -- these 5
  // major countries had ZERO dedicated RSS feed, relying purely on the
  // capped API pipeline, which explains the stall. JP/EG/CN feed URLs
  // confirmed via directory listing or live-fetched content, not guesses.
  // RS's Balkan Insight is confirmed real but covers the whole Balkans
  // region (like AllAfrica) -- correctly NOT allowlisted, relies on the
  // country-mention relevance check same as any wire. RU's path is a
  // standard-convention guess on an already-allowlisted domain, unverified.
  JP: [
    { source: 'japantimes.co.jp', feedUrl: 'https://www.japantimes.co.jp/feed' },
    // NEW (2026-08-02): NHK World, Japan's public broadcaster's English
    // service -- long-documented standard feed path, not individually
    // fetch-verified this session.
    { source: 'nhk.or.jp', feedUrl: 'https://www3.nhk.or.jp/nhkworld/en/news/feeds/' },
  ],
  EG: [
    { source: 'egyptindependent.com', feedUrl: 'https://www.egyptindependent.com/feed' },
    // NEW (2026-08-03): Mada Masr -- independent Egyptian outlet known for
    // investigative reporting, genuinely different editorial line from
    // Egypt Independent. Confirmed real and exact path via feed directory.
    { source: 'madamasr.com', feedUrl: 'https://www.madamasr.com/en/feed' },
    // NEW (2026-08-06): Ahram Online -- English edition of Al-Ahram,
    // Egypt's most-circulated newspaper (2nd oldest in the country,
    // founded 1876), genuinely different (state-linked, established
    // institution) from Egypt Independent/Mada Masr (both independent).
    // Confirmed real via Wikipedia + direct site visit; exact feed path
    // not independently verified this round, treat as a hypothesis.
    { source: 'ahram.org.eg', feedUrl: 'https://english.ahram.org.eg/rss.aspx' },
  ],
  CN: [
    { source: 'scmp.com', feedUrl: 'https://www.scmp.com/rss/91/feed' },
    // NEW (2026-08-03): china.org.cn -- state-run, flagged accordingly.
    // Confirmed exact path via a public RSS feed directory. Adding for
    // redundancy on China specifically since SCMP alone is a single point
    // of failure for the world's most populous country in this dataset.
    { source: 'china.org.cn', feedUrl: 'http://www.china.org.cn/rss/1201719.xml', stateMedia: true },
  ],
  RS: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/serbia/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'balkaninsight.com', feedUrl: 'https://balkaninsight.com/category/bi/serbia/feed/' }], // sharpened from the generic regional feed to their Serbia-specific category feed -- exact URL confirmed directly from Balkan Insight's own RSS feeds page, better signal-to-noise for country tagging
  RU: [
    { source: 'themoscowtimes.com', feedUrl: 'https://www.themoscowtimes.com/rss/news' }, // swapped from /rss (404) -- confirmed their RSS hub lives at /page/rss with category sub-feeds; /rss/news is the most likely News feed path, still unverified
    // NEW (2026-08-03): Meduza -- independent Russian/English outlet
    // headquartered in Riga after being forced out of Russia, genuinely
    // different ownership/editorial line from Moscow Times. Confirmed
    // exact path via feed directory.
    { source: 'meduza.io', feedUrl: 'https://meduza.io/rss/all' },
    // NEW (2026-08-06): TASS -- Russia's leading state news agency,
    // flagged accordingly. Added for editorial diversity alongside the
    // two independent/exile outlets above (same pattern as
    // china.org.cn/eng.belta.by elsewhere -- state narrative alongside
    // independent voices, clearly labeled either way). Exact path
    // confirmed via feed directory.
    { source: 'tass.com', feedUrl: 'https://tass.com/rss/v2.xml', stateMedia: true },
  ],
  AF: [
    { source: 'tolonews.com', feedUrl: 'https://tolonews.com/en/rss.xml' }, // swapped -- bare path returned valid XML but 100% non_english (Dari/Pashto edition); /en/ prefix is the standard pattern for their English section, unverified
    // NEW (2026-08-02): now confirmed 403. Google News fallback.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Afghanistan&hl=en&gl=AF&ceid=AF:en' },
    // NEW (2026-08-03): Khaama Press -- Afghanistan's largest English-
    // language news agency (2M+ monthly readers), genuinely different
    // outlet from Tolo News. Confirmed real and exact path via feed
    // directory.
    { source: 'khaama.com', feedUrl: 'https://www.khaama.com/feed' },
  ],
  LB: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Lebanon&hl=en&gl=LB&ceid=LB:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/lebanon/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'naharnet.com', feedUrl: 'https://www.naharnet.com/rss.xml' }], // swapped from /rss/lebanon (404) -- trying standard root-level path, still unverified
  // NEW (2026-07-28): UY and CL are already covered by the generic MercoPress
  // WORLD wire, but that clearly isn't surfacing enough Uruguay/Chile-tagged
  // content on its own (both stalled). MercoPress confirmed to publish
  // dedicated per-country category pages (en.mercopress.com/uruguay,
  // en.mercopress.com/chile both real, active). Feed path follows the
  // confirmed en.mercopress.com/rss/latin-america convention -- unverified
  // for these two specific country slugs, confirm via next run's log.
  UY: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/latin-america/uruguay/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'en.mercopress.com', feedUrl: 'https://en.mercopress.com/rss/uruguay' }],
  CL: [
    { source: 'en.mercopress.com', feedUrl: 'https://en.mercopress.com/rss/chile' },
    // I Love Chile: confirmed dead (0 articles ever) via live data audit
    // -- left in place, not re-researched this round.
    { source: 'ilovechile.cl', feedUrl: 'https://ilovechile.cl/feed' },
    // NEW (2026-08-06): The Santiago Times -- founded 1990, genuinely a
    // rival/competing English-language Chile outlet to I Love Chile per
    // Wikipedia ("ILC vies with The Santiago Times being the leader of
    // English language media in Chile"). Exact path confirmed via feed
    // directory.
    { source: 'santiagotimes.cl', feedUrl: 'https://santiagotimes.cl/feed' },
  ],
  // NEW (2026-07-29): Argentina had zero dedicated RSS feed despite already
  // having real English-language outlets allowlisted (batimes.com.ar,
  // buenosairesherald.com) -- same "major country, API-only" gap as the
  // FR/BR/CO/JP/EG/CN batch from earlier. Exact feed URL confirmed via
  // directory listing, not a guess.
  AR: [
    { source: 'batimes.com.ar', feedUrl: 'https://www.batimes.com.ar/feed' },
    // NEW (2026-08-03): Buenos Aires Herald -- historic English-language
    // Argentine paper (1876), revived as an online edition in 2023,
    // genuinely different outlet from Buenos Aires Times. Confirmed real
    // and exact path via feed directory.
    { source: 'buenosairesherald.com', feedUrl: 'https://buenosairesherald.com/feed' },
  ],
  // NEW (2026-07-30): Austria had zero RSS entry and zero allowlist entry
  // despite being a major European country. thelocal.at confirmed real,
  // same TheLocal network already trusted for DE/ES/FR/NO/SE.
  AT: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/austria/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.at', feedUrl: 'https://feeds.thelocal.com/rss/at' }], // switched to the confirmed-working TheLocal network pattern (same as NO, DK) instead of an untested thelocal.at/feed guess
  // NEW (2026-08-03): three completely missing countries closed at once --
  // Denmark, Switzerland, and Sweden were absent from the dataset entirely.
  // All three are confirmed live editions of the same TheLocal network
  // already trusted for DE/ES/FR/IT/AT/NO, same exact URL pattern.
  DK: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/denmark/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.dk', feedUrl: 'https://feeds.thelocal.com/rss/dk' }],
  CH: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/switzerland/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.ch', feedUrl: 'https://feeds.thelocal.com/rss/ch' }],
  SE: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/sweden/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.se', feedUrl: 'https://feeds.thelocal.com/rss/se' }],
  // NEW (2026-08-03): Portugal and Hungary -- both had zero coverage,
  // both confirmed real dedicated English-language outlets.
  PT: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Portugal&hl=en&gl=PT&ceid=PT:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/portugal/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'portugalresident.com', feedUrl: 'https://www.portugalresident.com/feed' }],
  HU: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/eastern-central-europe/hungary/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'dailynewshungary.com', feedUrl: 'https://dailynewshungary.com/feed/' }],
  // NEW (2026-08-03): Andorra, Liechtenstein, San Marino, Kiribati -- all
  // four had zero coverage. Checked for dedicated English-language
  // outlets; none exist in any meaningful form for any of the four
  // (domestic press is Catalan/German/Italian respectively for the first
  // three; Kiribati has no established English digital press). Same
  // category as Nauru/Palau/Tuvalu already in the dataset -- Google News
  // fallback rather than leaving a hard zero.
  AD: [
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Andorra&hl=en&gl=AD&ceid=AD:en' },
    // NEW (2026-08-03): Catalan News -- English-language news outlet
    // covering Barcelona and Catalonia, confirmed to actively cover
    // Andorra as tagged content (verified via a live example article on
    // an Andorra human-rights case). Closes the last remaining zero-
    // dedicated-source country in the dataset. Using their Andorra tag
    // page; exact feed path not independently verified this round, treat
    // as a hypothesis.
    { source: 'catalannews.com', feedUrl: 'https://www.catalannews.com/tag/andorra/feed' },
  ],
  LI: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/liechtenstein/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Liechtenstein&hl=en&gl=LI&ceid=LI:en' }],
  SM: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/san-marino/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=San+Marino&hl=en&gl=SM&ceid=SM:en' }],
  KI: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/oceania/kiribati/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Kiribati&hl=en&gl=KI&ceid=KI:en' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // NEW (2026-08-03): RNZ Pacific -- Kiribati's first real dedicated source (previously Google-News-only), same regional wire as FJ/TO/VU/WS/NR/MH/PW/FM
  // NEW (2026-07-30): found while pushing toward 150 healthy -- 31 major
  // countries had ZERO RSS entry despite most already having vetted
  // ALLOWLIST_BY_COUNTRY sources. DE/ES use the same confirmed
  // feeds.thelocal.com/rss/{cc} pattern already proven for NO/DK. CA and AU
  // use exact feed URLs confirmed via directory listing.
  DE: [
    { source: 'thelocal.de', feedUrl: 'https://feeds.thelocal.com/rss/de' },
    // NEW (2026-08-03): DW's dedicated Germany-section feed -- distinct
    // from the general dw.com wire already used cross-country for
    // mention-based matching. Confirmed exact path via feed directory.
    { source: 'dw.com', feedUrl: 'https://rss.dw.com/rdf/rss-en-ger' },
  ],
  ES: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/western-europe/spain/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thelocal.es', feedUrl: 'https://feeds.thelocal.com/rss/es' },
    // NEW (2026-08-06): El Pais English -- Spain's leading newspaper,
    // English edition, genuinely different from The Local. Confirmed
    // real via feed directory.
    { source: 'elpais.com', feedUrl: 'https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada' },
    // NEW (2026-08-06): SUR in English -- established English-language
    // outlet for southern Spain (Malaga/Costa del Sol/Andalucia),
    // genuinely different regional focus and audience (expat-oriented)
    // from both sources above. Confirmed real via feed directory; exact
    // feed path not independently verified this round, treat as a
    // hypothesis.
    { source: 'surinenglish.com', feedUrl: 'https://www.surinenglish.com/rss' },
  ],
  CA: [
    { source: 'globalnews.ca', feedUrl: 'https://globalnews.ca/feed' },
    // NEW (2026-08-02): CBC News top stories -- Canada's public
    // broadcaster, long-documented standard feed path, not individually
    // fetch-verified this session.
    { source: 'cbc.ca', feedUrl: 'https://www.cbc.ca/webfeed/rss/rss-topstories' },
  ],
  AU: [
    { source: 'sbs.com.au', feedUrl: 'https://www.sbs.com.au/news/feed' },
    // NEW (2026-08-02): ABC News Australia top stories -- public
    // broadcaster, long-documented standard feed path, not individually
    // fetch-verified this session.
    { source: 'abc.net.au', feedUrl: 'https://www.abc.net.au/news/feed/51120/rss.xml' },
  ],
  KR: [
    { source: 'koreaherald.com', feedUrl: 'https://www.koreaherald.com/rss' }, // confirmed dead (0 articles ever) via live data audit -- left in place, not re-researched this round
    // NEW (2026-08-06): The Korea Times -- "the world's window on Korea",
    // genuinely different outlet, added after confirming Korea Herald AND
    // Korea JoongAng Daily (below) both showing zero articles ever
    // despite being configured. Confirmed real via feed directory; exact
    // path not independently verified this round, treat as a hypothesis.
    { source: 'koreatimes.co.kr', feedUrl: 'https://www.koreatimes.co.kr/www/rss/rss.xml' },
    // NEW (2026-08-03): Korea JoongAng Daily -- published in partnership
    // with the New York Times, genuinely different outlet/ownership from
    // Korea Herald. Exact feed path confirmed via a detailed source
    // explaining it's not auto-discoverable/linked from the homepage,
    // which is exactly why it wasn't already in the allowlist -- not a
    // guess.
    { source: 'koreajoongangdaily.joins.com', feedUrl: 'https://koreajoongangdaily.joins.com/xmls/joins' },
  ],
  ID: [
    { source: 'thejakartapost.com', feedUrl: 'https://www.thejakartapost.com/rss' }, // still 404ing -- couldn't confirm a working replacement path this round (their listing services obscure the real URL), left in place in case it's a temporary outage
    // NEW (2026-08-02): Google News fallback so Indonesia isn't fully
    // dependent on the one dead feed above. Confirmed pattern (see Brunei
    // entry for rationale).
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=Indonesia&hl=en&gl=ID&ceid=ID:en' },
    // NEW (2026-08-03): Antara News -- Indonesia's national news agency,
    // English edition, genuinely different from Jakarta Post. Confirmed
    // real and exact path directly.
    { source: 'antaranews.com', feedUrl: 'https://en.antaranews.com/rss/news.xml' },
  ],
  // FIXED (2026-08-02): /rss 404'd. Confirmed real path directly from The
  // Star's own RSS directory page (thestar.com.my/RSS) -- their feeds live
  // under /rss/News, /rss/Business, etc, not a bare /rss. Worth knowing:
  // that same directory page's terms of use say the feed is "STRICTLY FOR
  // PERSONAL, NON-COMMERCIAL USE" and may not be used with ads attached --
  // same category of ToS flag as GNews' free tier, worth revisiting before
  // Stage 6 (ads).
  MY: [
    { source: 'thestar.com.my', feedUrl: 'https://www.thestar.com.my/rss/News' }, // confirmed dead (0 articles ever) via live data audit -- left in place, not re-researched this round
    // NEW (2026-08-03): Free Malaysia Today -- independent, bilingual
    // (English/Malay) news portal, genuinely different ownership from The
    // Star. Confirmed real and exact path via feed directory.
    { source: 'freemalaysiatoday.com', feedUrl: 'https://www.freemalaysiatoday.com/feed' },
    // NEW (2026-08-06): Bernama -- Malaysia's national news agency, has
    // its own dedicated RSS-serving subdomain (rss.bernama.com),
    // confirmed directly via their own service description page.
    // Genuinely different (state news agency vs. private outlets) from
    // both sources above.
    { source: 'bernama.com', feedUrl: 'https://rss.bernama.com/rssfeed.php' },
  ],
  PH: [
    { source: 'inquirer.net', feedUrl: 'https://www.inquirer.net/feed' },
    // NEW (2026-08-03): Rappler -- major independent Philippine digital
    // outlet, genuinely different ownership/editorial line from Inquirer.
    // Confirmed exact path via feed directory.
    { source: 'rappler.com', feedUrl: 'https://www.rappler.com/feed' },
  ],
  MX: [
    { source: 'mexiconewsdaily.com', feedUrl: 'https://mexiconewsdaily.com/feed' },
    // NEW (2026-08-03): The Yucatan Times -- confirmed real, active,
    // English-language, genuinely different outlet from Mexico News
    // Daily. Confirmed real and exact path via feed directory.
    { source: 'theyucatantimes.com', feedUrl: 'https://theyucatantimes.com/feed' },
  ],
  // NEW (2026-07-30): remaining major no-RSS countries, using their
  // already-allowlisted top domain with standard RSS path conventions.
  // Lower confidence than the batch above -- these specific paths weren't
  // individually confirmed via directory listing, so treat as educated
  // guesses on a verified-legitimate domain, not verified paths. Next run's
  // log will confirm or reject each.
  SA: [
    { source: 'arabnews.com', feedUrl: 'https://www.arabnews.com/rss.xml' },
    // NEW (2026-08-03): Saudi Gazette -- established English-language
    // Saudi daily since 1976, genuinely different outlet from Arab News.
    // Confirmed real and exact path via feed directory.
    { source: 'saudigazette.com.sa', feedUrl: 'https://saudigazette.com.sa/rssFeed/74' },
  ],
  AE: [
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/middle-east-north-africa/united-arab-emirates/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'thenationalnews.com', feedUrl: 'https://www.thenationalnews.com/rss' }, // newly 404ing -- may be a path change on their end, not re-researched this round
    // NEW (2026-08-02): Google News fallback so UAE isn't a hard zero.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=United+Arab+Emirates&hl=en&gl=AE&ceid=AE:en' },
  ],
  SG: [
    { source: 'straitstimes.com', feedUrl: 'https://www.straitstimes.com/news/singapore/rss.xml' },
    { source: 'tnp.sg', feedUrl: 'http://www.tnp.sg/rss.xml' }, // NEW (2026-08-02): confirmed valid via community RSS directory
  ],
  ZA: [
    { source: 'news24.com', feedUrl: 'https://www.news24.com/news24/rss' }, // confirmed dead (0 articles ever) via live data audit -- left in place, not re-researched this round
    { source: 'dailymaverick.co.za', feedUrl: 'https://www.dailymaverick.co.za/dmrss/' }, // NEW (2026-08-02): confirmed valid via community RSS directory
    // NEW (2026-08-06): Mail & Guardian -- established, independent SA
    // weekly (515K Facebook followers), genuinely different editorial
    // line from News24/Daily Maverick. Confirmed real and exact path via
    // feed directory.
    { source: 'mg.co.za', feedUrl: 'https://mg.co.za/feed/' },
  ],
  NZ: [
    { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/national.xml' },
    // NEW (2026-08-03): NZ Herald -- New Zealand's largest-circulation
    // newspaper, genuinely different outlet from RNZ (public
    // broadcaster). Their RSS system builds feeds dynamically per section
    // rather than exposing one static URL, so exact path not
    // independently verified this round -- treat as a hypothesis.
    { source: 'nzherald.co.nz', feedUrl: 'https://www.nzherald.co.nz/arc/outboundfeeds/rss/' },
  ],
  // NEW (2026-08-01): these 6 countries were previously removed from
  // countries.json entirely as confirmed dead ends. Fresh research this
  // session found real, active English-language sources for all of them --
  // re-added to countries.json and configured here. LS/TJ/FM/NR have
  // genuine single-country-dedicated outlets (allowlisted). MH/PW share a
  // real regional outlet (Marianas Business Journal, Guam-based, explicitly
  // covers both) -- correctly not allowlisted since it's regional, relies
  // on mentionsCountry same as oc-media.org for Armenia/Georgia. All paths
  // below are directory-confirmed except micronesiatoday.com and
  // advancenauru.com's exact feed path, which are educated guesses on
  // confirmed-real, confirmed-active sites.
  LS: [
    { source: 'lestimes.com', feedUrl: 'https://www.lestimes.com/feed' },
    // NEW (2026-08-01): lestimes.com returned a 503 last run -- adding The
    // Reporter as a second, genuinely different Lesotho outlet, exact
    // feed URL confirmed via directory listing.
    { source: 'thereporter.co.ls', feedUrl: 'https://www.thereporter.co.ls/feed' },
  ],
  TJ: [
    { source: 'asiaplustj.info', feedUrl: 'https://asiaplustj.info/en/rss.xml' },
    { source: 'eurasianet.org', feedUrl: 'https://eurasianet.org/rss.xml' }, // NEW (2026-08-03): Eurasianet -- same regional wire as KG/MN, genuinely different from Asia-Plus
  ],
  FM: [{ source: 'micronesiatoday.com', feedUrl: 'https://www.micronesiatoday.com/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // NEW (2026-08-03): RNZ Pacific regional wire added as second source
  NR: [{ source: 'advancenauru.com', feedUrl: 'https://advancenauru.com/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // NEW (2026-08-03): RNZ Pacific regional wire added as second source
  MH: [{ source: 'marshallislandsjournal.com', feedUrl: 'https://marshallislandsjournal.com/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // Marshall Islands Journal confirmed via Wikipedia as the country's sole newspaper since 1970. NEW: RNZ Pacific regional wire added as second source.
  PW: [{ source: 'islandtimes.org', feedUrl: 'https://islandtimes.org/feed' }, { source: 'rnz.co.nz', feedUrl: 'https://www.rnz.co.nz/rss/pacific.xml' }], // Island Times confirmed real and active with live current Palau-specific content. NEW: RNZ Pacific regional wire added as second source.
  // NEW (2026-08-02): comprehensive research pass -- found via analysis
  // that 17 configured countries had zero RSS entry at all (relying purely
  // on the capped API pipeline) and 155 more had only a single source.
  // Starting with the zero-RSS majors, since those are the clearest gap.
  IT: [
    { source: 'ansa.it', feedUrl: 'https://www.ansa.it/sito/ansait_rss.xml' }, // ANSA's English edition (ansa.it/english), Italy's major news agency, 2M+ followers, exact feed URL confirmed via directory listing
    // NEW (2026-08-03): The Local Italy -- same trusted TheLocal network
    // already used for DE/ES/FR/NO/AT, genuinely English-language (the
    // reason ilsole24ore.com and other options were skipped earlier this
    // session -- their feeds are Italian-primary and would mostly be
    // filtered out). Confirmed exact path via a public curated RSS
    // directory (plenaryapp/awesome-rss-feeds).
    { source: 'thelocal.it', feedUrl: 'https://feeds.thelocal.com/rss/it' },
  ],
  // NEW (2026-08-02): found via a comprehensive community-maintained RSS
  // directory (github.com/yavuz/news-feed-list-of-countries) with built-in
  // feed validation status -- all entries below confirmed "valid" there.
  IE: [
    { source: 'thejournal.ie', feedUrl: 'https://www.thejournal.ie/feed/' },
    { source: 'independent.ie', feedUrl: 'https://www.independent.ie/irish-news/rss' },
  ],
  IL: [
    { source: 'jpost.com', feedUrl: 'https://www.jpost.com/RSS/RssFeedsFrontPage.aspx' }, // Jerusalem Post, Israel's most-read English news site
    // Times of Israel: confirmed dead (0 articles ever) via live data
    // audit -- no stronger URL lead found this round, left in place.
    { source: 'timesofisrael.com', feedUrl: 'https://www.timesofisrael.com/feed/' },
    // NEW (2026-08-06): Haaretz -- independent daily since 1918,
    // genuinely different editorial line from Jerusalem Post. Confirmed
    // real via feed directory; exact feed path not independently
    // verified this round, treat as a hypothesis.
    { source: 'haaretz.com', feedUrl: 'https://www.haaretz.com/srv/haaretz-latest-headlines' },
  ],
  RO: [
    { source: 'biziday.ro', feedUrl: 'https://www.biziday.ro/feed/' },
    // NEW (2026-08-03): Romania Insider -- the leading English-language
    // news source dedicated to Romania since 2010. Confirmed real and
    // exact path via feed directory. Worth noting: the existing source
    // (biziday.ro) may itself be Romanian-language, in which case this
    // becomes the more valuable of the two for an English-language
    // aggregator -- worth checking in the next log.
    { source: 'romania-insider.com', feedUrl: 'https://www.romania-insider.com/feed' },
  ],
  CD: [
    // NEW (2026-08-06): Google News fallback -- every dedicated source configured for this country was confirmed producing zero articles ever via a live data audit, and there was no safety net at all until now.
    { source: 'news.google.com', feedUrl: 'https://news.google.com/rss/search?q=DR+Congo&hl=en&gl=CD&ceid=CD:en' },
    { source: 'globalvoices.org', feedUrl: 'https://globalvoices.org/-/world/sub-saharan-africa/dr-of-congo/feed/' }, // NEW (2026-08-03): Global Voices -- confirmed exact per-country RSS feed directly from their own feeds page. Citizen-journalism/commentary, lower volume than local dailies, but genuine English-language redundancy.
    { source: 'congoplanet.com', feedUrl: 'http://www.congoplanet.com/feeds/rss_congo_africa.xml' }],
  IR: [
    { source: 'khabaronline.ir', feedUrl: 'http://english.khabaronline.ir/rss/' }, // has repeatedly returned zero items in recent runs -- not removing (may recover) but real redundancy is overdue
    // NEW (2026-08-03): Tehran Times -- English-language daily since 1979,
    // state-controlled but genuinely different outlet/pipeline from
    // Khabaronline. Exact URL confirmed directly from Tehran Times' own
    // official RSS help page, not a guess.
    { source: 'tehrantimes.com', feedUrl: 'https://www.tehrantimes.com/rss' },
  ],
  FI: [
    { source: 'yle.fi', feedUrl: 'https://yle.fi/uutiset/rss/uutiset.rss?osasto=news' }, // Finland's national broadcaster, English edition
    // NEW (2026-08-03): Helsinki Times -- independent English-language
    // newspaper since 2007, genuinely different from Yle (state
    // broadcaster). Confirmed real via feed directory.
    { source: 'helsinkitimes.fi', feedUrl: 'https://www.helsinkitimes.fi/feed' },
  ],
  NL: [
    { source: 'nltimes.nl', feedUrl: 'https://nltimes.nl/rssfeed2' }, // "the fastest-growing publisher of English-language news from the Netherlands", exact URL confirmed
    { source: 'dutchnews.nl', feedUrl: 'https://www.dutchnews.nl/feed' }, // genuinely different second English-language Dutch outlet, exact URL confirmed
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

// RAW FETCH + SANITIZE (2026-08-02): replaces reliance on rss-parser's own
// parser.parseURL(). Added after a sweep of real failures across ~20
// countries in a single shard run fell into two buckets neither of which
// parser.parseURL() could recover from on its own:
//
//   1. Genuinely malformed source XML -- unescaped bare "&" in titles/links
//      (Invalid character in entity name -- Lesotho, Algeria, Bhutan), and
//      raw un-CDATA-wrapped HTML leaking into <description>/<content:encoded>
//      (Attribute without value / Unquoted attribute value / Unexpected
//      close tag -- Jordan, Oman, North Korea, Tuvalu). Confirmed via
//      synthetic reproductions matching the exact error text and column
//      position seen in the real logs, not a guess.
//   2. A response starting with the gzip magic byte (0x1f) instead of "<"
//      (Non-whitespace before first tag -- Marshall Islands) -- rss-parser's
//      internal fetch uses plain http.get/https.get with no Content-Encoding
//      handling at all, so a server that gzips regardless of what's
//      requested (common behind some CDNs) silently hands back compressed
//      bytes as if they were the XML body.
//
// This does its own fetch (with redirect + gzip/deflate/br handling, falling
// back to sniffing the gzip magic bytes even when the server doesn't
// declare Content-Encoding) so the raw text can be sanitized *before*
// rss-parser ever sees it, then hands the cleaned string to
// parser.parseString() instead. Feeds that were already fine pass through
// the sanitizer unchanged (verified: already-CDATA-wrapped and plain-text
// descriptions are left byte-for-byte alone).
function fetchRawXml(feedUrl) {
  const MAX_REDIRECTS = 5;
  const CONNECT_TIMEOUT_MS = 10000;
  return new Promise((resolve, reject) => {
    // settled guard: with retries/redirects recursing through doGet, and
    // several independent event listeners (data/end/error/timeout) all
    // capable of firing, this makes the "only resolve/reject once" intent
    // explicit and keeps the function easy to reason about.
    let settled = false;
    const safeResolve = (v) => { if (!settled) { settled = true; resolve(v); } };
    const safeReject = (e) => { if (!settled) { settled = true; reject(e); } };

    function doGet(rawUrl, redirectsLeft, base) {
      let parsedUrl;
      try {
        parsedUrl = base ? new URL(rawUrl, base) : new URL(rawUrl);
      } catch (e) {
        return safeReject(new Error(`Invalid feed URL "${rawUrl}": ${e.message}`));
      }
      const currentUrl = parsedUrl.toString();
      const lib = parsedUrl.protocol === 'http:' ? http : https;
      let req;
      try {
        req = lib.get(currentUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Encoding': 'gzip, deflate, br',
          },
          timeout: CONNECT_TIMEOUT_MS,
        }, (res) => {
          // CRASH FIX (2026-08-02): this whole callback is now wrapped in
          // try/catch. It runs as an http module event callback, not
          // inside the Promise executor's own synchronous scope, so a
          // throw in here does NOT automatically become a promise
          // rejection -- it becomes an uncaught exception that kills the
          // whole process. This is exactly what happened for real: a 302
          // with a bare "http://" Location header (confirmed, from
          // baltictimes.com) threw out of the previously-unguarded
          // redirect-URL resolution below, uncaught, and took the entire
          // run down mid-shard. Confirmed fixed via local reproduction of
          // that exact failure plus a battery of other malformed-response
          // cases (redirect loops, garbage Location headers, corrupt gzip,
          // connection resets) -- zero uncaught exceptions before this
          // shipped.
          try {
            if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirectsLeft > 0) {
              res.resume();
              return doGet(res.headers.location, redirectsLeft - 1, currentUrl);
            }
            if ([301, 302, 303, 307, 308].includes(res.statusCode) && redirectsLeft <= 0) {
              res.resume();
              return safeReject(new Error('Too many redirects'));
            }
            if (res.statusCode < 200 || res.statusCode >= 300) {
              res.resume();
              return safeReject(new Error(`Status code ${res.statusCode}`));
            }
            const chunks = [];
            res.on('data', (c) => chunks.push(c));
            res.on('error', (e) => safeReject(e));
            res.on('end', () => {
              // 'encoding' declared here, outside try/catch, deliberately --
              // a first version of this fix declared it with const inside
              // the try block, which meant the catch block's own reference
              // to it for the error message threw ReferenceError (const is
              // block-scoped; try and catch are separate blocks). Caught by
              // stress-testing before shipping, not in production.
              let encoding = '';
              try {
                const buf = Buffer.concat(chunks);
                encoding = (res.headers['content-encoding'] || '').toLowerCase();
                let out;
                if (encoding.includes('br')) out = zlib.brotliDecompressSync(buf);
                else if (encoding.includes('gzip')) out = zlib.gunzipSync(buf);
                else if (encoding.includes('deflate')) out = zlib.inflateSync(buf);
                else if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) out = zlib.gunzipSync(buf); // undeclared gzip fallback
                else out = buf;
                safeResolve(out.toString('utf8'));
              } catch (e) {
                safeReject(new Error(`Decompression failed (content-encoding="${encoding}"): ${e.message}`));
              }
            });
          } catch (e) {
            safeReject(e);
          }
        });
      } catch (e) {
        return safeReject(e);
      }
      req.on('timeout', () => req.destroy(new Error('timed out')));
      req.on('error', (e) => safeReject(e));
    }
    doGet(feedUrl, MAX_REDIRECTS, null);
  });
}

const CDATA_WRAP_TAGS = ['description', 'content:encoded', 'summary', 'itunes:summary'];

function sanitizeXml(xml) {
  let out = xml
    // Escape bare "&" not already starting a valid entity reference.
    .replace(/&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)/g, '&amp;')
    // Strip control characters invalid in XML 1.0 (leftover binary noise,
    // stray bytes, etc.) without touching normal whitespace.
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
  // CDATA-wrap raw HTML in known text-bearing tags that weren't already
  // CDATA-wrapped -- the standard fix for feed generators that forget to
  // escape embedded markup. Only touches tags containing a literal "<" so
  // plain-text descriptions (the common case) are left untouched.
  for (const tag of CDATA_WRAP_TAGS) {
    const re = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g');
    out = out.replace(re, (match, inner) => {
      if (inner.trim().startsWith('<![CDATA[')) return match;
      if (!inner.includes('<')) return match;
      const safeInner = inner.replace(/]]>/g, ']]]]><![CDATA[>');
      return `<${tag}><![CDATA[${safeInner}]]></${tag}>`;
    });
  }
  return out;
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
    const rawXml = await Promise.race([fetchRawXml(feedUrl), timeout]);
    const feed = await parser.parseString(sanitizeXml(rawXml));
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

// CRASH FIX (2026-08-02): a real run died with "RangeError: Invalid time
// value" thrown from Date.toISOString() inside buildRow -- some feed's
// isoDate/pubDate field was a string Date() couldn't parse into anything
// valid. Because this was unhandled inside the per-item loop, it didn't
// just drop that one item or one feed -- it killed the *entire remaining
// run*, silently losing every country still queued after whichever feed
// triggered it (confirmed: the log shows normal processing straight
// through Ireland, then nothing at all after the crash). A single bad date
// field from one feed should never be able to take out ~40 other
// countries' worth of coverage. This never throws: tries isoDate, falls
// back to pubDate if that's bad too, and returns null (not a crash) if
// neither parses.
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
    published_at: safeParseDate(item.isoDate) || safeParseDate(item.pubDate),
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
    if (reason === 'excluded_category' && DEBUG_CATEGORY_SOURCES.some((s) => row.source.toLowerCase().includes(s))) {
      // TEMPORARY (2026-07-30): the local-category exemption fix for ME
      // didn't change its excluded_category count at all (22/50 identical
      // before and after), meaning the assumed cause (local/city/metro
      // terms) was wrong. Logging the real raw category value here to see
      // what's actually being matched, instead of guessing again.
      console.log(`[DEBUG excluded_category] ${row.source}: "${safeStringify(row._rawCategory)}" -- title: "${row.title.slice(0, 60)}"`);
    }
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
    { onConflict: 'url_key', ignoreDuplicates: true }
  );
  if (error) {
    console.error(`[${label}] Supabase insert error: ${error.message}`);
    return { label, inserted: 0, error: error.message };
  }
  console.log(`[${label}] Upserted ${clean.length} articles.`);
  return { label, inserted: clean.length };
}

async function main() {
  const runStart = Date.now();
  // Job timeout is 15 minutes (see ingest-rss.yml). Leaving a ~3-minute
  // safety margin for clustering + final logging/exit, since a hard
  // GitHub Actions cancellation mid-request loses the whole run's progress,
  // while stopping early here still commits everything fetched so far.
  const TIME_BUDGET_MS = 11 * 60 * 1000; // reduced from 13 (2026-08-06) --
  // emergency reduction after multiple consecutive job failures
  // (#994-1004, mostly ~15-16min durations matching the job's 15min
  // timeout). Both feed-fetch load (33 new feeds added today) and
  // clustering per-call cost (confirmed 4.5s for a single small batch,
  // up from ~2s earlier today) grew simultaneously, compounding against
  // the same fixed ceiling. More real margin until this is revisited
  // with actual failure logs confirming root cause.

  // SHARDING (2026-08-02): the feed list grew from 144 to 202 feeds this
  // session (+40%), and no amount of per-feed delay tuning kept up --
  // confirmed via a real run losing 36 of 176 country groups to the time
  // budget, with several previously-healthy countries among the casualties.
  // Rather than keep shrinking margins, the workload is now split across
  // two separate scheduled triggers (SHARD=A and SHARD=B, both calling this
  // same script via workflow_dispatch, ~7 min apart), each handling roughly
  // half the feeds. The split is computed fresh every run (greedy
  // bin-packing by feed count, not just country count) rather than a
  // hardcoded list, so it self-rebalances automatically as countries get
  // added or removed -- no manual list-splitting maintenance required.
  //
  // INTERVAL DOUBLED to every 30 min per shard, still ~15 min apart from
  // each other (2026-08-06): was every 15 min per shard with zero buffer
  // against the job's 15-min timeout -- the moment any single run ran
  // long (which started happening as the feed list and clustering cost
  // both grew), the next scheduled trigger fired on top of the still-
  // running previous one. Repeated manual retries during an incident
  // compounded this into a real pile-up: multiple overlapping runs
  // fighting over GitHub's concurrency slots and the same Supabase
  // connection simultaneously, causing a ~95-minute total ingestion gap
  // (confirmed via live data -- zero new articles for that whole window).
  // 30 min per shard gives a full 2x buffer under the 15-min job ceiling
  // instead of running right up against it. Real cost: freshness drops
  // from ~15 min to ~30 min per shard. Worth revisiting once a run of
  // clean, non-overlapping executions is confirmed -- not before.
  // SHARD=both (the default, e.g. for manual workflow_dispatch runs with no
  // input) processes everyone, same as before sharding existed.
  const SHARD = (process.env.SHARD || 'both').toUpperCase();
  const allCountryCodesUnsharded = Object.keys(FEED_URLS_BY_COUNTRY);
  let allCountryCodes = allCountryCodesUnsharded;
  if (SHARD === 'A' || SHARD === 'B') {
    // Deterministic greedy split: sort countries by feed count descending,
    // then assign each one to whichever shard currently has fewer total
    // feeds. This keeps both shards close to balanced even though
    // individual countries have 1-3 feeds each, not a uniform amount.
    const sorted = [...allCountryCodesUnsharded].sort(
      (a, b) => FEED_URLS_BY_COUNTRY[b].length - FEED_URLS_BY_COUNTRY[a].length || a.localeCompare(b)
    );
    const shardFeedCounts = { A: 0, B: 0 };
    const shardAssignment = {};
    for (const code of sorted) {
      const target = shardFeedCounts.A <= shardFeedCounts.B ? 'A' : 'B';
      shardAssignment[code] = target;
      shardFeedCounts[target] += FEED_URLS_BY_COUNTRY[code].length;
    }
    allCountryCodes = allCountryCodesUnsharded.filter((code) => shardAssignment[code] === SHARD);
  }

  const totalFeeds = allCountryCodes.reduce((sum, c) => sum + FEED_URLS_BY_COUNTRY[c].length, 0);
  // Rotate the starting point each run so a time-budget cutoff doesn't
  // always sacrifice the same tail of countries. Confirmed via a real run
  // (2026-07-29): with a fixed iteration order, every major country added
  // late in the file (Japan, France, China, Brazil, Egypt, Poland, Russia,
  // Ukraine...) landed at the end and got skipped, every time the budget
  // was hit -- a silent, permanent blackout for exactly the countries this
  // session worked hardest to add. 15-minute buckets roughly match this
  // workflow's intended run cadence, so consecutive runs start at
  // meaningfully different points without needing any persisted state.
  const rotationOffset = Math.floor(Date.now() / (15 * 60 * 1000)) % allCountryCodes.length;
  const countryCodes = [...allCountryCodes.slice(rotationOffset), ...allCountryCodes.slice(0, rotationOffset)];
  console.log(`Starting RSS ingestion: ${totalFeeds} feed(s) across ${countryCodes.length} country group(s) (shard ${SHARD}, rotation offset ${rotationOffset})...\n`);

  const seenTitles = await loadExistingTitles();
  const { data: existingUrls } = await supabase.from('articles').select('url');
  const seenUrls = new Set((existingUrls || []).map((r) => r.url));
  console.log(`Loaded ${seenTitles.size} existing titles / ${seenUrls.size} existing URLs for dedup.\n`);

  const results = [];
  let lastSource = null;
  let budgetExceeded = false;
  for (const [countryIndex, country] of countryCodes.entries()) {
    if (Date.now() - runStart > TIME_BUDGET_MS) {
      const skipped = countryCodes.slice(countryIndex);
      console.warn(`\nTime budget (${TIME_BUDGET_MS / 60000}min) reached -- skipping remaining ${skipped.length} country group(s) this run: ${skipped.join(', ')}`);
      budgetExceeded = true;
      break;
    }

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
    { onConflict: 'url_key', ignoreDuplicates: true }
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
      await sleep(400); // reduced from 1000ms (2026-08-01) -- confirmed via direct calculation that at 202 feeds, the old 1s delay alone cost 3.4 min out of the 12-min time budget, the real driver behind recurring time-budget skips as the feed count grew 40% this session (144->202). Still a genuine politeness delay, not zero; the separate 4s same-domain delay elsewhere still protects against bursting any single server.
    }
  }

  const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
  console.log(`\nDone. ${totalInserted} articles processed across ${totalFeeds} feed(s)${budgetExceeded ? ' (run cut short by time budget)' : ''}.`);

  console.log('\nClustering related stories across countries...');
  // RSS runs every 30 minutes per shard (was 15, see the SHARDING comment
  // above), not every 3 hours like the API pipeline -- the function's
  // default process_window_hours (6h) was sized for that slower cadence. At RSS's frequency, a 6h window means re-scanning the
  // same growing backlog ~24 times within that window, which is what caused
  // a real timeout (confirmed: 478 of 483 articles in the last 6h were still
  // unclustered at the time it failed). 1 hour comfortably covers several
  // missed cycles' worth of buffer without re-scanning that much backlog.
  //
  // max_batch_size=30 is correctly tuned to PostgREST's real 8s
  // statement_timeout per call (see below) -- but a single 30-item call per
  // run was nowhere near enough at current volume. A live data audit
  // (2026-07-28) found only 1.6% of the last 7 days' articles had ever been
  // clustered, with a 6,900+ article backlog -- the vast majority of
  // articles were simply never reaching the clustering function at all
  // before aging out of the 1h window forever. Fix: call the RPC multiple
  // times per run instead of once, each call still safely bounded to 30
  // items / under 8s, but ~8x the total throughput per run.
  // max_batch_size=30 is correctly tuned to PostgREST's real 8s
  // statement_timeout per call (see below) -- but a single 30-item call per
  // run was nowhere near enough at current volume. A live data audit
  // (2026-07-28) found only 1.6% of the last 7 days' articles had ever been
  // clustered, with a 6,900+ article backlog -- the vast majority of
  // articles were simply never reaching the clustering function at all
  // before aging out of the 1h window forever. Fix: call the RPC multiple
  // times per run instead of once, each call still safely bounded / under 8s.
  //
  // Hard timeout reduced from 60s to 15s and iterations from 8 to 5
  // (2026-07-29): the original 60s x 8 was a worst case of ~8 minutes if
  // calls ran slow without technically erroring -- confirmed as a likely
  // contributor to some runs timing out at the 15-minute job ceiling while
  // others didn't, alongside feed count growing from 144 to 169 this same
  // session. 15s is still ~2x the real ~8s server-side ceiling.
  //
  // max_batch_size reduced from 30 to 6, calls raised from 5 to 15
  // (2026-08-03): the table has grown to ~53K articles (from a much
  // smaller base when 30-items-under-8s was tuned), and this session's
  // source expansion (330->425 feeds) accelerated that growth further.
  // Confirmed directly via EXPLAIN ANALYZE against production data: each
  // trigram similarity lookup (the `title % ...` condition, one of up to
  // 2 per row) now costs ~300-530ms on its own, driven by the GIN index
  // bitmap scan cost at this table size -- not bloat (recently vacuumed,
  // only 230 dead tuples) and not fixable by narrowing the time window
  // (73h->25h only cut cost ~2x, since the planner scans the trigram
  // index before applying the time filter). At 30 rows x up to 2 lookups
  // x ~400ms, that's 24-32s per call against the real ~8s ceiling --
  // confirmed as the cause of "canceling statement due to statement
  // timeout" in both ingest.js and ingest-rss.js logs from the same run
  // window. 6 rows x 2 lookups x ~530ms (worst case) = ~6.4s, safely
  // under 8s with real margin. Calls raised 5->15 to keep similar total
  // per-run throughput (90 vs the old 150-row ceiling) while each
  // individual call is now safe. The existing TIME_BUDGET_MS check below
  // still caps total clustering time regardless.
  const CLUSTER_HARD_TIMEOUT_MS = 15000;
  // max_batch_size reduced further from 6 to 3, calls raised from 15 to 25
  // (2026-08-06): table has grown to 63,313 rows (from ~53K when 6 was
  // tuned), and direct EXPLAIN ANALYZE confirms per-lookup cost has grown
  // to ~697ms (from ~300-530ms) -- the old 6-row batch's worst case
  // (6 x 2 x ~700ms ~= 8.4s) was landing right at or past the real ~8s
  // ceiling again, confirmed via live hourly data showing recurring
  // zero-clustered hours. This table will keep growing, so this time
  // building in more real margin (3 x 2 x ~900ms buffer ~= 5.4s) rather
  // than tuning right up to the edge -- the same problem will keep
  // recurring at each size threshold otherwise. A real fix (data
  // retention policy bounding how far the trigram index has to search)
  // is still the actual long-term answer; this is a stopgap.
  // EMERGENCY CUT (2026-08-06, same day): 35 -> 10. Confirmed per-call
  // cost has grown further still -- 4.5s for a single max_batch_size=2
  // call (up from ~2s earlier today) -- meaning 35 sequential calls could
  // alone consume several minutes, stacking on top of fetch time for the
  // 459 feeds now configured (33 added today). Multiple consecutive job
  // runs failed (#994-1004, ~15-16min durations matching the job's
  // 15min timeout). Protective cut pending actual failure-log
  // confirmation of root cause -- clustering throughput will be lower
  // until this is revisited with real data.
  const CLUSTER_CALLS_PER_RUN = 10;
  let clusteredOk = 0;
  for (let i = 0; i < CLUSTER_CALLS_PER_RUN; i++) {
    if (Date.now() - runStart > TIME_BUDGET_MS) {
      console.warn(`Time budget reached during clustering -- stopping after ${clusteredOk}/${CLUSTER_CALLS_PER_RUN} calls.`);
      break;
    }
    const clusterTimeout = new Promise((resolve) =>
      setTimeout(() => resolve({ error: { message: `Hard timeout after ${CLUSTER_HARD_TIMEOUT_MS}ms -- clustering RPC did not respond in time` } }), CLUSTER_HARD_TIMEOUT_MS)
    );
    const { error: clusterError } = await Promise.race([
      supabase.rpc('cluster_related_articles', { process_window_hours: 1, max_batch_size: 2 }),
      clusterTimeout,
    ]);
    if (clusterError) {
      console.error(`Clustering call ${i + 1}/${CLUSTER_CALLS_PER_RUN} failed (non-fatal): ${clusterError.message}`);
      break; // if one call is timing out, later ones will too -- stop wasting run time
    }
    clusteredOk++;
  }
  console.log(`Clustering complete (${clusteredOk}/${CLUSTER_CALLS_PER_RUN} calls succeeded).`);
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
