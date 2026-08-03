// send-digest.js
//
// Sends personalized digest emails, one per subscribed SAVED FILTER (not
// one per user) -- a user can save multiple filters (e.g. "Tech in Europe",
// "World News") and independently turn digest emails on/off for each one,
// at its own frequency, with its own unsubscribe link. If a user has 2
// filters subscribed, they get 2 separate, clearly-labeled emails, not one
// merged one -- matches the product decision that a digest belongs to a
// filter, not the account as a whole.
//
// Runs daily via GitHub Actions; internally decides who's actually due
// based on digest_frequency + digest_last_sent_at, so the same scheduled
// run correctly handles both daily and weekly subscribers without needing
// two separate workflows.
//
// Uses Brevo (free tier: 300 emails/day, no card required) instead of
// Resend -- switched because the project's existing domain was already
// verified on a separate Resend account. Requires a BREVO_API_KEY secret
// (Brevo dashboard -> SMTP & API -> API Keys) and a verified sender email
// (Brevo dashboard -> Senders, Domains & Dedicated IPs -> Senders).

const { createClient } = require('@supabase/supabase-js');
const countries = require('./countries.json');

// Built from the same countries.json ingest.js uses -- deliberately NOT
// importing ingest.js itself, since it process.exit(1)s at require-time if
// unrelated API keys (GNEWS_API_KEY etc.) aren't set, which this workflow's
// environment doesn't have. Duplicating this one-line derivation from the
// single shared data file is safe; duplicating the actual name data would
// not have been (that's exactly the bug that caused "IN"/"CN" instead of
// "India"/"China" to leak into emails).
const COUNTRY_NAME_BY_CODE = Object.fromEntries(countries.map((c) => [c.code, c.name]));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BREVO_API_KEY = process.env.BREVO_API_KEY;
const DIGEST_FROM_EMAIL = process.env.DIGEST_FROM_EMAIL; // must be a verified sender in Brevo -- no generic fallback like Resend's onboarding@resend.dev exists here
const DIGEST_FROM_NAME = process.env.DIGEST_FROM_NAME || 'Global Aggregate';
const SUPABASE_PROJECT_REF = process.env.SUPABASE_PROJECT_REF; // e.g. "nikvqivovodrfybfjjka" -- used to build the unsubscribe link

const missing = ['SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY', 'BREVO_API_KEY', 'DIGEST_FROM_EMAIL', 'SUPABASE_PROJECT_REF'].filter((k) => !process.env[k]);
if (missing.length > 0) {
  console.error(`Missing required environment variable(s): ${missing.join(', ')}`);
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const TOPIC_EMOJI = {
  Politics: '\u{1F3DB}\u{FE0F}',
  Business: '\u{1F4BC}',
  Tech: '\u{1F4BB}',
  Sports: '\u{26BD}',
  Health: '\u{1FA7A}',
  World: '\u{1F30D}',
};

function escapeHtml(str) {
  return (str || '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

async function getDueFilters() {
  const { data, error } = await supabase
    .from('saved_filter_groups')
    .select('id, user_id, group_name, country_list, topic_list, digest_frequency, digest_last_sent_at, digest_unsubscribe_token')
    .neq('digest_frequency', 'off');

  if (error) throw new Error(`Failed to load subscribed filters: ${error.message}`);

  const now = Date.now();
  return data.filter((filter) => {
    if (!filter.digest_last_sent_at) return true; // never sent -- due immediately
    const hoursSinceLastSend = (now - new Date(filter.digest_last_sent_at).getTime()) / (1000 * 60 * 60);
    if (filter.digest_frequency === 'daily') return hoursSinceLastSend >= 20;
    if (filter.digest_frequency === 'weekly') return hoursSinceLastSend >= 164;
    return false;
  });
}

async function getArticlesFor(filter) {
  const lookbackHours = filter.digest_frequency === 'weekly' ? 168 : 24;
  let query = supabase
    .from('articles')
    .select('title, description, url, source, country, topic, cluster_id, published_at, created_at')
    .gte('created_at', new Date(Date.now() - lookbackHours * 60 * 60 * 1000).toISOString())
    .order('created_at', { ascending: false })
    .limit(200);

  if (filter.country_list && filter.country_list.length > 0) {
    query = query.in('country', filter.country_list);
  }
  if (filter.topic_list && filter.topic_list.length > 0) {
    query = query.in('topic', filter.topic_list);
  }

  const { data, error } = await query;
  if (error) throw new Error(`Failed to load articles for filter ${filter.id}: ${error.message}`);

  const seenClusters = new Set();
  const perCountryCount = {};
  const TARGET_TOTAL = 20;
  // Scales the per-country cap inversely with filter breadth. Fixed at 3
  // was the actual bug behind "CHIN only sends 6 stories" -- CHIN is a
  // 2-country filter, so 3-per-country capped the whole digest at 6, far
  // below the intended 20-story target. For broad/global filters (no
  // country_list, or a long one), this naturally floors back to 3 --
  // diversity protection still applies there, since 3 x many-countries
  // already exceeds TARGET_TOTAL and the overall break below kicks in
  // first regardless.
  const numCountries = filter.country_list && filter.country_list.length > 0 ? filter.country_list.length : null;
  const MAX_PER_COUNTRY = numCountries ? Math.max(3, Math.ceil(TARGET_TOTAL / numCountries)) : 3;
  const deduped = [];
  for (const article of data) {
    if (article.cluster_id) {
      if (seenClusters.has(article.cluster_id)) continue;
      seenClusters.add(article.cluster_id);
    }
    const countryCount = perCountryCount[article.country] || 0;
    if (countryCount >= MAX_PER_COUNTRY) continue;
    perCountryCount[article.country] = countryCount + 1;
    deduped.push(article);
    if (deduped.length >= TARGET_TOTAL) break;
  }
  return deduped;
}

const BRAND_COLOR = '#0d9488'; // teal, matches the site's accent -- adjust here if it doesn't match exactly, this is the only place the primary brand color is defined
const SITE_URL = 'https://globalaggregate.org';

// Per-topic accent colors -- [background tint, border/text accent] pairs.
// Kept email-safe: solid hex colors only, no gradients/box-shadow (patchy
// support across Gmail/Outlook/Apple Mail), just background-color + a
// left border for the "card" look, which renders consistently everywhere.
const TOPIC_COLORS = {
  Politics: { bg: '#eef2ff', accent: '#4f46e5', text: '#3730a3' }, // indigo
  Business: { bg: '#ecfdf5', accent: '#059669', text: '#065f46' }, // emerald
  Tech: { bg: '#eff6ff', accent: '#2563eb', text: '#1e40af' }, // blue
  Sports: { bg: '#fff7ed', accent: '#ea580c', text: '#9a3412' }, // orange
  Health: { bg: '#fdf2f8', accent: '#db2777', text: '#9d174d' }, // pink
  World: { bg: '#f0fdfa', accent: '#0d9488', text: '#0f766e' }, // teal, matches brand
};
const DEFAULT_TOPIC_COLOR = { bg: '#f8fafc', accent: '#64748b', text: '#475569' };

function buildEmailHtml(filterName, articles, unsubscribeUrl) {
  const grouped = {};
  for (const a of articles) {
    if (!grouped[a.topic]) grouped[a.topic] = [];
    grouped[a.topic].push(a);
  }

  const dateStr = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });

  const sections = Object.entries(grouped)
    .map(([topic, items]) => {
      const emoji = TOPIC_EMOJI[topic] || '';
      const colors = TOPIC_COLORS[topic] || DEFAULT_TOPIC_COLOR;
      const rows = items
        .map((a, i) => {
          const countryName = COUNTRY_NAME_BY_CODE[a.country] || a.country;
          const isLast = i === items.length - 1;
          return `
          <tr>
            <td style="padding: 14px 16px; ${isLast ? '' : `border-bottom: 1px solid ${colors.bg === '#f8fafc' ? '#e2e8f0' : colors.bg};`}">
              <a href="${escapeHtml(a.url)}" style="color: #1a1a1a; text-decoration: none; font-weight: 600; font-size: 15px; line-height: 1.4;">${escapeHtml(a.title)}</a>
              <div style="margin-top: 6px;">
                <span style="display: inline-block; background: ${colors.bg}; color: ${colors.text}; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 10px;">${escapeHtml(countryName)}</span>
                <span style="color: #999; font-size: 12px; margin-left: 6px;">${escapeHtml(a.source)}</span>
              </div>
            </td>
          </tr>`;
        })
        .join('');
      return `
      <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; border-radius: 10px; overflow: hidden; border: 1px solid ${colors.bg === '#f8fafc' ? '#e2e8f0' : colors.bg};">
        <tr>
          <td style="background: ${colors.bg}; padding: 10px 16px; border-left: 4px solid ${colors.accent};">
            <span style="font-size: 14px; font-weight: 700; color: ${colors.text};">${emoji} ${escapeHtml(topic)}</span>
          </td>
        </tr>
        <tr>
          <td style="padding: 0; background: #ffffff; border-left: 4px solid ${colors.accent};">
            <table style="width: 100%; border-collapse: collapse;">${rows}</table>
          </td>
        </tr>
      </table>`;
    })
    .join('');

  return `<!DOCTYPE html><html><body style="font-family: -apple-system, sans-serif; max-width: 560px; margin: 0 auto; padding: 0; color: #1a1a1a; background: #fafafa;">
    <div style="background: ${BRAND_COLOR}; background: linear-gradient(135deg, ${BRAND_COLOR}, #0f766e); padding: 28px 24px 22px; border-radius: 0 0 14px 14px;">
      <div style="color: #ffffff; font-size: 20px; font-weight: 700; letter-spacing: -0.02em;">\u{1F30E} GlobalAggregate</div>
      <div style="color: rgba(255,255,255,0.85); font-size: 13px; margin-top: 3px;">${dateStr}</div>
    </div>
    <div style="padding: 20px 24px 24px;">
      <div style="background: #ffffff; border-radius: 12px; padding: 16px 20px; margin-bottom: 20px; border: 1px solid #eee;">
        <h1 style="font-size: 19px; margin: 0 0 2px; color: #1a1a1a;">${escapeHtml(filterName)}</h1>
        <p style="color: #888; font-size: 13px; margin: 0;">${articles.length} ${articles.length === 1 ? 'story' : 'stories'} from your saved filter</p>
      </div>
      ${sections}
      <p style="color: #aaa; font-size: 12px; margin-top: 12px; border-top: 1px solid #eee; padding-top: 16px; text-align: center;">
        <a href="${SITE_URL}" style="color: ${BRAND_COLOR}; text-decoration: none; font-weight: 600;">Open GlobalAggregate</a>
        &middot;
        <a href="${unsubscribeUrl}" style="color: #aaa;">Stop emails for this filter</a>
        <br style="margin-top: 4px;">
        <span style="display: inline-block; margin-top: 6px;">you can still get digests for your other saved filters, and re-enable this one anytime</span>
      </p>
    </div>
  </body></html>`;
}

async function sendEmail(to, subject, html) {
  const res = await fetch('https://api.brevo.com/v3/smtp/email', {
    method: 'POST',
    headers: {
      'api-key': BREVO_API_KEY,
      'Content-Type': 'application/json',
      accept: 'application/json',
    },
    body: JSON.stringify({
      sender: { email: DIGEST_FROM_EMAIL, name: DIGEST_FROM_NAME },
      to: [{ email: to }],
      subject,
      htmlContent: html,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Brevo API error ${res.status}: ${body}`);
  }
  return res.json();
}

async function main() {
  const filters = await getDueFilters();
  console.log(`${filters.length} subscribed filter(s) due for a digest.`);

  const emailCache = new Map();
  let sent = 0;
  let skippedEmpty = 0;
  let failed = 0;

  for (const filter of filters) {
    let email = emailCache.get(filter.user_id);
    if (!email) {
      const { data: userData, error: userError } = await supabase.auth.admin.getUserById(filter.user_id);
      if (userError || !userData?.user?.email) {
        console.error(`[filter ${filter.id}] Could not resolve email: ${userError?.message || 'no email on account'}`);
        failed++;
        continue;
      }
      email = userData.user.email;
      emailCache.set(filter.user_id, email);
    }

    let articles;
    try {
      articles = await getArticlesFor(filter);
    } catch (err) {
      console.error(`[${email} / "${filter.group_name}"] Failed to load articles: ${err.message}`);
      failed++;
      continue;
    }

    if (articles.length === 0) {
      console.log(`[${email} / "${filter.group_name}"] No matching articles this cycle -- skipping send, not marking as sent (will retry next run).`);
      skippedEmpty++;
      continue;
    }

    const unsubscribeUrl = `https://${SUPABASE_PROJECT_REF}.supabase.co/functions/v1/unsubscribe?token=${filter.digest_unsubscribe_token}`;
    const html = buildEmailHtml(filter.group_name, articles, unsubscribeUrl);
    const subject = `GlobalAggregate: ${articles.length} ${articles.length === 1 ? 'story' : 'stories'} for "${filter.group_name}"`;

    try {
      await sendEmail(email, subject, html);
      const { error: updateError } = await supabase
        .from('saved_filter_groups')
        .update({ digest_last_sent_at: new Date().toISOString() })
        .eq('id', filter.id);
      if (updateError) console.error(`[${email} / "${filter.group_name}"] Sent but failed to update digest_last_sent_at: ${updateError.message}`);
      console.log(`[${email} / "${filter.group_name}"] Sent (${articles.length} articles).`);
      sent++;
    } catch (err) {
      console.error(`[${email} / "${filter.group_name}"] Send failed: ${err.message}`);
      failed++;
    }

    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  console.log(`\nDone. Sent: ${sent}, skipped (no matching articles): ${skippedEmpty}, failed: ${failed}.`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Digest send failed:', err);
    process.exit(1);
  });
