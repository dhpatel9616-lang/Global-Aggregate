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
  const MAX_PER_COUNTRY = 3;
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
    if (deduped.length >= 20) break;
  }
  return deduped;
}

function buildEmailHtml(filterName, articles, unsubscribeUrl) {
  const grouped = {};
  for (const a of articles) {
    if (!grouped[a.topic]) grouped[a.topic] = [];
    grouped[a.topic].push(a);
  }

  const sections = Object.entries(grouped)
    .map(([topic, items]) => {
      const emoji = TOPIC_EMOJI[topic] || '';
      const rows = items
        .map(
          (a) => `
        <tr>
          <td style="padding: 12px 0; border-bottom: 1px solid #eee;">
            <a href="${escapeHtml(a.url)}" style="color: #1a1a1a; text-decoration: none; font-weight: 600; font-size: 15px;">${escapeHtml(a.title)}</a>
            <div style="color: #888; font-size: 12px; margin-top: 4px;">${escapeHtml(a.source)} &middot; ${escapeHtml(a.country)}</div>
          </td>
        </tr>`
        )
        .join('');
      return `
      <tr><td style="padding: 20px 0 8px;"><h2 style="font-size: 16px; margin: 0; color: #1a1a1a;">${emoji} ${escapeHtml(topic)}</h2></td></tr>
      ${rows}`;
    })
    .join('');

  return `<!DOCTYPE html><html><body style="font-family: -apple-system, sans-serif; max-width: 560px; margin: 0 auto; padding: 24px; color: #1a1a1a;">
    <h1 style="font-size: 20px; margin-bottom: 4px;">${escapeHtml(filterName)}</h1>
    <p style="color: #888; font-size: 13px; margin-top: 0;">${articles.length} stories from your saved filter</p>
    <table style="width: 100%; border-collapse: collapse;">${sections}</table>
    <p style="color: #aaa; font-size: 12px; margin-top: 32px; border-top: 1px solid #eee; padding-top: 16px;">
      <a href="${unsubscribeUrl}" style="color: #aaa;">Stop emails for this filter</a>
      &middot; you can still get digests for your other saved filters, and re-enable this one anytime
    </p>
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
    const subject = `${filter.group_name}: ${articles.length} stories (${filter.digest_frequency})`;

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
