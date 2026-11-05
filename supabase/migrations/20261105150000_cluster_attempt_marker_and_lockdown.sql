-- 2026-11-05: clustering throughput fix + lock down SECURITY DEFINER RPCs.
--
-- ROOT CAUSE (verified against the live function body, 2026-11-05):
-- cluster_related_articles picked seed rows with
--   where cluster_id is null and created_at > <window> ... order by created_at asc limit N
-- and nothing recorded that a row had already been tried. Rows with no
-- cross-country/cross-source match (the large majority) stay cluster_id IS NULL,
-- so every call re-selected the SAME oldest N unmatched rows. ingest-rss.js calls
-- the RPC up to 100x per run (batch 10) and ingest.js 50x (batch 2), but all of
-- those calls churned over the same ~10 rows. Newer stories were never reached
-- before aging out of the 6h process window, leaving ~96% of articles unclustered
-- and Trending empty.
--
-- FIX: stamp each seed row with cluster_attempted_at when it is evaluated, skip
-- stamped rows, and process newest-first so fresh stories get priority.
-- A later-arriving counterpart is itself evaluated as a seed and can match an
-- earlier unclustered row (second query in the function), so one attempt per row
-- loses no matches.

alter table public.articles
  add column if not exists cluster_attempted_at timestamptz;

create index if not exists articles_unattempted_created_at_idx
  on public.articles (created_at desc)
  where cluster_id is null and cluster_attempted_at is null;

create or replace function public.cluster_related_articles(
  sim_threshold double precision default 0.35,
  lookback_hours integer default 72,
  process_window_hours integer default 6,
  max_batch_size integer default 100
)
returns void
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  rec record;
  match_id uuid;
  new_cluster uuid;
  candidate_cutoff timestamptz;
  rec_cutoff timestamptz;
  boilerplate_pattern text := '(daily brief\y|\y(monday|tuesday|wednesday|thursday|friday|saturday|sunday|morning|evening|weekly)\s+briefing|week in review|front pages?|pre-open|class suspensions?|walang pasok|all of \w+ today|things to do)';
  bare_date_pattern text := '^[a-z]+,?\s+(\d{1,2}\s+[a-z]+|[a-z]+\s+\d{1,2}),?\s+\d{4}$';
  generic_incident_pattern text := '\y(man|woman|person|boy|girl|elderly (man|woman))\s+(found dead|dies|killed|arrested|stabbed|shot|goes missing)\y';
  same_country_sim_threshold double precision := 0.5;
begin
  perform set_config('pg_trgm.similarity_threshold', sim_threshold::text, true);

  candidate_cutoff := now() - ((process_window_hours + lookback_hours) || ' hours')::interval;
  rec_cutoff := now() - (process_window_hours || ' hours')::interval;

  -- news.google.com excluded here (as SEED) only -- see migration note above
  -- 2026-11-05: skip rows already attempted, newest first (see header).
  for rec in
    execute format(
      $q$select id, title, country, published_at, created_at
         from articles
         where cluster_id is null
           and cluster_attempted_at is null
           and created_at > %L::timestamptz
           and source <> 'news.google.com'
           and title !~* %L
           and title !~* %L
         order by created_at desc
         limit %L$q$,
      rec_cutoff, boilerplate_pattern, bare_date_pattern, max_batch_size
    )
  loop
    match_id := null;

    -- Mark as attempted up front so unmatched rows are never re-selected.
    update articles set cluster_attempted_at = now() where id = rec.id;

    if rec.title !~* generic_incident_pattern then
      -- news.google.com NOT excluded here (as CANDIDATE) -- restores real
      -- cross-country signal like the Iran-Oman Hormuz story
      execute format(
        $q$select a.cluster_id
           from articles a
           where a.cluster_id is not null
             and a.country <> %L
             and a.id <> %L
             and coalesce(a.published_at, a.created_at) > %L::timestamptz
             and a.title %% %L
             and a.title !~* %L
             and a.title !~* %L
             and a.title !~* %L
             and abs(extract(epoch from (coalesce(a.published_at, a.created_at) - %L::timestamptz))) < %L * 3600
           limit 1$q$,
        rec.country, rec.id, candidate_cutoff, rec.title, boilerplate_pattern, bare_date_pattern,
        generic_incident_pattern, coalesce(rec.published_at, rec.created_at), lookback_hours
      ) into match_id;

      if match_id is not null then
        update articles set cluster_id = match_id where id = rec.id;
        continue;
      end if;

      execute format(
        $q$select b.id
           from articles b
           where b.cluster_id is null
             and b.country <> %L
             and b.id <> %L
             and coalesce(b.published_at, b.created_at) > %L::timestamptz
             and b.title %% %L
             and b.title !~* %L
             and b.title !~* %L
             and b.title !~* %L
             and abs(extract(epoch from (coalesce(b.published_at, b.created_at) - %L::timestamptz))) < %L * 3600
           limit 1$q$,
        rec.country, rec.id, candidate_cutoff, rec.title, boilerplate_pattern, bare_date_pattern,
        generic_incident_pattern, coalesce(rec.published_at, rec.created_at), lookback_hours
      ) into match_id;

      if match_id is not null then
        new_cluster := gen_random_uuid();
        update articles set cluster_id = new_cluster where id in (rec.id, match_id);
        continue;
      end if;
    end if;

    perform set_config('pg_trgm.similarity_threshold', same_country_sim_threshold::text, true);

    execute format(
      $q$select a.cluster_id
         from articles a
         where a.cluster_id is not null
           and a.country = %L
           and a.source <> (select source from articles where id = %L)
           and a.id <> %L
           and coalesce(a.published_at, a.created_at) > %L::timestamptz
           and a.title %% %L
           and a.title !~* %L
           and a.title !~* %L
           and abs(extract(epoch from (coalesce(a.published_at, a.created_at) - %L::timestamptz))) < %L * 3600
         limit 1$q$,
      rec.country, rec.id, rec.id, candidate_cutoff, rec.title,
      boilerplate_pattern, bare_date_pattern, coalesce(rec.published_at, rec.created_at), lookback_hours
    ) into match_id;

    if match_id is not null then
      update articles set cluster_id = match_id where id = rec.id;
      perform set_config('pg_trgm.similarity_threshold', sim_threshold::text, true);
      continue;
    end if;

    execute format(
      $q$select b.id
         from articles b
         where b.cluster_id is null
           and b.country = %L
           and b.source <> (select source from articles where id = %L)
           and b.id <> %L
           and coalesce(b.published_at, b.created_at) > %L::timestamptz
           and b.title %% %L
           and b.title !~* %L
           and b.title !~* %L
           and abs(extract(epoch from (coalesce(b.published_at, b.created_at) - %L::timestamptz))) < %L * 3600
         limit 1$q$,
      rec.country, rec.id, rec.id, candidate_cutoff, rec.title,
      boilerplate_pattern, bare_date_pattern, coalesce(rec.published_at, rec.created_at), lookback_hours
    ) into match_id;

    if match_id is not null then
      new_cluster := gen_random_uuid();
      update articles set cluster_id = new_cluster where id in (rec.id, match_id);
    end if;

    perform set_config('pg_trgm.similarity_threshold', sim_threshold::text, true);
  end loop;
end;
$function$;

-- Security advisor 0028/0029: these SECURITY DEFINER functions were callable by
-- anon/authenticated via /rest/v1/rpc/*. cluster_related_articles WRITES
-- cluster_id, so anyone could trigger it. Callers are the ingestion scripts
-- (service_role) and pg_cron (postgres) only. get_distinct_countries is NOT
-- touched: the frontend calls it.
revoke execute on function public.cluster_related_articles(double precision, integer, integer, integer)
  from public, anon, authenticated;
revoke execute on function public.refresh_recent_title_trgm_index(integer)
  from public, anon, authenticated;
grant execute on function public.cluster_related_articles(double precision, integer, integer, integer)
  to service_role;
grant execute on function public.refresh_recent_title_trgm_index(integer)
  to service_role;
