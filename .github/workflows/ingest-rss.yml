name: Ingest RSS Feeds
on:
  # Lets you click "Run workflow" manually in GitHub Actions tab -- also
  # what the external scheduler (cron-job.org) calls via the REST API.
  # SHARDING (2026-08-02): the feed list outgrew what fits in one run's time
  # budget (confirmed: a run losing 36 of 176 country groups to the time
  # ceiling). Now split across two separate scheduled triggers, each
  # covering roughly half the feeds -- see ingest-rss.js for how the split
  # itself is computed. Leave shard blank (or omit inputs entirely) to
  # process everyone in one run, same as before sharding existed -- useful
  # for manual runs via the Actions tab "Run workflow" button.
  workflow_dispatch:
    inputs:
      shard:
        description: 'Which half to run: A, B, or blank/both for everyone'
        required: false
        default: 'both'
  # NATIVE CRON DISABLED (2026-08-01): moved to an external scheduler
  # (cron-job.org) calling workflow_dispatch on a reliable timer, after
  # confirmed multi-day silent gaps from GitHub's native schedule trigger
  # not firing reliably (this workflow specifically went 6 days without a
  # single scheduled run before the offset fix below, and individual
  # countries have gone 3+ days dark even after that fix). Left here,
  # commented out, in case of a future rollback -- do not re-enable
  # alongside the external scheduler, or the job will run twice on
  # overlapping schedules.
  # Interval doubled 15min -> 30min per shard on 2026-08-06 after a real
  # overlap-induced pile-up caused a ~95min total ingestion outage -- see
  # the SHARDING comment in ingest-rss.js for the full incident writeup.
  # The actual live schedule lives in cron-job.org now, not here; this is
  # historical/rollback reference only.
  # schedule:
  #   - cron: '4,34 * * * *'
jobs:
  ingest-rss:
    runs-on: ubuntu-latest
    # Hard backstop against hangs. GitHub Actions defaults to a 6-hour job
    # timeout if unset -- confirmed via a real run (#11, 2026-07-13) that sat
    # "In progress" for 3h47m+ on what should be a 1-2 minute job (13 feeds,
    # each with a 10s internal timeout, plus clustering). 10 minutes was
    # generous headroom for that 13-feed job. Bumped to 15 (2026-07-28) --
    # the feed list has grown 10x since (142 feeds / 135 countries), and a
    # run got hard-cancelled at the 10-minute mark with 18 country groups
    # never attempted, even after fixing the AllAfrica same-domain cascade
    # that caused it (see FEED_URLS_BY_COUNTRY comment in ingest-rss.js).
    # This is margin on top of that fix, not a substitute for it. Left
    # unchanged with sharding -- each shard now has real headroom under
    # this ceiling instead of running right up against it.
    timeout-minutes: 15
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
      - name: Set up Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Install dependencies
        run: npm install
      - name: Run RSS ingestion script
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          SHARD: ${{ github.event.inputs.shard || 'both' }}
        run: node ingest-rss.js
