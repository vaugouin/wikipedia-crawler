#!/bin/bash
# =============================================================================
# Ingest the Wikimedia Enterprise snapshots (frwiki, enwiki) when a new one is out,
# or resume one left unfinished (WIKIPEDIA-CRAWLER-025). Modelled on
# wikidata-crawler/run-if-new-dump.sh.
# =============================================================================
#
# WHAT IT DOES, in this order:
#   1. a flock: one execution at a time;
#   2. if the wikipedia-dump-ingest container runs, exit at once;
#   3. asks `wikipedia_dump_ingest.py --check` in a throwaway container:
#        0 = a snapshot waits (new, or a run left unfinished), 1 = nothing to do,
#        2 = cannot tell (Enterprise or database unreachable);
#      --check reads the state from T_WC_SERVER_VARIABLE and calls the Enterprise API
#      only from the 1st of the month until the new snapshot is in;
#   4. on 0: writes dumpdata/INGEST_IN_PROGRESS, stops the wikipedia-crawler container
#      (wikipedia-crawler.sh refuses to start it while the flag is there), and starts
#      `wikipedia_dump_ingest.py --auto` DETACHED (docker run -d): no nohup, no terminal.
#
# --auto ingests every language whose snapshot is not in yet, chunk by chunk, one forked
# process per chunk, and records each chunk done in T_WC_SERVER_VARIABLE. When every
# language is whole it deletes dumpdata/INGEST_IN_PROGRESS itself, and the crawler's own
# cron starts the crawler again. If it dies, the flag stays, the crawler stays stopped,
# and the next check resumes at the first chunk not done.
#
# On 1 with the flag still there (an --auto run finished whole but could not delete it),
# the flag is removed here.
#
# USAGE
#   ./run-if-new-snapshot.sh            # check, and start if there is work
#   ./run-if-new-snapshot.sh --dry-run  # check and say what it would do
#
# CRON, once a day (a snapshot comes out once a month; a dead run waits a day at most):
#   41 6 * * * /home/debian/docker/wikipedia-crawler/run-if-new-snapshot.sh >> \
#              /home/debian/docker/wikipedia-crawler/dumpdata/run-if-new-snapshot.log 2>&1
#
# The run's own output goes to dumpdata/ingest-auto-<date>-<time>.log and to
#   docker logs -f wikipedia-dump-ingest
# =============================================================================

set -uo pipefail

# cron starts with a minimal PATH, which does not always hold docker.
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

STACK=/home/debian/docker/wikipedia-crawler
IMAGE=wikipedia-crawler-python-app
INGEST=wikipedia-dump-ingest
CRAWLER=wikipedia-crawler
FLAG="$STACK/dumpdata/INGEST_IN_PROGRESS"
LOCK=/tmp/run-if-new-snapshot.lock

DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

cd "$STACK" || { echo "ERROR: $STACK not found."; exit 2; }
mkdir -p dumpdata

if ! command -v flock >/dev/null 2>&1; then
  echo "ERROR: flock not found (util-linux). Without a lock two runs could start at once."
  exit 2
fi
exec 9>"$LOCK" || { echo "ERROR: lock $LOCK not writable."; exit 2; }
if ! flock -n 9; then
  echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC'): another execution holds the lock, skipping."
  exit 0
fi

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC'): Wikipedia snapshot check ==="

if [ -n "$(docker ps -q -f name="^${INGEST}$" 2>/dev/null)" ]; then
  SINCE=$(docker inspect -f '{{.State.StartedAt}}' "$INGEST" 2>/dev/null | cut -c1-19)
  echo "The ingest is running (started ${SINCE:-?}). Nothing to do."
  exit 0
fi

docker build -q -t "$IMAGE" . >/dev/null || { echo "ERROR: image build failed."; exit 2; }

docker run --rm --network=host --env-file .env -v "$STACK":"$STACK" "$IMAGE" \
  python wikipedia_dump_ingest.py --check
VERDICT=$?

case "$VERDICT" in
  1)
    echo "Nothing to do: every snapshot is in."
    if [ -f "$FLAG" ] && [ "$DRY_RUN" = "0" ]; then
      rm -f "$FLAG" && echo "Stale $FLAG removed: the crawler may start again."
    fi
    exit 0 ;;
  2) echo "Nothing done: the check could not tell (see above)."; exit 2 ;;
  0) : ;;
  *) echo "Nothing done: unexpected exit code ($VERDICT)."; exit 2 ;;
esac

LOG="dumpdata/ingest-auto-$(date '+%Y%m%d-%H%M%S').log"

if [ "$DRY_RUN" = "1" ]; then
  echo "--dry-run: stopping here. I would have:"
  echo "  written $FLAG"
  echo "  stopped the $CRAWLER container if running ($(docker ps -q -f name="^${CRAWLER}$" | wc -l) running now)"
  echo "  started $INGEST detached: wikipedia_dump_ingest.py --auto, log $LOG"
  exit 0
fi

touch "$FLAG" || { echo "ERROR: cannot write $FLAG; not starting (the crawler could restart mid-run)."; exit 2; }
if [ -n "$(docker ps -q -f name="^${CRAWLER}$" 2>/dev/null)" ]; then
  echo "Stopping $CRAWLER (it resumes from its server variables when it starts again) ..."
  docker stop -t 120 "$CRAWLER" >/dev/null || { echo "ERROR: could not stop $CRAWLER; not starting."; exit 2; }
fi

docker run -d --rm --network=host --env-file .env -e PYTHONUNBUFFERED=1 \
  -v "$STACK":"$STACK" --name "$INGEST" "$IMAGE" \
  sh -c "python wikipedia_dump_ingest.py --auto --title-map dumpdata 2>&1 | tee -a $LOG" >/dev/null \
  || { echo "ERROR: docker run failed."; exit 2; }
echo "Started $INGEST detached. Log: $STACK/$LOG (or docker logs -f $INGEST)."
