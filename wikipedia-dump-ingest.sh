#!/bin/bash
#
# Backfill Wikipedia pages from the Wikimedia Enterprise monthly snapshots
# (wikipedia_dump_ingest.py, WIKIPEDIA-CRAWLER-024). Same image and env file as the
# crawler, its own container name, so it can run beside wikipedia-crawler-manual.
# Without --write it is a dry run: it reads the database and writes nothing to it.
# Stop the main crawler before a --write run: both would upsert the same rows.
#
# Usage (the report also lands in dumpdata/, ignored by git):
#   ./wikipedia-dump-ingest.sh --lang fr --chunks 0
#   ./wikipedia-dump-ingest.sh --lang fr --chunks 0 --write
#   ./wikipedia-dump-ingest.sh --lang fr --chunks 0-149 --write --title-map dumpdata

cd /home/debian/docker/wikipedia-crawler
docker build -q -t wikipedia-crawler-python-app . > /dev/null
mkdir -p dumpdata
strlog="dumpdata/ingest-$(date +%Y%m%d-%H%M%S).log"
strcontainername="${CONTAINER_NAME:-wikipedia-dump-ingest}"
docker run --rm --network="host" \
    --env-file /home/debian/docker/wikipedia-crawler/.env \
    -e PYTHONUNBUFFERED=1 \
    -v "$(pwd)":/home/debian/docker/wikipedia-crawler \
    --name "$strcontainername" wikipedia-crawler-python-app \
    python wikipedia_dump_ingest.py "$@" 2>&1 | tee "$strlog"
echo "Report: $(pwd)/$strlog"
