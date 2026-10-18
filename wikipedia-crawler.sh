#!/bin/bash

# WIKIPEDIA-CRAWLER-025: while the snapshot ingest runs (run-if-new-snapshot.sh writes the flag,
# wikipedia_dump_ingest.py --auto deletes it once every snapshot is in), the crawler stays stopped:
# both would upsert the same rows. A hand-run ingest container holds it back too.
if [ -f /home/debian/docker/wikipedia-crawler/dumpdata/INGEST_IN_PROGRESS ] || [ $(docker ps -q -f name=^wikipedia-dump-ingest$) ]; then
    echo "Snapshot ingest in progress: wikipedia-crawler not started."
    exit 0
fi

# Check if the wikipedia-crawler Docker container is running.
# The name filter is a regex, and an unanchored one also matches the on-demand
# `wikipedia-crawler-manual` container: without ^...$ this script would report
# "already running" (and skip the restart) whenever a manual run happens to be open.
if [ $(docker ps -q -f name=^wikipedia-crawler$) ]; then
    echo "wikipedia-crawler Docker container is already running."
else
    # Start the wikipedia-crawler container if it is not running
    cd /home/debian/docker/wikipedia-crawler
    docker build -t wikipedia-crawler-python-app .
    # Secrets are injected at runtime from a host-managed env file (kept OUTSIDE
    # the build context so it never lands in image layers, build cache, or registries).
    # docker run -it --rm --network="host" --env-file /home/debian/docker/wikipedia-crawler/.env -v $(pwd):/home/debian/docker/wikipedia-crawler --name wikipedia-crawler wikipedia-crawler-python-app
    docker run -d --rm --network="host" --env-file /home/debian/docker/wikipedia-crawler/.env -v $(pwd):/home/debian/docker/wikipedia-crawler --name wikipedia-crawler wikipedia-crawler-python-app
    echo "wikipedia-crawler Docker container started."
fi
