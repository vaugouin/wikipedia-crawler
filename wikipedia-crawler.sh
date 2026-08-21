#!/bin/bash

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
