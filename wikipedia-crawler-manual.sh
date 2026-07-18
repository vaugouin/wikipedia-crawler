#!/bin/bash
#
# Launch a SECOND, interactive wikipedia-crawler container to refresh a single
# Wikidata Qid on demand, in parallel with the always-on `wikipedia-crawler`
# container (which keeps running long jobs such as 202 person).
#
# It reuses the same image and the same host-managed env file as the main
# container, but runs under a DIFFERENT container name and drops you into a
# Python REPL instead of launching the full crawl. Nothing here writes the
# crawler's resume-state server variables, so the two containers cannot corrupt
# each other's checkpoints.
#
# Usage:
#   ./wikipedia-crawler-manual.sh
#   then, in the REPL:
#     >>> import wikipedia_functions as wf
#     >>> wf.f_wikipediaqidtosqleverything("Q24815")                    # item (default)
#     >>> wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")
#
# Or run one Qid non-interactively and exit:
#   ./wikipedia-crawler-manual.sh Q24815 --item-type movie

cd /home/debian/docker/wikipedia-crawler
docker build -t wikipedia-crawler-python-app .

if [ -z "$1" ]; then
    # No args: open an interactive Python shell.
    docker run -it --rm --network="host" \
        --env-file /home/debian/docker/wikipedia-crawler/.env \
        -v "$(pwd)":/home/debian/docker/wikipedia-crawler \
        --name wikipedia-crawler-manual wikipedia-crawler-python-app python
else
    # Args given: run one Qid via the module CLI, then exit.
    docker run -it --rm --network="host" \
        --env-file /home/debian/docker/wikipedia-crawler/.env \
        -v "$(pwd)":/home/debian/docker/wikipedia-crawler \
        --name wikipedia-crawler-manual wikipedia-crawler-python-app \
        python wikipedia_functions.py "$@"
fi
