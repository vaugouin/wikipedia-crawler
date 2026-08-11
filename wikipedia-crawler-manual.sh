#!/bin/bash
#
# Launch a SECOND, interactive wikipedia-crawler container to refresh Wikipedia
# content on demand -- a single Wikidata Qid, or a whole entity family -- in
# parallel with the always-on `wikipedia-crawler` container (which keeps running
# long jobs such as 202 person).
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
#     >>> wf.f_wikipediacontenttosqleverything("technical")             # whole family
#
# Or run one job non-interactively and exit:
#   ./wikipedia-crawler-manual.sh Q24815 --item-type movie
#   ./wikipedia-crawler-manual.sh --content-all list
#   ./wikipedia-crawler-manual.sh --content-all technical --limit 20
#
# Two knobs are read from the host environment and forwarded to the container,
# so a manual run can stay gentler than the main crawler on the Wikimedia APIs
# (the rate limiter is per-process: two containers at the default 20 rps add up
# to 40 rps):
#   WIKIPEDIA_CRAWLER_MAX_RPS=5 ./wikipedia-crawler-manual.sh --content-all list
#   WIKIPEDIA_CRAWLER_WORKERS=4 ./wikipedia-crawler-manual.sh --content-all list
#
# CONTAINER_NAME overrides the container name, so two families can run at once:
#   CONTAINER_NAME=wikipedia-crawler-manual-list ./wikipedia-crawler-manual.sh --content-all list

cd /home/debian/docker/wikipedia-crawler
docker build -t wikipedia-crawler-python-app .

arrenv=()
if [ -n "$WIKIPEDIA_CRAWLER_MAX_RPS" ]; then
    arrenv+=(-e "WIKIPEDIA_CRAWLER_MAX_RPS=$WIKIPEDIA_CRAWLER_MAX_RPS")
fi
if [ -n "$WIKIPEDIA_CRAWLER_WORKERS" ]; then
    arrenv+=(-e "WIKIPEDIA_CRAWLER_WORKERS=$WIKIPEDIA_CRAWLER_WORKERS")
fi
strcontainername="${CONTAINER_NAME:-wikipedia-crawler-manual}"

if [ -z "$1" ]; then
    # No args: open an interactive Python shell.
    docker run -it --rm --network="host" \
        --env-file /home/debian/docker/wikipedia-crawler/.env \
        "${arrenv[@]}" \
        -v "$(pwd)":/home/debian/docker/wikipedia-crawler \
        --name "$strcontainername" wikipedia-crawler-python-app python
else
    # Args given: run one job via the module CLI, then exit.
    docker run -it --rm --network="host" \
        --env-file /home/debian/docker/wikipedia-crawler/.env \
        "${arrenv[@]}" \
        -v "$(pwd)":/home/debian/docker/wikipedia-crawler \
        --name "$strcontainername" wikipedia-crawler-python-app \
        python wikipedia_functions.py "$@"
fi
