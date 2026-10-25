#!/bin/bash
# Switch wikipedia-crawler.sh off: it exits at its first test while the flag file CRON_OFF exists in
# this folder, so cron ticks start nothing. Undo with on.sh. Nothing is renamed (until
# 2026-10-25 this script renamed the launcher, which left the git clone dirty).
# A container already running is not stopped: docker stop wikipedia-crawler.
touch "$(dirname "$(readlink -f "$0")")/CRON_OFF" && echo "wikipedia-crawler: off (CRON_OFF written)"
