#!/bin/bash
#
# DISABLED since 1.1.37 (2026-11-15). The continuous live crawl (wikipedia_crawler.py, one
# action=parse call per page, around the clock) cost too much on the VPS and on the Wikimedia
# infrastructure. Wikipedia content now comes only from the monthly Wikimedia Enterprise
# snapshots: run-if-new-snapshot.sh (cron, once a day) starts wikipedia_dump_ingest.py --auto
# when a new snapshot is out. See README § "Backfill from the Wikimedia Enterprise snapshots".
#
# This script starts nothing and exits 0, so a cron line left behind is harmless. Remove that
# line on the VPS anyway (crontab -e) and stop a container still running:
#   docker stop wikipedia-crawler
# The previous launcher is in git history: git show df472e4:wikipedia-crawler.sh
# On-demand refreshes of one Qid or one family stay possible with wikipedia-crawler-manual.sh.

echo "$(date '+%Y-%m-%d %H:%M:%S'): wikipedia-crawler.sh is disabled (1.1.37); Wikipedia content comes from run-if-new-snapshot.sh. Remove this cron line."
exit 0
