#!/bin/bash
# Switch wikipedia-crawler.sh back on: removes the flag file CRON_OFF that off.sh wrote.
cd "$(dirname "$(readlink -f "$0")")" || exit 1
# A launcher renamed by the old off.sh (<name>-off.sh, before 2026-10-25) is put back,
# or dropped when git has already restored the tracked one.
[ -f wikipedia-crawler-off.sh ] && { [ -f wikipedia-crawler.sh ] && rm -f wikipedia-crawler-off.sh || mv wikipedia-crawler-off.sh wikipedia-crawler.sh; }
rm -f CRON_OFF && echo "wikipedia-crawler: on (CRON_OFF removed)"
