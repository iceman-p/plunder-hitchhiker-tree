#!/usr/bin/env bash
set -x

BASE_URL="https://derpibooru.org/api/v1/json/search/images?q=safe&per_page=50&sf=wilson_score"
OUTDIR=datadir

mkdir -p $OUTDIR

#curl $BASE_URL --output $OUTDIR/page-1.json
for i in {501..600}; do
    curl "${BASE_URL}&page=$i" --output $OUTDIR/page-$i.json
    sleep 1.25
done
