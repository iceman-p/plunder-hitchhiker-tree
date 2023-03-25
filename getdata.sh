#!/usr/bin/env bash
set -x

# safe, score.gte:37 results in 1,013,245 images at time of writing.
BASE_URL="https://derpibooru.org/api/v1/json/search/images?q=safe%2C+score.gte%3A37&per_page=50&sf=wilson_score"
OUTDIR=safe-score-gte-37
JSONDIR=$OUTDIR/json

mkdir -p $JSONDIR

#curl $BASE_URL --output $OUTDIR/page-1.json
for i in {4917,10292,10277}; do
#for i in {1..20265}; do
    curl "${BASE_URL}&page=$i" --output $JSONDIR/page-$i.json
    sleep 1.5
done
