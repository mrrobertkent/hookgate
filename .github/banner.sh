#!/bin/sh
# Renders banner-{light,dark}.svg to the README header PNGs: 4:1, twice the 830 px README column for high-DPI screens.
# The rounded corners stay transparent, so no background colour is set.
set -eu
cd "$(dirname "$0")"
docker run --rm -v "$PWD:/w" -w /w \
  alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 \
  sh -c 'apk add -q rsvg-convert font-inter && for t in light dark; do rsvg-convert -w 1680 -h 420 banner-$t.svg -o banner-$t.png; done'
