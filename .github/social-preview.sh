#!/bin/sh
# Renders social-preview.svg to the 1280x640 PNG uploaded in Settings > General > Social preview.
set -eu
cd "$(dirname "$0")"
docker run --rm -v "$PWD:/w" -w /w \
  alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 \
  sh -c 'apk add -q rsvg-convert font-inter && rsvg-convert -w 1280 -h 640 -b "#0b1220" social-preview.svg -o social-preview.png'
