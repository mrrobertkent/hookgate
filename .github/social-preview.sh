#!/bin/sh
# Renders social-preview.svg to dist/social-preview.png (git-ignored). Upload it in
# Settings > General > Social preview; GitHub keeps its own copy, so the PNG is not committed.
set -eu
cd "$(dirname "$0")/.."
mkdir -p dist
docker run --rm -v "$PWD:/w" -w /w \
  alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8 \
  sh -c 'apk add -q rsvg-convert font-inter && rsvg-convert -w 1280 -h 640 -b "#0b1220" .github/social-preview.svg -o dist/social-preview.png'
