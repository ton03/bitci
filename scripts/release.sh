#!/bin/sh
set -eu

version=${1:?usage: scripts/release.sh v0.0.1-alpha.1}
if ! printf '%s\n' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+-alpha\.[0-9]+$'; then
  echo "release version must match vMAJOR.MINOR.PATCH-alpha.N" >&2
  exit 2
fi

out=dist
rm -rf "$out"
mkdir -p "$out"

for target in darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do
  goos=${target%/*}
  goarch=${target#*/}
  name="bitci_${version#v}_${goos}_${goarch}"
  dir="$out/$name"
  mkdir -p "$dir"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath \
    -ldflags "-s -w -X github.com/ton03/bitci/internal/bitci.Version=$version" \
    -o "$dir/bitci" ./cmd/bitci
  tar -C "$out" -czf "$out/$name.tar.gz" "$name"
  rm -rf "$dir"
done

(cd "$out" && shasum -a 256 bitci_*.tar.gz > checksums.txt)
