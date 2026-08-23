#!/bin/sh
set -eu

base=${1:?usage: scripts/check-conventional-commits.sh BASE [HEAD]}
head=${2:-HEAD}
pattern='^(build|chore|ci|docs|feat|fix|perf|refactor|revert|test)(\([[:alnum:]./_-]+\))?!?: .+$'

git log --format='%h %s' "$base..$head" |
while IFS=' ' read -r sha subject; do
  if ! printf '%s\n' "$subject" | grep -Eq "$pattern"; then
    printf '%s has a non-conventional subject: %s\n' "$sha" "$subject" >&2
    exit 1
  fi
done
