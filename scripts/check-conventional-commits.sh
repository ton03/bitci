#!/bin/sh
set -eu

base=${1:?usage: scripts/check-conventional-commits.sh BASE [HEAD]}
head=${2:-HEAD}
pattern='^(build|chore|ci|docs|feat|fix|perf|refactor|revert|test)(\([[:alnum:]./_-]+\))?!?: .+$'

check_subject() {
  label=$1
  subject=$2
  if ! printf '%s\n' "$subject" | grep -Eq "$pattern"; then
    printf '%s has a non-conventional subject: %s\n' "$label" "$subject" >&2
    exit 1
  fi
}

if [ -n "${BITCI_PR_TITLE:-}" ]; then
  check_subject 'pull request title' "$BITCI_PR_TITLE"
fi

git log --format='%h %s' "$base..$head" |
while IFS=' ' read -r sha subject; do
  check_subject "$sha" "$subject"
done
