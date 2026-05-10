#!/usr/bin/env bash

set -euo pipefail

tag="${1:-${GITHUB_REF_NAME:-}}"
main_ref="${2:-origin/main}"

if [[ -z "$tag" ]]; then
  echo "Release tag is required" >&2
  exit 1
fi

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Release tag must match vMAJOR.MINOR.PATCH, got: $tag" >&2
  exit 1
fi

tag_commit="$(git rev-parse "${tag}^{commit}")"

if ! git merge-base --is-ancestor "$tag_commit" "$main_ref"; then
  echo "Release tag $tag points to $tag_commit, which is not reachable from $main_ref" >&2
  exit 1
fi
