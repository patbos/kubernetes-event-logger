#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

run_expect_fail() {
  if "$@" >"$workdir/stdout" 2>"$workdir/stderr"; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

cd "$workdir"
git init -b main >/dev/null
git config user.name "release-source-test"
git config user.email "release-source-test@example.invalid"

printf 'initial\n' > file.txt
git add file.txt
git commit -m "initial" >/dev/null
git tag v1.2.3

"$repo_root/scripts/validate-release-source.sh" v1.2.3 main
run_expect_fail "$repo_root/scripts/validate-release-source.sh" release-1.2.3 main

git switch -c feature >/dev/null 2>&1
printf 'feature\n' >> file.txt
git commit -am "feature" >/dev/null
git tag v1.2.4
git switch main >/dev/null 2>&1

run_expect_fail "$repo_root/scripts/validate-release-source.sh" v1.2.4 main
