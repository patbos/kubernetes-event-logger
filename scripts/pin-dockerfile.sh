#!/bin/bash
#
# Pin Dockerfile base images to immutable manifest-list digests.
# Usage: ./scripts/pin-dockerfile.sh [Dockerfile]
#
# This script rewrites each FROM image reference as:
#   image:tag@sha256:<digest>
# while preserving any --platform flag and AS alias.

set -euo pipefail

DOCKERFILE_PATH="${1:-Dockerfile}"

if [ ! -f "$DOCKERFILE_PATH" ]; then
  echo "Dockerfile not found: $DOCKERFILE_PATH" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not installed" >&2
  exit 1
fi

echo "🔐 Pinning Docker base images in $DOCKERFILE_PATH..."
echo ""

export DOCKERFILE_PATH

python3 <<'PYTHON'
import os
import re
import subprocess
import sys

dockerfile_path = os.environ["DOCKERFILE_PATH"]

from_re = re.compile(
    r"^(?P<prefix>\s*FROM(?:\s+--platform=\S+)?)\s+"
    r"(?P<image>\S+)"
    r"(?P<suffix>(?:\s+AS\s+\S+)?)\s*$",
    re.IGNORECASE,
)


def inspect_digest(image_ref: str) -> str | None:
    try:
        result = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", image_ref],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None

    for line in result.stdout.splitlines():
        if line.startswith("Digest:"):
            digest = line.split(":", 1)[1].strip()
            if re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                return digest
    return None


with open(dockerfile_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

updated_lines = []
changed = False

for line in lines:
    match = from_re.match(line.rstrip("\n"))
    if not match:
        updated_lines.append(line)
        continue

    image_with_optional_digest = match.group("image")
    source_ref = image_with_optional_digest.split("@", 1)[0]

    print(f"  {source_ref}...", end=" ", flush=True)
    digest = inspect_digest(source_ref)
    if not digest:
        print("⊘ keeping (digest lookup failed)")
        updated_lines.append(line)
        continue

    pinned_ref = f"{source_ref}@{digest}"
    new_line = f"{match.group('prefix')} {pinned_ref}{match.group('suffix')}\n"
    if new_line != line:
        changed = True
        print(f"✓ {digest[:19]}...")
    else:
        print("✓ already pinned")
    updated_lines.append(new_line)

if changed:
    with open(dockerfile_path, "w", encoding="utf-8") as f:
        f.writelines(updated_lines)
    print("")
    print("✅ Updated Dockerfile")
else:
    print("")
    print("✓ No changes needed")
PYTHON

echo ""
echo "💡 Next steps:"
echo "  1. Review the changes: git diff $DOCKERFILE_PATH"
echo "  2. Commit: git add $DOCKERFILE_PATH && git commit -m 'chore: pin Docker base images'"
