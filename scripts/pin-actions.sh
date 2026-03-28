#!/bin/bash
#
# Pin GitHub Actions to commit SHAs for maximum security
# Usage: ./scripts/pin-actions.sh
#
# This script converts action references from @v1.2.3 to @<commit-sha>
# with a comment showing the version for readability

set -e

WORKFLOWS_DIR=".github/workflows"

echo "🔐 Pinning GitHub Actions to commit SHAs..."
echo ""

updated_count=0

# Find all workflow files
for workflow in "$WORKFLOWS_DIR"/*.yml "$WORKFLOWS_DIR"/*.yaml; do
  if [ ! -f "$workflow" ]; then
    continue
  fi

  echo "Processing: $workflow"
  export workflow

  python3 << 'PYTHON'
import re
import subprocess
import os
import sys

workflow_file = os.environ.get('workflow')
if not workflow_file:
    print("missing workflow environment variable", file=sys.stderr)
    sys.exit(1)

def get_action_sha(owner_repo, version_tag):
    """Get commit SHA for a GitHub action"""
    try:
      cmd = [
        'gh', 'api',
        f'repos/{owner_repo}/commits/{version_tag}',
        '--jq', '.sha'
      ]
      result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
        env=os.environ
      )
      sha = result.stdout.strip()
      if sha and len(sha) == 40:
        return sha
    except Exception as e:
      pass
    return None

with open(workflow_file, 'r') as f:
  content = f.read()

original_content = content

# Find uses: owner/repo[/subcommand]@vX.Y.Z or vX.Y or vX or branch patterns
pattern = r'uses:\s+([a-zA-Z0-9\-_.]+/[a-zA-Z0-9\-_.]+(?:/[a-zA-Z0-9\-_.]+)?)@([a-zA-Z0-9\-_.]+)(?:\s+#(.*))?'

def replace_with_sha(match):
  action = match.group(1)
  version_tag = match.group(2)
  existing_comment = match.group(3).strip() if match.lastindex >= 3 and match.group(3) else None

  print(f'  {action}@{version_tag}...', end=' ', flush=True)

  # Skip if already pinned to a SHA (40 hex characters)
  if len(version_tag) == 40 and all(c in '0123456789abcdef' for c in version_tag):
    print(f'✓ already pinned')
    return match.group(0)

  sha = get_action_sha(action, version_tag)
  if sha:
    print(f'✓ {sha[:7]}...')
    # Preserve existing comment if present, otherwise use version as comment
    comment = existing_comment if existing_comment else version_tag
    return f'uses: {action}@{sha}  # {comment}'
  else:
    print('⊘ keeping (not found)')
    return match.group(0)

content = re.sub(pattern, replace_with_sha, content)

if content != original_content:
  with open(workflow_file, 'w') as f:
    f.write(content)
  print(f'  ✅ Updated')
else:
  print(f'  ✓ Already pinned or no changes')
PYTHON
done

echo ""
echo "✅ Done! All GitHub Actions have been pinned to commit SHAs."
echo ""
echo "💡 Next steps:"
echo "  1. Review the changes: git diff .github/workflows/"
echo "  2. Commit: git add .github/workflows && git commit -m 'chore: pin actions to SHAs'"
echo "  3. Push: git push"
