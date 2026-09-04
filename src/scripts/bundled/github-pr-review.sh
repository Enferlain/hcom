#!/bin/bash
# Bundled hcom GitHub PR review script

set -euo pipefail

usage() {
  echo "Usage: hcom run github-pr-review --pr-url URL --github-token TOKEN"
  echo "  --pr-url           The URL of the GitHub pull request API endpoint"
  echo "  --github-token     GitHub token to read the PR diff and write the comment"
  echo "  --allowlist        Optional comma-separated list of allowed actors"
  echo "  --actor            The user who triggered the action"
  exit 1
}

PR_URL=""
GITHUB_TOKEN=""
ALLOWLIST=""
ACTOR=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --pr-url)
      PR_URL="$2"
      shift 2
      ;;
    --github-token)
      GITHUB_TOKEN="$2"
      shift 2
      ;;
    --allowlist)
      ALLOWLIST="$2"
      shift 2
      ;;
    --actor)
      ACTOR="$2"
      shift 2
      ;;
    -h|--help)
      usage || exit 1
      ;;
    *)
      echo "Unknown argument: $1"
      usage || exit 1
      ;;
  esac
done

if [[ -z "$PR_URL" || -z "$GITHUB_TOKEN" ]]; then
  usage || exit 1
fi

if [[ -n "$ALLOWLIST" && -n "$ACTOR" ]]; then
  IFS=',' read -ra ALLOWED <<< "$ALLOWLIST"
  allowed_match=false
  for user in "${ALLOWED[@]}"; do
    if [[ "$user" == "$ACTOR" ]]; then
      allowed_match=true
      break
    fi
  done
  if [[ "$allowed_match" == "false" ]]; then
    echo "Actor $ACTOR is not in the allowlist. Exiting."
    exit 1
  fi
fi

# Fetch PR info
PR_DATA=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" "$PR_URL")
HEAD_SHA=$(echo "$PR_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('head',{}).get('sha',''))")
DIFF_URL=$(echo "$PR_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('diff_url',''))")
COMMENTS_URL=$(echo "$PR_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('comments_url',''))")
STATUSES_URL=$(echo "$PR_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('statuses_url',''))")

if [[ -z "$HEAD_SHA" || -z "$DIFF_URL" ]]; then
  echo "Failed to get PR data from $PR_URL"
  exit 1
fi

# Check for existing comment for this SHA to deduplicate
EXISTING_COMMENT=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" "$COMMENTS_URL" | \
  python3 -c "
import sys,json
try:
  comments = json.load(sys.stdin)
  for c in comments:
    if '### hcom PR Review' in c.get('body','') and '$HEAD_SHA' in c.get('body',''):
      print('found')
      sys.exit(0)
except:
  pass
")

if [[ "$EXISTING_COMMENT" == "found" ]]; then
  echo "Review for commit $HEAD_SHA already exists. Skipping."
  exit 0
fi

DIFF_TEXT=$(curl -sL -H "Authorization: Bearer $GITHUB_TOKEN" "$DIFF_URL")
CI_RESULT=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github.v3+json" "$STATUSES_URL")

TMP_DIR=$(mktemp -d)
export HCOM_DIR="$TMP_DIR"
THREAD_ID="github-review-$HEAD_SHA"

cat << PROMPT > "$TMP_DIR/prompt.txt"
Review this pull request diff. Focus on bugs, security issues, and bad practices.

Repository Instructions:
If there is an AGENTS.md or README.md, follow their guidelines.

CI Results:
$CI_RESULT

Return a structured JSON review schema matching this exact format (do not include any other text, just the JSON):
{
  "findings": [
    {
      "severity": "high|medium|low",
      "file": "path/to/file",
      "line": 123,
      "explanation": "why this is an issue",
      "recommendation": "how to fix it"
    }
  ]
}

When you have generated the JSON review schema, use 'hcom send @parent --thread $THREAD_ID' to send the JSON back to the workflow script. Then you may stop.
PROMPT

# Launch the agent and pipe the diff text so we don't exceed ARG_MAX
# Passing the diff via file to the agent
echo "$DIFF_TEXT" > "$TMP_DIR/diff.txt"

# Run the agent in headless mode
hcom 1 claude --headless --hcom-system-prompt "You are a senior code reviewer. You must output only JSON when sending back to the parent. Review the diff in the provided workspace file $TMP_DIR/diff.txt" < "$TMP_DIR/prompt.txt" > /dev/null 2>&1 &
AGENT_PID=$!

# Wait for the result
echo "Waiting for review result..."
RESULT=$(hcom events --wait 300 --thread "$THREAD_ID" --json | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('data',{}).get('message',{}).get('content',''))")

if [[ -z "$RESULT" ]]; then
  echo "Failed to get review result"
  kill $AGENT_PID 2>/dev/null || true
  exit 1
fi

CLEAN_JSON=$(echo "$RESULT" | sed 's/^```json//;s/^```//;s/```$//')

# Formatting body using python to avoid newline bugs
python3 -c "
import sys, json, urllib.request

result = sys.argv[1]
head_sha = sys.argv[2]
comments_url = sys.argv[3]
github_token = sys.argv[4]

comment_body = f'### hcom PR Review\n\nReviewed commit: \`{head_sha}\`\n\n'

try:
    data = json.loads(result)
    findings = data.get('findings', [])
    if not findings:
        comment_body += 'No major issues found in this diff.\n'
    else:
        for finding in findings:
            severity = finding.get('severity', '')
            file_path = finding.get('file', '')
            line = finding.get('line', '')
            explanation = finding.get('explanation', '')
            recommendation = finding.get('recommendation', '')

            comment_body += f'- **[{severity}]** \`{file_path}\`'
            if line and line != 'None' and line != 'null':
                comment_body += f' (line {line})'
            comment_body += f'\n  - {explanation}\n  - *Recommendation:* {recommendation}\n\n'
except:
    comment_body += '(Agent returned malformed JSON output)\n\n' + result

req = urllib.request.Request(
    comments_url,
    data=json.dumps({'body': comment_body}).encode('utf-8'),
    headers={
        'Authorization': 'Bearer ' + github_token,
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json'
    }
)
try:
    urllib.request.urlopen(req)
except Exception as e:
    print('Failed to post comment:', e)
    sys.exit(1)
" "$CLEAN_JSON" "$HEAD_SHA" "$COMMENTS_URL" "$GITHUB_TOKEN"

echo "Posted review to PR."
