#!/usr/bin/env bash
# Decides whether a push to main publishes a label release, and from which run.
# A merged pull request labelled `release` publishes without testing again
# when the commit on main holds exactly the tree its pull request tested, the
# pull request ran every job of this workflow to success, and every client
# carries the same version. Anything else answers release=no.
#
# Inputs: GH_TOKEN, GITHUB_REPOSITORY, GITHUB_SHA, GITHUB_OUTPUT, WORKFLOW_FILE.
# Outputs: release (yes or no), version, pr_run.
set -euo pipefail

say() { echo "release plan: $*"; }
answer() {
  echo "release=$1" >> "${GITHUB_OUTPUT}"
  echo "version=${2:-}" >> "${GITHUB_OUTPUT}"
  echo "pr_run=${3:-}" >> "${GITHUB_OUTPUT}"
  exit 0
}

pr="$(
  gh api "repos/${GITHUB_REPOSITORY}/commits/${GITHUB_SHA}/pulls" \
    --jq "[.[] | select(.merged_at != null and .merge_commit_sha == \"${GITHUB_SHA}\")][0]"
)"
if [[ -z "${pr}" || "${pr}" == "null" ]]; then
  say "no merged pull request produced ${GITHUB_SHA}"
  answer no
fi

number="$(jq -r '.number' <<<"${pr}")"
if ! jq -e '[.labels[].name] | index("release")' <<<"${pr}" >/dev/null; then
  say "pull request #${number} has no release label"
  answer no
fi

text="$(jq -r '"\(.title)\n\(.body // "")"' <<<"${pr}")"
if [[ "${text}" == *"[wire-break]"* || "${text}" == *"[skip-server]"* ]]; then
  echo "pull request #${number} skipped the server suites, so it cannot publish without a tag run" >&2
  exit 1
fi

head_sha="$(jq -r '.head.sha' <<<"${pr}")"
git fetch --no-tags --depth=1 origin "${head_sha}"
merged_tree="$(git rev-parse "${GITHUB_SHA}^{tree}")"
tested_tree="$(git rev-parse "${head_sha}^{tree}")"
if [[ "${merged_tree}" != "${tested_tree}" ]]; then
  echo "main ${GITHUB_SHA} differs from the tested head ${head_sha} of #${number}" >&2
  echo "bring the branch up to date with main before merging a release" >&2
  exit 1
fi

pr_run="$(
  gh api "repos/${GITHUB_REPOSITORY}/actions/workflows/${WORKFLOW_FILE}/runs?head_sha=${head_sha}&event=pull_request&status=success" \
    --jq '.workflow_runs | sort_by(.run_number) | last | .id // empty'
)"
if [[ -z "${pr_run}" ]]; then
  echo "${WORKFLOW_FILE} has no successful pull request run for ${head_sha}" >&2
  exit 1
fi

not_run="$(
  gh api --paginate "repos/${GITHUB_REPOSITORY}/actions/runs/${pr_run}/attempts/$(
    gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${pr_run}" --jq '.run_attempt'
  )/jobs?per_page=100" \
    --jq '.jobs[] | select((.name | test("^publish")) | not) | select(.conclusion != "success") | "\(.name): \(.conclusion)"'
)"
if [[ -n "${not_run}" ]]; then
  echo "run ${pr_run} of ${WORKFLOW_FILE} did not run every job to success:" >&2
  echo "${not_run}" >&2
  exit 1
fi

rust_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
python_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' foreign/python/Cargo.toml | head -n 1)"
typescript_version="$(jq -r '.version' foreign/typescript/package.json)"
if [[ "${rust_version}" != "${python_version}" || "${rust_version}" != "${typescript_version}" ]]; then
  echo "versions differ: Rust ${rust_version}, Python ${python_version}, TypeScript ${typescript_version}" >&2
  exit 1
fi

say "pull request #${number} publishes ${rust_version} from run ${pr_run}"
answer yes "${rust_version}" "${pr_run}"
