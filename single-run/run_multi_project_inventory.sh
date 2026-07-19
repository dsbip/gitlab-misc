#!/usr/bin/env bash
#
# Loop over every Composer project defined in a YAML config, authenticate to
# each one with Workload Identity Federation (WIF), run the DAG inventory, then
# revoke the credentials before moving to the next project.
#
# Rows from every project are appended into a single combined CSV (the header is
# written once, taken from the inventory script's own output so it stays in sync).
#
# Per project the sequence is:
#   1. Resolve the GitLab OIDC id_token for that project (id_token_var).
#   2. gcloud iam workload-identity-pools create-cred-config  -> external account config
#   3. gcloud auth login --cred-file=...                      -> authenticate
#   4. composer-dag-inventory/list_composer_dags.sh <project> -> inventory to a temp CSV
#   5. append data rows to the combined CSV
#   6. gcloud auth revoke / unset project / shred token files -> terminate auth
#
# A failure in one project is logged and skipped; remaining projects still run.
# The script exits non-zero if any project failed, so CI surfaces the problem
# while still publishing the rows that were collected.
#
# Config (flags override env vars):
#   --config     / TARGETS_FILE   YAML config. Default: single-run/projects.yml
#   --output     / OUTPUT_CSV     Combined CSV. Default: composer_dags_all_projects.csv
#   --inventory  / INVENTORY_SH   Inventory script. Default: composer-dag-inventory/list_composer_dags.sh
#
# Usage:
#   single-run/run_multi_project_inventory.sh
#   single-run/run_multi_project_inventory.sh --config single-run/projects.yml --output all.csv

set -uo pipefail

CONFIG_FILE="${TARGETS_FILE:-single-run/projects.yml}"
FINAL_CSV="${OUTPUT_CSV:-composer_dags_all_projects.csv}"
INVENTORY_SH="${INVENTORY_SH:-composer-dag-inventory/list_composer_dags.sh}"

WORKDIR=""
HEADER_WRITTEN=0
OK_COUNT=0
FAIL_COUNT=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

usage() {
  grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'
}

# Revoke any active gcloud credentials and remove on-disk secrets.
gcp_logout() {
  gcloud auth revoke --all --quiet >/dev/null 2>&1 || true
  gcloud config unset project --quiet >/dev/null 2>&1 || true
  if [[ -n "$WORKDIR" ]]; then
    rm -f "$WORKDIR"/token_* "$WORKDIR"/cred_* 2>/dev/null || true
  fi
}

cleanup() {
  gcp_logout
  [[ -n "$WORKDIR" ]] && rm -rf "$WORKDIR" 2>/dev/null || true
}

# Emit one TSV line per project: project_id, provider, service_account,
# id_token_var, location. Invalid/incomplete entries are reported and skipped.
parse_targets() {
  python3 - "$1" <<'PY'
import sys

try:
    import yaml
except ImportError:
    sys.stderr.write(
        "ERROR: PyYAML not available. Install it (apt-get install -y python3-yaml "
        "or pip install pyyaml).\n"
    )
    sys.exit(2)

path = sys.argv[1]
try:
    with open(path) as fh:
        cfg = yaml.safe_load(fh) or {}
except Exception as exc:
    sys.stderr.write(f"ERROR: could not parse {path}: {exc}\n")
    sys.exit(2)

projects = cfg.get("projects")
if not isinstance(projects, list):
    sys.stderr.write(f"ERROR: {path} must contain a top-level 'projects:' list.\n")
    sys.exit(2)

rows, bad = [], 0
for idx, entry in enumerate(projects, 1):
    if not isinstance(entry, dict):
        sys.stderr.write(f"WARN: entry #{idx} is not a mapping; skipping.\n")
        bad += 1
        continue
    pid = str(entry.get("project_id") or "").strip()
    prov = str(entry.get("wif_provider_url") or "").strip()
    sa = str(entry.get("wif_service_account") or "").strip()
    tok = str(entry.get("id_token_var") or "GCP_ID_TOKEN").strip()
    loc = str(entry.get("location") or "").strip()
    missing = [
        k
        for k, v in (
            ("project_id", pid),
            ("wif_provider_url", prov),
            ("wif_service_account", sa),
        )
        if not v
    ]
    if missing:
        sys.stderr.write(
            f"WARN: entry #{idx} ({pid or 'unnamed'}) missing {', '.join(missing)}; skipping.\n"
        )
        bad += 1
        continue
    rows.append("\t".join([pid, prov, sa, tok, loc]))

sys.stdout.write("\n".join(rows) + ("\n" if rows else ""))
sys.exit(0)
PY
}

# Authenticate to one project via WIF. Returns non-zero on failure.
gcp_login_wif() {
  local project_id="$1" provider="$2" service_account="$3" token="$4"
  local token_file="$WORKDIR/token_${project_id}" cred_file="$WORKDIR/cred_${project_id}"

  # Token file must not be world-readable.
  (umask 077; printf '%s' "$token" >"$token_file") || {
    log "  ERROR: could not write OIDC token file"; return 1; }

  if ! gcloud iam workload-identity-pools create-cred-config "$provider" \
        --service-account="$service_account" \
        --service-account-token-lifetime-seconds=3600 \
        --output-file="$cred_file" \
        --credential-source-file="$token_file" >/dev/null 2>&1; then
    log "  ERROR: create-cred-config failed for ${project_id}"
    return 1
  fi

  if ! gcloud auth login --cred-file="$cred_file" --quiet >/dev/null 2>&1; then
    log "  ERROR: gcloud auth login failed for ${project_id}"
    return 1
  fi

  if ! gcloud config set project "$project_id" --quiet >/dev/null 2>&1; then
    log "  ERROR: could not set project ${project_id}"
    return 1
  fi
  return 0
}

# Append a per-project CSV into the combined file (header written only once).
append_csv() {
  local src="$1"
  [[ -s "$src" ]] || { log "  WARN: inventory produced no output"; return 1; }
  if [[ "$HEADER_WRITTEN" -eq 0 ]]; then
    head -n 1 "$src" >"$FINAL_CSV" || return 1
    HEADER_WRITTEN=1
  fi
  tail -n +2 "$src" >>"$FINAL_CSV" || return 1
  return 0
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)    CONFIG_FILE="$2"; shift 2 ;;
    --output)    FINAL_CSV="$2"; shift 2 ;;
    --inventory) INVENTORY_SH="$2"; shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *)           die "Unknown argument: $1" ;;
  esac
done

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  command -v gcloud >/dev/null 2>&1 || die "gcloud not found on PATH"
  command -v python3 >/dev/null 2>&1 || die "python3 not found on PATH (needed to read ${CONFIG_FILE})"
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: ${CONFIG_FILE}"
  [[ -x "$INVENTORY_SH" || -f "$INVENTORY_SH" ]] || die "Inventory script not found: ${INVENTORY_SH}"

  local targets
  targets="$(parse_targets "$CONFIG_FILE")" || die "Could not read ${CONFIG_FILE}"
  [[ -n "$targets" ]] || die "No usable project entries in ${CONFIG_FILE}"

  WORKDIR="$(mktemp -d)"
  trap cleanup EXIT

  log "Config   : ${CONFIG_FILE}"
  log "Inventory: ${INVENTORY_SH}"
  log "Output   : ${FINAL_CSV}"
  log ""

  local project_id provider service_account token_var location token tmp_csv
  while IFS=$'\t' read -r project_id provider service_account token_var location; do
    [[ -z "$project_id" ]] && continue
    log "=== ${project_id} (SA: ${service_account}) ==="

    # Resolve the GitLab id_token by variable name (indirect expansion).
    token="${!token_var-}"
    if [[ -z "$token" ]]; then
      log "  ERROR: id_token variable '${token_var}' is empty/unset."
      log "         Declare it under 'id_tokens:' in .gitlab-ci.yml with an aud matching ${provider}."
      FAIL_COUNT=$((FAIL_COUNT + 1))
      continue
    fi

    if ! gcp_login_wif "$project_id" "$provider" "$service_account" "$token"; then
      FAIL_COUNT=$((FAIL_COUNT + 1))
      gcp_logout
      continue
    fi
    log "  authenticated via WIF"

    tmp_csv="$WORKDIR/out_${project_id}.csv"
    local -a inv_args=("$project_id" --output "$tmp_csv")
    [[ -n "$location" ]] && inv_args+=(--locations "$location")

    if bash "$INVENTORY_SH" "${inv_args[@]}"; then
      if append_csv "$tmp_csv"; then
        log "  appended $(( $(wc -l <"$tmp_csv") - 1 )) row(s)"
        OK_COUNT=$((OK_COUNT + 1))
      else
        log "  ERROR: could not append results for ${project_id}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
      fi
    else
      log "  ERROR: inventory failed for ${project_id}"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    gcp_logout
    log "  auth revoked"
    log ""
  done <<<"$targets"

  # Guarantee the artifact exists even if every project failed.
  if [[ "$HEADER_WRITTEN" -eq 0 ]]; then
    printf 'Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time\n' >"$FINAL_CSV"
  fi

  local rows=0
  [[ -f "$FINAL_CSV" ]] && rows=$(( $(wc -l <"$FINAL_CSV") - 1 ))
  log "Done: ${OK_COUNT} project(s) OK, ${FAIL_COUNT} failed, ${rows} DAG row(s) -> ${FINAL_CSV}"

  [[ "$FAIL_COUNT" -eq 0 ]]
}

main
