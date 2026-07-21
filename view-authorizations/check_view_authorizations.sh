#!/usr/bin/env bash
#
# Orchestrator: fetch BigQuery authorized-view ACLs across several GCP projects
# (each authenticated with Workload Identity Federation), then analyze the
# provided views.csv to flag views whose authorization chain is missing/broken.
#
# Pipeline of steps:
#   0. (placeholder) trigger a BigQuery job that would regenerate views.csv at
#      runtime. Disabled by default (RUN_BQ_JOB=false) - wired up later with the
#      real SQL. Uses its own placeholder WIF variables.
#   1. For each project in the YAML config: WIF auth -> `bq` fetch every
#      dataset's authorized views -> revoke. Produces authorizations_fetched.csv
#      and scanned_datasets.txt.
#   2. Run the analyzer (Python if available, else the shell version) over
#      views.csv + the fetched authorizations -> report CSVs + a broken-links
#      table.
#
# The YAML config names the CI/CD variables holding each project's WIF provider
# and service account (never the values). A single OIDC id_token, named by
# ID_TOKEN_VAR, is shared by every project - see single-run/ for the same model.
#
# Config (flags override env vars):
#   --config     / TARGETS_FILE   YAML config. Default: view-authorizations/projects.yml
#   --views      / VIEWS_CSV      Views CSV. Default: view-authorizations/views.csv
#   --out-dir    / OUT_DIR        Output directory. Default: current directory
#   --analyzer   / ANALYZER       python | shell | auto (default: auto)
#   --fail-on-broken              Exit non-zero if any authorization is MISSING.
#   --skip-fetch                  Reuse an existing authorizations_fetched.csv
#                                 (skip gcloud/bq entirely; handy for local runs).
#   ID_TOKEN_VAR (env)            Variable holding the OIDC token. Default GCP_ID_TOKEN.
#   RUN_BQ_JOB   (env)            "true" to run the placeholder BQ job step.

set -uo pipefail

CONFIG_FILE="${TARGETS_FILE:-view-authorizations/projects.yml}"
VIEWS_CSV="${VIEWS_CSV:-view-authorizations/views.csv}"
OUT_DIR="${OUT_DIR:-.}"
ANALYZER="${ANALYZER:-auto}"
ID_TOKEN_VAR="${ID_TOKEN_VAR:-GCP_ID_TOKEN}"
RUN_BQ_JOB="${RUN_BQ_JOB:-false}"
FAIL_ON_BROKEN=0
SKIP_FETCH=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { printf '%s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
trim() { local s=${1-}; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }
need_val() { [[ $# -ge 2 ]] || die "Option $1 requires a value"; }
valid_var_name() { [[ "${1-}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; }

gcp_logout() {
  gcloud auth revoke --all --quiet >/dev/null 2>&1 || true
  gcloud config unset project --quiet >/dev/null 2>&1 || true
  [[ -n "$WORKDIR" ]] && rm -f "$WORKDIR"/token_* "$WORKDIR"/cred_* 2>/dev/null || true
}
cleanup() { gcp_logout; [[ -n "$WORKDIR" ]] && rm -rf "$WORKDIR" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# YAML parsing (python3+PyYAML if present, else a small shell parser).
# Emits: project_id<TAB>provider_url_var<TAB>service_account_var per line.
# ---------------------------------------------------------------------------
parse_targets() {
  if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' >/dev/null 2>&1; then
    python3 - "$1" <<'PY'
import sys, yaml
path = sys.argv[1]
try:
    cfg = yaml.safe_load(open(path)) or {}
except Exception as exc:
    sys.stderr.write(f"ERROR: could not parse {path}: {exc}\n"); sys.exit(2)
projects = cfg.get("projects")
if not isinstance(projects, list):
    sys.stderr.write(f"ERROR: {path} must contain a top-level 'projects:' list.\n"); sys.exit(2)
seen = set()
for i, e in enumerate(projects, 1):
    if not isinstance(e, dict):
        sys.stderr.write(f"WARN: entry #{i} is not a mapping; skipping.\n"); continue
    pid = str(e.get("project_id") or "").strip()
    if not pid:
        sys.stderr.write(f"WARN: entry #{i} missing project_id; skipping.\n"); continue
    if pid in seen:
        sys.stderr.write(f"WARN: duplicate project_id '{pid}'; keeping the first.\n"); continue
    seen.add(pid)
    prov = str(e.get("wif_provider_url_var") or "WIF_PROVIDER_URL").strip()
    sa = str(e.get("wif_service_account_var") or "WIF_SERVICE_ACCOUNT").strip()
    sys.stdout.write("\t".join([pid, prov, sa]) + "\n")
PY
  else
    log "NOTE: python3/PyYAML unavailable; using the built-in shell YAML parser."
    parse_targets_sh "$1"
  fi
}

parse_targets_sh() {
  local file="$1" in_projects=0 pid="" prov="" sa="" open=0
  local seen=" " raw line key val
  _emit() {
    [[ "$open" -eq 1 ]] || return 0; open=0
    [[ -z "$pid" ]] && { log "WARN: an entry is missing project_id; skipping."; return 0; }
    case "$seen" in *" $pid "*) log "WARN: duplicate project_id '$pid'; keeping the first."; return 0 ;; esac
    seen="${seen}${pid} "
    printf '%s\t%s\t%s\n' "$pid" "${prov:-WIF_PROVIDER_URL}" "${sa:-WIF_SERVICE_ACCOUNT}"
  }
  _kv() { key="$(trim "${1%%:*}")"; val="$(trim "${1#*:}")"; val="${val%\"}"; val="${val#\"}"; val="${val%\'}"; val="${val#\'}"; }
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw//$'\r'/}"; line="${line%%\#*}"
    [[ -z "$(trim "$line")" ]] && continue
    if [[ "$line" =~ ^projects:[[:space:]]*$ ]]; then in_projects=1; continue; fi
    if [[ "$line" =~ ^[^[:space:]-] ]]; then _emit; in_projects=0; continue; fi
    [[ "$in_projects" -eq 1 ]] || continue
    if [[ "$line" =~ ^[[:space:]]*-[[:space:]]* ]]; then
      _emit; pid=""; prov=""; sa=""; open=1
      local rest; rest="$(trim "${line#*-}")"
      [[ "$rest" == *:* ]] && { _kv "$rest"; case "$key" in project_id) pid="$val";; wif_provider_url_var) prov="$val";; wif_service_account_var) sa="$val";; esac; }
      continue
    fi
    if [[ "$open" -eq 1 && "$line" == *:* ]]; then
      _kv "$line"; case "$key" in project_id) pid="$val";; wif_provider_url_var) prov="$val";; wif_service_account_var) sa="$val";; esac
    fi
  done <"$file"
  _emit
}

# ---------------------------------------------------------------------------
# Step 0: placeholder BigQuery job (disabled). Wired up later with real SQL.
# ---------------------------------------------------------------------------
trigger_bq_job_placeholder() {
  log "[bq-job] PLACEHOLDER step - not executing a real job."
  log "[bq-job] When enabled (RUN_BQ_JOB=true) this will:"
  log "[bq-job]   * auth with placeholder WIF: provider=\$${BQJOB_WIF_PROVIDER_VAR:-BQJOB_WIF_PROVIDER} sa=\$${BQJOB_WIF_SA_VAR:-BQJOB_WIF_SA}"
  log "[bq-job]   * run:  bq query --nouse_legacy_sql --format=csv \"<QUERY TBD>\" > ${VIEWS_CSV}"
  log "[bq-job]   * the query output (project,dataset,view,base_datasets) replaces views.csv"
  # --- Placeholder wiring; intentionally NOT run yet. --------------------------
  # local prov sa
  # prov="$(trim "${!BQJOB_WIF_PROVIDER_VAR-}")"; sa="$(trim "${!BQJOB_WIF_SA_VAR-}")"
  # local tok="${!ID_TOKEN_VAR-}"
  # gcp_login_wif "bq-job" "$prov" "$sa" "$tok" || return 1
  # bq query --project_id="<PROJECT>" --nouse_legacy_sql --format=csv \
  #   "${BQJOB_QUERY:-SELECT 'placeholder' AS project}" > "$VIEWS_CSV"
  # gcp_logout
  return 0
}

# ---------------------------------------------------------------------------
# WIF auth (shared with single-run/ - one token for all projects).
# ---------------------------------------------------------------------------
gcp_login_wif() {
  local project_id="$1" provider="$2" service_account="$3" token="$4"
  local token_file="$WORKDIR/token_${project_id}" cred_file="$WORKDIR/cred_${project_id}"
  (umask 077; printf '%s' "$token" >"$token_file") || { log "  ERROR: could not write token file"; return 1; }
  gcloud iam workload-identity-pools create-cred-config "$provider" \
    --service-account="$service_account" \
    --service-account-token-lifetime-seconds=3600 \
    --output-file="$cred_file" --credential-source-file="$token_file" >/dev/null 2>&1 \
    || { log "  ERROR: create-cred-config failed for ${project_id}"; return 1; }
  gcloud auth login --cred-file="$cred_file" --quiet >/dev/null 2>&1 \
    || { log "  ERROR: gcloud auth login failed for ${project_id}"; return 1; }
  gcloud config set project "$project_id" --quiet >/dev/null 2>&1 \
    || { log "  ERROR: could not set project ${project_id}"; return 1; }
  return 0
}

# ---------------------------------------------------------------------------
# Step 1: fetch authorized views for every dataset in a project via `bq`.
# Appends "project.dataset,view_fqn" rows to $AUTH_OUT and dataset names to
# $SCANNED_OUT. Requires jq.
# ---------------------------------------------------------------------------
fetch_project_authorizations() {
  local project="$1" datasets ds ds_fqn access_json
  # tr strips CR (Windows jq emits CRLF); on Linux CI this is a no-op.
  datasets="$(bq ls --datasets --project_id="$project" --format=json --max_results=100000 2>/dev/null \
              | jq -r '.[]?.datasetReference.datasetId // empty' 2>/dev/null | tr -d '\r')"
  if [[ -z "$datasets" ]]; then
    log "  (no datasets found in ${project}, or bq ls failed)"
    return 0
  fi
  local count=0 auth_count=0
  while IFS= read -r ds; do
    [[ -z "$ds" ]] && continue
    ds_fqn="${project}.${ds}"
    printf '%s\n' "$ds_fqn" >>"$SCANNED_OUT"
    count=$((count + 1))
    # Each access entry of type "view" is an authorized view on this dataset.
    access_json="$(bq show --format=json "${project}:${ds}" 2>/dev/null \
      | jq -r '.access[]? | select(.view) | "\(.view.projectId).\(.view.datasetId).\(.view.tableId)"' 2>/dev/null | tr -d '\r')"
    while IFS= read -r vf; do
      [[ -z "$vf" ]] && continue
      printf '%s,%s\n' "$ds_fqn" "$vf" >>"$AUTH_OUT"
      auth_count=$((auth_count + 1))
    done <<<"$access_json"
  done <<<"$datasets"
  log "  ${project}: ${count} dataset(s), ${auth_count} authorized-view grant(s)"
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)         need_val "$@"; CONFIG_FILE="$2"; shift 2 ;;
    --views)          need_val "$@"; VIEWS_CSV="$2"; shift 2 ;;
    --out-dir)        need_val "$@"; OUT_DIR="$2"; shift 2 ;;
    --analyzer)       need_val "$@"; ANALYZER="$2"; shift 2 ;;
    --fail-on-broken) FAIL_ON_BROKEN=1; shift ;;
    --skip-fetch)     SKIP_FETCH=1; shift ;;
    -h|--help)        grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)                die "Unknown argument: $1" ;;
  esac
done

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  command -v gcloud >/dev/null 2>&1 || [[ "$SKIP_FETCH" -eq 1 ]] || die "gcloud not found on PATH"
  [[ -f "$VIEWS_CSV" ]] || die "views CSV not found: $VIEWS_CSV"
  mkdir -p "$OUT_DIR"

  AUTH_OUT="$OUT_DIR/authorizations_fetched.csv"
  SCANNED_OUT="$OUT_DIR/scanned_datasets.txt"

  # Step 0: placeholder BQ job.
  if [[ "$RUN_BQ_JOB" == "true" ]]; then
    trigger_bq_job_placeholder || log "WARN: placeholder BQ job step reported an issue."
  else
    log "Step 0: BQ job step skipped (RUN_BQ_JOB != true)."
  fi

  # Step 1: fetch authorizations (unless reusing an existing file).
  if [[ "$SKIP_FETCH" -eq 1 ]]; then
    log "Step 1: --skip-fetch set; expecting an existing ${AUTH_OUT}."
    [[ -f "$AUTH_OUT" ]] || die "--skip-fetch set but ${AUTH_OUT} does not exist."
  else
    command -v jq >/dev/null 2>&1 || die "jq not found on PATH (needed to parse bq output)"
    command -v bq >/dev/null 2>&1 || die "bq not found on PATH (part of the Google Cloud SDK)"
    [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

    valid_var_name "$ID_TOKEN_VAR" || die "ID_TOKEN_VAR ('$ID_TOKEN_VAR') is not a valid variable name."
    local token="${!ID_TOKEN_VAR-}"
    [[ -n "$token" ]] || die "id_token variable '$ID_TOKEN_VAR' is empty/unset."

    local targets; targets="$(parse_targets "$CONFIG_FILE" | tr -d '\r')" || die "Could not read $CONFIG_FILE"
    [[ -n "$targets" ]] || die "No usable project entries in $CONFIG_FILE"

    WORKDIR="$(mktemp -d)"; trap cleanup EXIT
    printf 'dataset,authorized_view\n' >"$AUTH_OUT"
    : >"$SCANNED_OUT"

    local project_id provider_var sa_var provider service_account
    while IFS=$'\t' read -r project_id provider_var sa_var; do
      [[ -z "$project_id" ]] && continue
      log "=== ${project_id} ==="
      if ! valid_var_name "$provider_var" || ! valid_var_name "$sa_var"; then
        log "  ERROR: invalid variable name ('${provider_var}' / '${sa_var}'); skipping."; continue
      fi
      provider="$(trim "${!provider_var-}")"; service_account="$(trim "${!sa_var-}")"
      [[ -n "$provider" ]] || { log "  ERROR: WIF provider var '${provider_var}' empty/unset; skipping."; continue; }
      [[ -n "$service_account" ]] || { log "  ERROR: WIF service-account var '${sa_var}' empty/unset; skipping."; continue; }
      if ! gcp_login_wif "$project_id" "$provider" "$service_account" "$token"; then
        gcp_logout; continue
      fi
      fetch_project_authorizations "$project_id"
      gcp_logout
    done <<<"$targets"
    log ""
  fi

  # Step 2: analyze.
  local analyzer="$ANALYZER"
  if [[ "$analyzer" == "auto" ]]; then
    if command -v python3 >/dev/null 2>&1; then analyzer="python"; else analyzer="shell"; fi
  fi
  local -a an_args=(--views "$VIEWS_CSV" --authorizations "$AUTH_OUT" --out-dir "$OUT_DIR")
  [[ -f "$SCANNED_OUT" ]] && an_args+=(--scanned "$SCANNED_OUT")
  [[ "$FAIL_ON_BROKEN" -eq 1 ]] && an_args+=(--fail-on-broken)

  log "Step 2: analyzing with the ${analyzer} analyzer."
  if [[ "$analyzer" == "python" ]]; then
    python3 "$SCRIPT_DIR/analyze_view_auth.py" "${an_args[@]}"
  else
    bash "$SCRIPT_DIR/analyze_view_auth.sh" "${an_args[@]}"
  fi
}

main
