#!/usr/bin/env bash
#
# V2 of run_multi_project_inventory.sh - a drop-in replacement that does NOT
# require Python on the runner. The YAML config is parsed with python3+PyYAML
# when available (full YAML support), and otherwise with a built-in pure-shell
# parser, so the job also works on minimal runner images. Point the CI job's
# `script:` at this file to use it; the original is kept unchanged.
#
# The shell fallback supports the documented projects.yml layout: block-style
# entries, `- {k: v, ...}` one-liners, comments, blank lines, quoted values and
# CRLF files. It is NOT a general YAML parser - anchors, multi-line values and
# nested structures need python3+PyYAML.
#
# Everything else is identical to run_multi_project_inventory.sh:
#
# Loop over every Composer project defined in a YAML config, authenticate to
# each one with Workload Identity Federation (WIF), run the DAG inventory, then
# revoke the credentials before moving to the next project. Rows from every
# project are appended into a single combined CSV (header written once, taken
# from the inventory script's own output so it stays in sync).
#
# The YAML config stores NO WIF values - only the NAMES of the GitLab CI/CD
# variables that hold them (wif_provider_url_var / wif_service_account_var /
# id_token_var). Per project the sequence is:
#   1. Resolve the WIF provider URL, service account and OIDC id_token from the
#      CI/CD variables named by the YAML entry (indirect expansion).
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
#   --config     / TARGETS_FILE       YAML config. Default: single-run/projects.yml
#   --output     / OUTPUT_CSV         Combined CSV. Default: composer_dags_all_projects.csv
#   --inventory  / INVENTORY_SH       Inventory script. Default: composer-dag-inventory/list_composer_dags.sh
#   --project    / PROJECT_ID         Optional. Only inventory this project (it must
#                                     still be listed in the YAML config, since that is
#                                     where its WIF details come from).
#   --composer   / COMPOSER_INSTANCE  Optional. Only inventory this Composer environment.
#                                     Requires --project / PROJECT_ID.
#
# Scope selection:
#   neither set          -> every project in the YAML config, all environments
#   --project only       -> that project, ALL of its Composer environments
#   --project + --composer -> that project, ONLY that Composer environment
#
# Usage:
#   single-run/run_multi_project_inventory_v2.sh
#   single-run/run_multi_project_inventory_v2.sh --project composer-project-a
#   single-run/run_multi_project_inventory_v2.sh --project composer-project-a --composer my-env

set -uo pipefail

CONFIG_FILE="${TARGETS_FILE:-single-run/projects.yml}"
FINAL_CSV="${OUTPUT_CSV:-composer_dags_all_projects.csv}"
INVENTORY_SH="${INVENTORY_SH:-composer-dag-inventory/list_composer_dags.sh}"
# Optional runtime scope filters (set by CI pipeline variables).
PROJECT_FILTER="${PROJECT_ID:-}"
COMPOSER_FILTER="${COMPOSER_INSTANCE:-}"

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

# Strip leading/trailing whitespace (values pasted into the GitLab "Run
# pipeline" form often carry some).
trim() {
  local s=${1-}
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

# All options below take a value; fail clearly if it is missing.
need_val() { [[ $# -ge 2 ]] || die "Option $1 requires a value"; }

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

# ---------------------------------------------------------------------------
# Config parsing
#
# Both parsers emit one TSV line per project:
#   project_id, provider_url_var, service_account_var, id_token_var, location
# The *_var fields are NAMES of CI/CD variables (resolved later by the loop);
# invalid/incomplete entries are reported and skipped.
# ---------------------------------------------------------------------------

# Primary parser: python3 + PyYAML (full YAML support).
parse_targets_py() {
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

rows, bad, seen = [], 0, set()
for idx, entry in enumerate(projects, 1):
    if not isinstance(entry, dict):
        sys.stderr.write(f"WARN: entry #{idx} is not a mapping; skipping.\n")
        bad += 1
        continue
    pid = str(entry.get("project_id") or "").strip()
    prov_var = str(entry.get("wif_provider_url_var") or "WIF_PROVIDER_URL").strip()
    sa_var = str(entry.get("wif_service_account_var") or "WIF_SERVICE_ACCOUNT").strip()
    tok = str(entry.get("id_token_var") or "GCP_ID_TOKEN").strip()
    loc = str(entry.get("location") or "").strip()
    if not pid:
        sys.stderr.write(f"WARN: entry #{idx} missing project_id; skipping.\n")
        bad += 1
        continue
    if entry.get("wif_provider_url") or entry.get("wif_service_account"):
        sys.stderr.write(
            f"WARN: entry #{idx} ({pid}) uses the removed literal wif_provider_url/"
            "wif_service_account fields; use wif_provider_url_var / "
            "wif_service_account_var naming CI/CD variables instead. Skipping.\n"
        )
        bad += 1
        continue
    if pid in seen:
        sys.stderr.write(
            f"WARN: duplicate project_id '{pid}' (entry #{idx}); keeping the first entry.\n"
        )
        bad += 1
        continue
    seen.add(pid)
    rows.append("\t".join([pid, prov_var, sa_var, tok, loc]))

sys.stdout.write("\n".join(rows) + ("\n" if rows else ""))
sys.exit(0)
PY
}

# Fallback parser: pure shell, for runner images without python3/PyYAML.
# Supports the documented projects.yml layout only (see file header). Values
# must not contain '#' (treated as a comment start) or ','/'}' in flow style.
parse_targets_sh() {
  local file="$1"
  local found_projects=0 entry_open=0 idx=0
  local pid="" prov_var="" sa_var="" tok="" loc="" legacy=0 scalar=0
  local seen=" "
  local raw line rest kv key val
  local re_projects='^projects:[[:space:]]*$'
  local re_projects_empty='^projects:[[:space:]]*\[[[:space:]]*\][[:space:]]*$'
  local re_toplevel='^[^[:space:]-]'
  local re_flow='^[[:space:]]*-[[:space:]]*\{(.*)\}[[:space:]]*$'
  local re_dash_bare='^[[:space:]]*-[[:space:]]*$'
  local re_dash_item='^[[:space:]]*-[[:space:]]+(.*)$'

  _pt_set_field() {  # $1=key $2=value
    case "$1" in
      project_id)              pid="$2" ;;
      wif_provider_url_var)    prov_var="$2" ;;
      wif_service_account_var) sa_var="$2" ;;
      id_token_var)            tok="$2" ;;
      location)                loc="$2" ;;
      wif_provider_url|wif_service_account) legacy=1 ;;
      *) : ;;  # ignore unknown keys (parity with the python parser)
    esac
  }

  _pt_reset() { pid=""; prov_var=""; sa_var=""; tok=""; loc=""; legacy=0; scalar=0; }

  _pt_emit() {
    [[ "$entry_open" -eq 1 ]] || return 0
    entry_open=0
    if [[ "$scalar" -eq 1 ]]; then
      log "WARN: entry #${idx} is not a mapping; skipping."
      return 0
    fi
    if [[ -z "$pid" ]]; then
      log "WARN: entry #${idx} missing project_id; skipping."
      return 0
    fi
    if [[ "$legacy" -eq 1 ]]; then
      log "WARN: entry #${idx} (${pid}) uses the removed literal wif_provider_url/wif_service_account fields; use wif_provider_url_var / wif_service_account_var naming CI/CD variables instead. Skipping."
      return 0
    fi
    if [[ "$seen" == *" ${pid} "* ]]; then
      log "WARN: duplicate project_id '${pid}' (entry #${idx}); keeping the first entry."
      return 0
    fi
    seen="${seen}${pid} "
    printf '%s\t%s\t%s\t%s\t%s\n' "$pid" "${prov_var:-WIF_PROVIDER_URL}" \
      "${sa_var:-WIF_SERVICE_ACCOUNT}" "${tok:-GCP_ID_TOKEN}" "$loc"
  }

  _pt_parse_kv() {  # $1="key: value" -> sets key/val (trimmed, unquoted)
    key="$(trim "${1%%:*}")"
    val="$(trim "${1#*:}")"
    if [[ "$val" == \"*\" && ${#val} -ge 2 ]]; then
      val="${val#\"}"; val="${val%\"}"
    elif [[ "$val" == \'*\' && ${#val} -ge 2 ]]; then
      val="${val#\'}"; val="${val%\'}"
    fi
  }

  while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw//$'\r'/}"
    line="${line%%\#*}"                       # strip comments
    [[ -z "$(trim "$line")" ]] && continue    # skip blank lines

    if [[ "$line" =~ $re_projects || "$line" =~ $re_projects_empty ]]; then
      found_projects=1
      continue
    fi
    if [[ "$line" =~ $re_toplevel ]]; then
      # another top-level key ends the projects block
      _pt_emit
      continue
    fi
    [[ "$found_projects" -eq 1 ]] || continue

    if [[ "$line" =~ $re_flow ]]; then
      # flow-style one-liner: - {k: v, k2: v2}
      _pt_emit; _pt_reset; idx=$((idx + 1)); entry_open=1
      rest="${BASH_REMATCH[1]}"
      while [[ -n "$rest" ]]; do
        kv="${rest%%,*}"
        if [[ "$kv" == "$rest" ]]; then rest=""; else rest="${rest#*,}"; fi
        if [[ "$kv" == *:* ]]; then
          _pt_parse_kv "$kv"
          _pt_set_field "$key" "$val"
        fi
      done
      continue
    fi
    if [[ "$line" =~ $re_dash_bare ]]; then
      # bare dash: entry starts, fields on the following lines
      _pt_emit; _pt_reset; idx=$((idx + 1)); entry_open=1
      continue
    fi
    if [[ "$line" =~ $re_dash_item ]]; then
      # dash with content: first field inline, or a scalar list item
      _pt_emit; _pt_reset; idx=$((idx + 1)); entry_open=1
      rest="${BASH_REMATCH[1]}"
      if [[ "$rest" == *:* ]]; then
        _pt_parse_kv "$rest"
        _pt_set_field "$key" "$val"
      else
        scalar=1
      fi
      continue
    fi
    if [[ "$entry_open" -eq 1 && "$line" == *:* ]]; then
      # continuation line "  key: value" of the current entry
      _pt_parse_kv "$line"
      _pt_set_field "$key" "$val"
      continue
    fi
    log "WARN: unsupported line in ${file}: '$(trim "$line")' (shell parser); ignoring."
  done <"$file"
  _pt_emit

  if [[ "$found_projects" -eq 0 ]]; then
    log "ERROR: ${file} must contain a top-level 'projects:' list."
    return 2
  fi
  return 0
}

# Prefer python3 + PyYAML; fall back to the shell parser so the job also runs
# on images without Python.
parse_targets() {
  if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' >/dev/null 2>&1; then
    parse_targets_py "$1"
  else
    log "NOTE: python3/PyYAML not available; using the built-in shell YAML parser."
    parse_targets_sh "$1"
  fi
}

# ---------------------------------------------------------------------------
# WIF auth + CSV append
# ---------------------------------------------------------------------------

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
    --config)    need_val "$@"; CONFIG_FILE="$2"; shift 2 ;;
    --output)    need_val "$@"; FINAL_CSV="$2"; shift 2 ;;
    --inventory) need_val "$@"; INVENTORY_SH="$2"; shift 2 ;;
    --project)   need_val "$@"; PROJECT_FILTER="$2"; shift 2 ;;
    --composer)  need_val "$@"; COMPOSER_FILTER="$2"; shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *)           die "Unknown argument: $1" ;;
  esac
done

# Trim so exact-match filtering works even with stray whitespace from the
# GitLab form; an all-whitespace value degrades to "no filter", same as blank.
PROJECT_FILTER="$(trim "$PROJECT_FILTER")"
COMPOSER_FILTER="$(trim "$COMPOSER_FILTER")"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  command -v gcloud >/dev/null 2>&1 || die "gcloud not found on PATH"
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: ${CONFIG_FILE}"
  [[ -x "$INVENTORY_SH" || -f "$INVENTORY_SH" ]] || die "Inventory script not found: ${INVENTORY_SH}"

  # A Composer instance only makes sense within a single project.
  if [[ -n "$COMPOSER_FILTER" && -z "$PROJECT_FILTER" ]]; then
    die "COMPOSER_INSTANCE/--composer requires PROJECT_ID/--project to be set too."
  fi

  local targets
  # tr guards against CRLF from a Windows python3 tainting the last TSV field
  # (pipefail makes a parse_targets failure still fail the pipeline).
  targets="$(parse_targets "$CONFIG_FILE" | tr -d '\r')" || die "Could not read ${CONFIG_FILE}"
  [[ -n "$targets" ]] || die "No usable project entries in ${CONFIG_FILE}"

  # Narrow to a single project when requested. Its WIF details still come from
  # the YAML config, so the project must be listed there.
  if [[ -n "$PROJECT_FILTER" ]]; then
    local filtered
    filtered="$(awk -F'\t' -v p="$PROJECT_FILTER" '$1==p' <<<"$targets")"
    if [[ -z "$filtered" ]]; then
      log "ERROR: project '${PROJECT_FILTER}' is not defined in ${CONFIG_FILE}."
      log "       Known projects: $(cut -f1 <<<"$targets" | paste -sd, -)"
      die "Add it to ${CONFIG_FILE} (with its WIF details) and retry."
    fi
    targets="$filtered"
  fi

  WORKDIR="$(mktemp -d)"
  trap cleanup EXIT

  log "Config   : ${CONFIG_FILE}"
  log "Inventory: ${INVENTORY_SH}"
  log "Output   : ${FINAL_CSV}"
  if [[ -n "$PROJECT_FILTER" ]]; then
    log "Scope    : project ${PROJECT_FILTER}${COMPOSER_FILTER:+, Composer instance ${COMPOSER_FILTER}}"
  else
    log "Scope    : all projects in ${CONFIG_FILE}"
  fi
  log ""

  local project_id provider_var sa_var token_var location tmp_csv
  local provider service_account token
  while IFS=$'\t' read -r project_id provider_var sa_var token_var location; do
    [[ -z "$project_id" ]] && continue
    log "=== ${project_id} ==="

    # Resolve WIF details + OIDC token from the CI/CD variables named by the
    # YAML entry (indirect expansion). Values live only in GitLab, not in git.
    provider="$(trim "${!provider_var-}")"
    service_account="$(trim "${!sa_var-}")"
    token="${!token_var-}"
    if [[ -z "$provider" ]]; then
      log "  ERROR: WIF provider variable '${provider_var}' is empty/unset."
      log "         Define it in Settings > CI/CD > Variables (value: projects/<NUM>/locations/global/workloadIdentityPools/<POOL>/providers/<PROVIDER>)."
      FAIL_COUNT=$((FAIL_COUNT + 1))
      continue
    fi
    if [[ -z "$service_account" ]]; then
      log "  ERROR: WIF service-account variable '${sa_var}' is empty/unset."
      log "         Define it in Settings > CI/CD > Variables (value: the service account to impersonate)."
      FAIL_COUNT=$((FAIL_COUNT + 1))
      continue
    fi
    if [[ -z "$token" ]]; then
      log "  ERROR: id_token variable '${token_var}' is empty/unset."
      log "         Declare it under 'id_tokens:' in .gitlab-ci.yml with an aud matching ${provider}."
      FAIL_COUNT=$((FAIL_COUNT + 1))
      continue
    fi
    log "  WIF: ${service_account} via ${provider_var}"

    if ! gcp_login_wif "$project_id" "$provider" "$service_account" "$token"; then
      FAIL_COUNT=$((FAIL_COUNT + 1))
      gcp_logout
      continue
    fi
    log "  authenticated via WIF"

    tmp_csv="$WORKDIR/out_${project_id}.csv"
    local -a inv_args=("$project_id" --output "$tmp_csv")
    [[ -n "$location" ]] && inv_args+=(--locations "$location")
    [[ -n "$COMPOSER_FILTER" ]] && inv_args+=(--environment "$COMPOSER_FILTER")

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
