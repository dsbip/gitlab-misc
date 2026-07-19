#!/usr/bin/env bash
#
# Inventory Cloud Composer (Airflow) DAGs across a Google Cloud project.
#
# It queries the Composer 2 Airflow **stable REST API** directly:
#   * gcloud is used only to discover environments and to mint an access token
#     (`gcloud auth print-access-token`).
#   * curl fetches `/api/v1/dags` (paginated) — ONE bulk call per environment,
#     so it does not slow down as the number of DAGs grows. It never reads DAG
#     files from GCS and makes no per-DAG calls.
#
# For every RUNNING Composer environment it emits one CSV row per DAG:
#   Composer, Project, DAG_Name, Dag_path, Active?, Scheduled, Scheduled Time
#
# Auth: relies on the active gcloud account (in CI: a service-account key
# activated via `gcloud auth activate-service-account`). The identity needs:
#   - roles/composer.user   (list environments + call the Airflow REST API)
#
# Config (flags override env vars):
#   <project> (positional) / GCP_PROJECTS   Project id(s) to scan. Default: active gcloud project.
#   --locations / COMPOSER_LOCATIONS        Comma-separated regions. Default: europe-west2.
#   --output    / OUTPUT_CSV                 Output path. Default: composer_dags.csv
#   --environment (alias --composer)         Only inventory this one Composer
#                                            environment. Default: all RUNNING ones.
#
# Usage:
#   ./list_composer_dags.sh my-project-id
#   ./list_composer_dags.sh my-project-id --environment my-composer-env
#   ./list_composer_dags.sh --projects proj-a,proj-b --locations europe-west2 --output out.csv

set -uo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LOCATION="europe-west2"
CSV_HEADER="Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time"
PAGE_LIMIT=100

# jq program: for each DAG in an Airflow REST `/api/v1/dags` page, emit one
# CSV row (Composer, Project, DAG_Name, Dag_path, Active?, Scheduled, Sched Time).
# Expects --arg composer and --arg project.
ROWS_JQ='
def clean($s): ($s|tostring) as $t | ($t|ascii_downcase) as $l
  | if ($l=="" or $l=="none" or $l=="null" or $l=="manual"
        or $l=="never, external triggers only") then "" else $t end;
def sched:
  clean(.timetable_summary // "") as $ts
  | if $ts != "" then $ts
    else
      (.schedule_interval) as $si
      | if $si == null then ""
        elif ($si|type)=="string" then clean($si)
        elif ($si|type)=="object" then
          ( ($si.__type // "") as $tp
            | if $tp=="CronExpression" then (($si.value // "")|tostring)
              elif $tp=="TimeDelta" then
                ([ (if (($si.days)//0)!=0 then "\($si.days)d" else empty end),
                   (if (($si.seconds)//0)!=0 then "\($si.seconds)s" else empty end),
                   (if (($si.microseconds)//0)!=0 then "\($si.microseconds)m" else empty end)
                 ] | join(" ")) as $td
                | (if $td=="" then "TimeDelta" else "every \($td)" end)
              else (($si.value // "")|tostring) end )
        else "" end
    end;
(.dags // [])[]
| (sched) as $sc
| [ $composer, $project, (.dag_id // ""), (.fileloc // .filepath // ""),
    (if .is_paused then "No" else "Yes" end),
    (if $sc == "" then "No" else "Yes" end),
    $sc
  ] | @csv'

# Globals populated at runtime.
PROJECTS_ARG=""
LOCATIONS_ARG=""
OUTPUT_CSV=""
ENV_FILTER=""
TOKEN=""
TMP_BODY=""

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*" >&2; }

die() { log "ERROR: $*"; exit 1; }

usage() {
  # Print the leading comment block as help text (minus the shebang line).
  grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'
}

# Split a comma-separated string into trimmed, non-empty lines.
split_csv() {
  printf '%s\n' "$1" \
    | tr ',' '\n' \
    | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
    | grep -v '^$'
}

# ---------------------------------------------------------------------------
# Auth + Airflow REST API
# ---------------------------------------------------------------------------

refresh_token() {
  TOKEN="$(gcloud auth print-access-token 2>/dev/null)"
  [[ -n "$TOKEN" ]]
}

# GET a URL from the Airflow REST API using the cached token (refreshing once on
# a 401). Echoes the response body; returns non-zero on any non-200 status.
api_get() {
  local url="$1" http
  http="$(curl -sS -o "$TMP_BODY" -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" "$url" 2>/dev/null)"
  if [[ "$http" == "401" ]] && refresh_token; then
    http="$(curl -sS -o "$TMP_BODY" -w '%{http_code}' \
      -H "Authorization: Bearer $TOKEN" "$url" 2>/dev/null)"
  fi
  if [[ "$http" != "200" ]]; then
    log "  WARN: GET ${url} -> HTTP ${http:-000}"
    return 1
  fi
  cat "$TMP_BODY"
}

# Echo "env_id<TAB>state<TAB>airflowUri" for every environment in project+location.
list_environments() {
  gcloud composer environments list \
    --project="$1" --locations="$2" --format=json 2>/dev/null \
    | jq -r '.[]? | [(.name|split("/")|last), (.state // ""), (.config.airflowUri // "")] | @tsv'
}

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

parse_args() {
  OUTPUT_CSV="${OUTPUT_CSV:-composer_dags.csv}"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --projects)  PROJECTS_ARG="$2"; shift 2 ;;
      --locations) LOCATIONS_ARG="$2"; shift 2 ;;
      --output)    OUTPUT_CSV="$2"; shift 2 ;;
      --environment|--composer)
                   ENV_FILTER="$2"; shift 2 ;;
      -h|--help)   usage; exit 0 ;;
      -*)          die "Unknown option: $1" ;;
      *)
        # First bare argument is the project id.
        if [[ -z "$PROJECTS_ARG" ]]; then
          PROJECTS_ARG="$1"; shift
        else
          die "Unexpected argument: $1"
        fi
        ;;
    esac
  done
}

require_tools() {
  command -v gcloud >/dev/null 2>&1 || die "gcloud not found on PATH"
  command -v jq >/dev/null 2>&1 || die "jq not found on PATH"
  command -v curl >/dev/null 2>&1 || die "curl not found on PATH"
}

resolve_projects() {
  local raw="${PROJECTS_ARG:-${GCP_PROJECTS:-}}"
  if [[ -n "$raw" ]]; then
    split_csv "$raw"; return
  fi
  local default
  default="$(gcloud config get-value project 2>/dev/null)"
  # Print the default project if one is configured; main() errors if none.
  if [[ -n "$default" && "$default" != "(unset)" ]]; then
    printf '%s\n' "$default"
  fi
}

resolve_locations() {
  local raw="${LOCATIONS_ARG:-${COMPOSER_LOCATIONS:-}}"
  if [[ -n "$raw" ]]; then
    split_csv "$raw"
  else
    printf '%s\n' "$DEFAULT_LOCATION"
  fi
}

# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

# Fetch every DAG for one environment (paginated) and append CSV rows.
inventory_environment() {
  local project="$1" env_name="$2" base="${3%/}"
  log "Environment ${project}/${env_name}"

  local offset=0 count=0 page rows page_meta dags_n total
  while :; do
    if ! page="$(api_get "${base}/api/v1/dags?limit=${PAGE_LIMIT}&offset=${offset}")"; then
      log "  WARN: could not list DAGs for ${env_name}; skipping"
      return 0
    fi
    if ! printf '%s' "$page" | jq empty >/dev/null 2>&1; then
      log "  WARN: unparseable DAG response for ${env_name}; skipping"
      return 0
    fi

    rows="$(printf '%s' "$page" | jq -r --arg composer "$env_name" --arg project "$project" "$ROWS_JQ")"
    [[ -n "$rows" ]] && printf '%s\n' "$rows" >>"$OUTPUT_CSV"

    # DAG count + total in one jq call (tr strips the CR that Windows jq adds).
    page_meta="$(printf '%s' "$page" | jq -r '"\((.dags // [])|length)\t\(.total_entries // 0)"' | tr -d '\r')"
    IFS=$'\t' read -r dags_n total <<<"$page_meta"
    count=$((count + dags_n))
    offset=$((offset + PAGE_LIMIT))
    { [[ "$dags_n" -eq 0 ]] || [[ "$offset" -ge "$total" ]]; } && break
  done

  log "  ${count} DAG(s) written"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  parse_args "$@"
  require_tools

  local -a PROJECTS LOCATIONS
  mapfile -t PROJECTS < <(resolve_projects)
  mapfile -t LOCATIONS < <(resolve_locations)

  if [[ ${#PROJECTS[@]} -eq 0 ]]; then
    die "No project specified. Pass a project id, --projects / GCP_PROJECTS, or set a gcloud default project."
  fi

  refresh_token || die "Could not obtain an access token via 'gcloud auth print-access-token'. Authenticate first (e.g. gcloud auth activate-service-account / gcloud auth login)."

  TMP_BODY="$(mktemp)"
  trap 'rm -f "$TMP_BODY"' EXIT

  log "Projects : ${PROJECTS[*]}"
  log "Locations: ${LOCATIONS[*]}"
  [[ -n "$ENV_FILTER" ]] && log "Composer : ${ENV_FILTER} (only)"
  log "Output   : ${OUTPUT_CSV}"

  printf '%s\n' "$CSV_HEADER" >"$OUTPUT_CSV"

  local total_envs=0 project location env_name state airflow_uri
  for project in "${PROJECTS[@]}"; do
    for location in "${LOCATIONS[@]}"; do
      while IFS=$'\t' read -r env_name state airflow_uri; do
        [[ -z "$env_name" ]] && continue
        # Restrict to a single Composer environment when --environment is given.
        if [[ -n "$ENV_FILTER" && "$env_name" != "$ENV_FILTER" ]]; then
          continue
        fi
        if [[ "$state" != "RUNNING" ]]; then
          log "Environment ${project}/${env_name} @ ${location} state=${state}; skipping"
          continue
        fi
        # Fall back to a describe if the list payload didn't carry the URI.
        if [[ -z "$airflow_uri" ]]; then
          airflow_uri="$(gcloud composer environments describe "$env_name" \
            --project="$project" --location="$location" \
            --format='value(config.airflowUri)' 2>/dev/null)"
        fi
        if [[ -z "$airflow_uri" ]]; then
          log "Environment ${project}/${env_name}: no airflowUri; skipping"
          continue
        fi
        total_envs=$((total_envs + 1))
        inventory_environment "$project" "$env_name" "$airflow_uri"
      done < <(list_environments "$project" "$location" | tr -d '\r')
    done
  done

  log ""
  if [[ -n "$ENV_FILTER" && "$total_envs" -eq 0 ]]; then
    log "WARN: no RUNNING environment named '${ENV_FILTER}' found in ${PROJECTS[*]} @ ${LOCATIONS[*]}."
  fi
  log "Done: scanned ${total_envs} running environment(s)."
  log "CSV written to ${OUTPUT_CSV}"
}

main "$@"
