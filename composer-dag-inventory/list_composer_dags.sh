#!/usr/bin/env bash
#
# Inventory Cloud Composer (Airflow) DAGs across a Google Cloud project using
# only gcloud + jq. It never reads DAG files from GCS — every detail comes from
# the Airflow CLI, invoked through `gcloud composer environments run` (Composer 2
# runs this through the API, so no kubectl is required).
#
# For every RUNNING Composer environment it emits one CSV row per DAG:
#   Composer, Project, DAG_Name, Dag_path, Active?, Scheduled, Scheduled Time
#
# Data sources (all via gcloud composer environments run):
#   * DAG_Name / Dag_path / Active?  -> `airflow dags list -o json`   (1 call/env)
#   * Scheduled / Scheduled Time      -> `airflow dags details <id> -o json`
#                                        (1 call per DAG).
#
# NOTE: because schedule details are fetched per DAG, an environment with many
# DAGs means many `environments run` calls. Scope with --locations / a single
# project to keep runs quick.
#
# Auth: relies on the active gcloud account (in CI: a service-account key
# activated via `gcloud auth activate-service-account`). The identity needs:
#   - roles/composer.user   (list/describe/run)
#
# Config (flags override env vars):
#   <project> (positional) / GCP_PROJECTS   Project id(s) to scan. Default: active gcloud project.
#   --locations / COMPOSER_LOCATIONS        Comma-separated regions. Default: europe-west2.
#   --output    / OUTPUT_CSV                 Output path. Default: composer_dags.csv
#
# Usage:
#   ./list_composer_dags.sh my-project-id
#   ./list_composer_dags.sh --projects proj-a,proj-b --locations europe-west2 --output out.csv

set -uo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LOCATION="europe-west2"
CSV_HEADER="Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time"

# jq program that turns an Airflow `dags details` object (or the same shape from
# `dags list`) into a schedule display string. Empty output => not scheduled.
SCHED_JQ='
def clean($s): ($s|tostring) as $t | ($t|ascii_downcase) as $l
  | if ($l=="" or $l=="none" or $l=="null" or $l=="manual"
        or $l=="never, external triggers only") then "" else $t end;
(if type=="array" then (.[0] // {}) else . end) as $d
| clean($d.timetable_summary // "") as $ts
| if $ts != "" then $ts
  else
    ($d.schedule_interval) as $si
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
  end'

# Globals populated by parse_args().
PROJECTS_ARG=""
LOCATIONS_ARG=""
OUTPUT_CSV=""

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

# CSV-escape a single field: wrap in double quotes, doubling any inner quotes.
csv_escape() {
  local s=${1-}
  s=${s//\"/\"\"}
  printf '"%s"' "$s"
}

# Emit one CSV row (7 columns) with proper escaping.
write_row() {
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "${1-}")" "$(csv_escape "${2-}")" "$(csv_escape "${3-}")" \
    "$(csv_escape "${4-}")" "$(csv_escape "${5-}")" "$(csv_escape "${6-}")" \
    "$(csv_escape "${7-}")" >>"$OUTPUT_CSV"
}

# Pull the JSON object/array out of `environments run` output (which may carry a
# line or two of preamble). Echoes clean JSON; returns non-zero if unparseable.
extract_json() {
  local raw json
  raw="$(cat | tr -d '\r')"
  json="$(printf '%s\n' "$raw" | sed -n '/^[[{]/,/^[]}]/p')"
  # Note: `jq empty` returns 0 on EMPTY input, so require non-empty explicitly.
  if [[ -n "$json" ]] && printf '%s' "$json" | jq empty >/dev/null 2>&1; then
    printf '%s' "$json"; return 0
  fi
  if [[ -n "$raw" ]] && printf '%s' "$raw" | jq empty >/dev/null 2>&1; then
    printf '%s' "$raw"; return 0
  fi
  return 1
}

# ---------------------------------------------------------------------------
# gcloud / Airflow wrappers
# ---------------------------------------------------------------------------

# Run an Airflow CLI subcommand in an environment and emit clean JSON.
#   airflow_run PROJECT LOCATION ENV <airflow subcommand + args...>
# Returns non-zero if the output isn't parseable JSON.
airflow_run() {
  local project="$1" location="$2" env_name="$3"; shift 3
  gcloud composer environments run "$env_name" \
    --project="$project" --location="$location" \
    "$@" 2>/dev/null | extract_json
}

# Echo "env_id,state" lines for every environment in a project+location.
list_environments() {
  gcloud composer environments list \
    --project="$1" --locations="$2" \
    --format='csv[no-heading](name.basename(),state)' 2>/dev/null
}

# Fetch a single DAG's schedule via `airflow dags details`.
# Echoes: the schedule string, "" for unscheduled, or "__ERR__" if unavailable.
dag_schedule() {
  local project="$1" location="$2" env_name="$3" dag_id="$4" details
  if ! details="$(airflow_run "$project" "$location" "$env_name" \
        dags details -- "$dag_id" -o json)"; then
    printf '__ERR__'; return 0
  fi
  printf '%s' "$details" | jq -r "$SCHED_JQ"
}

# ---------------------------------------------------------------------------
# Field mapping
# ---------------------------------------------------------------------------

# Map Airflow's paused flag ("True"/"False"/true/false) to the Active? column.
active_from_paused() {
  case "$1" in
    True|true|1)   printf 'No' ;;   # paused -> not active
    False|false|0) printf 'Yes' ;;
    *)             printf 'Unknown' ;;
  esac
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

# Turn one DAG's `dags list` object into a CSV row. Returns 1 if it has no id.
inventory_dag() {
  local project="$1" location="$2" env_name="$3" obj="$4"
  local dag_id fpath paused active sched_out scheduled sched_time

  dag_id="$(jq -r '.dag_id // empty' <<<"$obj")"
  [[ -z "$dag_id" ]] && return 1
  fpath="$(jq -r '.fileloc // .filepath // empty' <<<"$obj")"
  paused="$(jq -r '(.paused // .is_paused) | tostring' <<<"$obj")"
  active="$(active_from_paused "$paused")"

  sched_out="$(dag_schedule "$project" "$location" "$env_name" "$dag_id")"
  case "$sched_out" in
    __ERR__) scheduled="Unknown"; sched_time="" ;;
    "")      scheduled="No";      sched_time="" ;;
    *)       scheduled="Yes";     sched_time="$sched_out" ;;
  esac

  write_row "$env_name" "$project" "$dag_id" "$fpath" \
    "$active" "$scheduled" "$sched_time"
}

inventory_environment() {
  local project="$1" location="$2" env_name="$3"
  log "Environment ${project}/${env_name} @ ${location}"

  local dags_json
  if ! dags_json="$(airflow_run "$project" "$location" "$env_name" \
        dags list -- -o json)"; then
    log "  WARN: no parseable DAG list for ${env_name}; skipping"
    return 0
  fi

  local count=0 obj
  while IFS= read -r obj; do
    [[ -z "$obj" ]] && continue
    if inventory_dag "$project" "$location" "$env_name" "$obj"; then
      count=$((count + 1))
    fi
  done < <(printf '%s' "$dags_json" | jq -c '.[]')

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

  log "Projects : ${PROJECTS[*]}"
  log "Locations: ${LOCATIONS[*]}"
  log "Output   : ${OUTPUT_CSV}"

  printf '%s\n' "$CSV_HEADER" >"$OUTPUT_CSV"

  local total_envs=0 project location env_name state
  for project in "${PROJECTS[@]}"; do
    for location in "${LOCATIONS[@]}"; do
      while IFS=, read -r env_name state; do
        [[ -z "$env_name" ]] && continue
        if [[ "$state" != "RUNNING" ]]; then
          log "Environment ${project}/${env_name} @ ${location} state=${state}; skipping"
          continue
        fi
        total_envs=$((total_envs + 1))
        inventory_environment "$project" "$location" "$env_name"
      done < <(list_environments "$project" "$location")
    done
  done

  log ""
  log "Done: scanned ${total_envs} running environment(s)."
  log "CSV written to ${OUTPUT_CSV}"
}

main "$@"
