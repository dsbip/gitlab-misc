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

DEFAULT_LOCATION="europe-west2"

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*" >&2; }

die() { log "ERROR: $*"; exit 1; }

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
# line or two of preamble). Echoes clean JSON, or nothing if unparseable.
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
# Argument / environment parsing
# ---------------------------------------------------------------------------

PROJECTS_ARG=""
LOCATIONS_ARG=""
OUTPUT_CSV="${OUTPUT_CSV:-composer_dags.csv}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --projects)  PROJECTS_ARG="$2"; shift 2 ;;
    --locations) LOCATIONS_ARG="$2"; shift 2 ;;
    --output)    OUTPUT_CSV="$2"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    -*) die "Unknown option: $1" ;;
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

command -v gcloud >/dev/null 2>&1 || die "gcloud not found on PATH"
command -v jq >/dev/null 2>&1 || die "jq not found on PATH"

# ---------------------------------------------------------------------------
# Resolve projects and locations
# ---------------------------------------------------------------------------

resolve_projects() {
  local raw="${PROJECTS_ARG:-${GCP_PROJECTS:-}}"
  if [[ -n "$raw" ]]; then
    printf '%s\n' "$raw" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$'
    return
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
    printf '%s\n' "$raw" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$'
    return
  fi
  printf '%s\n' "$DEFAULT_LOCATION"
}

# ---------------------------------------------------------------------------
# Fetch a single DAG's schedule via `airflow dags details`.
# Echoes: the schedule string, "" for unscheduled, or "__ERR__" if unavailable.
# ---------------------------------------------------------------------------

dag_schedule() {
  local project="$1" location="$2" env_name="$3" dag_id="$4"
  local details
  if ! details="$(gcloud composer environments run "$env_name" \
        --project="$project" --location="$location" \
        dags details -- "$dag_id" -o json 2>/dev/null | extract_json)"; then
    printf '__ERR__'; return 0
  fi
  printf '%s' "$details" | jq -r "$SCHED_JQ"
}

# ---------------------------------------------------------------------------
# Per-environment inventory
# ---------------------------------------------------------------------------

inventory_environment() {
  local project="$1" location="$2" env_name="$3"
  log "Environment ${project}/${env_name} @ ${location}"

  # List DAGs via the Airflow CLI (Composer 2 runs this through the API).
  local dags_json
  if ! dags_json="$(gcloud composer environments run "$env_name" \
        --project="$project" --location="$location" \
        dags list -- -o json 2>/dev/null | extract_json)"; then
    log "  WARN: no parseable DAG list for ${env_name}; skipping"
    return 0
  fi

  local count=0
  while IFS= read -r obj; do
    [[ -z "$obj" ]] && continue
    local dag_id fpath paused active scheduled sched_time sched_out
    dag_id="$(jq -r '.dag_id // empty' <<<"$obj")"
    [[ -z "$dag_id" ]] && continue
    fpath="$(jq -r '.fileloc // .filepath // empty' <<<"$obj")"
    paused="$(jq -r '(.paused // .is_paused) | tostring' <<<"$obj")"

    # Active? = not paused. Airflow emits "True"/"False" or true/false.
    case "$paused" in
      True|true|1) active="No" ;;
      False|false|0) active="Yes" ;;
      *) active="Unknown" ;;
    esac

    # Schedule via `dags details`.
    sched_out="$(dag_schedule "$project" "$location" "$env_name" "$dag_id")"
    if [[ "$sched_out" == "__ERR__" ]]; then
      scheduled="Unknown"; sched_time=""
    elif [[ -z "$sched_out" ]]; then
      scheduled="No"; sched_time=""
    else
      scheduled="Yes"; sched_time="$sched_out"
    fi

    write_row "$env_name" "$project" "$dag_id" "$fpath" \
      "$active" "$scheduled" "$sched_time"
    count=$((count + 1))
  done < <(printf '%s' "$dags_json" | jq -c '.[]')

  log "  ${count} DAG(s) written"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  mapfile -t PROJECTS < <(resolve_projects)
  mapfile -t LOCATIONS < <(resolve_locations)

  if [[ ${#PROJECTS[@]} -eq 0 ]]; then
    die "No project specified. Pass a project id, --projects / GCP_PROJECTS, or set a gcloud default project."
  fi

  log "Projects : ${PROJECTS[*]}"
  log "Locations: ${LOCATIONS[*]}"
  log "Output   : ${OUTPUT_CSV}"

  # CSV header.
  printf 'Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time\n' >"$OUTPUT_CSV"

  local total_envs=0
  for project in "${PROJECTS[@]}"; do
    for location in "${LOCATIONS[@]}"; do
      # List RUNNING environments in this project+location.
      local listing
      listing="$(gcloud composer environments list \
        --project="$project" --locations="$location" \
        --format='csv[no-heading](name.basename(),state)' 2>/dev/null)"
      [[ -z "$listing" ]] && continue
      while IFS=, read -r env_name state; do
        [[ -z "$env_name" ]] && continue
        if [[ "$state" != "RUNNING" ]]; then
          log "Environment ${project}/${env_name} @ ${location} state=${state}; skipping"
          continue
        fi
        total_envs=$((total_envs + 1))
        inventory_environment "$project" "$location" "$env_name"
      done <<<"$listing"
    done
  done

  log ""
  log "Done: scanned ${total_envs} running environment(s)."
  log "CSV written to ${OUTPUT_CSV}"
}

main "$@"
