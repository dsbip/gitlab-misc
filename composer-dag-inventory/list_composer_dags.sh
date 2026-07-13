#!/usr/bin/env bash
#
# Inventory Cloud Composer (Airflow) DAGs across a Google Cloud project using
# only gcloud / gcloud storage / jq — no Python REST client.
#
# For every RUNNING Composer environment it emits one CSV row per DAG:
#   Composer, Project, DAG_Name, Dag_path, Active?, Scheduled, Scheduled Time, Email Alert
#
# Data sources:
#   * DAG_Name / Dag_path / Active?  -> `airflow dags list -o json`, invoked via
#     `gcloud composer environments run` (Composer 2, no kubectl required).
#   * Scheduled / Scheduled Time / Email Alert -> parsed from the DAG *source*,
#     downloaded once per environment from its GCS dags bucket. These are
#     best-effort heuristics; where the source can't be read they read "Unknown".
#
# Auth: relies on the active gcloud account (in CI: a service-account key
# activated via `gcloud auth activate-service-account`). The identity needs
#   - roles/composer.user                                   (list/describe/run)
#   - roles/composer.environmentAndStorageObjectViewer      (read DAG source)
#
# Config (flags override env vars):
#   --projects   / GCP_PROJECTS         Comma-separated project IDs. Default: active gcloud project.
#   --locations  / COMPOSER_LOCATIONS   Comma-separated regions. Default: all compute regions.
#   --output     / OUTPUT_CSV           Output path. Default: composer_dags.csv
#
# Usage:
#   ./list_composer_dags.sh --projects my-proj --locations us-central1 --output out.csv

set -uo pipefail

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

# Emit one CSV row (8 columns) with proper escaping.
write_row() {
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "${1-}")" "$(csv_escape "${2-}")" "$(csv_escape "${3-}")" \
    "$(csv_escape "${4-}")" "$(csv_escape "${5-}")" "$(csv_escape "${6-}")" \
    "$(csv_escape "${7-}")" "$(csv_escape "${8-}")" >>"$OUTPUT_CSV"
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
    *) die "Unknown argument: $1" ;;
  esac
done

command -v gcloud >/dev/null 2>&1 || die "gcloud not found on PATH"
command -v jq >/dev/null 2>&1 || die "jq not found on PATH"

# Prefer 'gcloud storage'; fall back to gsutil for DAG source download.
STORAGE_CP=""
if gcloud storage --help >/dev/null 2>&1; then
  STORAGE_CP="gcloud storage cp"
elif command -v gsutil >/dev/null 2>&1; then
  STORAGE_CP="gsutil -m cp"
else
  log "WARN: neither 'gcloud storage' nor 'gsutil' available; Scheduled/Email columns will be Unknown."
fi

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
  [[ -n "$default" && "$default" != "(unset)" ]] || die "No project set. Use --projects / GCP_PROJECTS or set a gcloud default project."
  printf '%s\n' "$default"
}

resolve_locations() {
  local raw="${LOCATIONS_ARG:-${COMPOSER_LOCATIONS:-}}"
  if [[ -n "$raw" ]]; then
    printf '%s\n' "$raw" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$'
    return
  fi
  local regions
  regions="$(gcloud compute regions list --format='value(name)' 2>/dev/null)"
  if [[ -n "$regions" ]]; then
    printf '%s\n' "$regions"
    return
  fi
  log "WARN: could not list compute regions; using a static fallback list."
  cat <<'EOF'
us-central1
us-east1
us-east4
us-west1
us-west2
us-west3
us-west4
northamerica-northeast1
southamerica-east1
europe-west1
europe-west2
europe-west3
europe-west4
europe-west6
europe-north1
asia-east1
asia-east2
asia-northeast1
asia-northeast2
asia-northeast3
asia-south1
asia-southeast1
asia-southeast2
australia-southeast1
EOF
}

# ---------------------------------------------------------------------------
# Source-parsing heuristics (schedule + email) for one local DAG file
# ---------------------------------------------------------------------------

# Echo the raw schedule token found in a DAG file, or nothing.
extract_schedule() {
  local f="$1" line
  # Matches: schedule="..."  schedule_interval='...'  schedule=@daily
  #          schedule_interval=None   schedule=timedelta(days=1)
  line="$(grep -hoE "schedule(_interval)?[[:space:]]*=[[:space:]]*(None|['\"][^'\"]*['\"]|@[A-Za-z_]+|(timedelta|relativedelta)\([^)]*\))" "$f" 2>/dev/null | head -n1)"
  [[ -z "$line" ]] && return 0
  # Strip the "schedule... =" prefix and any surrounding quotes.
  local val
  val="$(printf '%s' "$line" | sed -E "s/^schedule(_interval)?[[:space:]]*=[[:space:]]*//")"
  val="${val%\'}"; val="${val#\'}"
  val="${val%\"}"; val="${val#\"}"
  printf '%s' "$val"
}

# Echo email recipients (semicolon-joined) from a DAG file, or nothing.
extract_emails() {
  local f="$1"
  grep -iE "email" "$f" 2>/dev/null \
    | grep -oE "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}" \
    | sort -u | paste -sd';' -
}

# ---------------------------------------------------------------------------
# Per-environment inventory
# ---------------------------------------------------------------------------

inventory_environment() {
  local project="$1" location="$2" env_name="$3"
  log "Environment ${project}/${env_name} @ ${location}"

  # DAG source download (best effort) for schedule/email parsing.
  local dag_local_root="" tmpdir="" gcs_prefix=""
  gcs_prefix="$(gcloud composer environments describe "$env_name" \
    --project="$project" --location="$location" \
    --format='value(config.dagGcsPrefix)' 2>/dev/null)"
  if [[ -n "$gcs_prefix" && -n "$STORAGE_CP" ]]; then
    tmpdir="$(mktemp -d)"
    if $STORAGE_CP -r "${gcs_prefix%/}" "$tmpdir" >/dev/null 2>&1; then
      # gs://bucket/dags -> $tmpdir/dags
      dag_local_root="$tmpdir/$(basename "$gcs_prefix")"
    else
      log "  WARN: could not download DAG source from ${gcs_prefix}"
    fi
  fi

  # List DAGs via the Airflow CLI (Composer 2 runs this through the API).
  local raw dags_json
  raw="$(gcloud composer environments run "$env_name" \
    --project="$project" --location="$location" \
    dags list -- -o json 2>/dev/null)"
  # Extract just the JSON array (airflow pretty-prints '[' and ']' at column 0).
  dags_json="$(printf '%s\n' "$raw" | sed -n '/^\[/,/^\]/p')"
  if [[ -z "$dags_json" ]] || ! printf '%s' "$dags_json" | jq empty >/dev/null 2>&1; then
    log "  WARN: no parseable DAG list for ${env_name}; skipping"
    [[ -n "$tmpdir" ]] && rm -rf "$tmpdir"
    return 0
  fi

  local count=0
  while IFS= read -r obj; do
    [[ -z "$obj" ]] && continue
    local dag_id fpath paused active scheduled sched_time email_alert
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

    # Defaults when source is unavailable.
    scheduled="Unknown"; sched_time=""; email_alert="Unknown"

    if [[ -n "$dag_local_root" && -n "$fpath" ]]; then
      # Map airflow fileloc -> local downloaded copy.
      local rel local_file
      rel="${fpath#/home/airflow/gcs/dags/}"
      rel="${rel#./}"; rel="${rel#/}"
      local_file="$dag_local_root/$rel"
      if [[ -f "$local_file" ]]; then
        local raw_sched emails
        raw_sched="$(extract_schedule "$local_file")"
        if [[ -z "$raw_sched" ]]; then
          scheduled="Unknown"; sched_time=""
        elif [[ "${raw_sched,,}" == "none" ]]; then
          scheduled="No"; sched_time=""
        else
          scheduled="Yes"; sched_time="$raw_sched"
        fi
        emails="$(extract_emails "$local_file")"
        if [[ -n "$emails" ]]; then
          email_alert="$emails"
        elif grep -qE "email_on_(failure|retry)[[:space:]]*=[[:space:]]*True" "$local_file" 2>/dev/null; then
          email_alert="Yes (no address)"
        else
          email_alert="No"
        fi
      fi
    fi

    write_row "$env_name" "$project" "$dag_id" "$fpath" \
      "$active" "$scheduled" "$sched_time" "$email_alert"
    count=$((count + 1))
  done < <(printf '%s' "$dags_json" | jq -c '.[]')

  log "  ${count} DAG(s) written"
  [[ -n "$tmpdir" ]] && rm -rf "$tmpdir"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  mapfile -t PROJECTS < <(resolve_projects)
  mapfile -t LOCATIONS < <(resolve_locations)

  log "Projects : ${PROJECTS[*]}"
  log "Locations: ${#LOCATIONS[@]} region(s)"
  log "Output   : ${OUTPUT_CSV}"

  # CSV header.
  printf 'Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time,Email Alert\n' >"$OUTPUT_CSV"

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
