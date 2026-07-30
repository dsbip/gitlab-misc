#!/usr/bin/env bash
#
# Sync Airflow (Cloud Composer) DAG-level RBAC from a YAML config, idempotently.
#
# v2: for each DAG in a role's Dags_list it grants a fixed capability BUNDLE
# (see DAG_ACTIONS / GLOBAL_PERMS below): view the DAG, view DAG code, view &
# create & edit DAG runs, and view task instances/runs/logs. This is a mix of
# per-DAG permissions (DAG:<dag_id>) and global Airflow resource permissions
# (DAG Runs, DAG Code, Task Instances, Task Logs), because Airflow gates
# DAG-run/task access on both.
#
# For each custom role in the config it ensures:
#   * the role exists,
#   * the role's per-DAG permissions match the bundle + config (adds missing,
#     removes stale DAG:* permissions - non-DAG permissions are left untouched),
#   * the global bundle permissions are present (add-only; never removed),
#   * every listed user account has the role,
#   * the "Op" and "Admin" roles are removed from every listed user.
# If Airflow already matches, nothing is changed.
#
# It uses `gcloud composer environments run` to drive the Airflow CLI, so the
# only tools needed are gcloud, jq, and (optionally) python3+PyYAML for the
# config (a pure-shell YAML parser is used as a fallback).
#
# Config YAML (one or more top-level custom roles):
#
#   custom_role_name:
#     roles:                 # OPTIONAL extra Airflow actions (added per DAG on
#       - can delete         # top of the bundle's can_read/can_edit)
#     Dags_list:             # DAG ids the bundle applies to
#       - dag_one
#       - dag_two
#     User_account_list:     # user emails to grant the role to
#       - user1@abc.com
#       - user2@abc.com
#
# "roles" values are normalized to Airflow actions: "can read" -> can_read,
# "edit" -> can_edit, "menu access" -> menu_access, etc.
#
# Config (flags override env vars):
#   --config      / RBAC_CONFIG          YAML file. Default: airflow-rbac-v2/rbac_config.yml
#   --project     / COMPOSER_PROJECT     GCP project id (required unless --dry-run w/ mocks)
#   --location    / COMPOSER_LOCATION    Composer region (required)
#   --environment / COMPOSER_ENVIRONMENT Composer environment name (required)
#   --dry-run                            Show planned changes, make none.
#   --create-missing-users               Create users absent from Airflow (see caveat below).
#   GCLOUD (env)                         gcloud binary (default: gcloud). Overridable for tests.
#
# Composer caveat: users usually appear in Airflow only after they first sign in.
# `users add-role` on an unknown user fails, so such users are warned+skipped
# unless --create-missing-users is set.

set -uo pipefail

CONFIG_FILE="${RBAC_CONFIG:-airflow-rbac-v2/rbac_config.yml}"
PROJECT="${COMPOSER_PROJECT:-}"
LOCATION="${COMPOSER_LOCATION:-}"
ENVIRONMENT="${COMPOSER_ENVIRONMENT:-}"
GCLOUD="${GCLOUD:-gcloud}"
DRY_RUN=0
CREATE_MISSING_USERS=0

# Roles that must never remain on a managed user.
FORBIDDEN_ROLES=("Op" "Admin")

# ---------------------------------------------------------------------------
# Permission bundle granted for each DAG in Dags_list. This encodes the desired
# DAG-level capabilities. In Airflow, per-DAG access is a combination of the
# per-DAG resource (DAG:<dag_id>) and the GLOBAL sub-resources (DAG Runs, DAG
# Code, Task Instances, Task Logs), so the bundle has two parts:
#
#   Capability                          per-DAG (DAG:<dag>)   global resource
#   1. view the DAG                     can_read              -
#   2. view DAG code                    can_read              can_read DAG Code
#   3. view DAG runs                    can_read              can_read DAG Runs
#   4. create DAG runs (trigger)        can_edit              can_create DAG Runs
#   5. view task instances/runs/logs    can_read              can_read Task Instances,
#                                                             can_read Task Logs
#   6. edit DAG runs (clear/mark)       can_edit              can_edit DAG Runs
#
# The per-DAG permission scopes access to that specific DAG; the global
# permission enables the corresponding UI page / REST endpoint. Any extra
# actions listed under a role's `roles:` in the config are added on top (per DAG).
DAG_ACTIONS=("can_read" "can_edit")   # applied to every DAG:<dag_id>

# Global permissions added once to the role (add-only; never removed by sync).
# Format: "action|resource".
GLOBAL_PERMS=(
  "can_read|DAG Code"
  "can_read|DAG Runs"
  "can_create|DAG Runs"
  "can_edit|DAG Runs"
  "can_read|Task Instances"
  "can_read|Task Logs"
)

PLANNED=0
CHANGES=0
FAILS=0

declare -A ROLE_EXISTS=()      # role -> 1
declare -A USER_EXISTS=()      # email -> 1
declare -A USER_ROLES=()       # email -> "|Role1|Role2|"
declare -A ROLE_DAGPERMS=()    # role -> space-sep "action|DAG:dag"
declare -A HAVE_PERM=()        # "role|action|resource" -> 1 (all current perms)
declare -A ACTIONS_OF=()       # role -> space-sep actions
declare -A DAGS_OF=()          # role -> space-sep dag ids
declare -A USERS_OF=()         # role -> space-sep emails
declare -a ROLE_ORDER=()       # roles in config order
declare -A DONE=()             # mutation dedup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { printf '%s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }
trim() { local s=${1-}; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }
need_val() { [[ $# -ge 2 ]] || die "Option $1 requires a value"; }

norm_action() {
  local s
  s="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[[:space:]-]+/_/g')"
  case "$s" in read|edit|create|delete) s="can_${s}" ;; esac
  printf '%s' "$s"
}

# Extract a JSON document from `gcloud composer environments run` output (which
# is preceded by preamble lines). Echoes clean JSON; non-zero if unparseable.
extract_json() {
  local raw js
  raw="$(cat | tr -d '\r')"
  js="$(printf '%s\n' "$raw" | sed -n '/^[[:space:]]*[[{]/,$p')"
  if [[ -n "$js" ]] && printf '%s' "$js" | jq empty >/dev/null 2>&1; then printf '%s' "$js"; return 0; fi
  if [[ -n "$raw" ]] && printf '%s' "$raw" | jq empty >/dev/null 2>&1; then printf '%s' "$raw"; return 0; fi
  return 1
}

# Run an Airflow CLI subcommand in the environment.
#   af "<airflow subcommand>" <post-'--' args...>
af() {
  local sub="$1"; shift
  # shellcheck disable=SC2086
  "$GCLOUD" composer environments run "$ENVIRONMENT" \
    --project "$PROJECT" --location "$LOCATION" \
    $sub -- "$@"
}

# Execute a mutating airflow command (honors --dry-run, counts changes).
run_mutation() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "  [plan] airflow $*"; PLANNED=$((PLANNED + 1)); return 0
  fi
  if af "$@" >/dev/null 2>&1; then
    log "  [done] airflow $*"; CHANGES=$((CHANGES + 1)); return 0
  fi
  log "  [FAIL] airflow $*"; FAILS=$((FAILS + 1)); return 1
}

role_exists()    { [[ -n "${ROLE_EXISTS[$1]:-}" ]]; }
user_exists()    { [[ -n "${USER_EXISTS[$1]:-}" ]]; }
user_has_role()  { [[ "${USER_ROLES[$1]:-}" == *"|$2|"* ]]; }

# ---------------------------------------------------------------------------
# Config parsing -> ROLE/ACTION/DAG/USER TSV (python if available, else shell)
# ---------------------------------------------------------------------------
parse_config() {
  if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' >/dev/null 2>&1; then
    parse_config_py "$1"
  else
    log "NOTE: python3/PyYAML unavailable; using the built-in shell YAML parser."
    parse_config_sh "$1"
  fi
}

parse_config_py() {
  python3 - "$1" <<'PY'
import sys, re, yaml

def norm_action(a):
    s = re.sub(r'[\s\-]+', '_', str(a).strip().lower())
    return 'can_' + s if s in ('read', 'edit', 'create', 'delete') else s

def getlist(spec, *keys):
    for k in keys:
        v = spec.get(k)
        if v:
            return v if isinstance(v, list) else [v]
    return []

path = sys.argv[1]
try:
    cfg = yaml.safe_load(open(path)) or {}
except Exception as exc:
    sys.stderr.write(f"ERROR: could not parse {path}: {exc}\n"); sys.exit(2)
if not isinstance(cfg, dict):
    sys.stderr.write(f"ERROR: {path} top level must be a mapping of role -> spec.\n"); sys.exit(2)

for role, spec in cfg.items():
    role = str(role).strip()
    if not role or not isinstance(spec, dict):
        sys.stderr.write(f"WARN: role '{role}' has no valid spec; skipping.\n"); continue
    sys.stdout.write(f"ROLE\t{role}\n")
    for a in getlist(spec, 'roles', 'Roles'):
        sys.stdout.write(f"ACTION\t{role}\t{norm_action(a)}\n")
    for d in getlist(spec, 'Dags_list', 'dags_list', 'DAGs_list', 'dag_list'):
        sys.stdout.write(f"DAG\t{role}\t{str(d).strip()}\n")
    for u in getlist(spec, 'User_account_list', 'user_account_list', 'users'):
        sys.stdout.write(f"USER\t{role}\t{str(u).strip()}\n")
PY
}

# Pure-shell parser for the documented structure (block or inline [ ] lists).
parse_config_sh() {
  local file="$1" role="" sub="" raw line key val item
  local emit_inline
  _sub_of() {  # map a yaml key to a normalized section, or empty
    case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
      roles) printf 'actions' ;;
      dags_list|dag_list) printf 'dags' ;;
      user_account_list|users) printf 'users' ;;
      *) printf '' ;;
    esac
  }
  _emit() {  # $1 = raw item
    local v; v="$(trim "$1")"; v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
    [[ -z "$v" ]] && return 0
    case "$sub" in
      actions) printf 'ACTION\t%s\t%s\n' "$role" "$(norm_action "$v")" ;;
      dags)    printf 'DAG\t%s\t%s\n' "$role" "$v" ;;
      users)   printf 'USER\t%s\t%s\n' "$role" "$v" ;;
    esac
  }
  _emit_inline() {  # $1 = "[a, b, c]" or scalar
    local body="$1"; body="$(trim "$body")"
    body="${body#[}"; body="${body%]}"
    local IFS=','; local part
    for part in $body; do _emit "$part"; done
  }
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw//$'\r'/}"
    line="$(printf '%s' "$line" | sed -E 's/^#.*$//; s/[[:space:]]#.*$//')"
    [[ -z "$(trim "$line")" ]] && continue
    if [[ "$line" =~ ^[^[:space:]#][^:]*:[[:space:]]*$ ]]; then
      role="$(trim "${line%:}")"; sub=""
      printf 'ROLE\t%s\n' "$role"
      continue
    fi
    if [[ "$line" =~ ^[[:space:]]+([A-Za-z_]+):[[:space:]]*(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"; val="$(trim "${BASH_REMATCH[2]}")"
      sub="$(_sub_of "$key")"
      [[ -n "$val" ]] && _emit_inline "$val"
      continue
    fi
    if [[ "$line" =~ ^[[:space:]]+-[[:space:]]*(.*)$ ]]; then
      _emit "${BASH_REMATCH[1]}"
      continue
    fi
  done <"$file"
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)                need_val "$@"; CONFIG_FILE="$2"; shift 2 ;;
    --project)               need_val "$@"; PROJECT="$2"; shift 2 ;;
    --location)              need_val "$@"; LOCATION="$2"; shift 2 ;;
    --environment)           need_val "$@"; ENVIRONMENT="$2"; shift 2 ;;
    --dry-run)               DRY_RUN=1; shift ;;
    --create-missing-users)  CREATE_MISSING_USERS=1; shift ;;
    -h|--help)               grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)                       die "Unknown argument: $1" ;;
  esac
done

# ---------------------------------------------------------------------------
# Load desired state from config
# ---------------------------------------------------------------------------
load_config() {
  local type role val
  while IFS=$'\t' read -r type role val; do
    [[ -z "$type" ]] && continue
    case "$type" in
      ROLE)
        role="$(trim "$role")"
        [[ -z "$role" ]] && continue
        if [[ -z "${ACTIONS_OF[$role]+x}${DAGS_OF[$role]+x}${USERS_OF[$role]+x}" ]]; then
          case " ${ROLE_ORDER[*]-} " in *" $role "*) : ;; *) ROLE_ORDER+=("$role") ;; esac
        fi
        : "${ACTIONS_OF[$role]:=}"; : "${DAGS_OF[$role]:=}"; : "${USERS_OF[$role]:=}"
        ;;
      ACTION) val="$(trim "$val")"; [[ -n "$val" ]] && ACTIONS_OF["$role"]="${ACTIONS_OF[$role]:+${ACTIONS_OF[$role]} }$val" ;;
      DAG)    val="$(trim "$val")"; [[ -n "$val" ]] && DAGS_OF["$role"]="${DAGS_OF[$role]:+${DAGS_OF[$role]} }$val" ;;
      USER)   val="$(trim "$val")"; [[ -n "$val" ]] && USERS_OF["$role"]="${USERS_OF[$role]:+${USERS_OF[$role]} }$val" ;;
    esac
  done < <(parse_config "$CONFIG_FILE" | tr -d '\r')
  [[ ${#ROLE_ORDER[@]} -gt 0 ]] || die "No roles found in $CONFIG_FILE"
}

# ---------------------------------------------------------------------------
# Fetch current Airflow state
# ---------------------------------------------------------------------------
fetch_state() {
  local roles_json perms_json users_json

  roles_json="$(af "roles list" -o json | extract_json)" \
    || die "Could not read Airflow roles (gcloud/airflow 'roles list' failed)."
  while IFS= read -r r; do [[ -n "$r" ]] && ROLE_EXISTS["$r"]=1; done \
    < <(printf '%s' "$roles_json" | jq -r '.[].name // empty' | tr -d '\r')

  perms_json="$(af "roles list" -p -o json | extract_json)" \
    || die "Could not read Airflow role permissions."
  local rname ract rres
  while IFS=$'\t' read -r rname ract rres; do
    [[ -z "$rname" || -z "$ract" || -z "$rres" ]] && continue
    # Track every permission (resource may contain spaces, e.g. "DAG Runs").
    HAVE_PERM["${rname}|${ract}|${rres}"]=1
    # Per-DAG resources (DAG:<dag>, no spaces) are also synced add/remove.
    [[ "$rres" == DAG:* ]] && \
      ROLE_DAGPERMS["$rname"]="${ROLE_DAGPERMS[$rname]:+${ROLE_DAGPERMS[$rname]} }${ract}|${rres}"
  done < <(printf '%s' "$perms_json" \
            | jq -r '.[] | [(.name // ""), (.action // .permission // ""), (.resource // .view_menu // "")] | @tsv' \
            | tr -d '\r')

  users_json="$(af "users list" -o json | extract_json)" \
    || die "Could not read Airflow users."
  local uemail uroles cleaned part set parts
  while IFS=$'\t' read -r uemail uroles; do
    [[ -z "$uemail" ]] && continue
    USER_EXISTS["$uemail"]=1
    cleaned="$(printf '%s' "$uroles" | tr -d "[]{}\"'")"
    set="|"
    IFS=',' read -ra parts <<<"$cleaned"
    for part in "${parts[@]}"; do part="$(trim "$part")"; [[ -n "$part" ]] && set="${set}${part}|"; done
    USER_ROLES["$uemail"]="$set"
  done < <(printf '%s' "$users_json" \
            | jq -r '.[] | [ (.email // .username // ""),
                             ( .roles | if type=="array"
                                        then (map(if type=="object" then .name else tostring end) | join(","))
                                        else tostring end ) ] | @tsv' \
            | tr -d '\r')
}

# ---------------------------------------------------------------------------
# Reconcile one role
# ---------------------------------------------------------------------------
reconcile_role() {
  local R="$1"
  log "=== role: ${R} ==="

  # Per-DAG actions = bundle actions + any extra actions from the config.
  local -a dag_actions=("${DAG_ACTIONS[@]}")
  local a
  for a in ${ACTIONS_OF[$R]:-}; do
    case " ${dag_actions[*]} " in *" $a "*) : ;; *) dag_actions+=("$a") ;; esac
  done

  # Desired per-DAG permissions (DAG:<dag> resources have no spaces).
  local -A desired=()
  local d
  for a in "${dag_actions[@]}"; do
    for d in ${DAGS_OF[$R]:-}; do
      desired["${a}|DAG:${d}"]=1
    done
  done

  # Ensure the role exists.
  if ! role_exists "$R"; then
    run_mutation "roles create" "$R"
    ROLE_EXISTS["$R"]=1
  fi

  # Current per-DAG permissions of the role.
  local -A current=()
  local p
  for p in ${ROLE_DAGPERMS[$R]:-}; do current["$p"]=1; done

  # Group additions/removals by action. Values are NEWLINE-separated resources
  # because global resources (e.g. "DAG Runs") contain spaces.
  local -A add_by_action=() del_by_action=()
  local key act res
  # Per-DAG additions.
  for key in "${!desired[@]}"; do
    [[ -n "${current[$key]:-}" ]] && continue
    act="${key%%|*}"; res="${key#*|}"
    add_by_action["$act"]="${add_by_action[$act]:+${add_by_action[$act]}$'\n'}$res"
  done
  # Global bundle additions (add-only; never removed). Skip if already present.
  local gp gact gres
  for gp in "${GLOBAL_PERMS[@]}"; do
    gact="${gp%%|*}"; gres="${gp#*|}"
    [[ -n "${HAVE_PERM["${R}|${gact}|${gres}"]:-}" ]] && continue
    add_by_action["$gact"]="${add_by_action[$gact]:+${add_by_action[$gact]}$'\n'}$gres"
  done
  # Per-DAG removals only (stale DAG:* permissions). Globals are never removed.
  for key in "${!current[@]}"; do
    [[ -n "${desired[$key]:-}" ]] && continue
    act="${key%%|*}"; res="${key#*|}"
    del_by_action["$act"]="${del_by_action[$act]:+${del_by_action[$act]}$'\n'}$res"
  done

  local -a res_arr
  for act in "${!add_by_action[@]}"; do
    res_arr=()
    while IFS= read -r res; do [[ -n "$res" ]] && res_arr+=("$res"); done <<<"${add_by_action[$act]}"
    run_mutation "roles add-perms" "$R" -a "$act" -r "${res_arr[@]}"
  done
  for act in "${!del_by_action[@]}"; do
    res_arr=()
    while IFS= read -r res; do [[ -n "$res" ]] && res_arr+=("$res"); done <<<"${del_by_action[$act]}"
    run_mutation "roles del-perms" "$R" -a "$act" -r "${res_arr[@]}"
  done

  # Users: grant the role, strip forbidden roles.
  local u bad key2
  for u in ${USERS_OF[$R]:-}; do
    if ! user_exists "$u"; then
      if [[ "$CREATE_MISSING_USERS" -eq 1 ]]; then
        run_mutation "users create" --use-random-password --username "$u" --email "$u" \
          --firstname "$u" --lastname "user" --role "$R"
        USER_EXISTS["$u"]=1; USER_ROLES["$u"]="|${R}|"
      else
        log "  WARN: user '$u' not found in Airflow; skipping (use --create-missing-users)."
      fi
      continue
    fi
    if ! user_has_role "$u" "$R"; then
      key2="add|$u|$R"
      [[ -z "${DONE[$key2]:-}" ]] && { run_mutation "users add-role" -e "$u" -r "$R"; DONE[$key2]=1; }
    fi
    for bad in "${FORBIDDEN_ROLES[@]}"; do
      if user_has_role "$u" "$bad"; then
        key2="rm|$u|$bad"
        [[ -z "${DONE[$key2]:-}" ]] && { run_mutation "users remove-role" -e "$u" -r "$bad"; DONE[$key2]=1; }
      fi
    done
  done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  command -v jq >/dev/null 2>&1 || die "jq not found on PATH"
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"
  command -v "$GCLOUD" >/dev/null 2>&1 || die "gcloud ('$GCLOUD') not found on PATH"
  [[ -n "$PROJECT" ]]     || die "project not set (--project / COMPOSER_PROJECT)"
  [[ -n "$LOCATION" ]]    || die "location not set (--location / COMPOSER_LOCATION)"
  [[ -n "$ENVIRONMENT" ]] || die "environment not set (--environment / COMPOSER_ENVIRONMENT)"

  load_config
  log "Config   : ${CONFIG_FILE}  (${#ROLE_ORDER[@]} role(s))"
  log "Composer : ${PROJECT}/${ENVIRONMENT} @ ${LOCATION}"
  [[ "$DRY_RUN" -eq 1 ]] && log "Mode     : DRY-RUN (no changes will be made)"
  log ""

  fetch_state

  local R
  for R in "${ROLE_ORDER[@]}"; do
    reconcile_role "$R"
  done

  log ""
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "Done (dry-run): ${PLANNED} change(s) would be applied."
  elif [[ "$CHANGES" -eq 0 && "$FAILS" -eq 0 ]]; then
    log "Done: already in sync; no changes made."
  else
    log "Done: ${CHANGES} change(s) applied, ${FAILS} failure(s)."
  fi
  [[ "$FAILS" -eq 0 ]]
}

main
