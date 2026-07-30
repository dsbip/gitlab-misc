#!/usr/bin/env bash
#
# Sync Airflow (Cloud Composer) DAG-level RBAC from a YAML config, idempotently,
# using the Airflow STABLE REST API (not the airflow CLI).
#
# WHY REST: Cloud Composer blocks the RBAC-mutation airflow CLI commands via
# `gcloud composer environments run` -
#   ERROR: ... 'roles add-perms' is not supported in Cloud Composer.
# The REST API (POST/PATCH /api/v1/roles, PATCH /api/v1/users) is the supported
# way and is faster (one call per role / per user, no per-resource CLI loop).
#
# For each DAG in a role's Dags_list it grants a fixed capability BUNDLE
# (DAG_ACTIONS + GLOBAL_PERMS below): view the DAG, view DAG code, view/create/
# edit DAG runs, view task instances/runs/logs. Then it grants the role to the
# listed users and removes "Op"/"Admin" from them. No-ops when already in sync.
#
# Auth: `gcloud auth print-access-token` (the runner must be authenticated - via
# WIF or a service-account key - and the identity must map to the Airflow Admin
# role, which RBAC writes require). The Airflow URL is read from
# `gcloud composer environments describe`.
#
# Config YAML (one or more top-level custom roles):
#   custom_role_name:
#     roles:                 # OPTIONAL extra actions added per DAG (e.g. can delete)
#       - can delete
#     Dags_list:
#       - dag_one
#     User_account_list:
#       - user1@abc.com
#
# Flags (override env vars):
#   --config      / RBAC_CONFIG          Default: airflow-rbac-rest/rbac_config.yml
#   --project     / COMPOSER_PROJECT     GCP project id (required)
#   --location    / COMPOSER_LOCATION    Composer region (required)
#   --environment / COMPOSER_ENVIRONMENT Composer environment name (required)
#   --airflow-uri / AIRFLOW_URI          Skip the describe and use this URL directly
#   --dry-run                            Show planned changes, make none.
#   --create-missing-users               POST /users for users absent from Airflow (caveat below).
#   GCLOUD (env)                         gcloud binary (default: gcloud).
#
# Composer caveat: users usually appear only after first sign-in; PATCH on an
# unknown user is skipped with a warning unless --create-missing-users is set.

set -uo pipefail

CONFIG_FILE="${RBAC_CONFIG:-airflow-rbac-rest/rbac_config.yml}"
PROJECT="${COMPOSER_PROJECT:-}"
LOCATION="${COMPOSER_LOCATION:-}"
ENVIRONMENT="${COMPOSER_ENVIRONMENT:-}"
AIRFLOW_URI="${AIRFLOW_URI:-}"
GCLOUD="${GCLOUD:-gcloud}"
DRY_RUN=0
CREATE_MISSING_USERS=0

FORBIDDEN_ROLES=("Op" "Admin")

# Per-DAG actions applied to every DAG:<dag_id>.
DAG_ACTIONS=("can_read" "can_edit")
# Global permissions the bundle needs (action|resource); resources may have spaces.
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
TOKEN=""
TMP_BODY=""

declare -A ACTIONS_OF=()        # role -> space-sep extra actions
declare -A DAGS_OF=()           # role -> space-sep dag ids
declare -A USERS_OF=()          # role -> space-sep emails
declare -a ROLE_ORDER=()        # roles in config order
declare -A USER_WANT_ROLES=()   # email -> "|role1|role2|" wanted from config
declare -a USER_ORDER=()        # managed emails in config order
declare -A USER_NAME=()         # email -> airflow username
declare -A USER_CUR_ROLES=()    # email -> "|Role1|Role2|" current

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

# True if two associative arrays (by name) have the same key set.
sets_equal() {
  local -n _a="$1" _b="$2"
  [[ "${#_a[@]}" -eq "${#_b[@]}" ]] || return 1
  local k
  for k in "${!_a[@]}"; do [[ -n "${_b[$k]:-}" ]] || return 1; done
  return 0
}

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

parse_config_sh() {
  local file="$1" role="" sub="" raw line key val
  _sub_of() {
    case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
      roles) printf 'actions' ;;
      dags_list|dag_list) printf 'dags' ;;
      user_account_list|users) printf 'users' ;;
      *) printf '' ;;
    esac
  }
  _emit() {
    local v; v="$(trim "$1")"; v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
    [[ -z "$v" ]] && return 0
    case "$sub" in
      actions) printf 'ACTION\t%s\t%s\n' "$role" "$(norm_action "$v")" ;;
      dags)    printf 'DAG\t%s\t%s\n' "$role" "$v" ;;
      users)   printf 'USER\t%s\t%s\n' "$role" "$v" ;;
    esac
  }
  _emit_inline() { local b="$1"; b="$(trim "$b")"; b="${b#[}"; b="${b%]}"; local IFS=','; local p; for p in $b; do _emit "$p"; done; }
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    line="${raw//$'\r'/}"
    line="$(printf '%s' "$line" | sed -E 's/^#.*$//; s/[[:space:]]#.*$//')"
    [[ -z "$(trim "$line")" ]] && continue
    if [[ "$line" =~ ^[^[:space:]#][^:]*:[[:space:]]*$ ]]; then
      role="$(trim "${line%:}")"; sub=""; printf 'ROLE\t%s\n' "$role"; continue
    fi
    if [[ "$line" =~ ^[[:space:]]+([A-Za-z_]+):[[:space:]]*(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"; val="$(trim "${BASH_REMATCH[2]}")"; sub="$(_sub_of "$key")"
      [[ -n "$val" ]] && _emit_inline "$val"; continue
    fi
    if [[ "$line" =~ ^[[:space:]]+-[[:space:]]*(.*)$ ]]; then _emit "${BASH_REMATCH[1]}"; continue; fi
  done <"$file"
}

load_config() {
  local type role val
  while IFS=$'\t' read -r type role val; do
    [[ -z "$type" ]] && continue
    case "$type" in
      ROLE)
        role="$(trim "$role")"; [[ -z "$role" ]] && continue
        case " ${ROLE_ORDER[*]-} " in *" $role "*) : ;; *) ROLE_ORDER+=("$role") ;; esac
        : "${ACTIONS_OF[$role]:=}"; : "${DAGS_OF[$role]:=}"; : "${USERS_OF[$role]:=}"
        ;;
      ACTION) val="$(trim "$val")"; [[ -n "$val" ]] && ACTIONS_OF["$role"]="${ACTIONS_OF[$role]:+${ACTIONS_OF[$role]} }$val" ;;
      DAG)    val="$(trim "$val")"; [[ -n "$val" ]] && DAGS_OF["$role"]="${DAGS_OF[$role]:+${DAGS_OF[$role]} }$val" ;;
      USER)
        val="$(trim "$val")"; [[ -z "$val" ]] && continue
        USERS_OF["$role"]="${USERS_OF[$role]:+${USERS_OF[$role]} }$val"
        if [[ -z "${USER_WANT_ROLES[$val]:-}" ]]; then USER_ORDER+=("$val"); USER_WANT_ROLES["$val"]="|"; fi
        case "${USER_WANT_ROLES[$val]}" in *"|$role|"*) : ;; *) USER_WANT_ROLES["$val"]="${USER_WANT_ROLES[$val]}${role}|" ;; esac
        ;;
    esac
  done < <(parse_config "$CONFIG_FILE" | tr -d '\r')
  [[ ${#ROLE_ORDER[@]} -gt 0 ]] || die "No roles found in $CONFIG_FILE"
}

# ---------------------------------------------------------------------------
# Auth + REST plumbing
# ---------------------------------------------------------------------------
refresh_token() { TOKEN="$("$GCLOUD" auth print-access-token 2>/dev/null)"; [[ -n "$TOKEN" ]]; }

# api_call METHOD PATH [JSON_BODY] -> echoes the HTTP status code; the response
# body is left in $TMP_BODY. NOTE: this is called via $(...) (a subshell), so it
# must communicate the status by *return value* (stdout) rather than a global -
# a global set in the subshell would not reach the caller. The body travels via
# the $TMP_BODY file, which persists across the subshell.
api_call() {
  local method="$1" path="$2" body="${3:-}" url="${AIRFLOW_URI%/}$2" code
  local -a a=(-sS -o "$TMP_BODY" -w '%{http_code}' -X "$method" -H "Authorization: Bearer $TOKEN")
  [[ -n "$body" ]] && a+=(-H "Content-Type: application/json" --data "$body")
  code="$(curl "${a[@]}" "$url" 2>/dev/null)"
  if [[ "$code" == "401" ]] && refresh_token; then
    a=(-sS -o "$TMP_BODY" -w '%{http_code}' -X "$method" -H "Authorization: Bearer $TOKEN")
    [[ -n "$body" ]] && a+=(-H "Content-Type: application/json" --data "$body")
    code="$(curl "${a[@]}" "$url" 2>/dev/null)"
  fi
  printf '%s' "${code:-000}"
}

# api_mutate METHOD PATH JSON DESC  (honors --dry-run, counts changes)
api_mutate() {
  local method="$1" path="$2" body="$3" desc="$4" code
  if [[ "$DRY_RUN" -eq 1 ]]; then log "  [plan] ${desc}"; PLANNED=$((PLANNED + 1)); return 0; fi
  code="$(api_call "$method" "$path" "$body")"
  if [[ "$code" == 2* ]]; then log "  [done] ${desc} (HTTP ${code})"; CHANGES=$((CHANGES + 1)); return 0; fi
  log "  [FAIL] ${desc} (HTTP ${code}): $(head -c 300 "$TMP_BODY" 2>/dev/null)"; FAILS=$((FAILS + 1)); return 1
}

# Build a JSON actions array from "action|resource" lines on stdin.
build_actions_json() {
  jq -R -n '[inputs | select(length>0) | split("|") | {action:{name:.[0]}, resource:{name:.[1]}}]'
}

# ---------------------------------------------------------------------------
# Fetch current users (paginated) into USER_NAME / USER_CUR_ROLES
# ---------------------------------------------------------------------------
fetch_users() {
  local offset=0 limit=100 body code total got uname email roles
  while :; do
    code="$(api_call GET "/api/v1/users?limit=${limit}&offset=${offset}")"
    [[ "$code" == "200" ]] || die "Could not list Airflow users (HTTP ${code})."
    body="$(cat "$TMP_BODY")"
    while IFS=$'\t' read -r uname email roles; do
      [[ -z "$email" && -z "$uname" ]] && continue
      [[ -z "$email" ]] && email="$uname"
      USER_NAME["$email"]="$uname"
      USER_CUR_ROLES["$email"]="|${roles}|"
    done < <(printf '%s' "$body" | jq -r '.users[]? | [(.username // ""), (.email // ""), (.roles // [] | map(.name) | join("|"))] | @tsv' | tr -d '\r')
    got="$(printf '%s' "$body" | jq '(.users // []) | length')"
    total="$(printf '%s' "$body" | jq '.total_entries // 0')"
    offset=$((offset + limit))
    { [[ "${got:-0}" -eq 0 ]] || [[ "$offset" -ge "${total:-0}" ]]; } && break
  done
}

# ---------------------------------------------------------------------------
# Reconcile one role's permissions
# ---------------------------------------------------------------------------
reconcile_role() {
  local R="$1"
  log "=== role: ${R} ==="

  # Desired per-DAG permissions (bundle actions + extra actions from config).
  local -a dag_actions=("${DAG_ACTIONS[@]}")
  local a d
  for a in ${ACTIONS_OF[$R]:-}; do
    case " ${dag_actions[*]} " in *" $a "*) : ;; *) dag_actions+=("$a") ;; esac
  done
  local -A desired_dag=()
  for a in "${dag_actions[@]}"; do
    for d in ${DAGS_OF[$R]:-}; do desired_dag["${a}|DAG:${d}"]=1; done
  done
  local -A globals=(); local gp
  for gp in "${GLOBAL_PERMS[@]}"; do globals["$gp"]=1; done

  # Current permissions (GET role; 404 => doesn't exist).
  local code; code="$(api_call GET "/api/v1/roles/${R}")"
  local exists=0 p res
  local -A current=()
  if [[ "$code" == "200" ]]; then
    exists=1
    while IFS= read -r p; do [[ -n "$p" ]] && current["$p"]=1; done \
      < <(jq -r '.actions[]? | "\(.action.name)|\(.resource.name)"' "$TMP_BODY" | tr -d '\r')
  elif [[ "$code" == "404" ]]; then
    exists=0
  else
    log "  ERROR: could not read role ${R} (HTTP ${code})"; FAILS=$((FAILS + 1)); return 0
  fi

  # Final = keep non-DAG (+ desired DAG) from current, plus desired DAG + globals.
  local -A final=()
  for p in "${!current[@]}"; do
    res="${p#*|}"
    if [[ "$res" == DAG:* ]]; then
      [[ -n "${desired_dag[$p]:-}" ]] && final["$p"]=1   # keep desired DAG, drop stale
    else
      final["$p"]=1                                      # keep non-DAG (globals, website, ...)
    fi
  done
  for p in "${!desired_dag[@]}"; do final["$p"]=1; done
  for p in "${!globals[@]}"; do final["$p"]=1; done

  if [[ $exists -eq 1 ]] && sets_equal current final; then
    log "  permissions already in sync"
    return 0
  fi

  local actions_json payload
  actions_json="$(printf '%s\n' "${!final[@]}" | build_actions_json)"
  payload="$(jq -n --arg n "$R" --argjson a "$actions_json" '{name:$n, actions:$a}')"
  if [[ $exists -eq 0 ]]; then
    api_mutate POST "/api/v1/roles" "$payload" "create role ${R} (${#final[@]} perms)"
  else
    api_mutate PATCH "/api/v1/roles/${R}?update_mask=actions" "$payload" "update role ${R} permissions (${#final[@]} perms)"
  fi
}

# ---------------------------------------------------------------------------
# Reconcile one managed user's roles (add wanted role(s); drop Op/Admin)
# ---------------------------------------------------------------------------
reconcile_user() {
  local email="$1"
  local uname="${USER_NAME[$email]:-}"
  if [[ -z "$uname" ]]; then
    if [[ "$CREATE_MISSING_USERS" -eq 1 ]]; then
      local roles_json payload want r
      local -a want_arr=()
      IFS='|' read -ra want_arr <<<"${USER_WANT_ROLES[$email]#|}"
      roles_json="$(printf '%s\n' "${want_arr[@]}" | jq -R -n '[inputs|select(length>0)|{name:.}]')"
      payload="$(jq -n --arg u "$email" --argjson r "$roles_json" '{username:$u, email:$u, first_name:$u, last_name:"user", roles:$r, password:(now|tostring)}')"
      api_mutate POST "/api/v1/users" "$payload" "create user ${email}"
    else
      log "  WARN: user '${email}' not found in Airflow; skipping (use --create-missing-users)."
    fi
    return 0
  fi

  # desired = (current + wanted) - forbidden
  local -A desired=() curset=()
  local r
  while IFS= read -r r; do [[ -n "$r" ]] && { curset["$r"]=1; desired["$r"]=1; }; done \
    < <(printf '%s' "${USER_CUR_ROLES[$email]}" | tr '|' '\n')
  # add wanted
  while IFS= read -r r; do [[ -n "$r" ]] && desired["$r"]=1; done \
    < <(printf '%s' "${USER_WANT_ROLES[$email]}" | tr '|' '\n')
  # drop forbidden
  local bad; for bad in "${FORBIDDEN_ROLES[@]}"; do unset 'desired[$bad]'; done

  if sets_equal curset desired; then
    log "  user ${email}: roles already in sync"
    return 0
  fi
  local roles_json payload
  roles_json="$(printf '%s\n' "${!desired[@]}" | jq -R -n '[inputs|select(length>0)|{name:.}]')"
  payload="$(jq -n --argjson r "$roles_json" '{roles:$r}')"
  api_mutate PATCH "/api/v1/users/${uname}?update_mask=roles" "$payload" "set roles for ${email} -> [$(printf '%s ' "${!desired[@]}")]"
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
    --airflow-uri)           need_val "$@"; AIRFLOW_URI="$2"; shift 2 ;;
    --dry-run)               DRY_RUN=1; shift ;;
    --create-missing-users)  CREATE_MISSING_USERS=1; shift ;;
    -h|--help)               grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)                       die "Unknown argument: $1" ;;
  esac
done

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  command -v jq >/dev/null 2>&1 || die "jq not found on PATH"
  command -v curl >/dev/null 2>&1 || die "curl not found on PATH"
  command -v "$GCLOUD" >/dev/null 2>&1 || die "gcloud ('$GCLOUD') not found on PATH"
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

  load_config

  refresh_token || die "Could not obtain an access token via '$GCLOUD auth print-access-token'."

  if [[ -z "$AIRFLOW_URI" ]]; then
    [[ -n "$PROJECT" && -n "$LOCATION" && -n "$ENVIRONMENT" ]] \
      || die "Set --airflow-uri, or --project/--location/--environment to look it up."
    AIRFLOW_URI="$("$GCLOUD" composer environments describe "$ENVIRONMENT" \
      --project "$PROJECT" --location "$LOCATION" --format='value(config.airflowUri)' 2>/dev/null)"
    [[ -n "$AIRFLOW_URI" ]] || die "Could not resolve airflowUri for ${ENVIRONMENT}."
  fi

  TMP_BODY="$(mktemp)"; trap 'rm -f "$TMP_BODY"' EXIT

  log "Config   : ${CONFIG_FILE}  (${#ROLE_ORDER[@]} role(s))"
  log "Airflow  : ${AIRFLOW_URI}"
  [[ "$DRY_RUN" -eq 1 ]] && log "Mode     : DRY-RUN (no changes will be made)"
  log ""

  fetch_users

  local R email
  for R in "${ROLE_ORDER[@]}"; do reconcile_role "$R"; done
  for email in "${USER_ORDER[@]}"; do
    log "=== user: ${email} ==="
    reconcile_user "$email"
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
