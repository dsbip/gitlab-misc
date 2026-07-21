#!/usr/bin/env bash
#
# Shell (bash + awk/sort) equivalent of analyze_view_auth.py. Same inputs and
# byte-identical output CSVs, for runner images without Python. See the Python
# file's header for the full explanation of BigQuery authorized-view chains.
#
# Inputs:
#   --views VIEWS_CSV            project,dataset,view,base_datasets
#   --authorizations AUTH_CSV    dataset,authorized_view (fetched from ACLs)
#   --scanned SCANNED_FILE       optional list of scanned project.dataset lines
#   --out-dir DIR                where the report CSVs are written
#   --fail-on-broken             exit 2 if any MISSING authorization is found
#
# Outputs (in --out-dir): view_auth_edges.csv, view_auth_broken.csv,
# view_auth_chains.csv, plus a table of broken links on stdout.

set -uo pipefail

VIEWS_CSV="${VIEWS_CSV:-view-authorizations/views.csv}"
AUTH_CSV="${AUTH_CSV:-}"
SCANNED_FILE="${SCANNED_FILE:-}"
OUT_DIR="${OUT_DIR:-.}"
FAIL_ON_BROKEN=0

log() { printf '%s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --views)          VIEWS_CSV="$2"; shift 2 ;;
    --authorizations) AUTH_CSV="$2"; shift 2 ;;
    --scanned)        SCANNED_FILE="$2"; shift 2 ;;
    --out-dir)        OUT_DIR="$2"; shift 2 ;;
    --fail-on-broken) FAIL_ON_BROKEN=1; shift ;;
    -h|--help)        grep '^#' "$0" | grep -v '^#!' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)                die "Unknown argument: $1" ;;
  esac
done

[[ -f "$VIEWS_CSV" ]] || die "views CSV not found: $VIEWS_CSV"
mkdir -p "$OUT_DIR"

declare -A AUTHED=()      # "base|view_fqn" -> 1
declare -A SCANNED=()     # "project.dataset" -> 1
declare -A BASE_OF=()     # view_fqn -> space-separated base datasets
declare -A HOSTED=()      # "project.dataset" -> space-separated view_fqns
declare -A SEEN_VIEW=()   # dedup view_fqn
declare -a ORDER=()       # view_fqns in file order
HAVE_SCANNED=0

trim() { local s=${1-}; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }

# Normalize a base-dataset token to "project.dataset" (echo empty if invalid).
norm_dataset() {
  local tok="$1" default="$2" e proj ds
  e="$(trim "${tok//:/.}")"
  [[ -z "$e" ]] && return 0
  if [[ "$e" != *.* ]]; then
    [[ -z "$default" ]] && { log "WARN: base dataset '$tok' has no project; skipping."; return 0; }
    printf '%s.%s' "$default" "$e"; return 0
  fi
  proj="${e%%.*}"; ds="${e#*.}"; ds="${ds%%.*}"
  printf '%s.%s' "$proj" "$ds"
}

# ---------------------------------------------------------------------------
# Load authorizations + scanned datasets
# ---------------------------------------------------------------------------
if [[ -n "$AUTH_CSV" && -f "$AUTH_CSV" ]]; then
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    raw="${raw//$'\r'/}"; raw="${raw//\"/}"
    [[ -z "$(trim "$raw")" ]] && continue
    [[ "$(trim "$raw")" == \#* ]] && continue
    IFS=, read -r ds vf _ <<<"$raw"
    ds="$(trim "${ds//:/.}")"; vf="$(trim "${vf//:/.}")"
    [[ "$ds" == "dataset" ]] && continue
    [[ -n "$ds" && -n "$vf" ]] && AUTHED["${ds}|${vf}"]=1
  done <"$AUTH_CSV"
fi

if [[ -n "$SCANNED_FILE" && -f "$SCANNED_FILE" ]]; then
  HAVE_SCANNED=1
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    raw="$(trim "${raw//$'\r'/}")"; raw="${raw//:/.}"
    [[ -z "$raw" || "$raw" == \#* ]] && continue
    SCANNED["$raw"]=1
  done <"$SCANNED_FILE"
fi

# ---------------------------------------------------------------------------
# Load views (cols 4..N form base_datasets, so `read` keeps the remainder)
# ---------------------------------------------------------------------------
line_no=0
while IFS= read -r raw || [[ -n "$raw" ]]; do
  line_no=$((line_no + 1))
  raw="${raw//$'\r'/}"
  [[ -z "$(trim "$raw")" ]] && continue
  [[ "$(trim "$raw")" == \#* ]] && continue
  IFS=, read -r c_proj c_ds c_view c_bases <<<"$raw"
  c_proj="$(trim "${c_proj//\"/}")"; c_ds="$(trim "${c_ds//\"/}")"
  c_view="$(trim "${c_view//\"/}")"; c_bases="${c_bases//\"/}"
  [[ "$line_no" -eq 1 && "$c_proj" == "project" ]] && continue
  if [[ -z "$c_proj" || -z "$c_ds" || -z "$c_view" ]]; then
    log "WARN: views row ${line_no} missing project/dataset/view; skipping."
    continue
  fi
  vf="${c_proj}.${c_ds}.${c_view}"
  if [[ -n "${SEEN_VIEW[$vf]:-}" ]]; then
    log "WARN: duplicate view '${vf}'; keeping the first definition."
    continue
  fi
  SEEN_VIEW["$vf"]=1
  # Split base list on ; , | and whitespace; normalize each.
  local_bases=""
  norm_toks="$(printf '%s' "$c_bases" | tr ';,|' '   ')"
  for tok in $norm_toks; do
    nd="$(norm_dataset "$tok" "$c_proj")"
    [[ -z "$nd" ]] && continue
    case " $local_bases " in *" $nd "*) : ;; *) local_bases="${local_bases:+$local_bases }$nd" ;; esac
  done
  BASE_OF["$vf"]="$local_bases"
  ds_fqn="${c_proj}.${c_ds}"
  HOSTED["$ds_fqn"]="${HOSTED[$ds_fqn]:+${HOSTED[$ds_fqn]} }$vf"
  ORDER+=("$vf")
done <"$VIEWS_CSV"

# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------
edge_status() {  # $1=view_fqn $2=base
  if [[ -n "${AUTHED["${2}|${1}"]:-}" ]]; then printf 'OK'
  elif [[ "$HAVE_SCANNED" -eq 1 ]]; then
    [[ -n "${SCANNED[$2]:-}" ]] && printf 'MISSING' || printf 'UNKNOWN'
  else printf 'MISSING'; fi
}

# Echo the space-separated downstream closure of a view (including itself).
reachable_views() {
  local start="$1" cur b w
  declare -A seen=()
  local -a stack=("$start")
  while ((${#stack[@]})); do
    cur="${stack[-1]}"; unset 'stack[-1]'
    [[ -n "${seen[$cur]:-}" ]] && continue
    seen["$cur"]=1
    for b in ${BASE_OF[$cur]:-}; do
      for w in ${HOSTED[$b]:-}; do
        [[ -z "${seen[$w]:-}" ]] && stack+=("$w")
      done
    done
  done
  printf '%s\n' "${!seen[@]}"
}

# ---------------------------------------------------------------------------
# Emit edges + chains
# ---------------------------------------------------------------------------
edges_tmp="$(mktemp)"; chains_tmp="$(mktemp)"
trap 'rm -f "$edges_tmp" "$chains_tmp"' EXIT

n_missing=0; n_unknown=0
for vf in "${ORDER[@]}"; do
  for b in ${BASE_OF[$vf]:-}; do
    st="$(edge_status "$vf" "$b")"
    if [[ "$st" == "OK" ]]; then auth="Yes"; else auth="No"; fi
    [[ "$st" == "MISSING" ]] && n_missing=$((n_missing + 1))
    [[ "$st" == "UNKNOWN" ]] && n_unknown=$((n_unknown + 1))
    printf '%s,%s,%s,%s\n' "$vf" "$b" "$auth" "$st" >>"$edges_tmp"
  done
done

n_broken_chains=0
for vf in "${ORDER[@]}"; do
  miss=""; unk=""
  for w in $(reachable_views "$vf"); do
    for b in ${BASE_OF[$w]:-}; do
      st="$(edge_status "$w" "$b")"
      if [[ "$st" == "MISSING" ]]; then
        case " $miss " in *" ${w}->${b} "*) : ;; *) miss="${miss:+$miss }${w}->${b}" ;; esac
      elif [[ "$st" == "UNKNOWN" ]]; then
        case " $unk " in *" ${w}->${b} "*) : ;; *) unk="${unk:+$unk }${w}->${b}" ;; esac
      fi
    done
  done
  if [[ -n "$miss" ]]; then status="BROKEN"; n_broken_chains=$((n_broken_chains + 1))
  elif [[ -n "$unk" ]]; then status="UNKNOWN"
  else status="INTACT"; fi
  # sort + ';'-join the link lists to match the Python output exactly
  miss_j="$(printf '%s\n' $miss | LC_ALL=C sort | paste -sd';' -)"
  unk_j="$(printf '%s\n' $unk | LC_ALL=C sort | paste -sd';' -)"
  printf '%s,%s,%s,%s\n' "$vf" "$status" "$miss_j" "$unk_j" >>"$chains_tmp"
done

# ---------------------------------------------------------------------------
# Write sorted report CSVs (LC_ALL=C matches Python's code-point sort)
# ---------------------------------------------------------------------------
{
  printf 'view,base_dataset,authorized,status\n'
  LC_ALL=C sort "$edges_tmp"
} >"$OUT_DIR/view_auth_edges.csv"

{
  printf 'view,base_dataset,authorized,status\n'
  LC_ALL=C sort "$edges_tmp" | awk -F, '$4 != "OK"'
} >"$OUT_DIR/view_auth_broken.csv"

{
  printf 'view,chain_status,broken_links,unknown_links\n'
  LC_ALL=C sort "$chains_tmp"
} >"$OUT_DIR/view_auth_chains.csv"

# ---------------------------------------------------------------------------
# Table of broken links + summary
# ---------------------------------------------------------------------------
log "Views: ${#ORDER[@]}  Edges: $(wc -l <"$edges_tmp" | tr -d ' ')  MISSING: ${n_missing}  UNKNOWN: ${n_unknown}  Broken chains: ${n_broken_chains}"
log "Reports written to ${OUT_DIR}/"

broken_rows="$(LC_ALL=C sort "$edges_tmp" | awk -F, '$4 != "OK"')"
if [[ -z "$broken_rows" ]]; then
  printf 'All view authorizations are present. No broken links found.\n'
else
  {
    printf 'View\tBase dataset\tStatus\n'
    printf '%s\n' "$broken_rows" | awk -F, '{print $1"\t"$2"\t"$4}'
  } | awk -F'\t' '
    { for (i=1;i<=NF;i++){ v[NR,i]=$i; if(length($i)>w[i]) w[i]=length($i) } n=NR }
    END {
      sep=""; for(i=1;i<=NF;i++){ s=""; for(j=0;j<w[i]+2;j++)s=s"-"; sep=sep (i>1?"+":"") s }
      cnt=n-1
      printf "Broken / unverifiable view authorizations (%d):\n", cnt
      for(r=1;r<=n;r++){
        line=""; for(i=1;i<=NF;i++){ line=line (i>1?"|":"") sprintf(" %-*s ", w[i], v[r,i]) }
        if(r==1){ print sep; print line; print sep } else print line
      }
      print sep
    }'
fi

if [[ "$FAIL_ON_BROKEN" -eq 1 && "$n_missing" -gt 0 ]]; then
  exit 2
fi
exit 0
