#!/usr/bin/env python3
"""
Inventory of Cloud Composer (Airflow) DAGs across a Google Cloud project.

For every RUNNING Composer environment found in the target project(s), this
script queries the environment's Airflow REST API (Composer 2 / Airflow 2.x)
and emits a CSV row per DAG with the following columns:

    Composer, Project, DAG_Name, Dag_path, Active?, Scheduled, Scheduled Time, Email Alert

It is designed to run unattended from a CI pipeline (see .gitlab-ci.yml). All
inputs are taken from environment variables / CLI flags and authentication uses
Application Default Credentials.

Auth model (Composer 2):
    The Airflow web server accepts a Google OAuth access token with the
    cloud-platform scope. `google.auth.default()` picks up the service-account
    key referenced by GOOGLE_APPLICATION_CREDENTIALS (or the pipeline's
    Workload Identity). The identity needs at least:
        - roles/composer.user           (list/describe envs, call Airflow API)
        - roles/composer.environmentAndStorageObjectViewer  (optional)

Environment variables (all optional; CLI flags win):
    GCP_PROJECTS        Comma-separated project IDs. Default: active gcloud project.
    COMPOSER_LOCATIONS  Comma-separated regions to scan. Default: all compute regions.
    OUTPUT_CSV          Output path. Default: composer_dags.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Optional

try:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
except ImportError:  # pragma: no cover - surfaced with a helpful message
    sys.stderr.write(
        "Missing dependency: install with `pip install -r requirements.txt`\n"
    )
    raise

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

CSV_HEADER = [
    "Composer",
    "Project",
    "DAG_Name",
    "Dag_path",
    "Active?",
    "Scheduled",
    "Scheduled Time",
    "Email Alert",
]

# Matches an email address, used to detect alert recipients in DAG source.
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# Matches `email=[...]` or `'email': [...]` / `"email": [...]` assignments.
_EMAIL_ASSIGN_RE = re.compile(
    r"""(?:['"]email['"]\s*:|(?<![A-Za-z_])email\s*=)\s*(\[[^\]]*\]|['"][^'"]*['"])""",
    re.DOTALL,
)
_EMAIL_ON_FAILURE_RE = re.compile(r"email_on_(?:failure|retry)\s*=\s*True")


def log(msg: str) -> None:
    """Write progress to stderr so stdout/CSV stays clean."""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def run_gcloud(args: List[str]) -> str:
    """Run a gcloud command and return stdout, raising on failure."""
    cmd = ["gcloud", *args]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        # gcloud on Windows is a .cmd shim; shell=True lets it resolve on PATH.
        shell=(os.name == "nt"),
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(cmd)}` failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


def get_default_project() -> Optional[str]:
    try:
        out = run_gcloud(["config", "get-value", "project"]).strip()
        return out or None
    except RuntimeError:
        return None


def resolve_projects(cli_projects: Optional[str]) -> List[str]:
    raw = cli_projects or os.environ.get("GCP_PROJECTS", "")
    projects = [p.strip() for p in raw.split(",") if p.strip()]
    if not projects:
        default = get_default_project()
        if default:
            projects = [default]
    if not projects:
        raise SystemExit(
            "No project specified. Set --projects / GCP_PROJECTS or a gcloud "
            "default project."
        )
    return projects


def resolve_locations(cli_locations: Optional[str]) -> List[str]:
    raw = cli_locations or os.environ.get("COMPOSER_LOCATIONS", "")
    locations = [l.strip() for l in raw.split(",") if l.strip()]
    if locations:
        return locations
    # Fall back to enumerating every compute region.
    try:
        out = run_gcloud(
            ["compute", "regions", "list", "--format=value(name)"]
        )
        regions = [r.strip() for r in out.splitlines() if r.strip()]
        if regions:
            return regions
    except RuntimeError as exc:
        log(f"WARN: could not list compute regions ({exc}); using fallback list.")
    # Static fallback of common Composer regions.
    return [
        "us-central1", "us-east1", "us-east4", "us-west1", "us-west2",
        "us-west3", "us-west4", "northamerica-northeast1", "southamerica-east1",
        "europe-west1", "europe-west2", "europe-west3", "europe-west4",
        "europe-west6", "europe-north1", "asia-east1", "asia-east2",
        "asia-northeast1", "asia-northeast2", "asia-northeast3",
        "asia-south1", "asia-southeast1", "asia-southeast2",
        "australia-southeast1",
    ]


def list_environments(project: str, location: str) -> List[Dict[str, Any]]:
    """List Composer environments in one project+location as parsed JSON."""
    try:
        out = run_gcloud(
            [
                "composer", "environments", "list",
                f"--project={project}",
                f"--locations={location}",
                "--format=json",
            ]
        )
    except RuntimeError as exc:
        # A location with no Composer/API disabled is normal; log and skip.
        log(f"  skip {project}/{location}: {exc}")
        return []
    try:
        return json.loads(out) or []
    except json.JSONDecodeError:
        return []


def describe_environment(name: str) -> Optional[Dict[str, Any]]:
    """Full describe of an environment given its fully-qualified name."""
    try:
        out = run_gcloud(
            ["composer", "environments", "describe", name, "--format=json"]
        )
        return json.loads(out)
    except (RuntimeError, json.JSONDecodeError) as exc:
        log(f"  WARN: describe failed for {name}: {exc}")
        return None


def format_schedule(dag: Dict[str, Any]) -> str:
    """Render an Airflow schedule into a human-readable string."""
    # Airflow >= 2.4 exposes a ready-made summary.
    summary = dag.get("timetable_summary")
    if summary:
        return str(summary)

    sched = dag.get("schedule_interval")
    if sched is None:
        return ""
    if isinstance(sched, str):
        return sched
    if isinstance(sched, dict):
        stype = sched.get("__type", "")
        if stype == "CronExpression":
            return sched.get("value") or ""
        if stype == "TimeDelta":
            parts = []
            for unit in ("days", "seconds", "microseconds"):
                val = sched.get(unit)
                if val:
                    parts.append(f"{val}{unit[0]}")
            return "every " + " ".join(parts) if parts else "TimeDelta"
        if stype == "RelativeDelta":
            return "RelativeDelta"
        return sched.get("value") or json.dumps(sched)
    return str(sched)


def is_scheduled(dag: Dict[str, Any]) -> bool:
    if dag.get("timetable_summary"):
        summary = str(dag["timetable_summary"]).strip().lower()
        if summary in ("", "none", "never, external triggers only"):
            return False
        return True
    return dag.get("schedule_interval") is not None


class AirflowClient:
    """Thin wrapper over the Composer 2 Airflow stable REST API."""

    def __init__(self, base_url: str, session: AuthorizedSession):
        self.base_url = base_url.rstrip("/")
        self.session = session
        self._source_cache: Dict[str, str] = {}

    def _get(self, path: str, **kwargs: Any):
        url = f"{self.base_url}{path}"
        return self.session.request("GET", url, timeout=60, **kwargs)

    def list_dags(self) -> List[Dict[str, Any]]:
        dags: List[Dict[str, Any]] = []
        offset = 0
        limit = 100
        while True:
            resp = self._get(f"/api/v1/dags?limit={limit}&offset={offset}")
            if resp.status_code != 200:
                raise RuntimeError(
                    f"GET /api/v1/dags -> {resp.status_code}: {resp.text[:300]}"
                )
            payload = resp.json()
            batch = payload.get("dags", [])
            dags.extend(batch)
            total = payload.get("total_entries", len(dags))
            offset += limit
            if offset >= total or not batch:
                break
        return dags

    def get_source(self, file_token: str) -> str:
        """Fetch (and cache) DAG source by file_token. Empty string on failure."""
        if not file_token:
            return ""
        if file_token in self._source_cache:
            return self._source_cache[file_token]
        source = ""
        try:
            resp = self._get(
                f"/api/v1/dagSources/{file_token}",
                headers={"Accept": "text/plain"},
            )
            if resp.status_code == 200:
                source = resp.text
            else:
                log(
                    f"    WARN: dagSources {file_token[:12]}... -> "
                    f"{resp.status_code}"
                )
        except Exception as exc:  # network/etc. - non-fatal
            log(f"    WARN: source fetch failed: {exc}")
        self._source_cache[file_token] = source
        return source


def detect_email_alert(source: str) -> str:
    """Return alert recipients found in DAG source, or 'No'/'Yes (no address)'."""
    if not source:
        return "Unknown"
    addresses: List[str] = []
    for match in _EMAIL_ASSIGN_RE.finditer(source):
        addresses.extend(_EMAIL_RE.findall(match.group(1)))
    # Dedupe while preserving order.
    seen = set()
    unique = [a for a in addresses if not (a in seen or seen.add(a))]
    if unique:
        return ";".join(unique)
    if _EMAIL_ON_FAILURE_RE.search(source):
        return "Yes (no address)"
    return "No"


def inventory_environment(
    env: Dict[str, Any], project: str, writer: csv.writer
) -> int:
    """Query one environment's DAGs and write CSV rows. Returns row count."""
    name = env.get("name", "")
    short_name = name.split("/")[-1] if name else env.get("name", "unknown")
    state = env.get("state", "UNKNOWN")

    log(f"Environment {project}/{short_name} (state={state})")
    if state != "RUNNING":
        log("  not RUNNING, skipping")
        return 0

    detail = describe_environment(name) if name else env
    airflow_uri = (
        (detail or {}).get("config", {}).get("airflowUri")
        if detail
        else None
    )
    if not airflow_uri:
        log(f"  WARN: no airflowUri for {short_name}, skipping")
        return 0

    credentials, _ = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
    session = AuthorizedSession(credentials)
    client = AirflowClient(airflow_uri, session)

    try:
        dags = client.list_dags()
    except Exception as exc:
        log(f"  ERROR: could not list DAGs for {short_name}: {exc}")
        return 0

    log(f"  {len(dags)} DAG(s) found")
    rows = 0
    for dag in dags:
        dag_id = dag.get("dag_id", "")
        fileloc = dag.get("fileloc") or dag.get("filepath") or ""
        active = "No" if dag.get("is_paused", False) else "Yes"
        scheduled = "Yes" if is_scheduled(dag) else "No"
        schedule_time = format_schedule(dag) if scheduled == "Yes" else ""
        source = client.get_source(dag.get("file_token", ""))
        email_alert = detect_email_alert(source)

        writer.writerow(
            [
                short_name,
                project,
                dag_id,
                fileloc,
                active,
                scheduled,
                schedule_time,
                email_alert,
            ]
        )
        rows += 1
    return rows


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inventory Cloud Composer DAGs into a CSV."
    )
    parser.add_argument(
        "--projects",
        help="Comma-separated GCP project IDs (default: GCP_PROJECTS env or "
        "active gcloud project).",
    )
    parser.add_argument(
        "--locations",
        help="Comma-separated regions to scan (default: COMPOSER_LOCATIONS env "
        "or all compute regions).",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("OUTPUT_CSV", "composer_dags.csv"),
        help="Output CSV path (default: OUTPUT_CSV env or composer_dags.csv).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    projects = resolve_projects(args.projects)
    locations = resolve_locations(args.locations)

    log(f"Projects : {', '.join(projects)}")
    log(f"Locations: {len(locations)} region(s)")
    log(f"Output   : {args.output}")

    total_rows = 0
    total_envs = 0
    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)

        for project in projects:
            for location in locations:
                envs = list_environments(project, location)
                for env in envs:
                    total_envs += 1
                    total_rows += inventory_environment(env, project, writer)

    log(f"\nDone: {total_rows} DAG row(s) across {total_envs} environment(s).")
    log(f"CSV written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
