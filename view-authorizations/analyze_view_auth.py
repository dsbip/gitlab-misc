#!/usr/bin/env python3
"""
Analyze BigQuery authorized-view chains.

Given:
  * a "views" CSV describing every view and the base datasets its SQL reads
    (columns: project,dataset,view,base_datasets), and
  * an "authorizations" CSV of the authorized views actually present on each
    dataset (columns: dataset,authorized_view), as fetched from the datasets'
    ACLs,

this reports, for every view, whether it is authorized on each base dataset it
reads, and walks the *chain* of views (a view built on a view built on a view)
to flag any link where the authorization is missing or cannot be verified.

BigQuery authorized views: to let view V read data in dataset B without the
caller holding access to B, V must be added to B's access list ("authorized on
B"). So the atomic requirement is the pair (view V, base dataset B); a chain
V4 -> dataset3(view3) -> dataset2(view2) -> dataset1 is intact only when every
such pair along it is authorized.

Identifiers are normalized to `project.dataset[.object]`. `project:dataset`
(bq style) is accepted. base_datasets may be separated by `; , | ` or spaces,
and - to survive an unquoted comma-separated field - everything from the 4th
CSV column onward is treated as base_datasets.

Outputs (into --out-dir):
  view_auth_edges.csv   every (view, base_dataset) with authorized + status
  view_auth_broken.csv  the subset that is MISSING or UNKNOWN
  view_auth_chains.csv  per-view chain_status + broken/unknown links
and prints a table of the broken links to stdout.

status values:
  OK       - the view is authorized on the base dataset
  MISSING  - the base dataset was scanned and the authorization is absent
  UNKNOWN  - the base dataset was not scanned (e.g. its project isn't in the
             config), so the authorization could not be verified
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

EDGES_FILE = "view_auth_edges.csv"
BROKEN_FILE = "view_auth_broken.csv"
CHAINS_FILE = "view_auth_chains.csv"

_SPLIT_RE = re.compile(r"[;,|\s]+")


def log(msg: str) -> None:
    sys.stderr.write(msg + "\n")


def norm_dataset(entry: str, default_project: Optional[str]) -> Optional[str]:
    """Normalize a base-dataset token to `project.dataset` (or None if empty)."""
    e = entry.strip().replace(":", ".")
    if not e:
        return None
    parts = [p for p in e.split(".") if p != ""]
    if not parts:
        return None
    if len(parts) == 1:
        if not default_project:
            log(f"WARN: base dataset '{entry}' has no project and none to default to; skipping.")
            return None
        return f"{default_project}.{parts[0]}"
    if len(parts) > 2:
        log(f"WARN: base dataset '{entry}' looks over-qualified; using '{parts[0]}.{parts[1]}'.")
    return f"{parts[0]}.{parts[1]}"


def split_bases(field: str) -> List[str]:
    return [b for b in _SPLIT_RE.split(field.strip()) if b]


def read_views(path: str) -> List[Tuple[str, str, str, str]]:
    """Return (project, dataset, view, base_field) rows from the views CSV."""
    rows: List[Tuple[str, str, str, str]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for i, rec in enumerate(csv.reader(fh), 1):
            if not rec or all(not c.strip() for c in rec):
                continue
            if rec[0].lstrip().startswith("#"):
                continue
            if i == 1 and rec[0].strip().lower() == "project":
                continue  # header
            if len(rec) < 4:
                log(f"WARN: views row {i} has fewer than 4 columns; skipping: {rec}")
                continue
            project, dataset, view = (rec[0].strip(), rec[1].strip(), rec[2].strip())
            if not (project and dataset and view):
                log(f"WARN: views row {i} missing project/dataset/view; skipping.")
                continue
            # Rejoin cols 4..N so an unquoted comma-separated list still works.
            base_field = ",".join(c for c in rec[3:])
            rows.append((project, dataset, view, base_field))
    return rows


def read_authorizations(path: Optional[str]) -> Set[Tuple[str, str]]:
    """Return the set of (base_dataset, authorized_view_fqn) pairs present."""
    authed: Set[Tuple[str, str]] = set()
    if not path or not os.path.exists(path):
        return authed
    with open(path, newline="", encoding="utf-8") as fh:
        for rec in csv.reader(fh):
            if not rec or all(not c.strip() for c in rec):
                continue
            if rec[0].lstrip().startswith("#"):
                continue
            if rec[0].strip().lower() == "dataset":
                continue  # header
            if len(rec) < 2:
                continue
            ds = rec[0].strip().replace(":", ".")
            vf = rec[1].strip().replace(":", ".")
            if ds and vf:
                authed.add((ds, vf))
    return authed


def read_scanned(path: Optional[str]) -> Set[str]:
    scanned: Set[str] = set()
    if not path or not os.path.exists(path):
        return scanned
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            t = line.strip().replace(":", ".")
            if t and not t.startswith("#"):
                scanned.add(t)
    return scanned


class Model:
    def __init__(
        self,
        views: List[Tuple[str, str, str, str]],
        authed: Set[Tuple[str, str]],
        scanned: Set[str],
    ):
        self.authed = authed
        self.scanned = scanned
        self.base_of: Dict[str, List[str]] = {}
        self.hosted: Dict[str, List[str]] = defaultdict(list)
        self.order: List[str] = []
        seen: Set[str] = set()
        for project, dataset, view, base_field in views:
            vf = f"{project}.{dataset}.{view}"
            if vf in seen:
                log(f"WARN: duplicate view '{vf}'; keeping the first definition.")
                continue
            seen.add(vf)
            bases: List[str] = []
            for tok in split_bases(base_field):
                nd = norm_dataset(tok, project)
                if nd and nd not in bases:
                    bases.append(nd)
            self.base_of[vf] = bases
            self.hosted[f"{project}.{dataset}"].append(vf)
            self.order.append(vf)

    def edge_status(self, view_fqn: str, base: str) -> str:
        if (base, view_fqn) in self.authed:
            return "OK"
        if self.scanned:
            return "MISSING" if base in self.scanned else "UNKNOWN"
        # No scanned-datasets info: assume everything referenced was scanned.
        return "MISSING"

    def edges(self) -> List[Tuple[str, str, str, str]]:
        out: List[Tuple[str, str, str, str]] = []
        for vf in self.order:
            for b in self.base_of[vf]:
                st = self.edge_status(vf, b)
                out.append((vf, b, "Yes" if st == "OK" else "No", st))
        return out

    def reachable_views(self, vf: str) -> Set[str]:
        """All views in vf's downstream dependency closure (incl. vf)."""
        seen: Set[str] = set()
        stack = [vf]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            for b in self.base_of.get(cur, []):
                for w in self.hosted.get(b, []):
                    if w not in seen:
                        stack.append(w)
        return seen

    def chains(self) -> List[Tuple[str, str, str, str]]:
        status_map = {(vf, b): self.edge_status(vf, b) for vf in self.order for b in self.base_of[vf]}
        out: List[Tuple[str, str, str, str]] = []
        for vf in self.order:
            miss: Set[str] = set()
            unk: Set[str] = set()
            for w in self.reachable_views(vf):
                for b in self.base_of.get(w, []):
                    st = status_map[(w, b)]
                    if st == "MISSING":
                        miss.add(f"{w}->{b}")
                    elif st == "UNKNOWN":
                        unk.add(f"{w}->{b}")
            if miss:
                status = "BROKEN"
            elif unk:
                status = "UNKNOWN"
            else:
                status = "INTACT"
            out.append((vf, status, ";".join(sorted(miss)), ";".join(sorted(unk))))
        return out


def write_csv(path: str, header: List[str], rows: List[Tuple]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def print_table(broken: List[Tuple[str, str, str, str]]) -> None:
    if not broken:
        print("All view authorizations are present. No broken links found.")
        return
    cols = ["View", "Base dataset", "Status"]
    data = [(v, b, st) for (v, b, _, st) in broken]
    widths = [len(c) for c in cols]
    for row in data:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    line = "+".join("-" * (w + 2) for w in widths)
    def fmt(row):
        return "|".join(f" {cell:<{widths[i]}} " for i, cell in enumerate(row))
    print(f"Broken / unverifiable view authorizations ({len(data)}):")
    print(line)
    print(fmt(cols))
    print(line)
    for row in data:
        print(fmt(row))
    print(line)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze BigQuery authorized-view chains.")
    p.add_argument("--views", default=os.environ.get("VIEWS_CSV", "view-authorizations/views.csv"),
                   help="Input views CSV (project,dataset,view,base_datasets).")
    p.add_argument("--authorizations", default=os.environ.get("AUTH_CSV", ""),
                   help="Fetched authorizations CSV (dataset,authorized_view).")
    p.add_argument("--scanned", default=os.environ.get("SCANNED_FILE", ""),
                   help="Optional list of scanned datasets (one project.dataset per line).")
    p.add_argument("--out-dir", default=os.environ.get("OUT_DIR", "."),
                   help="Directory for the report CSVs.")
    p.add_argument("--fail-on-broken", action="store_true",
                   help="Exit non-zero if any MISSING authorization is found.")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if not os.path.exists(args.views):
        log(f"ERROR: views CSV not found: {args.views}")
        return 1
    os.makedirs(args.out_dir, exist_ok=True)

    model = Model(
        read_views(args.views),
        read_authorizations(args.authorizations),
        read_scanned(args.scanned),
    )

    edges = model.edges()
    broken = [e for e in edges if e[3] != "OK"]
    chains = model.chains()

    edges_sorted = sorted(edges)
    write_csv(os.path.join(args.out_dir, EDGES_FILE),
              ["view", "base_dataset", "authorized", "status"], edges_sorted)
    write_csv(os.path.join(args.out_dir, BROKEN_FILE),
              ["view", "base_dataset", "authorized", "status"], sorted(broken))
    write_csv(os.path.join(args.out_dir, CHAINS_FILE),
              ["view", "chain_status", "broken_links", "unknown_links"], sorted(chains))

    n_missing = sum(1 for e in edges if e[3] == "MISSING")
    n_unknown = sum(1 for e in edges if e[3] == "UNKNOWN")
    n_broken_chains = sum(1 for c in chains if c[1] == "BROKEN")
    log(f"Views: {len(model.order)}  Edges: {len(edges)}  "
        f"MISSING: {n_missing}  UNKNOWN: {n_unknown}  Broken chains: {n_broken_chains}")
    log(f"Reports written to {args.out_dir}/")

    print_table(sorted(broken))

    if args.fail_on_broken and n_missing > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
