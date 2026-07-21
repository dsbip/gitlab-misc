# view-authorizations — BigQuery authorized-view chain checker

Finds views whose **authorized-view chain is missing or broken** across multiple
GCP projects, and publishes the result as a downloadable (and emailable) report.

## Background — BigQuery authorized views

To let a view `V` read a dataset `B` without the caller holding access to `B`,
`V` must be added to `B`'s access list — *"V is authorized on B"*. When views are
layered (a view on a view on a view), **every** link needs its own
authorization:

```
proj4.ds4.view4  reads  proj3.ds3   ->  ds3 must authorize view4
proj3.ds3.view3  reads  proj2.ds2   ->  ds2 must authorize view3
proj2.ds2.view2  reads  proj1.ds1   ->  ds1 must authorize view2
```

If any link is missing, everything above it in the chain silently breaks. This
tool checks the whole chain and highlights the breaks.

## Inputs

| File | What it is |
| --- | --- |
| [`views.csv`](views.csv) | `project,dataset,view,base_datasets` — every view and the datasets its SQL reads. `base_datasets` may list several (separated by `; , \| ` or spaces; a bare `dataset` defaults to the row's project). |
| [`projects.yml`](projects.yml) | Projects to scan + the **names** of the CI/CD variables holding each project's WIF provider / service account (values live in GitLab, not git). |

`base_datasets` robustness: everything from the 4th CSV column onward is treated
as the base list, so both `"proj.a,proj.b"` (quoted) and `proj.a,proj.b`
(unquoted) work.

## What it does

1. **(placeholder)** optionally trigger a BigQuery job to (re)generate
   `views.csv` at runtime — disabled by default (`RUN_BQ_JOB=false`), wired up
   later with the real SQL, with its own placeholder WIF variables.
2. For each project: authenticate with **Workload Identity Federation**, run
   `bq` to read every dataset's authorized views, then revoke. Produces
   `authorizations_fetched.csv` + `scanned_datasets.txt`.
3. **Analyze**: for every `(view, base_dataset)` pair, is the view authorized on
   that dataset? Walk each view's downstream chain and flag broken links.

## Outputs (the artifact)

| File | Contents |
| --- | --- |
| `view_auth_edges.csv` | every `view,base_dataset,authorized,status` |
| `view_auth_broken.csv` | just the `MISSING` / `UNKNOWN` rows |
| `view_auth_chains.csv` | per view: `chain_status` (INTACT/BROKEN/UNKNOWN) + the broken/unknown links |
| `authorizations_fetched.csv`, `scanned_datasets.txt` | the raw fetched ACLs |
| `SUMMARY.txt` | short summary + artifact links, for the email |

`status`: **OK** authorized · **MISSING** base scanned but authorization absent ·
**UNKNOWN** base dataset not scanned (its project isn't in `projects.yml`), so it
couldn't be verified.

A broken table is also printed to the job log, e.g.:

```
Broken / unverifiable view authorizations (1):
-----------------------------------+----------------------------+---------
 View                              | Base dataset               | Status
-----------------------------------+----------------------------+---------
 composer-project-b.marts.orders_v | composer-project-a.curated | MISSING
-----------------------------------+----------------------------+---------
```

## Python and shell versions

The analysis exists twice, byte-for-byte identical output, so it runs with or
without Python on the runner:

- [`analyze_view_auth.py`](analyze_view_auth.py) — Python.
- [`analyze_view_auth.sh`](analyze_view_auth.sh) — pure bash + awk/sort.

[`check_view_authorizations.sh`](check_view_authorizations.sh) orchestrates the
fetch and calls whichever analyzer you pick (`--analyzer python|shell|auto`).

## Running

```bash
# In CI (Linux) the fetch runs for real; locally you can reuse a fetched file:
view-authorizations/check_view_authorizations.sh \
  --config view-authorizations/projects.yml \
  --views  view-authorizations/views.csv \
  --out-dir view-auth-report            # add --skip-fetch to reuse an existing
                                        # authorizations_fetched.csv (no gcloud/bq)

# Analyzer only (no cloud), e.g. to re-check after editing the CSVs:
python view-authorizations/analyze_view_auth.py \
  --views views.csv --authorizations authorizations_fetched.csv \
  --scanned scanned_datasets.txt --out-dir report
```

Requirements: `gcloud` + `bq` + `jq` for the fetch; `python3` (or the shell
analyzer) for the analysis. `--fail-on-broken` makes the job exit non-zero when
any authorization is MISSING.

## GitLab pipeline + emailing the report

See [`.gitlab-ci.yml`](.gitlab-ci.yml). WIF setup (provider/SA variables,
`WIF_AUDIENCE`, one shared `id_token`) mirrors [`../single-run/`](../single-run/).

**Emailing the artifact, natively:** GitLab does not attach artifacts to emails
itself, but it emails pipeline results natively:

1. **Settings → Integrations → Pipeline emails** — add recipients; GitLab emails
   every pipeline result to them.
2. This job writes `SUMMARY.txt` (with the broken list and stable artifact
   download URLs) so recipients click straight from the email to the report.
3. Add a **pipeline schedule** for a regular cadence (the schedule owner is
   emailed on failure by default; Pipeline emails covers success too).

If you truly need the CSV **attached**, the optional `email-report` job sends it
via an SMTP relay (`swaks`) — that needs `SMTP_*` variables and is not a native
GitLab feature, so prefer the Pipeline emails route above.
