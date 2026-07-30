# gitlab-misc

Miscellaneous scripts and GitLab CI pipelines for operational tasks.

## Contents

| Path | Description |
| --- | --- |
| [`composer-dag-inventory/`](composer-dag-inventory/) | Inventory all DAGs across every running Cloud Composer environment in a GCP project and export a CSV. Two implementations: Python (Airflow REST API) and Bash (gcloud CLI). |
| [`.gitlab-ci.yml`](.gitlab-ci.yml) | Pipeline that runs the inventory and publishes the CSV as a downloadable artifact. |
| [`single-run/`](single-run/) | Loop over multiple GCP projects, authenticate to each with Workload Identity Federation, run the DAG inventory, and append everything into one combined CSV artifact. Optional per-project / per-instance runtime filters. |
| [`view-authorizations/`](view-authorizations/) | Check BigQuery authorized-view chains across projects: fetch each dataset's authorized views over WIF, then flag views whose authorization chain is missing or broken. Python + shell analyzers; emailable report. |
| [`airflow-rbac/`](airflow-rbac/) | Hourly GitLab stage that assigns user emails DAG-level Airflow (Cloud Composer) permissions from a YAML config, idempotently, and strips `Op`/`Admin` from those users. gcloud + Airflow CLI. |
| [`airflow-rbac-v2/`](airflow-rbac-v2/) | Same as `airflow-rbac/` but grants a fixed per-DAG capability **bundle** (view DAG/code/runs, create & edit DAG runs, view task instances & logs) — a mix of per-DAG and global Airflow permissions. |

---

## Composer DAG Inventory

Discovers every **RUNNING** Cloud Composer environment in the target GCP
project(s) and writes one CSV row per DAG. There are two interchangeable
implementations — pick whichever fits your runner:

| Script | Approach | Best for |
| --- | --- | --- |
| [`list_composer_dags.py`](composer-dag-inventory/list_composer_dags.py) | Airflow **stable REST API** (`/api/v1/dags`) via Application Default Credentials. | One bulk call per environment; needs `google-auth`. |
| [`list_composer_dags.sh`](composer-dag-inventory/list_composer_dags.sh) | Same Airflow **stable REST API**, via `curl` with a token from `gcloud auth print-access-token`. | No Python deps — just `gcloud` + `curl` + `jq`. |

Both hit the **same bulk endpoint** (`/api/v1/dags`, one paginated call per
environment — so runtime does **not** grow per-DAG), emit the **same columns**,
read only from Airflow (never from the GCS bucket), and need only
`roles/composer.user`. They differ only in language/dependencies:

- **Python** authenticates with `google-auth` (Application Default Credentials).
- **Bash** authenticates with an access token from `gcloud auth print-access-token`
  and parses the JSON with `jq`.

### Output columns

| Column | Meaning |
| --- | --- |
| `Composer` | Composer environment name. |
| `Project` | GCP project ID. |
| `DAG_Name` | Airflow `dag_id`. |
| `Dag_path` | Source file location (`fileloc`) inside the environment. |
| `Active?` | `Yes` if the DAG is unpaused, `No` if paused. |
| `Scheduled` | `Yes` if the DAG has a schedule, `No` if manual/trigger-only. |
| `Scheduled Time` | Cron expression or interval summary (blank if not scheduled). |

### How it works

1. Enumerates Composer environments with `gcloud composer environments list`
   for the target project(s), scanning **`europe-west2`** by default (override
   with `--locations` / `COMPOSER_LOCATIONS`).
2. For each **RUNNING** environment, reads `config.airflowUri`.
3. Calls the Composer 2 Airflow **stable REST API** (`/api/v1/dags`, paginated) —
   one bulk call per environment, no IAP client ID juggling required. Python uses
   Application Default Credentials; Bash uses `curl` with a
   `gcloud auth print-access-token` bearer token. Neither reads from GCS.

### Requirements

- Python 3.8+
- [`google-cloud-sdk`](https://cloud.google.com/sdk) (`gcloud` on `PATH`)
- `pip install -r composer-dag-inventory/requirements.txt`
- A credential (service-account key or Workload Identity) with, at minimum,
  `roles/composer.user` on the target project(s).

### Run locally

```bash
pip install -r composer-dag-inventory/requirements.txt

# Authenticate (either works):
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
# ...or: gcloud auth application-default login

# Scan a project (region defaults to europe-west2):
python composer-dag-inventory/list_composer_dags.py my-gcp-project --output composer_dags.csv

# Override region(s) or scan several projects:
python composer-dag-inventory/list_composer_dags.py \
  --projects my-proj-a,my-proj-b \
  --locations europe-west2,europe-west1 \
  --output composer_dags.csv
```

Or the dependency-free Bash version (needs `gcloud`, `curl`, and `jq`):

```bash
gcloud auth activate-service-account --key-file=/path/to/sa-key.json
chmod +x composer-dag-inventory/list_composer_dags.sh

composer-dag-inventory/list_composer_dags.sh my-gcp-project --output composer_dags.csv
```

The project id can be passed positionally (as above) or via `--projects` /
`GCP_PROJECTS`; if omitted it falls back to the active gcloud project. Region
defaults to **`europe-west2`** — override with `--locations` /
`COMPOSER_LOCATIONS` (comma-separated). `OUTPUT_CSV` is also honored.

To restrict the run to a single Composer environment, pass `--environment`
(alias `--composer`); by default every RUNNING environment is inventoried:

```bash
composer-dag-inventory/list_composer_dags.sh my-gcp-project --environment my-composer-env
```

### Run in GitLab CI

1. In **Settings → CI/CD → Variables**, add:
   - `GCP_SA_KEY` — **type `File`**, value = the service-account JSON key.
   - *(optional)* `GCP_PROJECTS` — comma-separated project IDs.
   - *(optional)* `COMPOSER_LOCATIONS` — comma-separated regions.
2. Run the pipeline (manual "play" on the `composer-dag-inventory` job, or a
   scheduled pipeline).
3. Download `composer_dags.csv` from the job's **Artifacts** panel.

See [`.gitlab-ci.yml`](.gitlab-ci.yml) for the full job definition.
