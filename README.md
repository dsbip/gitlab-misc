# gitlab-misc

Miscellaneous scripts and GitLab CI pipelines for operational tasks.

## Contents

| Path | Description |
| --- | --- |
| [`composer-dag-inventory/`](composer-dag-inventory/) | Inventory all DAGs across every running Cloud Composer environment in a GCP project and export a CSV. Two implementations: Python (Airflow REST API) and Bash (gcloud CLI). |
| [`.gitlab-ci.yml`](.gitlab-ci.yml) | Pipeline that runs the inventory and publishes the CSV as a downloadable artifact. |

---

## Composer DAG Inventory

Discovers every **RUNNING** Cloud Composer environment in the target GCP
project(s) and writes one CSV row per DAG. There are two interchangeable
implementations — pick whichever fits your runner:

| Script | Approach | Best for |
| --- | --- | --- |
| [`list_composer_dags.py`](composer-dag-inventory/list_composer_dags.py) | Airflow **stable REST API** (`/api/v1/dags`) via Application Default Credentials. | Accurate schedule straight from Airflow's runtime; needs `google-auth`. |
| [`list_composer_dags.sh`](composer-dag-inventory/list_composer_dags.sh) | `gcloud composer environments run … dags list` + parses DAG source from the GCS bucket. | No Python deps — just `gcloud` + `jq`. |

Both emit the **same columns**. The differences:

- **Python** reads `is_paused` and `schedule_interval` from Airflow's runtime
  metadata (authoritative for `Active?` / `Scheduled` / `Scheduled Time`) and
  parses source only for `Email Alert`.
- **Bash** reads `Active?` from `airflow dags list`, but derives
  `Scheduled` / `Scheduled Time` **and** `Email Alert` by parsing DAG source
  from the environment's GCS bucket (heuristic; unreadable source → `Unknown`).
  It needs read access to that bucket (`roles/composer.environmentAndStorageObjectViewer`)
  and depends on `gcloud` + `jq` only.

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
| `Email Alert` | Alert recipient address(es) parsed from the DAG source; `No` if none; `Yes (no address)` if `email_on_failure=True` but no address found; `Unknown` if source could not be read. |

### How it works

1. Enumerates Composer environments per region using `gcloud composer environments list`.
2. For each **RUNNING** environment, reads `config.airflowUri` from the describe output.
3. Calls the Composer 2 Airflow **stable REST API** (`/api/v1/dags`, paginated)
   using Application Default Credentials — no IAP client ID juggling required.
4. Reads the DAG source (`/api/v1/dagSources/{file_token}`, cached per file) to
   detect configured email alerts.

> **Note on `Email Alert`:** Airflow's REST API does not expose task-level email
> settings, so this column is derived by parsing DAG source for `email=` /
> `'email':` assignments and `email_on_failure=True`. Treat it as a best-effort
> signal, not an authoritative audit.

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

# Scan the active gcloud project across all regions:
python composer-dag-inventory/list_composer_dags.py --output composer_dags.csv

# Or scope it explicitly (much faster):
python composer-dag-inventory/list_composer_dags.py \
  --projects my-proj-a,my-proj-b \
  --locations us-central1,europe-west1 \
  --output composer_dags.csv
```

Or the dependency-free Bash version (needs `gcloud` and `jq`):

```bash
gcloud auth activate-service-account --key-file=/path/to/sa-key.json
chmod +x composer-dag-inventory/list_composer_dags.sh

composer-dag-inventory/list_composer_dags.sh \
  --projects my-proj-a,my-proj-b \
  --locations us-central1,europe-west1 \
  --output composer_dags.csv
```

Configuration can also come from environment variables: `GCP_PROJECTS`,
`COMPOSER_LOCATIONS`, `OUTPUT_CSV` (both scripts honor them).

> **Tip:** scanning *all* compute regions issues one `gcloud composer
> environments list` per region. Set `--locations` / `COMPOSER_LOCATIONS` to the
> regions you actually use to keep runs quick.

### Run in GitLab CI

1. In **Settings → CI/CD → Variables**, add:
   - `GCP_SA_KEY` — **type `File`**, value = the service-account JSON key.
   - *(optional)* `GCP_PROJECTS` — comma-separated project IDs.
   - *(optional)* `COMPOSER_LOCATIONS` — comma-separated regions.
2. Run the pipeline (manual "play" on the `composer-dag-inventory` job, or a
   scheduled pipeline).
3. Download `composer_dags.csv` from the job's **Artifacts** panel.

See [`.gitlab-ci.yml`](.gitlab-ci.yml) for the full job definition.
