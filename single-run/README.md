# single-run — multi-project Composer DAG inventory (WIF)

One GitLab CI job that walks a list of GCP projects, authenticates to each with
**Workload Identity Federation**, runs the Composer DAG inventory, revokes the
credentials, and appends every project's rows into **one combined CSV artifact**.

| File | Purpose |
| --- | --- |
| [`.gitlab-ci.yml`](.gitlab-ci.yml) | The pipeline job (`id_tokens`, artifact, deps). |
| [`projects.yml`](projects.yml) | YAML array of projects + WIF connection details. |
| [`run_multi_project_inventory.sh`](run_multi_project_inventory.sh) | The loop: auth → inventory → append → revoke. |

It reuses [`../composer-dag-inventory/list_composer_dags.sh`](../composer-dag-inventory/list_composer_dags.sh)
to do the actual DAG listing, so the CSV columns stay identical:

```
Composer,Project,DAG_Name,Dag_path,Active?,Scheduled,Scheduled Time
```

## Per-project sequence

1. Resolve that project's GitLab OIDC token (`id_token_var`).
2. `gcloud iam workload-identity-pools create-cred-config` → external-account config.
3. `gcloud auth login --cred-file=…` + `gcloud config set project`.
4. Run the inventory script into a temp CSV.
5. Append its data rows to the combined CSV (header written once, taken from the
   inventory script's own output so it can never drift).
6. `gcloud auth revoke --all`, unset project, shred the token/cred files.

A failure in one project is logged and skipped — the remaining projects still
run. The job exits non-zero if **any** project failed, but the artifact is still
published (`when: always`), so you get partial results plus a visible failure.

## Narrowing the run at runtime

Two optional pipeline variables (prefilled on GitLab's **Run pipeline** form)
scope a single run. Leave both blank for the full sweep.

| `PROJECT_ID` | `COMPOSER_INSTANCE` | What runs |
| --- | --- | --- |
| *(blank)* | *(blank)* | Every project in `projects.yml`, all Composer instances |
| `my-proj` | *(blank)* | **Only `my-proj`**, all of its Composer instances |
| `my-proj` | `my-env` | **Only `my-env`** inside `my-proj` |

Notes:

- `PROJECT_ID` must still be listed in `projects.yml` — that's where its WIF
  provider and service account come from. An unknown id fails fast and prints
  the known project ids.
- `COMPOSER_INSTANCE` without `PROJECT_ID` is rejected (an instance name is only
  meaningful within one project).
- Only the selected project is authenticated; the others are never touched.
- If the named instance doesn't exist, you get a warning and a header-only CSV
  (the run itself still succeeds).
- Values are whitespace-trimmed, so stray spaces pasted into the form are
  harmless; an all-whitespace value counts as blank.
- Duplicate `project_id` entries in `projects.yml` are warned about and only the
  first one is used.

The same filters work on the command line:

```bash
single-run/run_multi_project_inventory.sh --project my-proj
single-run/run_multi_project_inventory.sh --project my-proj --composer my-env
```

and directly on the inventory script:

```bash
composer-dag-inventory/list_composer_dags.sh my-proj --environment my-env
```

## Wiring it up

### 1. Point GitLab at this config

GitLab reads one CI config per project, so either:

- **Settings → CI/CD → General pipelines → CI/CD configuration file** →
  `single-run/.gitlab-ci.yml`, or
- add to the root `.gitlab-ci.yml`:
  ```yaml
  include:
    - local: single-run/.gitlab-ci.yml
  ```

### 2. Add your projects to `projects.yml`

```yaml
projects:
  - project_id: composer-project-a
    wif_provider_url: projects/111111111111/locations/global/workloadIdentityPools/gitlab-pool/providers/gitlab
    wif_service_account: composer-inventory@composer-project-a.iam.gserviceaccount.com
    id_token_var: GCP_ID_TOKEN_A     # optional, defaults to GCP_ID_TOKEN
    location: europe-west2           # optional, defaults to europe-west2
```

`wif_provider_url` is the provider **resource name** (no scheme) — exactly what
`gcloud iam workload-identity-pools create-cred-config` expects.

### 3. Declare a matching `id_tokens` entry

> **This is the one manual step you cannot avoid.** GitLab resolves `id_tokens`
> when the pipeline is *created*, so they cannot be generated inside a loop.
> Every distinct `id_token_var` in `projects.yml` must be declared in
> `.gitlab-ci.yml`:

```yaml
  id_tokens:
    GCP_ID_TOKEN_A:
      aud: https://iam.googleapis.com/projects/111111111111/locations/global/workloadIdentityPools/gitlab-pool/providers/gitlab
```

- Projects sharing **one** WIF provider → omit `id_token_var` (defaults to
  `GCP_ID_TOKEN`) and declare only that single token.
- Projects with **different** providers → one `id_tokens` entry each.

The `aud` must be an audience the provider accepts (conventionally the provider
resource prefixed with `https://iam.googleapis.com/`).

### 4. GCP side, per project

- The WIF provider must trust this GitLab project — e.g. an attribute condition on
  `assertion.project_path == "your-group/your-repo"`.
- The impersonated service account needs **`roles/composer.user`**.
- The service account must allow the WIF principal to impersonate it
  (`roles/iam.workloadIdentityUser`).

## Running locally

```bash
export GCP_ID_TOKEN_A=... GCP_ID_TOKEN_B=...   # OIDC tokens
single-run/run_multi_project_inventory.sh \
  --config single-run/projects.yml \
  --output composer_dags_all_projects.csv \
  --inventory composer-dag-inventory/list_composer_dags.sh
```

Paths default to repo-root-relative, matching how CI invokes it. Requires
`gcloud`, `python3` + PyYAML (to read the config), plus `jq` and `curl` for the
inventory script itself.
