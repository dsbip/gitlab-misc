# airflow-rbac-v2 — DAG-level RBAC sync for Cloud Composer (capability bundle)

A GitLab stage that assigns individual user accounts (emails) a fixed set of
**DAG-level Airflow capabilities** from a YAML config, idempotently, every hour.
It drives the Airflow CLI through `gcloud composer environments run`.

> **v2 vs v1:** [`../airflow-rbac/`](../airflow-rbac/) grants exactly the actions
> you list under `roles:` on each `DAG:<dag_id>`. **v2** instead grants a fixed
> capability **bundle** per DAG (below) — the common "operator" set — and treats
> `roles:` as optional extra actions.

## The capability bundle (granted for each DAG in `Dags_list`)

| # | Capability | per-DAG (`DAG:<dag>`) | global resource |
| --- | --- | --- | --- |
| 1 | view the DAG | `can_read` | — |
| 2 | view DAG code | `can_read` | `can_read` on `DAG Code` |
| 3 | view DAG runs | `can_read` | `can_read` on `DAG Runs` |
| 4 | create DAG runs (trigger) | `can_edit` | `can_create` on `DAG Runs` |
| 5 | view task instances / runs / logs | `can_read` | `can_read` on `Task Instances`, `Task Logs` |
| 6 | edit DAG runs (clear / mark) | `can_edit` | `can_edit` on `DAG Runs` |

Airflow gates a DAG's runs/tasks on **both** the per-DAG `DAG:<dag_id>`
permission (which scopes access to that specific DAG) **and** the global
resource permission (which enables the UI page / REST endpoint). So the bundle
grants, per role:

- **per DAG:** `can_read` + `can_edit` on `DAG:<dag_id>` (synced — added/removed
  to match the config),
- **once (global):** `can_read` on `DAG Code`, `Task Instances`, `Task Logs`;
  `can_read` + `can_create` + `can_edit` on `DAG Runs` — **add-only**, never
  removed, and other permissions on the role are left untouched.

These constants live at the top of
[`airflow_rbac_sync.sh`](airflow_rbac_sync.sh) (`DAG_ACTIONS`, `GLOBAL_PERMS`) —
edit them to change the bundle.

## What it does per run

1. **Creates** the custom role if it doesn't exist.
2. **Syncs per-DAG permissions** to the bundle (+ any extra `roles:` actions):
   adds missing, removes stale `DAG:*` ones.
3. **Ensures the global bundle permissions** exist on the role (add-only).
4. **Grants the role** to every listed user.
5. **Removes `Op` and `Admin`** from every listed user.

If Airflow already matches, it changes nothing (prints `already in sync`). Safe
to run hourly.

## Config

```yaml
custom_role_name:            # an Airflow custom role (one or more of these)
  roles:                     # OPTIONAL extra actions, added per DAG on top of
    - can delete             # the bundle (e.g. "can delete"); omit for bundle only
  Dags_list:                 # DAG ids the bundle applies to
    - dag_one
    - dag_two
  User_account_list:         # user emails to grant the role to
    - user1@abc.com
    - user2@abc.com
```

Inline lists (`roles: [can delete]`) work too. See
[`rbac_config.yml`](rbac_config.yml).

## Running

```bash
airflow-rbac-v2/airflow_rbac_sync.sh \
  --config airflow-rbac-v2/rbac_config.yml \
  --project  "$COMPOSER_PROJECT" \
  --location "$COMPOSER_LOCATION" \
  --environment "$COMPOSER_ENVIRONMENT"
# --dry-run                 show the planned changes without applying them
# --create-missing-users    create users that aren't in Airflow yet (see caveat)
```

Requires `gcloud` + `jq`; `python3`+PyYAML is used to read the config when
present, otherwise a built-in shell YAML parser is used (identical result).

### Airflow CLI commands used

Everything goes through `gcloud composer environments run <env> --location <loc>
<subcommand> -- <args>`:

| Purpose | Airflow command |
| --- | --- |
| read roles / permissions | `roles list -o json`, `roles list -p -o json` |
| read users + their roles | `users list -o json` |
| create a role | `roles create <role>` |
| add / remove permissions | `roles add-perms <role> -a <action> -r <resource...>` / `del-perms` |
| grant / revoke a user role | `users add-role -e <email> -r <role>` / `remove-role` |

`add-perms` batches all resources for a given action into one call, so the
global bundle for an action is added in a single call.

### Composer caveat

In Cloud Composer, a user typically appears in Airflow only after their **first
sign-in**. `users add-role` on an unknown user fails, so such users are
**warned and skipped** unless you pass `--create-missing-users` (which runs
`users create`; note that manually created users can interact oddly with
Composer's Google-based auto-provisioning — prefer having users sign in first).

## GitLab pipeline (hourly)

See [`.gitlab-ci.yml`](.gitlab-ci.yml). GitLab doesn't run jobs hourly by itself
— add a **pipeline schedule** (CI/CD → Schedules) with cron `0 * * * *`. The job
runs on scheduled pipelines and offers a manual "play" button otherwise.

Set CI/CD variables `COMPOSER_PROJECT`, `COMPOSER_LOCATION`,
`COMPOSER_ENVIRONMENT`. Authentication uses the runner's gcloud credentials, or
Workload Identity Federation if `WIF_PROVIDER_URL` / `WIF_SERVICE_ACCOUNT` /
`WIF_AUDIENCE` are set (same model as [`../single-run/`](../single-run/)).

## Idempotency

Every mutating call is guarded by the current Airflow state fetched at the start,
so re-runs are no-ops once in sync. The job prints `already in sync; no changes
made` when nothing needed doing, and a per-change log otherwise. `--dry-run`
prints the plan (`[plan] airflow ...`) without touching Airflow.
