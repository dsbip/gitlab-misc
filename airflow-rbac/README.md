# airflow-rbac — DAG-level RBAC sync for Cloud Composer

A GitLab stage that assigns individual user accounts (emails) **DAG-level Airflow
permissions** from a YAML config, idempotently, every hour. It drives the
Airflow CLI through `gcloud composer environments run`.

## What it does

For each custom role in the config it makes Airflow match the YAML:

1. **Creates** the custom role if it doesn't exist.
2. **Syncs the role's per-DAG permissions**: adds the `(action, DAG:<dag>)`
   permissions from the config and removes stale `DAG:*` ones. Non-DAG
   permissions on the role (e.g. `Website`, menu access) are left untouched.
3. **Grants the role** to every listed user.
4. **Removes `Op` and `Admin`** from every listed user.

If Airflow already matches the config, it changes nothing (and says so). Safe to
run every hour.

## Config

```yaml
custom_role_name:            # an Airflow custom role (one or more of these)
  roles:                     # Airflow permission actions
    - can read               #   "can read" -> can_read, "edit" -> can_edit,
    - can edit               #   "menu access" -> menu_access, ...
  Dags_list:                 # DAG ids -> resource DAG:<dag_id>
    - dag_one
    - dag_two
  User_account_list:         # user emails to grant the role to
    - user1@abc.com
    - user2@abc.com
```

The permission set for the role becomes the cross-product of `roles` × `Dags_list`
(e.g. `can_read` and `can_edit` on `DAG:dag_one` and `DAG:dag_two`). Inline lists
(`roles: [can read, can edit]`) work too. See [`rbac_config.yml`](rbac_config.yml).

## Running

```bash
airflow-rbac/airflow_rbac_sync.sh \
  --config airflow-rbac/rbac_config.yml \
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
| add / remove permissions | `roles add-perms <role> -a <action> -r DAG:<dag>` / `del-perms` |
| grant / revoke a user role | `users add-role -e <email> -r <role>` / `remove-role` |

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

## Debugging: permissions that disappear after being added

If per-DAG permissions get removed from a role "after some time", drop
[`inspect_fab_perms.py`](inspect_fab_perms.py) into the environment's `dags/`
(GCS) folder. It's a **read-only** DAG that snapshots the FAB RBAC tables
(`ab_view_menu`, `ab_permission_view`, `ab_permission_view_role`, ...) for a set
of DAGs and prints them to the task log — Composer's metadata DB is private, so
this is how you query it. Set Airflow Variables `inspect_fab_dags`
(comma-separated dag_ids) and `inspect_fab_role`, trigger it before and after the
perms vanish (or schedule it `*/5 * * * *`), and watch whether each DAG's
`ab_view_menu` **id changes** (the resource is being deleted+recreated, orphaning
role assignments) or stays constant (the assignment is being deleted directly).
