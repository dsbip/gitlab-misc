# airflow-rbac-rest — DAG-level RBAC for Cloud Composer via the Airflow REST API

Same capability-bundle behavior as [`../airflow-rbac-v2/`](../airflow-rbac-v2/),
but it talks to the **Airflow stable REST API** instead of the airflow CLI.

## Why REST (not the CLI)

Cloud Composer **blocks the RBAC-mutation airflow CLI commands** when run via
`gcloud composer environments run`:

```
ERROR: (gcloud.composer.environments.run) INVALID_ARGUMENT: Found 1 problem:
  1) Airflow CLI command: 'roles add-perms' is not supported in Cloud Composer.
```

`roles add-perms` / `del-perms` / `create` and the `users` role commands are all
unavailable that way. The supported path is the Airflow REST API, which Composer
exposes — and it's faster (one call per role / per user, no per-resource loop).

| Operation | REST call |
| --- | --- |
| read a role's permissions | `GET /api/v1/roles/{role}` (404 = doesn't exist) |
| create a role with perms | `POST /api/v1/roles` |
| update a role's perms | `PATCH /api/v1/roles/{role}?update_mask=actions` |
| read users + roles | `GET /api/v1/users` (paginated) |
| set a user's roles | `PATCH /api/v1/users/{username}?update_mask=roles` |

## The capability bundle (per DAG in `Dags_list`)

| # | Capability | per-DAG (`DAG:<dag>`) | global resource |
| --- | --- | --- | --- |
| 1 | view the DAG | `can_read` | — |
| 2 | view DAG code | `can_read` | `can_read` on `DAG Code` |
| 3 | view DAG runs | `can_read` | `can_read` on `DAG Runs` |
| 4 | create DAG runs (trigger) | `can_edit` | `can_create` on `DAG Runs` |
| 5 | view task instances / runs / logs | `can_read` | `can_read` on `Task Instances`, `Task Logs` |
| 6 | edit DAG runs (clear / mark) | `can_edit` | `can_edit` on `DAG Runs` |

No `can_delete` anywhere — users cannot delete the DAG. Extra actions under a
role's `roles:` are added per DAG on top (see `rbac_config.yml`). The bundle
constants live at the top of `airflow_rbac_sync.sh` (`DAG_ACTIONS`,
`GLOBAL_PERMS`).

## What each run does

1. `GET /api/v1/users` (paginated) to learn current users + their roles.
2. **Per role:** `GET /api/v1/roles/{role}`; compute the target permission set
   = keep non-DAG perms (incl. globals and anything unmanaged) + the bundle +
   the per-DAG perms (dropping stale `DAG:*`). If it differs, `POST` (new role)
   or `PATCH` (existing). Per-DAG perms are synced; global/other perms are
   preserved.
3. **Per user:** set roles = (current ∪ the role(s) that list them) − `Op` −
   `Admin`, via one `PATCH` (only if it differs).

`PATCH` replaces the whole set, so the script reads-modifies-writes to preserve
what it doesn't manage. If Airflow already matches, it makes no writes.

## Auth & prerequisites

- The runner must be authenticated (WIF or a service-account key); the script
  uses `gcloud auth print-access-token` for the bearer token and
  `gcloud composer environments describe` for the Airflow URL.
- **The identity must map to the Airflow `Admin` role** — RBAC writes require it.
- Needs `gcloud`, `curl`, `jq`; `python3`+PyYAML for the config (shell parser
  fallback otherwise).

## Running

```bash
airflow-rbac-rest/airflow_rbac_sync.sh \
  --config airflow-rbac-rest/rbac_config.yml \
  --project "$COMPOSER_PROJECT" \
  --location "$COMPOSER_LOCATION" \
  --environment "$COMPOSER_ENVIRONMENT"
# --airflow-uri https://...composer.googleusercontent.com   # skip the describe
# --dry-run                                                 # plan only
# --create-missing-users                                    # POST /users (caveat)
```

Composer caveat: a user usually appears in Airflow only after first sign-in;
unknown users are warned + skipped unless `--create-missing-users` is set.

## GitLab pipeline (hourly)

See [`.gitlab-ci.yml`](.gitlab-ci.yml). Add a **pipeline schedule** (CI/CD →
Schedules) with cron `0 * * * *`. It authenticates (WIF optional), then runs the
sync; `--dry-run` prints the plan without writing.
