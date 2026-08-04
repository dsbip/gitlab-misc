"""
inspect_fab_perms - a read-only debug DAG for airflow-rbac.

Snapshots the FAB (Flask-AppBuilder) RBAC tables for a set of DAGs so you can
see, over time, whether a custom role keeps or loses its per-DAG can_read /
can_edit permissions - and whether the underlying DAG resource (the
ab_view_menu row) is being deleted+recreated ("churn").

Why this exists: in Cloud Composer the Airflow metadata DB (a private Cloud SQL
instance) can't be queried directly, so this runs the query from inside Airflow
over the metadata session and prints the result to the task log.

Configure via Airflow Variables (Admin > Variables) - no code edit needed:
  inspect_fab_dags : comma-separated dag_ids to inspect, e.g. "dag1,dag2,dag3"
  inspect_fab_role : (optional) a role to summarize, e.g. "cmp_asg_group"

Usage:
  * Trigger it right after your sync script adds the permissions, and again
    after they vanish, then compare the two task logs; OR
  * change schedule_interval to "*/5 * * * *" to build a timeline and grep the
    logs for the exact moment things change.

How to read it:
  * If a DAG's VIEW_MENU id CHANGES between runs -> the DAG:<dag_id> resource
    was deleted+recreated (churn); that orphans every role's assignment on it.
  * If the id stays constant but the role's PERM rows disappear -> the
    assignment (ab_permission_view_role) is being deleted directly.

This DAG never modifies permissions. Written for Composer 2 (PostgreSQL) +
Airflow 2.6.x. Drop it in the environment's dags/ (GCS) folder.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from sqlalchemy import bindparam, text


@provide_session
def snapshot_fab_perms(session=None, **_):
    dag_ids = [d.strip() for d in Variable.get("inspect_fab_dags", default_var="").split(",") if d.strip()]
    role = Variable.get("inspect_fab_role", default_var="").strip()
    if not dag_ids:
        print("Set the Airflow Variable 'inspect_fab_dags' to a comma-separated "
              "list of dag_ids (e.g. 'dag1,dag2,dag3'); nothing to inspect.")
        return

    names = [f"DAG:{d}" for d in dag_ids]
    ts = datetime.utcnow().isoformat()

    # 1) view_menu id per DAG resource. Watch this id across runs: if it CHANGES,
    #    the DAG:<dag_id> resource was deleted+recreated (churn), which orphans
    #    every role's assignment on it. A missing row = resource absent entirely.
    id_q = text("SELECT id, name FROM ab_view_menu WHERE name IN :names ORDER BY name") \
        .bindparams(bindparam("names", expanding=True))
    id_rows = session.execute(id_q, {"names": names}).fetchall()
    present = {r.name for r in id_rows}
    print(f"[{ts}] VIEW_MENU IDS: " + (", ".join(f"{r.name}=#{r.id}" for r in id_rows) or "(none)"))
    missing = [n for n in names if n not in present]
    if missing:
        print(f"[{ts}] NO ab_view_menu ROW (resource absent) for: {', '.join(missing)}")

    # 2) which roles currently hold which action on those DAG resources.
    perm_q = text(
        """
        SELECT vm.id AS view_menu_id, vm.name AS resource,
               a.name AS action, r.name AS role
        FROM ab_view_menu vm
        LEFT JOIN ab_permission_view pv       ON pv.view_menu_id = vm.id
        LEFT JOIN ab_permission a             ON a.id = pv.permission_id
        LEFT JOIN ab_permission_view_role pvr ON pvr.permission_view_id = pv.id
        LEFT JOIN ab_role r                   ON r.id = pvr.role_id
        WHERE vm.name IN :names
        ORDER BY vm.name, a.name, r.name
        """
    ).bindparams(bindparam("names", expanding=True))
    rows = session.execute(perm_q, {"names": names}).fetchall()
    for row in rows:
        m = row._mapping
        print(f"[{ts}] PERM resource={m['resource']} vm_id={m['view_menu_id']} "
              f"action={m['action']} role={m['role']}")

    # 3) compact per-DAG summary for the role of interest.
    if role:
        held = {
            (row._mapping["resource"], row._mapping["action"])
            for row in rows if row._mapping["role"] == role
        }
        print(f"[{ts}] SUMMARY for role '{role}':")
        for d in dag_ids:
            r = f"DAG:{d}"
            print(f"[{ts}]   {r}: "
                  f"can_read={'Y' if (r, 'can_read') in held else 'N'} "
                  f"can_edit={'Y' if (r, 'can_edit') in held else 'N'}")


with DAG(
    dag_id="inspect_fab_perms",
    description="Debug: snapshot FAB RBAC tables for given DAGs (read-only).",
    schedule_interval=None,  # set to "*/5 * * * *" to build a timeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["debug", "rbac"],
) as dag:
    PythonOperator(task_id="snapshot", python_callable=snapshot_fab_perms)
