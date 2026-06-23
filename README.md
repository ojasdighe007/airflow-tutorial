# Airflow Tutorial

A learning repo for Apache **Airflow 3.1.6**. It runs the full Airflow stack  
(`CeleryExecutor` + Postgres + Redis) locally via Docker Compose, with DAGs  
authored under `dags/` using the Airflow 3.x **Task SDK** (`airflow.sdk`).

---

## Repo layout

```
.
├── docker-compose.yml      # Airflow 3.1.6 reference stack (CeleryExecutor), lightly modified
├── pyproject.toml          # uv project; pins apache-airflow==3.1.6
├── uv.lock                 # locked dependencies (committed for reproducibility)
├── .python-version         # 3.12
├── .env                    # local env vars (AIRFLOW_UID=50000)
├── dags/                   # your DAGs (bind-mounted into the containers)
├── logs/                   # task logs (bind-mounted; written by scheduler/worker/triggerer)
├── config/                 # optional airflow.cfg
├── plugins/                # custom operators / hooks
└── notes/                  # tutorial notes
```

---

## Prerequisites

- **Docker Desktop** (running) — the primary way to run the stack.
  - Recommended resources: ≥ 4 GB RAM, ≥ 2 CPUs, ≥ 10 GB free disk (the
  `airflow-init` container warns if you're below this).
  - Verify Docker works: `docker run hello-world`.
- **[uv](https://docs.astral.sh/uv/)** — for the local Python env (DAG authoring,
linting, and running `airflow` CLI commands outside the containers).
- **Python 3.12** — pinned via `.python-version`; uv will fetch it if missing.
- **Git**.

---

## 1. Clone

```bash
git clone https://github.com/ojasdighe007/airflow-tutorial.git
cd airflow-tutorial
```

## 2. Create the `.env` file

The Compose file reads `AIRFLOW_UID` (and optionally `AIRFLOW_PROJ_DIR`,
`AIRFLOW_IMAGE_NAME`, etc.). A minimal `.env` is all you need:

```bash
echo "AIRFLOW_UID=50000" > .env
```

> On **Linux**, set this to your own UID instead so bind-mounted `logs/` aren't
> owned by root: `echo "AIRFLOW_UID=$(id -u)" > .env`. On macOS/Windows the
> default `50000` is fine.

## 3. Run Airflow with Docker Compose (primary path)

First-time bootstrap (runs DB migration + creates the admin user, then exits):

```bash
docker compose up airflow-init
```

Then start the whole stack:

```bash
docker compose up -d
```

Open the UI at **[http://localhost:8080](http://localhost:8080)** and log in with the default dev
credentials `**airflow` / `airflow`**.

Useful follow-ups:

```bash
docker compose ps                 # check service health
docker compose logs -f airflow-scheduler
docker compose down               # stop, keep metadata DB
docker compose down -v            # stop AND wipe the metadata DB (full reset)
```

Your DAGs in `dags/` are bind-mounted, so editing a file is picked up by the
`airflow-dag-processor` within seconds — no rebuild needed.

## 4. (Optional) Local Python env with uv

Useful for editor autocompletion, linting, and running `airflow` CLI commands
against a local SQLite metadata DB (separate from the Dockerized Postgres).

```bash
uv sync                                  # creates .venv from pyproject.toml + uv.lock
source .venv/bin/activate
airflow version                          # confirms apache-airflow==3.1.6
```

> Note: the local venv defaults to a SQLite metadata DB and `SequentialExecutor`,
> which is fine for `airflow dags list` / parsing checks but **not** for actually
> running the DAGs — use the Docker stack for that. Running CLI DB commands (e.g.
> `airflow dags reserialize`) against a fresh, unmigrated SQLite DB will fail with
> `no such table: ...`; run `airflow db migrate` first if you need a local DB.

---

## Verifying the first DAG

1. With the stack up, the example DAGs are hidden
  (`AIRFLOW__CORE__LOAD_EXAMPLES=false`), so the UI shows only `first_dag`.
2. Trigger `first_dag` from the UI (it has `schedule=None`, so it only runs on
  manual trigger).
3. Watch `first_task → second_task → third_task` run in order.
4. Task logs land under
  `logs/dag_id=first_dag/run_id=.../task_id=.../attempt=1.log` on the host.

---

## Topics covered

Each DAG in `dags/` has a matching write-up in `notes/`.

| DAG (`dags/`) | Topic | Notes (`notes/`) |
| --- | --- | --- |
| `1_first_dag.py`, `2_dag_versioning.py` | First DAG, versioning | `1_first_dag.md`, `2_airflow_3x_architecture.md` |
| `03_operators.py` | Operators (`@task.python` / `@task.bash`) | - |
| `04_xcoms_auto.py`, `05_xcoms_kwargs.py` | XComs (TaskFlow vs manual), `**kwargs` context | `3_xcoms.md` |
| `06_parallel_tasks.py`, `07_conditional_branches.py` | Parallel fan-out, branching, trigger rules | `4_trigger_rules.md`, `5_parallel_and_branching.md` |
| `08_schedule_preset.py`, `09_schedule_cron.py`, `10_schedule_delta.py`, `12_special_dates.py` | Scheduling: presets, cron, delta, events | `6_scheduling.md` |
| `11_incremental_load.py` | Incremental loads via `data_interval_*` | `7_incremental_load.md` |
| `13_sensors.py` | Sensors (poke vs reschedule) | `8_sensors.md` |
| `14_dynamic_task_mapping.py` | Dynamic task mapping (`.expand` / `.partial`) | `9_dynamic_task_mapping.md` |
| `15_taskgroups.py` | TaskGroups | `10_taskgroups.md` |
| `16_connections_hooks.py` | Connections, Hooks, Variables | `11_connections_hooks.md` |
| `17_callbacks_retries.py`, `18_pools_concurrency.py` | Retries/backoff, callbacks, pools, concurrency | `12_reliability.md` |
| `19_trigger_rules.py` | Trigger rules (`all_done`, `one_failed`, branch joins, ...) | `4_trigger_rules.md`, `13_trigger_rules.md` |

> Some DAGs (connections/hooks, sensors, pools) are **illustrative** - they reference
> connections, files, or pools that must be configured in Airflow before they run.

---

## Managing dependencies

This is a uv project. To add a package:

```bash
uv add <package>        # updates pyproject.toml + uv.lock
```

If you want the new dependency available **inside** the Airflow containers, add it
to the image (rebuild) or use `_PIP_ADDITIONAL_REQUIREMENTS` in Compose for quick
local experiments (not recommended for anything persistent).