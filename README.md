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

## Managing dependencies

This is a uv project. To add a package:

```bash
uv add <package>        # updates pyproject.toml + uv.lock
```

If you want the new dependency available **inside** the Airflow containers, add it
to the image (rebuild) or use `_PIP_ADDITIONAL_REQUIREMENTS` in Compose for quick
local experiments (not recommended for anything persistent).