# `docker-compose.yml` — Origin, Modifications, and Structure

This note documents the `docker-compose.yml` at the root of this repo: where it came from, what was changed from the upstream reference, and what each section actually does.

---

## 1. Origin

The file was **not written from scratch**. It is the official reference compose file shipped by the Apache Airflow project for Airflow **3.1.6**, with a few small local modifications.

- Upstream source: <https://airflow.apache.org/docs/apache-airflow/3.1.6/docker-compose.yaml>
- Base image used: `apache/airflow:3.1.6` (referenced via `AIRFLOW_IMAGE_NAME`)
- License header (Apache 2.0) at the top is preserved verbatim from upstream.

The upstream file is intended for **local development only** — it explicitly warns against production use. This project inherits that scope.

---

## 2. Modifications vs. upstream Airflow 3.1.6

Diffing the local file against the upstream reference yields exactly three differences:

```diff
@@ x-airflow-common.environment @@
-    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
+    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

@@ services.postgres @@
     volumes:
       - postgres-db-volume:/var/lib/postgresql/data
+    ports:
+      - "5432:5432"
     healthcheck:

@@ end of file @@
-  postgres-db-volume:
+  postgres-db-volume:   # no trailing newline
```

### 2.1 `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`

- **Upstream value:** `'true'` — loads the bundled "example DAGs" that ship with Airflow on first start.
- **Local value:** `'false'` — keeps the UI clean so only DAGs from `./dags` show up.
- **Why:** This is a learning repo where you want to see *your* DAGs, not dozens of Airflow tutorial DAGs cluttering the home view.
- **Effect:** Takes effect on the next `airflow-init` / scheduler reload. If example DAGs were previously loaded into the metadata DB, they remain there until you delete them from the UI or wipe the `postgres-db-volume`.

### 2.2 Postgres port `5432:5432` published to the host

- **Upstream:** Postgres is only reachable inside the Docker network (no `ports:` mapping). Other Airflow containers reach it via the service name `postgres`.
- **Local:** Adds
  ```yaml
  ports:
    - "5432:5432"
  ```
  so the metadata database is reachable on `localhost:5432` from the host machine.
- **Why:** Convenient for using DB clients (DBeaver, `psql`, JetBrains DataGrip, etc.) to inspect Airflow's metadata DB (DAG runs, task instances, XComs, connections, etc.) without `docker exec`-ing into the container.
- **Trade-off:** This binds Postgres on `0.0.0.0:5432` on your machine. If you already run a local Postgres on 5432, the container won't start. Credentials are the default `airflow / airflow / airflow` — fine for local dev, **never** do this in production.

### 2.3 Missing trailing newline

- Cosmetic only. The local file does not end in `\n` after `postgres-db-volume:`. Some linters/editors will flag this; functionally irrelevant.

### 2.4 What was *not* changed

Everything else is upstream-default, including:
- Executor: `CeleryExecutor` with Redis 7.2 as broker.
- All 7 Airflow services (`apiserver`, `scheduler`, `dag-processor`, `worker`, `triggerer`, `init`, `cli`) plus optional `flower`.
- Volume mounts for `./dags`, `./logs`, `./config`, `./plugins`.
- `env_file: ${ENV_FILE_PATH:-.env}` — already present upstream (no `.env` file is currently committed; defaults kick in).
- Healthchecks, restart policies, dependency ordering.

---

## 3. File structure and purpose, section by section

### 3.1 Top-level shape

```yaml
x-airflow-common: &airflow-common   # YAML anchor — shared config
services: ...                       # the actual containers
volumes: ...                        # named persistent volumes
```

The `x-` prefix marks an "extension field" — Docker Compose ignores it as a service but lets you define reusable blocks via YAML anchors (`&name`) and merge them later with `<<: *name`. This is the trick that keeps the file DRY across the 8 Airflow services.

### 3.2 `x-airflow-common` — the shared blueprint

Defines defaults that every Airflow container reuses:

| Field | Purpose |
|---|---|
| `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.6}` | Image for all Airflow services. Override via env var if you build a custom image. |
| `env_file` | Optionally loads extra env vars from `.env` (path overridable via `ENV_FILE_PATH`). |
| `environment` (anchored as `&airflow-common-env`) | Core Airflow config: executor, auth manager, DB URL, Celery broker/result backend, Fernet key, examples toggle, health-check toggle, optional PIP requirements, custom config path. |
| `volumes` | Bind-mounts `./dags`, `./logs`, `./config`, `./plugins` into `/opt/airflow/...` so you edit DAGs on the host and the containers see them immediately. |
| `user: "${AIRFLOW_UID:-50000}:0"` | Runs as the Airflow user inside the container; on Linux you set `AIRFLOW_UID=$(id -u)` in `.env` to avoid root-owned files in `./logs`. |
| `depends_on` (anchored as `&airflow-common-depends-on`) | Waits for `redis` and `postgres` to be healthy before starting. |

Key environment values worth knowing:
- `AIRFLOW__CORE__EXECUTOR: CeleryExecutor` — tasks run on worker pods via Redis.
- `AIRFLOW__CORE__AUTH_MANAGER: ...FabAuthManager` — uses the classic Flask AppBuilder auth (username/password login).
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow` — metadata DB connection.
- `AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0` — Celery broker.
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/` — new in Airflow 3.x; workers/triggerers call back into the API server for task execution lifecycle.
- `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'` — exposes a tiny HTTP health endpoint on the scheduler (port 8974) used by the scheduler's `healthcheck`.
- `AIRFLOW_CONFIG: /opt/airflow/config/airflow.cfg` — if you drop an `airflow.cfg` into `./config`, it will be picked up.

### 3.3 `services`

#### `postgres` — metadata database
- Image: `postgres:16`.
- Stores all Airflow state: DAG runs, task instances, XComs, connections, variables, users.
- Persists via the named volume `postgres-db-volume`.
- **Local mod:** exposes `5432` to the host (see §2.2).
- Healthcheck: `pg_isready` so other services only start once Postgres accepts connections.

#### `redis` — Celery broker
- Image pinned to `redis:7.2-bookworm` (Airflow project pins it due to Redis's 2024 licensing change away from BSD).
- Internal-only (`expose: 6379`, no host port).
- Healthcheck via `redis-cli ping`.

#### `airflow-apiserver` — the web UI + REST API
- New name in Airflow 3.x (replaces the old `airflow-webserver`).
- Command: `api-server`.
- Published on host port **8080** → that's where you open `http://localhost:8080` in your browser.
- Healthcheck hits `/api/v2/version`.

#### `airflow-scheduler` — the brain
- Decides *when* each task in each DAG should run, queues them on Celery.
- Healthcheck against `http://localhost:8974/health` (the internal scheduler health server enabled above).

#### `airflow-dag-processor` — parses DAG files
- New as a separate service in Airflow 3.x (in 2.x it ran inside the scheduler).
- Continuously scans `/opt/airflow/dags` and writes serialized DAGs into the metadata DB.
- Healthcheck uses `airflow jobs check --job-type DagProcessorJob`.

#### `airflow-worker` — runs the tasks
- Command: `celery worker`.
- Picks tasks off Redis and executes them.
- Extra env: `DUMB_INIT_SETSID: "0"` so Celery's warm-shutdown signal handling works correctly (see Airflow docs on signal propagation).
- Depends on the apiserver being healthy (workers need it to report task state in Airflow 3.x).
- Healthcheck inspects the worker via Celery's ping, with a fallback path for the older provider import.

#### `airflow-triggerer` — async sensor / deferrable task host
- Hosts the async event loop that drives "deferrable" operators (e.g. sensors that yield instead of holding a worker slot).
- Healthcheck: `airflow jobs check --job-type TriggererJob`.

#### `airflow-init` — one-shot bootstrap
- Runs as **root** (`user: "0:0"`) once at startup, then exits.
- A bash script that:
  1. Warns if `AIRFLOW_UID` is unset.
  2. Checks host resources (≥ 4 GB RAM, ≥ 2 CPUs, ≥ 10 GB disk) and prints warnings if low.
  3. Creates `/opt/airflow/{logs,dags,plugins,config}` if missing.
  4. Prints the Airflow version.
  5. Runs `airflow config list` to materialize a default config if absent.
  6. `chown`s shared volumes to `${AIRFLOW_UID}:0` so the non-root Airflow user can write to them.
- Extra env: `_AIRFLOW_DB_MIGRATE: 'true'` (runs `airflow db migrate`) and `_AIRFLOW_WWW_USER_CREATE: 'true'` (creates the admin user, defaulting to `airflow / airflow`).
- All other services `depend_on` this completing successfully.

#### `airflow-cli` — debug helper
- Only runs when you activate the `debug` profile (`docker compose --profile debug run airflow-cli ...`).
- `CONNECTION_CHECK_MAX_COUNT: "0"` disables the entrypoint's pre-flight DB/Redis check so you can run `airflow ...` commands ad-hoc.

#### `flower` — Celery monitoring UI
- Only runs with `--profile flower`.
- Published on host port **5555**.

### 3.4 `volumes`

```yaml
volumes:
  postgres-db-volume:
```

A single named Docker volume backing Postgres. Removing it (`docker compose down -v`) wipes **all** Airflow metadata — DAG run history, connections, users, the lot. The DAG files themselves live on the host in `./dags`, so they survive.

---

## 4. How everything fits together at runtime

```
                     +---------------------+
   Host :8080  --->  | airflow-apiserver   |  (Web UI + REST + Execution API)
   Host :5432  --->  | postgres (metadata) |  <--- (added: host access for DB clients)
                     +----------+----------+
                                |
              +-----------------+-----------------+----------------------+
              |                 |                 |                      |
       airflow-scheduler   airflow-dag-     airflow-triggerer     airflow-worker
       (queues tasks)      processor        (deferrable ops)      (executes tasks)
                           (parses DAGs)
                                |
                                v
                          +-----+-----+
                          |   redis   |  (Celery broker, internal-only)
                          +-----------+

   ./dags     -> /opt/airflow/dags     (read by dag-processor & worker)
   ./logs     -> /opt/airflow/logs     (written by scheduler / workers / triggerer)
   ./config   -> /opt/airflow/config   (optional airflow.cfg)
   ./plugins  -> /opt/airflow/plugins  (custom operators / hooks)
```

Startup order, enforced by `depends_on` + healthchecks:

1. `postgres` and `redis` start; wait until healthy.
2. `airflow-init` runs DB migrations, creates the admin user, fixes volume ownership, then exits.
3. All long-running Airflow services (`apiserver`, `scheduler`, `dag-processor`, `triggerer`, `worker`) start in parallel.
4. `worker` additionally waits for `apiserver` to be healthy.

---

## 5. Quick reference — commands

```bash
# First-time / after pulling new image
docker compose up airflow-init

# Normal day-to-day
docker compose up -d

# With Flower (Celery dashboard at :5555)
docker compose --profile flower up -d

# Ad-hoc airflow CLI in a throwaway container
docker compose --profile debug run --rm airflow-cli airflow dags list

# Stop, keep state
docker compose down

# Stop and wipe metadata DB (full reset)
docker compose down -v
```

---

## 6. Summary of local deltas (TL;DR)

| Change | Reason |
|---|---|
| `LOAD_EXAMPLES: 'false'` | Hide Airflow's bundled tutorial DAGs from the UI. |
| Publish Postgres `5432:5432` | Inspect the metadata DB from host-side tools. |
| Trailing newline removed | Incidental; no functional effect. |

Everything else is the unmodified upstream Apache Airflow 3.1.6 reference compose.
