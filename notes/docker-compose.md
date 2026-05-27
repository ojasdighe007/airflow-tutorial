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

Each Airflow service is a single-purpose process. Together they form a small distributed system: a **metadata store** (Postgres) holds the source of truth, a **message broker** (Redis) shuttles work, a **scheduler** decides what to run, a **dag-processor** ingests DAG code, **workers** and a **triggerer** execute tasks, and an **apiserver** is the front door for humans and the workers themselves. The sections below describe each container's job *and* how it talks to its neighbours.

#### `postgres` — metadata database (the source of truth)
- **Image:** `postgres:16`.
- **Role in architecture:** Every other Airflow service is essentially stateless and reads/writes its state here. It stores DAG run history, task instance state, XComs, connections, variables, pools, users, serialized DAG definitions written by the dag-processor, and Celery task results.
- **Who connects to it:**
  - `airflow-init` runs `airflow db migrate` against it on first boot.
  - `airflow-scheduler` polls it constantly for runnable task instances.
  - `airflow-apiserver` reads/writes from it on every UI page load and API call.
  - `airflow-dag-processor` writes serialized DAGs into it.
  - `airflow-worker` updates task state into it via the apiserver's execution API.
  - `airflow-triggerer` writes trigger / deferral state into it.
  - Configured via `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow`.
- **Persistence:** Backed by the **named volume** `postgres-db-volume` (see §3.4) — survives `docker compose down`, wiped only by `docker compose down -v`.
- **Local mod:** Published on host port `5432` so DB clients can poke at the metadata (see §2.2).
- **Healthcheck:** `pg_isready -U airflow` every 10s. Other services use `depends_on: { condition: service_healthy }` so nothing starts until Postgres accepts connections — this is what prevents the classic "DB connection refused" race at boot.

#### `redis` — Celery broker (the work queue)
- **Image:** pinned to `redis:7.2-bookworm` (Airflow stays on 7.2 due to Redis's 2024 licensing change away from BSD).
- **Role in architecture:** The message broker for `CeleryExecutor`. The scheduler **publishes** task messages here, and one of the workers **consumes** each message. Redis is the only piece that allows the scheduler and workers to be decoupled and horizontally scaled.
- **Who connects to it:**
  - `airflow-scheduler` pushes tasks onto the queue via Celery.
  - `airflow-worker` pops tasks off the queue.
  - `flower` (optional) introspects queue length and worker activity.
  - Configured via `AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0`.
- **Note on the result backend:** Celery task *results* (not Airflow state, just Celery's own ack metadata) are stored in **Postgres**, not Redis, via `AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://...`. So Redis is purely a transient broker.
- **Networking:** Internal-only (`expose: 6379` rather than `ports:`), so it's reachable from other containers as `redis:6379` but never from the host.
- **Healthcheck:** `redis-cli ping`. No persistence configured — restarting Redis loses any in-flight queue messages, but Airflow re-queues based on scheduler state, so this is acceptable for dev.

#### `airflow-apiserver` — web UI + REST + execution API (the front door)
- **Image:** Airflow common (`apache/airflow:3.1.6`). **Command:** `api-server`.
- **Role in architecture:** This is the name in Airflow 3.x for what used to be `airflow-webserver`. It serves three audiences from one process:
  1. **Humans** — the web UI at `http://localhost:8080`.
  2. **External clients** — the public REST API at `/api/v2/*`.
  3. **Internal services** — the new **Task Execution API** at `/execution/*`, which workers and triggerers call to fetch task context and report state. This is the key architectural change in Airflow 3.x: workers no longer talk to the metadata DB directly; they go through this API. That's why `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` points at `http://airflow-apiserver:8080/execution/` and why `airflow-worker.depends_on` includes the apiserver being healthy.
- **Ports:** `8080:8080` published to the host.
- **Healthcheck:** `curl --fail http://localhost:8080/api/v2/version` every 30s.
- **Depends on:** `postgres` + `redis` healthy *and* `airflow-init` having completed (so the DB schema and admin user exist before serving requests).

#### `airflow-scheduler` — the brain
- **Command:** `scheduler`.
- **Role in architecture:** The control plane. In a tight loop it:
  1. Reads serialized DAGs from Postgres (written there by `airflow-dag-processor`).
  2. Evaluates schedule intervals, data-aware triggers, and dependencies to decide which task instances are eligible.
  3. Marks them `queued` in Postgres and pushes Celery messages onto Redis.
  4. Updates task state as workers report back via the apiserver.
- **What it does *not* do (anymore):** parse DAG `.py` files — that's now `airflow-dag-processor`'s job. This split (new in Airflow 3.x) means a buggy or slow DAG file can't stall scheduling decisions.
- **Inbound connections:** None public. Exposes an internal HTTP health server on `:8974` (enabled by `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'`).
- **Healthcheck:** `curl --fail http://localhost:8974/health` against that internal server.

#### `airflow-dag-processor` — DAG file parser
- **Command:** `dag-processor`.
- **Role in architecture:** Continuously scans the `/opt/airflow/dags` bind-mounted directory, imports each `.py` file in a subprocess, and writes the resulting serialized DAG structure into Postgres. The scheduler then reads only the serialized form, never the source files.
- **Why this is a separate service in Airflow 3.x:** Parsing user DAG code is unsafe (it can hang, leak memory, or `sys.exit`). Isolating it means scheduling decisions stay snappy and a misbehaving DAG can't crash the scheduler. In Airflow 2.x this ran as a subprocess of the scheduler; in 3.x it's a peer service.
- **Inputs:** the `./dags` bind mount → `/opt/airflow/dags` (see §3.4).
- **Outputs:** rows in Postgres tables like `serialized_dag`, `dag_code`, `dag`, `import_error`.
- **Healthcheck:** `airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"`.

#### `airflow-worker` — task executor
- **Command:** `celery worker`.
- **Role in architecture:** This is where your DAG code actually runs. A worker is a Celery consumer that:
  1. Subscribes to the Celery queue on Redis.
  2. Picks up a task message.
  3. Calls the apiserver's `/execution/` API to get task context (connections, variables, XComs, etc.) and to heartbeat / report state. *(This is the big change in Airflow 3.x — workers no longer need direct Postgres credentials.)*
  4. Forks a process to run the operator's `execute()` method.
  5. Writes logs to `/opt/airflow/logs` (bind-mounted to `./logs`, so you can `tail -f` on the host).
- **Extra env:** `DUMB_INIT_SETSID: "0"` so Celery's warm-shutdown signal handling works correctly (see Airflow docs on signal propagation). Without this, SIGTERM during `docker compose down` can leave tasks in a bad state.
- **Depends on:** Redis (broker) + Postgres (result backend) + `airflow-apiserver` (execution API) all healthy, plus `airflow-init` completed.
- **Scaling:** This is the service you scale (`docker compose up -d --scale airflow-worker=N`) when you have lots of concurrent tasks.
- **Healthcheck:** Pings the local Celery app; the `||` fallback handles the older import path for backwards compatibility.

#### `airflow-triggerer` — async deferrable-task host
- **Command:** `triggerer`.
- **Role in architecture:** Hosts an `asyncio` event loop dedicated to **deferrable operators** — sensors and operators that "defer" themselves (yield) instead of blocking a worker slot while they wait. Classic example: a sensor waiting 6 hours for a file to arrive. On a normal worker that's 6 hours of an occupied slot; on the triggerer it's a single coroutine using ~zero resources.
- **Lifecycle of a deferred task:**
  1. Worker starts the task, hits a `defer()` call.
  2. The task's "trigger" object is serialized into Postgres.
  3. Worker slot is freed; the triggerer picks the trigger up and runs it in its event loop.
  4. When the trigger fires, the triggerer marks the task ready in Postgres.
  5. Scheduler re-queues the task; a worker resumes it from the deferred point.
- **Healthcheck:** `airflow jobs check --job-type TriggererJob`.

#### `airflow-init` — one-shot bootstrap
- **Runs as root** (`user: "0:0"`) once at startup, then exits. All long-running services use `depends_on: { airflow-init: { condition: service_completed_successfully } }` to wait for it.
- **Role in architecture:** The "first run" setup that's too invasive to keep doing on every service start. It is essentially a privileged sidecar.
- **What its bash script does:**
  1. Warns if `AIRFLOW_UID` is unset (Linux hosts need this to avoid root-owned files in the bind mounts).
  2. Sanity-checks host resources (≥ 4 GB RAM, ≥ 2 CPUs, ≥ 10 GB disk) and warns if low.
  3. Creates `/opt/airflow/{logs,dags,plugins,config}` if missing — important because empty bind mounts on a fresh checkout mean the dirs exist on the host but may be empty inside the container.
  4. Prints the Airflow version (useful in CI logs to confirm which image you booted).
  5. Runs `airflow config list` to materialize a default `airflow.cfg` if absent.
  6. `chown -R "${AIRFLOW_UID}:0" /opt/airflow/...` so the non-root Airflow user (UID 50000 by default) can write to the bind-mounted host directories.
- **Extra env that triggers Airflow's built-in init hooks:**
  - `_AIRFLOW_DB_MIGRATE: 'true'` → runs `airflow db migrate`, creating/upgrading the metadata schema.
  - `_AIRFLOW_WWW_USER_CREATE: 'true'` → creates the admin user (`airflow / airflow` by default — change via `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`).
- **Why it must run as root:** so it can `chown` the bind mounts. The actual Airflow services then run as UID 50000 for the principle of least privilege.

#### `airflow-cli` — debug helper
- **Profile-gated:** only spins up under `docker compose --profile debug run --rm airflow-cli ...`.
- **Role in architecture:** A throwaway container that shares the exact same image, env, and bind mounts as the rest of the cluster, so you can run any `airflow <subcommand>` against the same metadata DB without disturbing the running services.
- **Extra env:** `CONNECTION_CHECK_MAX_COUNT: "0"` disables the entrypoint's pre-flight DB/Redis ping — useful when you want to run subcommands like `airflow info` even if the cluster is half-down.

#### `flower` — Celery monitoring dashboard
- **Profile-gated:** only spins up under `docker compose --profile flower up`.
- **Role in architecture:** A web UI on top of Celery showing live worker count, queue depth, task throughput, per-worker task history. It only inspects the Celery broker (Redis) and workers; it does **not** read from the Airflow metadata DB, so it sees raw Celery messages, not Airflow task instances.
- **Ports:** `5555:5555` on the host.
- **Healthcheck:** `curl --fail http://localhost:5555/`.

### 3.4 Storage: bind mounts and named volumes

Airflow's containers are stateless, but Airflow as a system is very much stateful. The compose file deals with this in **two completely different ways**, and it's worth being explicit about which is which.

#### 3.4.1 Bind mounts (host directory ⇄ container directory)

A **bind mount** literally mounts a path on the host filesystem into a path inside the container. Edits on either side are visible immediately on the other — there's no copy and no Docker-managed storage layer in between. In compose syntax it looks like `./host/path:/container/path`.

The `x-airflow-common.volumes` block defines four bind mounts that every Airflow service inherits:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
```

`${AIRFLOW_PROJ_DIR:-.}` resolves to the repo root by default (the directory you run `docker compose` from).

| Host path | Container path | Purpose | Written by | Read by |
|---|---|---|---|---|
| `./dags` | `/opt/airflow/dags` | Your DAG source files (`.py`). Edits are picked up live. | You (the developer) | `airflow-dag-processor` (parses them); `airflow-worker` (imports them at task run time) |
| `./logs` | `/opt/airflow/logs` | Per-task logs, organized by `dag_id/run_id/task_id/attempt.log`. Surfaced in the UI. | `airflow-scheduler`, `airflow-worker`, `airflow-triggerer` | `airflow-apiserver` (to render logs in the UI), you (`tail -f` from the host) |
| `./config` | `/opt/airflow/config` | Optional `airflow.cfg` and any other config (loaded via `AIRFLOW_CONFIG=/opt/airflow/config/airflow.cfg`). | `airflow-init` (creates a default `airflow.cfg` on first run), you (when overriding settings) | Every Airflow service at boot |
| `./plugins` | `/opt/airflow/plugins` | Custom operators, hooks, macros, UI plugins. | You | Every Airflow service at import time |

**Why bind mounts here (instead of named volumes or `COPY` into the image):**
- **`./dags`:** so you can edit a DAG file in your editor and have the dag-processor pick it up in seconds — the whole point of local development. With a named volume you'd lose direct host access; with `COPY` you'd have to rebuild the image on every change.
- **`./logs`:** so task logs are inspectable with normal host tools (`grep`, `less`, IDE search) and survive `docker compose down`. They also survive `docker compose down -v` because they're not a named volume.
- **`./config`:** so you can drop in a custom `airflow.cfg` from the host and have all services see it.
- **`./plugins`:** same rationale as DAGs — iterate on plugin code without rebuilding images.

**Gotchas with bind mounts:**
- **UID mismatch (Linux):** Files written by the containerized Airflow user (UID 50000) will appear as owned by UID 50000 on the host. If your host UID is different, you can't edit those log files without `sudo`. Fix: set `AIRFLOW_UID=$(id -u)` in `.env` so the container runs as *your* UID. `airflow-init` honors this and `chown`s the directories accordingly. On macOS/Windows this is handled by Docker Desktop's filesystem shim, so it's usually invisible.
- **Empty-dir behavior:** Unlike named volumes, bind mounts do **not** copy the image's contents into an empty host directory on first start. If `./dags` is empty on the host, `/opt/airflow/dags` is empty in the container too — even if the image originally had files there. `airflow-init` handles this by `mkdir -p`ing the four paths.
- **Performance on macOS/Windows:** Bind mounts go through a filesystem virtualization layer on non-Linux Docker. For very large DAG repos this can be slow to scan. The dev-only nature of this compose file makes that acceptable.
- **`docker compose down -v` does NOT touch bind mounts.** That flag only removes named volumes. Your DAGs, logs, plugins, and config are always safe from compose commands; they live on your host filesystem.

#### 3.4.2 Named volumes (Docker-managed storage)

The bottom of the file declares one **named volume**:

```yaml
volumes:
  postgres-db-volume:
```

A named volume is storage that Docker creates and manages itself (under `/var/lib/docker/volumes/<name>` on Linux; inside the Docker Desktop VM on macOS/Windows). It is mounted into `postgres` at `/var/lib/postgresql/data`, where Postgres keeps its data files.

Why a named volume (and not a bind mount) for the database:
- **Performance:** Postgres data files are extremely IO-sensitive. Docker-managed volumes use native storage drivers and avoid the host-filesystem translation layer that bind mounts on macOS/Windows go through.
- **Permissions sanity:** Postgres expects very specific ownership on its data dir. Letting Docker manage it sidesteps host-UID issues entirely.
- **Opacity is fine here:** You shouldn't be poking at Postgres's raw files from the host — you use a SQL client (which is why §2.2 publishes port 5432 to the host instead).

**Lifecycle:** This volume persists across `docker compose down`. It is destroyed by `docker compose down -v` — which wipes **all** Airflow metadata (DAG run history, connections, users, XComs, everything). DAG source files in `./dags` survive because they're a bind mount, not a named volume.

#### 3.4.3 Bind mounts vs. named volumes — at a glance

| | Bind mount | Named volume |
|---|---|---|
| Syntax | `./host/path:/container/path` | `volume-name:/container/path` (and declared under top-level `volumes:`) |
| Location on host | Wherever you point it | `/var/lib/docker/volumes/<name>` (or the Docker Desktop VM) |
| Edit from host | Yes, directly with any tool | Only by entering the container |
| Survives `docker compose down` | Yes | Yes |
| Survives `docker compose down -v` | **Yes** | **No** |
| Best for | Source code, logs, configs you want to inspect | Database files, anything performance-sensitive or that should be opaque |
| Used here for | `./dags`, `./logs`, `./config`, `./plugins` | `postgres-db-volume` |

---

## 4. How everything fits together at runtime

```
   Host :8080  -----> airflow-apiserver  ──┐  (Web UI + REST /api/v2 + Execution API /execution)
   Host :5432  -----> postgres            │  (metadata: DAGs, runs, tasks, users, XComs, conns)
   Host :5555  -----> flower (optional)   │
                                          │
            ┌─────────────────────────────┴──────────────────────────────┐
            │                                                            │
            ▼                                                            ▼
   ┌──────────────────┐   reads serialized DAGs    ┌─────────────────────────────┐
   │ airflow-         │ ◀────────────────────────  │ postgres (metadata DB)      │
   │   scheduler      │   writes task state        │  └─ named volume:           │
   │ (queues tasks)   │ ─────────────────────────▶ │     postgres-db-volume      │
   └────────┬─────────┘                            └──────────────▲──────────────┘
            │ pushes Celery msgs                                  │ writes serialized DAGs
            ▼                                                     │
   ┌──────────────────┐                            ┌──────────────┴──────────────┐
   │      redis       │ ◀── pops tasks ──          │  airflow-dag-processor      │
   │ (Celery broker,  │                  │         │  (parses ./dags/*.py)       │
   │  internal-only)  │                  │         └─────────────────────────────┘
   └──────────────────┘                  │
                                         │
                              ┌──────────┴──────────┐         ┌──────────────────────┐
                              │   airflow-worker    │ ◀────── │  airflow-triggerer   │
                              │  (executes tasks,   │  defer/ │  (asyncio loop for   │
                              │   N replicas)       │  resume │   deferrable ops)    │
                              └──────────┬──────────┘         └──────────────────────┘
                                         │
                                         │ HTTP to /execution/* on apiserver
                                         ▼
                              ┌─────────────────────┐
                              │  airflow-apiserver  │  (workers/triggerer call back
                              │   (execution API)   │   here for context + state)
                              └─────────────────────┘

  Bind mounts (host ⇄ container, edits visible both ways):
    ./dags     -> /opt/airflow/dags     (you write; dag-processor & worker read)
    ./logs    <-> /opt/airflow/logs     (scheduler/worker/triggerer write; apiserver UI reads)
    ./config   -> /opt/airflow/config   (optional airflow.cfg)
    ./plugins  -> /opt/airflow/plugins  (custom operators / hooks)

  Named volume (Docker-managed, opaque to host):
    postgres-db-volume -> /var/lib/postgresql/data
```

Startup order, enforced by `depends_on` + healthchecks:

1. `postgres` and `redis` start; wait until healthy.
2. `airflow-init` runs as root: `airflow db migrate`, create admin user, `chown` bind mounts, then exits.
3. All long-running Airflow services (`apiserver`, `scheduler`, `dag-processor`, `triggerer`, `worker`) start in parallel.
4. `worker` additionally waits for `apiserver` to be healthy (because it needs the execution API to fetch task context).
5. Profile-gated `flower` and `airflow-cli` only start when their profile is activated.

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
