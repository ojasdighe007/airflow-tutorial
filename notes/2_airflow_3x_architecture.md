# Airflow 3.x — Architecture, Workflow, Executors

A concise reference for how Airflow 3.x runs DAGs, what each service does, the
executor options, and what changed from 2.x. Maps to the `CeleryExecutor` stack
in this repo's `docker-compose.yml`.

---

## 1. Overall workflow + the services

### The end-to-end lifecycle

```
[you write dags/*.py]
   │  dag-processor parses + serializes  → Postgres
   ▼
DAG registered ─► (schedule or manual trigger) ─► DAG run created   (scheduler + Postgres)
   ▼  scheduler evaluates dependencies
task: none → scheduled → queued                  (scheduler sets state in Postgres)
   ▼  executor publishes to broker; worker pops
task: queued → running                           (worker runs it, reports via Execution API)
   │
   ├─ if deferrable: running → deferred → (triggerer) → scheduled → running
   ▼
task: running → success / failed / up_for_retry
   ▼  downstream tasks become eligible → repeat
DAG run: running → success / failed
```

**Core principle of 3.x:** the metadata DB is the single source of truth, but the
two riskiest activities — *parsing user DAG code* and *running user task code* —
are isolated behind hard boundaries (the `dag-processor` and the Execution API).

### Independent services and their sub-components


| Service                                                                | Main purpose                                                                                                                             | Key sub-component(s)                                                                   |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| **airflow-init**                                                       | One-shot bootstrap: `db migrate`, create admin user, `chown` bind mounts, then exits.                                                    | —                                                                                      |
| **dag-processor**                                                      | Standalone service that scans `dags/`, imports each file in a subprocess, writes serialized DAGs to Postgres.                            | per-file **parsing subprocess** (isolation).                                           |
| **postgres**                                                           | Metadata DB = source of truth (DAG runs, task state, XComs, connections, serialized DAGs). Every other service is effectively stateless. | —                                                                                      |
| **scheduler**                                                          | The brain. Reads serialized DAGs, creates DAG runs, evaluates dependencies, marks tasks `queued`.                                        | **executor** (embedded; dispatches queued tasks). Internal **health server** on :8974. |
| **executor** *(sub-component of scheduler — not a standalone service)* | Pluggable strategy that answers "task is `queued` — how/where do I run it?". Publishes to broker / spawns subprocess / launches pod.     | n/a — runs *inside* the scheduler process. See §2 for the executor types.              |
| **redis**                                                              | Celery **broker** — transient work queue between scheduler and workers. (Celery *results* go to Postgres, not Redis.)                    | —                                                                                      |
| **worker**                                                             | Runs the actual task code (Celery consumer). Reports state/fetches context via the Execution API — never touches Postgres directly.      | forked **task execution process**; writes logs to `./logs`.                            |
| **apiserver**                                                          | The "front door": Web UI + REST `/api/v2/`* + the internal **Execution API** `/execution/`*.                                             | Web UI, REST API, **Task Execution API** (workers call back here).                     |
| **triggerer**                                                          | Hosts an `asyncio` loop for **deferrable** tasks (sensors that `defer()`), freeing worker slots while waiting.                           | **trigger event loop**.                                                                |
| **flower** *(optional)*                                                | Celery monitoring dashboard (queue depth, worker activity).                                                                              | —                                                                                      |


> **Service vs. sub-component:** the **scheduler** is a standalone service; its
> **executor** is a sub-component running *inside* it (not its own service). Same
> idea: the **apiserver** is the service, the **Execution API** is one of its
> sub-roles.

### On "queues" (common confusion)

- `queued` is a task **state** in Postgres — not a message queue. The scheduler sets it.
- The real queue is the **Celery queue in Redis** (default name `default`). The
executor publishes to it; workers consume from it.
- There is **no separate queue for the scheduler vs. the executor.** The executor
has only a transient in-memory buffer, then pushes to the one broker queue.
Multiple named queues exist *only* if you opt into task routing by adding tasks in different queues based on workload/other seggregations.

### Why Redis for Queue and not "just existing PostgresDB"

- **Different workloads.** Postgres = durable source of truth (DAG runs, task state,
XComs, results, connections). Redis = transient, high-churn message bus carrying
short "go run task X" envelopes that are written once, popped once, deleted.
- **Latency / waiting model.** Celery workers `BRPOP` from Redis — a blocking pop
that wakes up sub-ms when work arrives, zero CPU while idle. The SQLA broker
(`sqla+postgresql://`) has no real blocking primitive, so workers **poll** the
queue table on an interval, adding latency *and* constant query load on the
metadata DB.
- **MVCC hates queue tables.** High insert/delete churn on one table creates dead
tuples fast → autovacuum pressure, index bloat, lock contention on hot rows.
Redis just frees memory once a message is acked — no vacuum, no bloat.
- **Celery semantics map naturally to Redis/AMQP** — named queues, ack/visibility
timeout, requeue on worker death, pub/sub for control commands. SQL tables can
emulate these, but you're paying a relational engine to pretend to be a queue.
- **Failure isolation.** If the broker hiccups, the UI, scheduler state, and run
history stay readable in Postgres. Putting the queue in Postgres couples the
failure domains — a runaway queue table can slow down the very UI you'd use to
triage it.
- **Not duplicated data.** Once a worker pops a message, Redis is done with it.
The durable record ("task X ran, here's its return value") lives in Postgres via
`result_backend = db+postgresql://…`. Redis is the conveyor belt; Postgres is
the warehouse.
- **You *can* swap Redis out.** `sqla+postgresql://` as a Celery broker works for
tiny setups. Or skip the broker entirely with `LocalExecutor`,
`KubernetesExecutor`, `EdgeExecutor` (3.x), or cloud-queue executors (SQS, Batch).

### Delivery guarantees & failure modes (what if a worker dies after pop?)

Redis doesn't truly "free on pop." Celery uses a **visibility timeout** pattern
plus Airflow layers its own zombie detection on top — two independent safety nets.

- **Atomic pop → unacked zset.** A Lua script in Redis pops the message from the
queue list *and* inserts it into an "unacked" sorted set keyed by
`now + visibility_timeout` (default **3600 s**, via
`broker_transport_options.visibility_timeout`). No in-between state where the
message is lost.
- **Happy path.** Worker finishes → sends ACK → message removed from unacked zset.
- **Crash before/during/after task.** Message stays in unacked zset; once the
deadline expires, any worker re-delivers it. → **At-least-once delivery**, so
task code should be idempotent.
- `task_acks_late = True` (Airflow's default) means the ACK is sent *after*
the task completes — so mid-task crashes are actually covered by
visibility_timeout. With `acks_late=False` you'd rely solely on Airflow's
zombie detection.
- **Airflow's belt-and-suspenders.** Even if Celery loses a message entirely:
  - `[scheduler] task_queued_timeout` (default **600 s**): a task stuck in
  `queued` with no worker progress is reset to `scheduled` and re-enqueued.
  - `[scheduler] scheduler_zombie_task_threshold` (default **300 s**): a
  `running` task whose worker stops heart-beating is failed → retried.
- **Real holes to know about:**
  - Tasks longer than `visibility_timeout` get re-delivered to a *second* worker
  while the first is still running → bump the timeout for long tasks.
  - Redis without persistence (no AOF) loses in-flight messages on crash —
  Airflow's timeouts still recover them, just with delay. Enable AOF in prod.
  - At-least-once means occasional duplicate runs around worker crashes — design
  tasks to be safe to re-run.

### Failure scenarios by task state — and how Airflow recovers

The primitives are explained above (visibility_timeout, `acks_late`,
`task_queued_timeout`, zombie detection). This table is the scenario matrix —
*what* can go wrong in each state and *which* primitive catches it.


| TI state     | Failure scenario                                                     | Detected by                                                                                      | Recovery action                                                                                         | Default window |
| ------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- | -------------- |
| **queued**   | Executor's Redis publish fails (Redis down, network blip)            | `[scheduler] task_queued_timeout`                                                                | Reset `queued → scheduled`, re-enqueue                                                                  | 600 s          |
| **queued**   | Message in Redis but no worker free (all workers down / at capacity) | `task_queued_timeout` (resolves itself when a worker BRPOPs)                                     | Re-enqueue if window elapses; otherwise picked up automatically                                         | 600 s          |
| **queued**   | Worker popped from Redis but crashed before forking task-runner      | Celery `visibility_timeout` (primary) + `task_queued_timeout` (backstop)                         | Re-deliver original message; or scheduler resets TI and publishes a new one                             | 3600 s / 600 s |
| **queued**   | Task-runner forked but can't reach apiserver to report `running`     | Same as above                                                                                    | Same                                                                                                    | 3600 s / 600 s |
| **running**  | Task raises a Python exception                                       | Task runner → Execution API                                                                      | `running → up_for_retry` (if retries left) or `failed`; Celery ACK sent                                 | immediate      |
| **running**  | Task exceeds `execution_timeout`                                     | Task runner SIGTERMs (then SIGKILLs) user code, reports failure                                  | Same as above                                                                                           | immediate      |
| **running**  | External kill — UI "Mark failed", `kill -TERM` to worker             | Task runner catches signal, reports failure                                                      | Same as above                                                                                           | immediate      |
| **running**  | Worker host crash / OOM-killer / SIGKILL mid-task                    | `[scheduler] scheduler_zombie_task_threshold` (primary) + Celery `visibility_timeout` (backstop) | `running → up_for_retry`/`failed`; original message also re-delivered (because `task_acks_late = True`) | 300 s / 3600 s |
| **running**  | Apiserver unreachable when reporting final state                     | Task runner retries; persistent failure looks like a zombie                                      | Same as above                                                                                           | 300 s          |
| **running**  | Network partition — task alive but no heartbeats reaching apiserver  | Zombie detection                                                                                 | Same; if the partition heals later → duplicate run                                                      | 300 s          |
| **deferred** | Trigger errors in the triggerer                                      | Triggerer → Execution API                                                                        | `deferred → failed` (retry logic applies)                                                               | immediate      |


**What happens after the failure is recorded** (same for every row above):

- **Retry decision.** If `try_number < max_tries` → `up_for_retry`, wait
`retry_delay` (optionally `retry_exponential_backoff`), then
`scheduled → queued` and a fresh Celery message is published. `try_number`
increments and the next attempt logs to a new `attempt=N.log` file.
- **Downstream propagation.** A terminal `failed` triggers downstream
`trigger_rule` evaluation: `all_success` (default) → `upstream_failed`;
`all_done` → still runs; `one_failed`/`all_failed` → runs because of the
failure.
- **DAG run.** Any terminal `failed`/`upstream_failed` leaf → DAG run ends
`failed`. `on_failure_callback` and `on_dag_run_failure_callback` fire;
email / SLA-miss notifications go out if configured.
- **History.** Per-attempt logs are preserved on disk; `task_instance_history`
keeps the full attempt timeline (visible in the UI).

**Duplicate-execution caveat.** If the worker finishes the task body but dies
*before* the Execution API call returns, the task stays `running` in Postgres
→ zombie detection retries it → side effects can happen twice. This is the
inherent "at-least-once" cost; keep task code idempotent
(`INSERT … ON CONFLICT`, deterministic output paths keyed by `logical_date`, etc.).

### TL;DR — the safety nets, stacked

Airflow gives you **four layers** so that no single failure (worker, broker,
network, apiserver) loses a task. Each layer has a primary job and acts as a
backstop for the one above.


| Layer                          | Mechanism                                               | Default  | Catches                                                                    |
| ------------------------------ | ------------------------------------------------------- | -------- | -------------------------------------------------------------------------- |
| 1. Broker                      | Celery atomic pop → unacked zset + `visibility_timeout` | 3600 s   | Worker crash *after queue pop, before ACK* (needs `task_acks_late = True`) |
| 2. Scheduler — stuck `queued`  | `[scheduler] task_queued_timeout`                       | 600 s    | TI `queued` with no worker progress (lost message, no workers, etc.)       |
| 3. Scheduler — stuck `running` | `[scheduler] scheduler_zombie_task_threshold`           | 300 s    | TI `running` with stale heartbeat (host crash, partition, OOM)             |
| 4. Task — application retry    | `retries` / `retry_delay` / `retry_exponential_backoff` | per task | Any reported failure (exceptions, timeouts)                                |


Operational backstops that sit alongside the above:

- **Redis AOF persistence** (`appendonly yes`) — keeps in-flight messages across
Redis restarts. Without it, layers 2 & 3 still recover, just with delay.
- **Idempotent task code** — the only real defense against the at-least-once
duplicate-execution window.
- `**trigger_rule`** — controls downstream blast radius on terminal failure.
- `**task_instance_history` + per-attempt logs** — full audit trail per try.

**Rule of thumb:** Layers 1–3 guarantee a task *will* get another chance; layer
4 decides *how many* chances; idempotency makes those extra chances safe.

---

## 2. Executor types and use cases

The **executor** is a pluggable strategy (set via `AIRFLOW__CORE__EXECUTOR`) that
answers: *"a task is `queued` — how/where do I run it?"* It always runs **inside
the scheduler**; what it delegates to may be independent (workers/pods).


| Executor                                    | How it runs tasks                                              | Use case                                                            |
| ------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------- |
| **LocalExecutor**                           | Subprocesses on the scheduler host                             | Local dev / single machine. The 3.x floor (also supports SQLite).   |
| **CeleryExecutor** *(this repo)*            | Publishes to a broker; separate Celery **workers** consume     | Distributed, horizontally scalable, queue-based routing.            |
| **KubernetesExecutor**                      | One **pod per task**, then torn down                           | Per-task isolation (own image/resources), elastic, no idle workers. |
| **Celery/LocalKubernetesExecutor** (hybrid) | Routes per task by `queue` to Celery/Local **or** K8s pods     | Mostly cheap tasks + a few heavy/isolated ones.                     |
| **EdgeExecutor** *(new in 3.x)*             | Remote **edge workers pull** work over HTTP                    | Run tasks in other networks/datacenters; HTTP-only workers.         |
| **ECS / Batch** (AWS provider)              | Tasks as ECS tasks / Batch jobs                                | Run on existing AWS infra without a worker fleet.                   |
| **Multiple executors** (2.10+/3.x)          | Several executors at once; route per task via `executor="..."` | One deployment, mixed strategies chosen per task.                   |


Notes:

- `SequentialExecutor` was **removed in 3.0** (LocalExecutor now covers SQLite).
- Executors are partly provider-supplied: `LocalExecutor` is core, but
`CeleryExecutor` (celery provider) and `KubernetesExecutor` (`cncf.kubernetes`)
ship in providers.
- **No executor is itself a standalone service** — it's embedded in the scheduler.
Independent pieces are the *workers* (Celery/edge) or *pods* (Kubernetes) it manages.

---

## 3. Major enhancements: 3.x vs 2.x


| Area                    | Airflow 2.x                                                          | Airflow 3.x                                                                  |
| ----------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **DAG parsing**         | Subprocess *of the scheduler*                                        | **Standalone `dag-processor` service** — a bad DAG can't stall scheduling    |
| **Worker ↔ DB**         | Workers query Postgres directly (needed DB creds)                    | **Task Execution API** — workers speak HTTP to the apiserver only            |
| **Remote/edge workers** | Impractical (DB access required)                                     | First-class: **EdgeExecutor**, HTTP-only workers across networks             |
| **Webserver**           | `airflow-webserver`                                                  | Renamed `**apiserver`**; also hosts the Execution API; new React UI          |
| **Authoring API**       | Imports scattered across `airflow.decorators`, `airflow.models`, ... | Stable **Task SDK** (`airflow.sdk`) — decoupled from internals, versioned    |
| **REST API**            | `/api/v1` (experimental-ish)                                         | Stable `**/api/v2`**                                                         |
| `**catchup` default**   | `True` (surprised beginners with backfills)                          | `**False`**                                                                  |
| **Scheduling model**    | Time-centric (DAG-level `schedule_interval`)                         | First-class **data/asset-aware scheduling** (Assets) alongside time          |
| **DAG versioning**      | Limited; UI reflected latest parse                                   | **DAG versioning** — runs tracked against the DAG version that produced them |
| **Backfills**           | CLI-driven, outside scheduler                                        | **Scheduler-managed backfills** (triggerable from UI/API)                    |
| **Security posture**    | Workers inside the trust boundary                                    | Workers isolated behind the Execution API → smaller attack surface           |


**Throughline:** draw hard boundaries around the two riskiest things — parsing
DAG code (`dag-processor`) and running task code (workers via the Execution API) —
so the scheduler + DB core stays fast, secure, and able to support remote
execution and a stable authoring contract.

---

## Quick map to this repo

- Executor: `CeleryExecutor` embedded in **airflow-scheduler**.
- Broker: **redis**, single `default` queue.
- Task runner: **airflow-worker**, reporting via the apiserver's Execution API.
- Examples hidden (`LOAD_EXAMPLES=false`); UI at `http://localhost:8080`
(`airflow` / `airflow`). See `notes/docker-compose.md` for per-service detail.

