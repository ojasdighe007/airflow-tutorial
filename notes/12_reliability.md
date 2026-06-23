# Reliability & Resource Control

Two related concerns for production DAGs: **surviving failures** (retries + callbacks) and **controlling load** (pools + concurrency). See `dags/17_callbacks_retries.py` and `dags/18_pools_concurrency.py`. Reference tables for these params also live in `notes/6_scheduling.md`.

## 1. Retries & exponential backoff — `17_callbacks_retries.py`

```python
default_args={
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,   # 10s -> 20s -> 40s ...
    "max_retry_delay": timedelta(minutes=5),
}
```

- Without backoff the wait is always `retry_delay`; with it, the wait grows (base = `retry_delay`) up to `max_retry_delay`.
- Great for flaky external services so you don't hammer something that's already struggling.

## Callbacks

Run a function on a lifecycle event — for alerting, cleanup, metrics. Each receives the **context** dict.

```python
def on_failure(context):
    print(f"ALERT: {context['task_instance'].task_id} failed")

@task.python(on_failure_callback=on_failure, on_retry_callback=..., on_success_callback=...)
def flaky_task():
    ...
```

- In Airflow 3 the old `email_on_failure` / SMTP params and classic **SLA** are deprecated/removed — use **callbacks** (or Deadline Alerts) instead.

## 2. Pools & concurrency — `18_pools_concurrency.py`

```python
@dag(max_active_runs=1, max_active_tasks=2)        # DAG-level caps
def ...:
    @task.python(pool="api_pool", priority_weight=10)  # task-level
    def call_api(): ...
```

| Control | Scope | Purpose |
| --- | --- | --- |
| `pool` | across **all** DAGs sharing it | cap concurrency for a resource (e.g. rate-limited API, DB) |
| `priority_weight` | within contended slots | higher value runs first |
| `max_active_tasks` | one DAG | max task instances running at once |
| `max_active_runs` | one DAG | max concurrent runs of the DAG |

- A **pool** is a named bucket of N slots; tasks assigned to it wait when all slots are busy. Create it first: `airflow pools set api_pool 2 "rate-limited API"` (or Admin -> Pools).
- Use pools to protect fragile downstreams; use `max_active_*` to bound a single DAG's footprint.

## Rule of thumb

- Expect transient failures → set `retries` + backoff; wire `on_failure_callback` for visibility.
- Shared limited resource → put those tasks in a **pool**.
- Runaway parallelism → cap with `max_active_tasks` / `max_active_runs`.
