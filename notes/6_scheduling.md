# DAG Scheduling

How and when a DAG runs is controlled mainly by `start_date` + `schedule`, with `end_date` and `catchup` as modifiers. See `dags/08_schedule_preset.py`.

> **Key idea:** Airflow runs a DAG at the **end** of each interval. With `@daily` and `start_date = Jun 12`, the run for the Jun 12 interval actually fires at the start of Jun 13.

## Core variables

### 1. `start_date` (Required)

When the DAG becomes eligible. First run = `start_date` + one interval.

```python
from airflow.sdk import dag
from pendulum import datetime

@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule="@daily",
)
def my_dag():
    ...
```

- Always set `tz` → timezone-aware, predictable schedules.
- Use a **fixed** datetime. Never `datetime.now()` / dynamic dates → buggy, drifting runs.

### 2. `end_date` (Optional)

No runs scheduled after this datetime. Omit to run indefinitely.

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    end_date=datetime(2026, 12, 31, tz="Asia/Kolkata"),
    schedule="@daily",
)
def my_dag():
    ...
```

### 3. `catchup` (Optional, usually `False` in Airflow 3)

Controls whether missed intervals between `start_date` and now get backfilled.

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=False,
)
def my_dag():
    ...
```

- `True` → runs every missed interval from `start_date` to now.
- `False` → only runs from the current interval onward.
- Related: **backfill** is the manual CLI way to run a past date range on demand.

### 4. `schedule` (Required)

When/how often the DAG runs. Accepts several types:

**a. Presets (string)** — all fire at midnight (00:00) except `@hourly`/`@once`:

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule="@daily",
)
def my_dag():
    ...
```


| Preset                  | Cron equiv  | Meaning                |
| ----------------------- | ----------- | ---------------------- |
| `@once`                 | —           | run one time only      |
| `@hourly`               | `0 * * * *` | top of every hour      |
| `@daily`                | `0 0 * * *` | midnight               |
| `@weekly`               | `0 0 * * 0` | Sunday midnight        |
| `@monthly`              | `0 0 1 * *` | 1st of month, midnight |
| `@yearly` / `@annually` | `0 0 1 1 *` | Jan 1, midnight        |


**b. Cron expression (string)** — `"min hour day month weekday"`:

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule="30 14 * * 1-5",   # 2:30 PM on weekdays (Mon-Fri)
)
def my_dag():
    ...
```

**c. Timedelta (relative)** — spacing measured from `start_date`/last run:

```python
from datetime import timedelta

@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule=timedelta(hours=6),   # every 6 hours
)
def my_dag():
    ...
```

**d. `None`** — no automatic schedule; trigger manually or from another DAG:

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule=None,
)
def my_dag():
    ...
```

**e. Dataset / asset-based** — event-driven; runs when an upstream dataset updates:

```python
from airflow.sdk import Asset

@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule=[Asset("s3://bucket/data.csv")],
)
def my_dag():
    ...
```

**f. Timetable** — custom Python schedule for irregular/complex cases:

```python
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule=CronTriggerTimetable("0 9 * * 1-5", timezone="Asia/Kolkata"),
)
def my_dag():
    ...
```

**g. Events (explicit date list)** — run only on a fixed list of datetimes; no recurring pattern. Good for predictable-but-irregular dates (holidays, releases, sporting events). See `dags/12_special_dates.py`.

```python
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(event_dates=[
    datetime(2026, 1, 1, tz="America/Halifax"),
    datetime(2026, 1, 26, tz="America/Halifax"),
])

@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 1, 1, tz="America/Halifax"),
    end_date=datetime(2026, 1, 31, tz="America/Halifax"),
    schedule=special_dates,
    catchup=True,
)
def my_dag():
    ...
```

- Runs **once per listed datetime** — nothing in between. The list must be finite and reasonably sized (it's loaded in full).
- Give the `event_dates` a `tz` so they aren't interpreted as UTC. `restrict_to_events=True` makes manual runs snap to the most recent event instead of "now".
- Pair with `start_date` / `end_date` to bound the active window, and `catchup=True` if you want past listed dates to backfill.

**Specific timestamps (not just dates):** despite the name, each entry is a full `datetime`, so the **time-of-day is honored** (omit it and it defaults to `00:00`). You can even schedule **multiple runs on the same day** at different times:

```python
special_times = EventsTimetable(event_dates=[
    datetime(2026, 1, 1, 14, 30, tz="America/Halifax"),  # Jan 1, 2:30 PM
    datetime(2026, 1, 1, 18, 0,  tz="America/Halifax"),  # Jan 1, 6:00 PM
    datetime(2026, 1, 26, 9, 15, tz="America/Halifax"),  # Jan 26, 9:15 AM
])
```

- For **recurring** time-of-day runs (e.g. every weekday at 2:30 PM) use `CronTriggerTimetable` instead — `EventsTimetable` is only for an explicit, finite list of moments.

## Other scheduling-related `@dag` params (tuning)


| Param                             | Purpose                                     | Example                             |
| --------------------------------- | ------------------------------------------- | ----------------------------------- |
| `max_active_runs`                 | concurrent DAG runs (caps catchup/backfill) | `max_active_runs=1`                 |
| `max_active_tasks`                | concurrent task instances across the DAG    | `max_active_tasks=16`               |
| `dagrun_timeout`                  | kill a run exceeding this duration          | `dagrun_timeout=timedelta(hours=2)` |
| `max_consecutive_failed_dag_runs` | auto-pause after N failed runs              | `=3`                                |


## Per-task timing (via `default_args` or on tasks)


| Param                                                               | Purpose                                                               |
| ------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `depends_on_past`                                                   | wait for this task's previous run to succeed                          |
| `wait_for_downstream`                                               | wait for full downstream of previous run                              |
| `retries` / `retry_delay`                                           | retry count and base delay                                            |
| `retry_exponential_backoff`                                         | `True` → wait grows exponentially each retry                          |
| `max_retry_delay`                                                   | upper cap on the delay when backoff is on                             |
| `execution_timeout`                                                 | max runtime for a single task                                         |
| `on_failure_callback` / `on_success_callback` / `on_retry_callback` | run a function (alert, cleanup) on that event                         |
| `pool`                                                              | limit concurrency for a class of tasks via a named slot pool          |
| `priority_weight`                                                   | ordering when slots are scarce (higher runs first)                    |
| `queue`                                                             | route the task to a specific worker queue (Celery/K8s)                |
| `trigger_rule`                                                      | when a task fires vs upstream states (see `notes/4_trigger_rules.md`) |
| `owner`                                                             | task owner shown in the UI / for filtering                            |


```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule="@daily",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "depends_on_past": False},
)
def my_dag():
    ...
```

### Retries & exponential backoff

By default retries use a **fixed** `retry_delay` (same wait every time). Opt into backoff so the wait grows after each failure — ideal for flaky external APIs/DBs so you don't hammer a struggling service.

```python
@dag(
    dag_id="my_dag",
    start_date=datetime(2026, 6, 12, tz="Asia/Kolkata"),
    schedule="@daily",
    default_args={
        "retries": 4,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,         # 1m -> 2m -> 4m -> 8m ...
        "max_retry_delay": timedelta(minutes=30),  # caps the growth
    },
)
def my_dag():
    ...
```

- With backoff on, `retry_delay` is the **base**; each retry roughly doubles (with jitter) until it hits `max_retry_delay`.
- Without backoff, the wait is always `retry_delay`.

### Callbacks (instead of email/SLA in Airflow 3)

The old `email_on_failure` / SMTP params and classic **SLA** are deprecated/removed in Airflow 3. Use **callbacks** (or the newer Deadline Alerts) for notifications.

```python
def alert(context):
    print(f"Task failed: {context['task_instance'].task_id}")

@task.python(on_failure_callback=alert, on_retry_callback=alert)
def risky_task():
    ...
```

### Per-task override examples

Any `default_args` value can be overridden on a single task; the task-level arg wins.

```python
@task.python(retries=5, pool="api_pool", priority_weight=10)
def special_task():
    ...
```

### `default_args`: DAG-declared but task-applied

`default_args` is **passed at the DAG level**, but its keys are **task-level parameters**. It's a convenience dict — Airflow applies each key as the **default** for every task, so you don't repeat them on each task.

- Keys like `retries`, `retry_delay`, `depends_on_past`, `execution_timeout`, `owner` all configure **individual tasks**.
- A task can **override** any of them; the task-level value wins:

```python
@task.python(retries=5)   # ignores the default of 2 for this task only
def special_task():
    ...
```


|                       | Truly DAG-level                                                          | `default_args` (DAG-declared, task-applied)                               |
| --------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Examples              | `schedule`, `start_date`, `catchup`, `max_active_runs`, `dagrun_timeout` | `retries`, `retry_delay`, `depends_on_past`, `execution_timeout`, `owner` |
| Scope                 | the DAG / DAG run as a whole                                             | each individual task                                                      |
| Overridable per task? | No                                                                       | Yes (task-level arg wins)                                                 |


> `start_date` can live in **both** places, but the modern/recommended spot is directly on `@dag(...)`, not inside `default_args`.

## Rule of thumb

- **Define a schedule:** `start_date` + `schedule` (the two required pieces).
- **Modify the window/backfill:** `end_date`, `catchup`.
- **Tune throughput/failures:** `max_active_runs`, `max_active_tasks`, `dagrun_timeout`.
- Set most timing 90% of the time with just the first four; reach for the rest only when tuning.

