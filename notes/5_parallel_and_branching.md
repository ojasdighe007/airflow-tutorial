# Parallel Tasks & Conditional Branching

Two related patterns built on the same ETL skeleton: one `extract` → three parallel `transform` tasks → `load`. See `dags/06_parallel_tasks.py` and `dags/07_conditional_branches.py`.

## 1. Parallel Tasks (`06_parallel_tasks.py`)

**Goal:** fan-out — run independent transforms (api, db, s3) at the same time, then join into a single load.

- `extract_task` pushes one dict to XCom under `return_value` holding all three datasets.
- Each `transform_task_*` pulls its slice, transforms it, and pushes back under `transformed_data`.
- `load_task` pulls all three transformed results and echoes them.

### Dependency syntax for fan-out / fan-in

```python
extract >> [transform_api, transform_db, transform_s3] >> load_task
```

- A **list** on either side of `>>` creates parallel branches.
- `extract >> [a, b, c]` → fan-out: all three start once extract finishes.
- `[a, b, c] >> load` → fan-in: load waits for **all three** (default `all_success`).
- The three transforms run concurrently (subject to executor / pool slots).

### Gotcha

- All transforms pull from the **same** `extract_task` XCom key (`return_value`) but read **different keys** inside the dict — keeps them independent.
- Parallelism is only real if your executor allows it (e.g. LocalExecutor/Celery). SequentialExecutor will still run them one by one.

## 2. Conditional Branching (`07_conditional_branches.py`)

**Goal:** at runtime, choose **which** downstream path to run — load the data, or skip loading on weekends.

- Same extract + 3 parallel transforms.
- `extract_task` also pushes a `weekend_flag`.
- A `@task.branch` task (`decider_task`) decides the path; non-chosen branches are **skipped**.

### The branch operator

```python
@task.branch
def decider_task(**kwargs):
    weekend_flag = kwargs['ti'].xcom_pull(
        task_ids='extract_task', key='return_value')['weekend_flag']
    return 'no_load_task' if weekend_flag else 'load_task'
```

- `@task.branch` must **return the task_id (str) — or a list of task_ids — to run**.
- All sibling tasks **not** returned are marked `skipped`.
- The returned id must exactly match a real downstream task id.

#### Returning a single id vs a list

```python
@task.branch
def decider_task(**kwargs):
    flag = kwargs['ti'].xcom_pull(task_ids='extract_task', key='return_value')

    # Single task_id (str) -> only that task runs
    if flag['weekend_flag']:
        return 'no_load_task'

    # List of task_ids -> all listed tasks run, the rest are skipped
    if flag.get('full_refresh'):
        return ['load_task', 'archive_task']

    return 'load_task'
```

- **String** `'no_load_task'` → only `no_load_task` runs; the others are `skipped`.
- **List** `['load_task', 'archive_task']` → both run in parallel; `no_load_task` is `skipped`.
- All returned ids must be **direct downstream** tasks of the branch:

```python
extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [load_task, no_load_task, archive_task]
```

### Dependencies

```python
extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [load_task, no_load_task]
```

- The decider sits between the transforms and the two possible end tasks.
- Only one of `load_task` / `no_load_task` actually runs per DAG run.

### Gotcha

- Anything downstream of a skipped branch is also skipped by default (`all_success`). If you later need to **join** branches back together, the join task needs a looser `trigger_rule` like `none_failed_min_one_success` (see `notes/4_trigger_rules.md`).

## Parallel vs Branch — quick contrast

| | Parallel (`[a, b, c]`) | Branch (`@task.branch`) |
| --- | --- | --- |
| Intent | run **all** paths | run **one (or some)** path |
| Non-chosen tasks | n/a — all run | marked `skipped` |
| Decision | static (graph shape) | dynamic (runtime value) |
| Join behaviour | `all_success` works | may need looser trigger rule |
