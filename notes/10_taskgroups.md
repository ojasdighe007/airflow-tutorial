# TaskGroups

A **TaskGroup** visually and logically groups related tasks into a collapsible box in the UI — without creating a separate DAG. See `dags/15_taskgroups.py`.

## Basic usage

```python
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")

with TaskGroup(group_id="extract") as extract:
    transform.override(task_id="transform_api")("api")
    transform.override(task_id="transform_db")("db")

with TaskGroup(group_id="load") as load:
    transform.override(task_id="load_warehouse")("warehouse")

start >> extract >> load     # wire whole groups like single nodes
```

## Key points

- **`group_id` prefixes task ids:** a task in `group_id="extract"` becomes `extract.transform_api`. This keeps ids unique and is how you reference them in `xcom_pull(task_ids="extract.transform_api")`.
- **Nestable:** put a `TaskGroup` inside another for deeper structure.
- **Wire groups directly:** `groupA >> groupB` connects their boundaries, so you don't hand-wire every edge.
- **`.override(task_id=...)`** lets you reuse one `@task` function under multiple unique ids inside a group.

## TaskGroups vs SubDAGs

- SubDAGs (old, removed/deprecated) actually spun up a child DAG → deadlocks and scheduler pain.
- **TaskGroups are pure UI/organisational grouping** within the same DAG — no extra scheduling overhead. Always prefer TaskGroups.

## Rule of thumb

- Use TaskGroups to tame large DAGs into readable sections (e.g. `extract` / `transform` / `load`).
- Remember the `group_id.` prefix when pulling XComs or setting dependencies by string id.
