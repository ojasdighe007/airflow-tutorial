# Incremental Loads

Instead of reprocessing the whole source every run, process **only the slice of data for the current run's interval**. See `dags/11_incremental_load.py`.

## The key context values

Airflow injects per-run time boundaries into the task context (see `notes/3_xcoms.md`):

| Value | Meaning |
| --- | --- |
| `data_interval_start` | start of this run's window (inclusive) |
| `data_interval_end` | end of this run's window (exclusive) |
| `logical_date` | the run's logical timestamp (the anchor) |
| `ds` / `ds_nodash` | logical date as `YYYY-MM-DD` / `YYYYMMDD` |

```python
@task.python
def extract_window(**kwargs):
    start = kwargs["data_interval_start"]
    end = kwargs["data_interval_end"]
    print(f"WHERE updated_at >= {start} AND updated_at < {end}")
```

## Why this matters

- **Bounded work:** each run touches one window (e.g. one day), not the full history.
- **Correct backfills:** with `catchup=True`, every missed interval reruns with *its own* `data_interval_*`, so historical runs load the right slice automatically.
- **Idempotency:** design the load so re-running an interval **overwrites** that partition rather than duplicating it (e.g. delete-then-insert, or `MERGE` on a key). Then retries/backfills are safe.

## Logical date vs "now"

- Use `data_interval_*` / `logical_date`, **never** `datetime.now()`, to decide the window. `now()` makes runs non-deterministic and breaks backfills.
- Airflow runs at the **end** of an interval, so `data_interval_end` is roughly "when this run fires" and `data_interval_start` is one schedule-step earlier.

## Rule of thumb

- Slice by the interval, make the load idempotent on a key/partition, and you get free, correct backfills.
