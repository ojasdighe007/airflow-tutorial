# Sensors

A **sensor** is a task that **waits for a condition** before letting downstream tasks run — a file arriving, a partition landing, an API turning ready, another DAG finishing. See `dags/13_sensors.py`.

## Three ways to write one

```python
# 1. TaskFlow sensor: return True to succeed, False to keep waiting.
@task.sensor(poke_interval=30, timeout=300, mode="poke")
def wait_for_flag():
    return is_ready()

# 2. Built-in FileSensor (needs an 'fs_default' connection)
from airflow.providers.standard.sensors.filesystem import FileSensor
wait_for_file = FileSensor(task_id="wait_for_file", filepath="/tmp/data.csv",
                           poke_interval=60, timeout=1800, mode="reschedule")

# 3. PythonSensor: poke a callable until it returns truthy
from airflow.providers.standard.sensors.python import PythonSensor
```

## Key parameters

| Param | Purpose |
| --- | --- |
| `poke_interval` | seconds between checks |
| `timeout` | give up (fail) after this many seconds |
| `mode` | `"poke"` or `"reschedule"` |
| `soft_fail` | mark **skipped** instead of failed on timeout |

## poke vs reschedule (important)

| | `poke` | `reschedule` |
| --- | --- | --- |
| Worker slot | **held** the whole time | **released** between checks |
| Good for | short waits (seconds–minutes) | long waits (minutes–hours) |
| Cost | can starve the pool if many wait at once | frees slots, lighter on the cluster |

A roomful of `poke` sensors can occupy every worker slot doing nothing — switch long waits to `reschedule`.

## Deferrable = the scalable upgrade

For long waits, **deferrable** sensors/operators (`async`, run on the **triggerer**) free the worker entirely and are even cheaper than `reschedule`. Reach for them when you have many or very long waits.

## Rule of thumb

- Short, cheap check → `@task.sensor` in `poke` mode.
- Long wait → `reschedule` (or deferrable) so you don't hog worker slots.
- Always set a `timeout` so a stuck condition doesn't wait forever.
