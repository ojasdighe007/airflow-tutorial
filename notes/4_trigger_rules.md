# Trigger Rules — Tolerating *Specific* Upstream Failures

**Goal:** `task1 >> [task2, task3, task4] >> task5`, but `task5` should still run when **task4** fails — yet **not** when task2 or task3 fail.

## The gotcha

A task's `trigger_rule` applies **uniformly to all of its upstream tasks**. There is **no per-edge trigger rule** in Airflow. So you cannot tell `task5` to "tolerate task4 but stay strict about task2/task3" with a single rule.

- Default rule is `all_success` → any upstream failure blocks the task.
- Switching `task5` to `all_done` would let it run even when task2/task3 fail → violates the requirement.

## The fix: change the graph, not just the rule

Insert a small **buffer (absorber) task** downstream of task4. It runs regardless of task4's state (`trigger_rule="all_done"`) and always succeeds. Then `task5` keeps the default `all_success` rule against task2, task3, and the buffer — never task4 directly.

```python
from airflow.utils.trigger_rule import TriggerRule

@task.python(trigger_rule=TriggerRule.ALL_DONE)  # runs even if task4 failed
def task4_buffer():
    print("task4 finished (success or fail) - continuing")

t1 >> [t2, t3, t4]
t4 >> t4b               # buffer absorbs task4's outcome
[t2, t3, t4b] >> t5     # task5 = all_success over task2, task3, buffer
```

See `dags/06_trigger_rules.py` for the full DAG.

## Why it works

`task5` still uses `all_success`, so a failure in task2 or task3 propagates `upstream_failed` and blocks it. But task4 is **no longer a direct parent** of task5 — the buffer is. Since the buffer ends in `success` no matter what task4 did, task4 failing never reaches task5.

## Behaviour

| Scenario | task4_buffer | task5 |
| --- | --- | --- |
| Everything succeeds | runs, succeeds | runs |
| **task4 fails** | runs (all_done), succeeds | **runs** ✅ |
| task2 or task3 fails | runs | **skipped / upstream_failed** ❌ |

## Common trigger rules (reference)

| Rule | task runs when... |
| --- | --- |
| `all_success` (default) | all upstream succeeded |
| `all_failed` | all upstream failed |
| `all_done` | all upstream finished (any state) |
| `one_success` | at least one upstream succeeded |
| `one_failed` | at least one upstream failed |
| `none_failed` | no upstream failed (success or skipped ok) |
| `none_failed_min_one_success` | no failures + at least one success |

## Alternative (and why the buffer is usually better)

You could instead `try/except` inside `task4` so it swallows its own error and always ends `success`. That avoids the extra task, **but** task4 then never shows as failed in the UI — you lose visibility for debugging. The buffer pattern keeps task4 honestly marked as failed while still letting the pipeline continue.

## Rule of thumb

- Need to tolerate **all** upstream failures → just set a looser `trigger_rule` on the join task.
- Need to tolerate **only specific** upstream tasks → **restructure with a buffer task**; trigger rules alone can't express per-parent tolerance.
