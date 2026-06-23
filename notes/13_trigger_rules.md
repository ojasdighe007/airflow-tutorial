# Trigger Rules — When a Task Decides to Run

A task's **`trigger_rule`** controls when it becomes eligible to run, based on the
final state of **all** its upstream tasks. The default is `all_success`. See
`dags/19_trigger_rules.py` for a runnable demo where one upstream task fails on
purpose so you can watch each rule react.

> Key idea: the rule is evaluated over **all upstream tasks together** — there is
> **no per-edge rule**. (For tolerating only *specific* parents, see the buffer
> pattern in `notes/4_trigger_rules.md`.)

## Setting a rule

```python
from airflow.task.trigger_rule import TriggerRule

@task.python(trigger_rule=TriggerRule.ALL_DONE)
def cleanup():
    ...
```

## The demo graph

```python
start >> [good_a, good_b, bad_c]   # bad_c always fails

[good_a, good_b, bad_c] >> cleanup                 # ALL_DONE
[good_a, good_b, bad_c] >> notify_on_any_success   # ONE_SUCCESS
[good_a, good_b, bad_c] >> alert_on_any_failure    # ONE_FAILED
[good_a, good_b]        >> summarize               # NONE_FAILED_MIN_ONE_SUCCESS
```

With `bad_c` failing, you'll see:

| Join task | Rule | Outcome |
| --- | --- | --- |
| `cleanup` | `all_done` | **runs** (every parent finished) |
| `notify_on_any_success` | `one_success` | **runs** (a, b succeeded) |
| `alert_on_any_failure` | `one_failed` | **runs** (c failed) |
| `summarize` | `none_failed_min_one_success` | **runs** (its parents a, b are clean) |

If `summarize` had depended on `bad_c` too, it would be **skipped /
`upstream_failed`** because a failure is present.

## Common rules (reference)

| Rule | Task runs when... | Typical use |
| --- | --- | --- |
| `all_success` (default) | all upstream succeeded | normal pipelines |
| `all_failed` | all upstream failed | fallback / recovery path |
| `all_done` | all upstream finished (any state) | cleanup, teardown, notifications |
| `all_skipped` | all upstream were skipped | default-branch handling |
| `one_success` | ≥1 upstream succeeded | proceed as soon as any source is ready |
| `one_failed` | ≥1 upstream failed | fail-fast alerting |
| `one_done` | ≥1 upstream succeeded or failed | react to first finisher |
| `none_failed` | no upstream failed (success/skipped ok) | joins after a branch |
| `none_failed_min_one_success` | no failures **and** ≥1 success | branch joins that need real work |
| `none_skipped` | no upstream was skipped | require every branch to run |

## How states propagate

- A **failed** task pushes `upstream_failed` down `all_success` edges, which
  cascades and skips the rest of that path.
- A **skipped** task (e.g. from `@task.branch`) is *not* a failure — rules like
  `none_failed*` tolerate skips, which is why they're the usual branch-join rule.
- `all_done` / `one_failed` ignore the difference and just look at completion.

## Rule of thumb

- Cleanup that must **always** run → `all_done`.
- Alerting the moment anything breaks → `one_failed`.
- Joining branches where some paths are skipped → `none_failed_min_one_success`.
- Need to tolerate **only specific** failing parents → restructure with a buffer
  task (`notes/4_trigger_rules.md`); a single rule can't express per-parent logic.
