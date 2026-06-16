# XComs — Short Notes

**XCom** = "Cross-Communication". The mechanism Airflow uses to pass **small** bits of data between tasks within a DAG run.

## Core idea

- Tasks run in **separate processes** (often separate machines/pods), so they can't share Python variables directly.
- XComs bridge this: a task **pushes** a value, a downstream task **pulls** it.
- Values are stored in the **Airflow metadata database**, keyed by `(dag_id, run_id, task_id, key)`.

## Golden rule: keep it small

- XComs are for **metadata / small payloads** (IDs, file paths, counts, small dicts) — not big datasets.
- The value must be **serializable** (JSON by default).
- For large data, pass a **reference** (e.g. an S3 path) via XCom and store the actual data elsewhere.

## Default key

- When you return a value from a task, it is stored under the key `**return_value`**.
- A custom `xcom_push(key=..., value=...)` lets you store multiple named values from one task.

## What are `**kwargs`?

`**kwargs` is standard **Python syntax** — it collects any extra keyword arguments passed to a function into a single **dict** named `kwargs`. It has nothing to do with Airflow by itself.

```python
def f(**kwargs):
    print(kwargs)   # {'a': 1, 'b': 2}

f(a=1, b=2)
```

### What Airflow does for us using `**kwargs`

When Airflow runs a task, it builds a **task context** — a dict of runtime values for *that specific run* — and **injects it into your function as keyword arguments**. Declaring `**kwargs` (or specific named params) is how you receive it. So Airflow is doing several things for you automatically:

- **Wires up the runtime context** — you never construct `ti`, `run_id`, or the logical date yourself; Airflow computes them per run and hands them in.
- **Gives you the `TaskInstance` (`ti`)** — your gateway to `xcom_push` / `xcom_pull`, retries info, state, etc.
- **Provides scheduling values** — `ds`, `logical_date`, `data_interval_start/end` so the same code adapts to each run's date window (key for incremental loads).
- **Exposes DAG/run metadata** — `dag`, `task`, `params`, `conf` (e.g. values from a manual trigger).
- **Templates fields** — Jinja templates like `{{ ds }}` are resolved from this same context before your task runs.

```python
@task.python
def my_task(**kwargs):
    ti = kwargs['ti']          # the TaskInstance (used for xcom_push/pull)
    print(kwargs['ds'])        # logical date as YYYY-MM-DD string
    print(kwargs['run_id'])    # this DAG run's id
```

Common keys in the context dict:


| Key                                         | What it is                                                 |
| ------------------------------------------- | ---------------------------------------------------------- |
| `ti` / `task_instance`                      | the TaskInstance — gateway to `xcom_push` / `xcom_pull`    |
| `ds` / `ds_nodash`                          | logical (execution) date string, `2026-06-12` / `20260612` |
| `logical_date`                              | the run's logical datetime (pendulum)                      |
| `data_interval_start` / `data_interval_end` | the run's time window (great for incremental loads)        |
| `run_id`                                    | unique id of this DAG run                                  |
| `dag` / `task`                              | the DAG and task objects                                   |
| `params` / `conf`                           | DAG params / values passed at trigger time                 |


Notes:

- `ti` is the important one for XComs — that's why manual style starts every task with `ti = kwargs['ti']`.
- You can also pull specific keys directly instead of `**kwargs`, e.g. `def my_task(ti=None, ds=None):` — Airflow fills them by name.
- In **TaskFlow (auto)** style you usually don't need `kwargs` at all, since data arrives as normal function arguments.

---

# Auto (TaskFlow) vs Kwargs (manual) — Implementation Differences

Both DAGs (`04_xcoms_auto.py` and `05_xcoms_kwargs.py`) do the same Extract → Transform → Load flow. The only difference is **how data moves between tasks**.

## 1. Auto / TaskFlow style — `04_xcoms_auto.py`

```4:31:dags/04_xcoms_auto.py
def xcoms_auto_dag():

    @task.python
    def first_task():
        print("Extracting data ... This is the first task")
        fetched_data = {"key": [1,2,3,4,5]}
        return fetched_data
    
    @task.python
    def second_task(data:dict):
        print("Transforming data ... This is the second task")
        fetched_data = data['key']
        transformed_data = fetched_data*2
        transformed_data_dict =  {"transformed_data": transformed_data}
        return transformed_data_dict
    
    @task.python
    def third_task(data: dict):
        print("This is the third task")
        load_data = data
        return load_data
    
    # Defining the task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)
```

**How XComs work here:**

- A task simply `**return`s** a value → Airflow auto-pushes it under key `return_value`.
- You **pull** by passing the previous task's handle as a **function argument**: `second_task(first)`.
- The handle `first` is an `**XComArg`** — at runtime Airflow resolves it to the actual returned value and injects it.
- Dependencies are created **implicitly** by data flow: passing `first` into `second_task(first)` already makes `first → second`. The explicit `first >> second >> third` line is redundant here (harmless, but the wiring is already done by the arguments).

## 2. Kwargs / manual style — `05_xcoms_kwargs.py`

```6:42:dags/05_xcoms_kwargs.py
def xcoms_kwargs_dag():

    @task.python
    def first_task(**kwargs):

        # Extracting ti from kwargs to push XComs manually
        ti = kwargs['ti']

        print("Extracting data ... This is the first task")
        fetched_data = {"key": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value = fetched_data)
    
    @task.python
    def second_task(**kwargs):
        print("Transforming data ... This is the second task")

        #Pulling XComs pushed by first task
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')['key']
        transformed_data = fetched_data*2
        transformed_data_dict =  {"transformed_data": transformed_data}
        ti.xcom_push(key='transformed_result', value = transformed_data_dict)
    
    @task.python
    def third_task(**kwargs):
        print("This is the third task")

        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids = 'second_task', key = 'transformed_result')
        return load_data
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third
```

**How XComs work here:**

- Each task grabs the **task instance** from the context: `ti = kwargs['ti']`.
- You **push explicitly**: `ti.xcom_push(key='return_result', value=...)`.
- You **pull explicitly**, naming the source task and key: `ti.xcom_pull(task_ids='first_task', key='return_result')`.
- Because the tasks don't share data via arguments, Airflow does **not** know the order automatically — you **must** declare `first >> second >> third` yourself. Forget it and the tasks may run in parallel / wrong order and the pulls return `None`.

---

## Side-by-side


| Aspect         | Auto (TaskFlow)                            | Kwargs (manual)                               |
| -------------- | ------------------------------------------ | --------------------------------------------- |
| Push           | `return value` (auto)                      | `ti.xcom_push(key, value)`                    |
| Pull           | Function argument (`second_task(first)`)   | `ti.xcom_pull(task_ids=..., key=...)`         |
| XCom key       | `return_value` (default)                   | Custom key you choose                         |
| Dependencies   | Inferred from data flow                    | Must be set manually (`>>`)                   |
| Access to `ti` | Not needed                                 | Required (`kwargs['ti']`)                     |
| Coupling       | Loose — refer to handles, not string names | Tight — hard-coded `task_ids` / `key` strings |


---

## Pros & Cons

### Auto / TaskFlow

**Pros**

- Cleaner, more Pythonic — tasks look like normal functions.
- Less boilerplate (no `ti`, no manual push/pull).
- Dependencies inferred automatically from arguments → fewer wiring bugs.
- Refactor-safe: no magic strings for `task_ids` / `key`.

**Cons**

- Less explicit control over **keys** (everything is `return_value` unless you do more work).
- Harder to push **multiple distinct values** from a single task.
- Slightly more "magic" — the `XComArg` resolution is hidden, which can confuse beginners.

### Kwargs / manual

**Pros**

- Full, explicit control: custom keys, push many values per task, pull from any specific task.
- Useful when consuming XComs from a task you **didn't** pass as an argument (e.g. sensors, branching, non-linear graphs).
- Maps directly to the underlying API — good for understanding what XCom *actually* does.

**Cons**

- Verbose boilerplate (`ti = kwargs['ti']` everywhere).
- **Must** declare dependencies manually — easy to forget → `None` results or race conditions.
- Fragile: relies on hard-coded `task_ids` / `key` strings that break silently on rename.

## Rule of thumb

- **Default to TaskFlow (auto)** for normal linear/data-flow DAGs — it's the modern Airflow 3.x recommendation.
- **Reach for manual `xcom_push` / `xcom_pull`** only when you need custom keys, multiple outputs, or cross-task access that argument-passing can't express.

