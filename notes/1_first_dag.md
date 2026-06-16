# `1_first_dag.py` — Concepts Covered

This note walks through every concept used in `dags/1_first_dag.py`, the first DAG written in this tutorial repo. The file is short, but it touches the core building blocks of Airflow 3.x's **TaskFlow API**.

Full source under discussion:

```1:26:dags/1_first_dag.py
from airflow.sdk import dag, task

@dag
def first_dag():

    @task.python
    def first_task():
        print("This is the first task")
    
    @task.python
    def second_task():
        print("This is the second task")
    
    @task.python
    def third_task():
        print("This is the third task")
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiating the DAG
first_dag()
```

---

## 1. `from airflow.sdk import dag, task`

### What it is
- `airflow.sdk` is the **Task SDK** introduced as the stable, public author-facing API in Airflow 3.x.
- It is the replacement for importing `DAG`, `task`, `dag`, etc. from scattered locations like `airflow.decorators`, `airflow.models.dag`, `airflow.operators.python`, etc.

### Why it matters
- Everything you need to *author* a DAG (decorators, context helpers, sensors, hooks-lite) is meant to live under `airflow.sdk`.
- It decouples DAG code from internal Airflow modules — your DAG file no longer needs to know whether the scheduler, worker, or API server is running it.
- In Airflow 2.x you would have written:

```python
from airflow.decorators import dag, task
```

  In Airflow 3.x the canonical import is `from airflow.sdk import dag, task`. Both still work in 3.x, but the SDK form is the forward-compatible one.

---

## 2. The `@dag` decorator

### What it does
`@dag` turns a regular Python function into a **DAG factory**. When you later call the function (`first_dag()` at the bottom of the file), Airflow:

1. Executes the function body in a special context.
2. Captures every `@task`-decorated callable invoked inside as a node in the DAG.
3. Captures dependencies declared with `>>` / `<<` / `set_upstream` / `set_downstream`.
4. Registers the resulting `DAG` object in the module's namespace so the **DAG processor** can find it.

### Why use the decorator form?
- The function name becomes the **`dag_id`** by default (here: `first_dag`).
- The function's docstring becomes the DAG's description.
- It encourages a clean, Pythonic style: tasks are just functions, dependencies are normal Python expressions.
- Compare to the older "context manager" style:

  ```python
  with DAG(dag_id="first_dag", schedule=None, start_date=...) as dag:
      ...
  ```

  The decorator form removes the boilerplate of constructing a `DAG` object explicitly.

### What you're *not* setting here (and the defaults)
Because `@dag` is used bare (no arguments), the DAG inherits framework defaults:

| Parameter        | Default in this file              | Notes                                                                 |
| ---------------- | --------------------------------- | --------------------------------------------------------------------- |
| `dag_id`         | `"first_dag"` (from function name) | Override with `@dag(dag_id="...")`.                                  |
| `schedule`       | `None`                            | DAG only runs when triggered manually.                                |
| `start_date`     | `None` → effectively "now"        | In production you almost always want an explicit `start_date`.        |
| `catchup`        | Airflow 3.x default is `False`    | In 2.x the default was `True`, which surprised many beginners.        |
| `tags`, `owner`  | unset                             | Add them for filtering in the UI.                                     |

A more "real-world" version would look like:

```python
@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial"],
)
def first_dag():
    ...
```

---

## 3. The `@task.python` decorator

### What it does
`@task.python` turns a regular Python function into an Airflow **task** that runs in a local Python process (under the `PythonOperator` family, internally).

- Each call to the decorated function inside the DAG body creates a **task instance node** in the DAG graph.
- The function name becomes the **`task_id`** by default (here: `first_task`, `second_task`, `third_task`).
- The function's return value becomes an **XCom** automatically (TaskFlow API style) — though these three tasks return `None`, so nothing is pushed.

### `@task` vs `@task.python`
- `@task` alone is the generic form and defaults to a Python task.
- `@task.python` is the **explicit** form. It is functionally equivalent here, but is the recommended pattern in Airflow 3.x because the `task` decorator is now a namespace with many variants:
  - `@task.python` — run in the local worker process.
  - `@task.virtualenv` — run inside a one-shot virtualenv.
  - `@task.external_python` — run with a pre-built Python interpreter.
  - `@task.bash` — run a Bash command.
  - `@task.docker`, `@task.kubernetes` — run inside a container / pod.
  - `@task.branch` — branching logic.
  - `@task.short_circuit` — skip downstream if condition fails.

  Using `@task.python` makes the execution environment explicit and future-proofs the file as more variants are added.

### What the function body does
```python
print("This is the first task")
```

Inside an Airflow task, `print` (and Python `logging`) goes to the **task log**, which is what you see in the UI at:

`logs/dag_id=first_dag/run_id=.../task_id=first_task/attempt=1.log`

That is exactly the path you can see in this repo from the earlier manual run:

`logs/dag_id=first_dag/run_id=manual__2026-05-26T21:36:55+00:00/task_id=first_task/attempt=1.log`

---

## 4. Task *definition* vs task *instantiation*

This is the single most important concept in the file, and it's easy to miss:

```python
first = first_task()   # <-- this is NOT executing the print()
```

Calling `first_task()` inside a `@dag` function does **not** run the function body. Instead, it:

1. Creates a **task instance reference** (technically an `XComArg` / operator instance) in the DAG graph.
2. Returns a handle (`first`) that you can use to wire up dependencies and consume return values.

The actual `print("This is the first task")` only runs later, on a worker, when the scheduler decides it is time for that task in a DAG run.

### Why three separate variables?
```python
first = first_task()
second = second_task()
third = third_task()
```

Each line registers a node in the DAG. The variables are just convenient handles so the next line can express the order without re-calling the functions.

You could also chain them inline:

```python
first_task() >> second_task() >> third_task()
```

…but the named-variable form is preferred when you'll reference the same task more than once (e.g., fan-out / fan-in patterns).

---

## 5. The `>>` operator — dependency declaration

```python
first >> second >> third
```

### What it means
- `a >> b` is read as **"a is upstream of b"** — i.e. `b` runs after `a` succeeds.
- Equivalent forms:
  - `a.set_downstream(b)`
  - `b.set_upstream(a)`
  - `b << a`

The chain `first >> second >> third` is left-associative and creates two edges:

- `first → second`
- `second → third`

### How the DAG is actually built
After this line runs, Airflow's internal graph looks like:

```
first_task ──▶ second_task ──▶ third_task
```

This is a **linear DAG** — the simplest topology. More interesting topologies use the same operator on lists:

```python
first >> [second, third]          # fan-out
[second, third] >> fourth         # fan-in
```

### Why `>>` and not, say, `,`?
Airflow overloads the bit-shift operators (`__rshift__` / `__lshift__`) on `BaseOperator` / `XComArg` specifically because they:

- Read like an arrow visually (`>>`).
- Are not normally used in DAG code, so the overload has no surprising overlap.
- Allow expressive chaining without parentheses.

---

## 6. The trailing `first_dag()` call

```python
#Instantiating the DAG
first_dag()
```

### Why it's there
- `@dag` only turns the function into a *factory*. It does **not** auto-register the DAG.
- To make the DAG visible to Airflow, the factory must be **called at module import time**, so the resulting `DAG` object exists in the module's globals.
- The Airflow **DAG processor** imports each `.py` file under `dags/` and scans the resulting module's globals for `DAG` instances.

### What happens without this line
If you delete `first_dag()`, the file imports cleanly, but no DAG appears in the UI — the function was defined but never invoked, so no `DAG` object is ever created.

### Common variant
You'll often see the return value captured:

```python
dag_obj = first_dag()
```

This is purely cosmetic — assigning to a name is not required because the `@dag` decorator already arranges for the created DAG to be discoverable. The bare `first_dag()` call in this file is enough.

---

## 7. Putting it all together — what Airflow sees

When the scheduler / DAG processor imports `dags/1_first_dag.py`, this sequence happens:

1. `from airflow.sdk import dag, task` — pulls in the decorators.
2. `@dag def first_dag(): ...` — defines a DAG factory named `first_dag`.
3. `first_dag()` at the bottom — invokes the factory:
   1. Enters a DAG-building context.
   2. Defines three `@task.python` callables (`first_task`, `second_task`, `third_task`).
   3. Calls each one once to register three nodes.
   4. `first >> second >> third` adds two edges.
   5. Returns a fully-constructed `DAG(dag_id="first_dag")` object.
4. The DAG processor sees a `DAG` instance in the module and serializes it into the metadata DB.
5. The UI now lists **`first_dag`** with three tasks in a linear chain.
6. Triggering a run causes the scheduler to schedule `first_task` → `second_task` → `third_task` in order.

---

## 8. Concept checklist

By the end of this file you should be comfortable with:

- [x] The `airflow.sdk` import surface as the modern entry point.
- [x] The `@dag` decorator and the DAG-factory pattern.
- [x] The `@task.python` decorator and its place in the wider `@task.*` family.
- [x] The distinction between **defining** a task function and **instantiating** it inside a DAG.
- [x] The `>>` bit-shift operator for declaring upstream/downstream relationships.
- [x] Why the DAG factory must be **called** at module level for Airflow to discover it.
- [x] Where `print()` output lands (the per-attempt task log under `logs/dag_id=.../task_id=.../attempt=N.log`).

---

## 9. Natural next steps (foreshadowing later DAGs)

The file deliberately leaves a lot unsaid. The next DAGs in this repo and the wider tutorial typically introduce:

- **Scheduling**: `schedule="@daily"`, cron strings, `Dataset` / `Asset`-based scheduling.
- **`start_date` and `catchup`**: controlling backfills.
- **Passing data between tasks** via TaskFlow return values (XComs done implicitly).
- **DAG versioning** (see the sibling file `dags/2_dag_versioning.py`).
- **Parameterization**: `params=...`, Jinja templating, `dag_run.conf`.
- **Other `@task.*` flavors**: `@task.bash`, `@task.branch`, `@task.virtualenv`.
