# Dynamic Task Mapping

**The problem:** sometimes you don't know *how many* things you'll process until
the DAG actually runs (e.g. "process every file that landed today"). Dynamic
mapping lets Airflow build the right number of parallel tasks **at run time**.
See `dags/14_dynamic_task_mapping.py`.

## The 3 functions, step by step

```python
@task.python
def get_files():
    return ["a.csv", "b.csv", "c.csv"]   # the list isn't known until this runs

@task.python
def process_file(filename: str, bucket: str):
    print(f"Processing {filename} from {bucket}")
    return len(filename)

@task.python
def summarize(sizes: list[int]):
    print(f"Processed {len(sizes)} files")

sizes = process_file.partial(bucket="s3://raw").expand(filename=get_files())
summarize(sizes)
```

**1. `get_files` — produce the list.**
Returns the items to work on. The length is decided at run time, not when you
write the DAG.

**2. `process_file` — runs once per item.**
This is the worker. Airflow makes **one copy of this task per file** in the list.
Each copy gets a different `filename` but the **same** `bucket`. It returns one
value per copy (here, `len(filename)`).

**3. `summarize` — collect the results.**
Runs **once** after all the copies finish. It receives a **list** containing every
copy's return value (e.g. `[5, 5, 5]`), so you can total/report on them.

## The two key methods

- **`.expand(filename=...)`** → "make one task instance per item in this list."
  This is what fans the work out. The list can come from another task's output
  (like `get_files()`) or be a plain literal.
- **`.partial(bucket=...)`** → "this argument is the **same** for every instance."
  Use it for the fixed/shared args so you only have to vary the one(s) in
  `.expand`.

> Expanding on **two** args creates the **cross product** (every combination),
> e.g. 3 files × 2 buckets = 6 task instances.

## vs static parallel (`notes/5_parallel_and_branching.md`)

| | Static parallel `[a, b, c]` | Dynamic mapping `.expand()` |
| --- | --- | --- |
| How many tasks | fixed when you write the DAG | decided at **run time** |
| The tasks | different functions | **same** function, many copies |
| In the UI | separate nodes | one node with N mapped instances |

## Rule of thumb

- Same operation over an unknown-length list → **dynamic mapping**.
- A few fixed, different steps → static parallel.
- Keep the list **reasonably sized** — a huge list means a huge number of task
  instances.
