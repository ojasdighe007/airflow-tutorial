from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="pools_concurrency_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    # DAG-level caps:
    max_active_runs=1,    # at most 1 run of THIS dag at a time
    max_active_tasks=2,   # at most 2 task instances running concurrently in this dag
)
def pools_concurrency_demo():

    # 'pool' caps concurrency for a class of tasks across ALL dags sharing it.
    # The pool must be created first (Admin -> Pools, or:
    #   airflow pools set api_pool 2 "rate-limited API").
    # priority_weight decides ordering when slots are scarce (higher runs first).
    @task.python(pool="api_pool", priority_weight=10)
    def call_api(n: int):
        print(f"Calling rate-limited API #{n}")

    @task.python(priority_weight=1)
    def low_priority():
        print("Runs later when slots are contended")

    [call_api.override(task_id=f"call_api_{i}")(i) for i in range(4)] >> low_priority()

pools_concurrency_demo()
