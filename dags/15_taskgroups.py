from airflow.sdk import dag, task, TaskGroup
from pendulum import datetime
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id="taskgroups_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
)
def taskgroups_demo():

    @task.python
    def transform(source: str):
        print(f"Transforming {source}")

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Tasks inside a group get their ids prefixed: e.g. extract.transform_api
    with TaskGroup(group_id="extract") as extract:
        # Groups can nest for deeper structure.
        with TaskGroup(group_id="sources"):
            transform.override(task_id="transform_api")("api")
            transform.override(task_id="transform_db")("db")

    with TaskGroup(group_id="load") as load:
        transform.override(task_id="load_warehouse")("warehouse")

    # Wire whole groups together like single nodes.
    start >> extract >> load >> end

taskgroups_demo()
