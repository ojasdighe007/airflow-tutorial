from datetime import timedelta
from airflow.sdk import dag, task
from pendulum import datetime


def on_failure(context):
    ti = context["task_instance"]
    print(f"ALERT: {ti.task_id} failed on run {context['run_id']}")


def on_retry(context):
    ti = context["task_instance"]
    print(f"Retrying {ti.task_id} (attempt {ti.try_number})")


def on_success(context):
    print(f"OK: {context['task_instance'].task_id} succeeded")


@dag(
    dag_id="callbacks_retries_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    default_args={
        # Applied to every task; backoff grows the wait after each failure.
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,   # 10s -> 20s -> 40s ...
        "max_retry_delay": timedelta(minutes=5),
    },
)
def callbacks_retries_demo():

    @task.python(on_failure_callback=on_failure, on_retry_callback=on_retry)
    def flaky_task():
        # Fails on purpose to demonstrate retries + callbacks.
        raise ValueError("Simulated transient failure")

    @task.python(on_success_callback=on_success)
    def steady_task():
        print("This one just works")

    steady_task() >> flaky_task()

callbacks_retries_demo()
