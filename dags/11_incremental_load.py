from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="incremental_load",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=True,
)
def incremental_load():

    @task.python
    def extract_window(**kwargs):
        # Only pull the rows for THIS run's interval, not the whole table.
        start = kwargs["data_interval_start"]
        end = kwargs["data_interval_end"]
        print(f"Extracting rows WHERE updated_at >= {start} AND updated_at < {end}")
        # Illustrative: pretend we fetched some rows for this window
        return {"window_start": str(start), "window_end": str(end), "row_count": 42}

    @task.python
    def load_window(batch: dict):
        # Idempotent load: re-running the same interval overwrites the same partition.
        print(f"Loading {batch['row_count']} rows for {batch['window_start']} -> {batch['window_end']}")

    load_window(extract_window())

incremental_load()
