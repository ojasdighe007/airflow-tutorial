from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.python import PythonSensor

@dag(
    dag_id="sensors_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
)
def sensors_demo():

    # 1. TaskFlow sensor: return True to "succeed", False to keep poking.
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def wait_for_flag(**kwargs):
        ready = True  # replace with a real check (api/db/file)
        print(f"Polling for readiness... ready={ready}")
        return ready

    # 2. Classic FileSensor (illustrative): needs a 'fs_default' connection.
    #    mode="reschedule" frees the worker slot between pokes (good for long waits).
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/incoming/data.csv",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )

    # 3. PythonSensor: poke a custom callable until it returns truthy.
    def _check():
        return True

    wait_for_condition = PythonSensor(
        task_id="wait_for_condition",
        python_callable=_check,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    @task.python
    def process():
        print("All sensors satisfied - processing now")

    [wait_for_flag(), wait_for_file, wait_for_condition] >> process()

sensors_demo()
