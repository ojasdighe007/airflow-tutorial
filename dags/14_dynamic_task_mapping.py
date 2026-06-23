from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="dynamic_task_mapping",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
)
def dynamic_task_mapping():

    # Produces a list at runtime -> we don't know its length until it runs.
    @task.python
    def get_files():
        return ["a.csv", "b.csv", "c.csv"]

    # One mapped task instance is created PER element of the list.
    # .partial() fixes args that are the same for every instance;
    # .expand() supplies the per-instance arg.
    @task.python
    def process_file(filename: str, bucket: str):
        print(f"Processing {filename} from {bucket}")
        return len(filename)

    # Reduce step: receives the list of all mapped return values.
    @task.python
    def summarize(sizes: list[int]):
        print(f"Processed {len(sizes)} files, total size proxy = {sum(sizes)}")

    sizes = process_file.partial(bucket="s3://raw").expand(filename=get_files())
    summarize(sizes)

dynamic_task_mapping()
