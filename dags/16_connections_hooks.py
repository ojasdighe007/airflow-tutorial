from airflow.sdk import dag, task, Variable
from pendulum import datetime
from airflow.hooks.base import BaseHook

@dag(
    dag_id="connections_hooks_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
)
def connections_hooks_demo():

    @task.python
    def read_connection():
        # A Connection stores credentials/endpoint, configured in the UI/CLI/env
        # (Admin -> Connections, or AIRFLOW_CONN_MY_API=...). Never hard-code secrets.
        conn = BaseHook.get_connection("my_api")  # illustrative: must exist
        print(f"host={conn.host} login={conn.login} schema={conn.schema}")

    @task.python
    def read_variable():
        # Variables hold non-secret config (paths, flags, small params).
        target = Variable.get("target_bucket", default_var="s3://default-bucket")
        print(f"Loading into {target}")

    @task.python
    def use_provider_hook():
        # Illustrative provider hook usage. Requires:
        #   pip install apache-airflow-providers-postgres
        #   a 'postgres_default' connection configured in Airflow.
        #
        # from airflow.providers.postgres.hooks.postgres import PostgresHook
        # hook = PostgresHook(postgres_conn_id="postgres_default")
        # rows = hook.get_records("SELECT count(*) FROM users")
        # print(rows)
        print("Provider hooks (e.g. PostgresHook) wrap a Connection into a ready client")

    read_connection() >> read_variable() >> use_provider_hook()

connections_hooks_demo()
