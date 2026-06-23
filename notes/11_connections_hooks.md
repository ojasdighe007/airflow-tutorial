# Connections & Hooks

How Airflow talks to **external systems** (databases, APIs, cloud storage) without hard-coding secrets. See `dags/16_connections_hooks.py`.

## The two pieces

- **Connection** = stored **credentials + endpoint** (host, login, password, schema, extras), referenced by a `conn_id`. Managed outside your code: UI (Admin -> Connections), CLI, or env var `AIRFLOW_CONN_<ID>`.
- **Hook** = a thin **client** that reads a Connection and gives you ready methods (`get_records`, `insert_rows`, `get_conn`, ...). Hooks are how operators talk to systems under the hood.

```python
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("my_api")   # the Connection must already exist
print(conn.host, conn.login, conn.schema)
```

## Provider hooks (illustrative)

Most real hooks come from **provider packages** and need the package + a configured connection:

```python
# pip install apache-airflow-providers-postgres ; create 'postgres_default' connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id="postgres_default")
rows = hook.get_records("SELECT count(*) FROM users")
```

## Variables (not the same thing)

- **Variables** hold **non-secret config** (paths, flags, small params), via `Variable.get("key", default_var=...)`.
- Use **Connections** for credentials, **Variables** for tunables. Both are managed outside code.

```python
from airflow.sdk import Variable
target = Variable.get("target_bucket", default_var="s3://default-bucket")
```

## Security

- **Never hard-code secrets** in DAG files. Use Connections (or a secrets backend like AWS Secrets Manager / Vault).
- Keep connection extras (tokens, keys) out of git.

## Rule of thumb

- Credentials/endpoints → **Connection** + a **Hook** (or an operator that wraps one).
- Plain config values → **Variable**.
- The DAG references things by `conn_id` / key; the actual secrets live in Airflow, not the repo.
