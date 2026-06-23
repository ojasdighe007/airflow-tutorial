from airflow.sdk import dag, task
from pendulum import datetime
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id="trigger_rules_demo",
    start_date=datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
)
def trigger_rules_demo():

    # A task's trigger_rule decides when it runs based on the state of ALL its
    # upstream tasks. The default is ALL_SUCCESS (every parent must succeed).

    start = EmptyOperator(task_id="start")

    @task.python
    def good_a():
        print("A ok")

    @task.python
    def good_b():
        print("B ok")

    # Fails on purpose so we can see how the join tasks below react.
    @task.python
    def bad_c():
        raise ValueError("C failed on purpose")

    # ALL_DONE: runs once every upstream has finished, regardless of state.
    # Great for cleanup/teardown that must always happen.
    @task.python(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup():
        print("Always runs - good for teardown")

    # ONE_SUCCESS: fires as soon as ANY upstream succeeds (doesn't wait for all).
    @task.python(trigger_rule=TriggerRule.ONE_SUCCESS)
    def notify_on_any_success():
        print("At least one upstream succeeded")

    # ONE_FAILED: fires as soon as ANY upstream fails - handy for alerting.
    @task.python(trigger_rule=TriggerRule.ONE_FAILED)
    def alert_on_any_failure():
        print("Something upstream failed - sending alert")

    # NONE_FAILED_MIN_ONE_SUCCESS: tolerates skips but not failures, and needs
    # at least one real success. Common join rule after a branch.
    @task.python(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def summarize():
        print("No upstream failed and at least one succeeded")

    a, b, c = good_a(), good_b(), bad_c()

    start >> [a, b, c]
    [a, b, c] >> cleanup()
    [a, b, c] >> notify_on_any_success()
    [a, b, c] >> alert_on_any_failure()
    [a, b] >> summarize()

    # --- Buffer (absorber) pattern -------------------------------------------
    # Goal: t_join should tolerate t_flaky failing, but still stay strict about
    # t_solid. A single trigger_rule can't express per-parent tolerance, so we
    # restructure the graph: a buffer sits below t_flaky, runs on ALL_DONE, and
    # always succeeds - so t_flaky's failure never reaches t_join directly.
    @task.python
    def t_solid():
        print("Required upstream - must succeed")

    @task.python
    def t_flaky():
        raise ValueError("Tolerated upstream failed on purpose")

    @task.python(trigger_rule=TriggerRule.ALL_DONE)
    def t_flaky_buffer():
        # Absorbs t_flaky's outcome and always ends 'success'.
        print("t_flaky finished (success or fail) - continuing")

    # t_join keeps the default all_success, but over t_solid + the buffer,
    # never over t_flaky directly.
    @task.python
    def t_join():
        print("Runs if t_solid succeeded, regardless of t_flaky")

    solid, flaky = t_solid(), t_flaky()
    buffer = t_flaky_buffer()
    start >> [solid, flaky]
    flaky >> buffer
    [solid, buffer] >> t_join()

trigger_rules_demo()
