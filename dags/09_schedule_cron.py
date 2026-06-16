from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id = "scheduled_cron",
    start_date= datetime(year = 2026, month = 6, day = 12, tz = "Asia/Kolkata"),
    schedule= CronTriggerTimetable("13 18 * * MON-FRI", timezone="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup = True
)
def scheduled_cron():

    @task.python
    def first_task():
        print(f"This is the first task")

    
    @task.python
    def second_task():
        print("This is the second task")
    
    @task.python
    def third_task():
        print("This is the third task")
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiating the DAG
scheduled_cron()