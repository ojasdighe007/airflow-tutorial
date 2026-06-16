from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable


# event_dates are full timestamps: pass tz (naive datetimes are treated as UTC)
# and the time-of-day is honoured (multiple runs on the same day are allowed).
special_dates = EventsTimetable(
    event_dates=[
    datetime(2026, 1, 1, 14, 30, tz="America/Halifax"),   # Jan 1, 2:30 PM
    datetime(2026, 1, 1, 18, 0,  tz="America/Halifax"),   # Jan 1, 6:00 PM
    datetime(2026, 1, 26, 9, 15, tz="America/Halifax"),   # Jan 26, 9:15 AM
    datetime(2026, 1, 30, 0, 0,  tz="America/Halifax"),   # Jan 30, midnight
])

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=1, day=1, tz="America/Halifax"),
    end_date=datetime(year=2026, month=1, day=31, tz="America/Halifax"),
    catchup=True
)
def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Running task for special event on {execution_date}")

    special_event = special_event_task()

# Instantiating the DAG
special_dates_dag()