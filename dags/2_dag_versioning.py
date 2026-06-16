from asyncio import sleep
from airflow.sdk import dag, task

@dag
def versioned_dag():

    @task.python
    def first_task():
        print("This is the first task")
    
    @task.python
    def second_task():
        print("This is the second task")
    
    @task.python
    def third_task():
        print("This is the third task")

    @task.python
    def fourth_task():
        print("This is the version task. DAG version 4.0!!!")

    @task.python
    def fifth_task():
        print("This is the version task. DAG version 5.0!!!")

    @task.python
    def sixth_task():
        print("This is the version task. DAG version 6.0!!!")
    
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()
    fifth = fifth_task()
    sixth = sixth_task()

    first >> second >> third >> fourth >> fifth >> sixth

#Instantiating the DAG
versioned_dag()