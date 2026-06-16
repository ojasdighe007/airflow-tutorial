from airflow.sdk import dag, task

@dag
def operators_dag():

    @task.python
    def first_task():
        print("This is the first task")
    
    @task.python
    def second_task():
        print("This is the second task")
    
    @task.bash
    def run_bash_task():
        return "echo 'This is the bash task'"
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = run_bash_task()

    first >> second >> third

#Instantiating the DAG
operators_dag() 