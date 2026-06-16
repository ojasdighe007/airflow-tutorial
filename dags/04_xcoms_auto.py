from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_dag_auto"
)
def xcoms_auto_dag():

    @task.python
    def first_task():
        print("Extracting data ... This is the first task")
        fetched_data = {"key": [1,2,3,4,5]}
        return fetched_data
    
    @task.python
    def second_task(data:dict):
        print("Transforming data ... This is the second task")
        fetched_data = data['key']
        transformed_data = fetched_data*2
        transformed_data_dict =  {"transformed_data": transformed_data}
        return transformed_data_dict
    
    @task.python
    def third_task(data: dict):
        print("This is the third task")
        load_data = data
        return load_data
    
    # Defining the task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

    first >> second >> third

#Instantiating the DAG
xcoms_auto_dag()