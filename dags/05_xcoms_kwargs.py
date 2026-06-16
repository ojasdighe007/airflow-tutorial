from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_dag_kwargs"
)
def xcoms_kwargs_dag():

    @task.python
    def first_task(**kwargs):

        # Extracting ti from kwargs to push XComs manually
        ti = kwargs['ti']

        print("Extracting data ... This is the first task")
        fetched_data = {"key": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value = fetched_data)
    
    @task.python
    def second_task(**kwargs):
        print("Transforming data ... This is the second task")

        #Pulling XComs pushed by first task
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')['key']
        transformed_data = fetched_data*2
        transformed_data_dict =  {"transformed_data": transformed_data}
        ti.xcom_push(key='transformed_result', value = transformed_data_dict)
    
    @task.python
    def third_task(**kwargs):
        print("This is the third task")

        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids = 'second_task', key = 'transformed_result')
        return load_data
    
    # Defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiating the DAG
xcoms_kwargs_dag()