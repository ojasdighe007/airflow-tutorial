from airflow.sdk import dag, task
from upath.implementations.cloud import S3Path

@dag
def conditional_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data": [1,2,3],
                                "db_extracted_data": [4,5,6],
                                "s3_extracted_data": [7,8,9]
                                ,"weekend_flag": False}
        ti.xcom_push(key = 'return_value', value = extracted_data_dict)
    
    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_value')['api_extracted_data']
        transformed_api_data = [i*10 for i in api_extracted_data]
        ti.xcom_push(key = 'transformed_data', value = transformed_api_data)
    
    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_value')['db_extracted_data']
        transformed_db_data = [i*100 for i  in db_extracted_data]
        ti.xcom_push(key = 'transformed_data', value = transformed_db_data)
        
    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_value')['s3_extracted_data']
        transformed_s3_data = [i*100 for i  in s3_extracted_data]
        ti.xcom_push(key = 'transformed_data', value = transformed_s3_data)
    
    # Creating the decider task 
    @task.branch 
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids = 'extract_task', key = 'return_value')['weekend_flag']
        task_to_run = ''
        if weekend_flag:
            task_to_run = 'no_load_task'
        else:
            task_to_run = 'load_task'
        return task_to_run
    
    @task.bash 
    def load_task(**kwargs):
        print("Loading data to the destination")
        api_data  = kwargs['ti'].xcom_pull(task_ids = 'transform_task_api', key = 'transformed_data')
        db_data  = kwargs['ti'].xcom_pull(task_ids = 'transform_task_db', key = 'transformed_data')
        s3_data  = kwargs['ti'].xcom_pull(task_ids = 'transform_task_s3', key = 'transformed_data')

        return f"echo 'Loaded Data: {api_data}, {db_data}, {s3_data}' "

        
    @task.bash 
    def no_load_task(**kwargs):
        return f"echo 'It is a weekend, so no loading'"

    # Defining the task dependencies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load_task = load_task()
    no_load_task = no_load_task()

    extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [load_task,no_load_task]

#Instantiating the DAG
conditional_dag()