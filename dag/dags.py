from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, LoadMode, RenderConfig, DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos.constants import TestBehavior
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

PROJECT_ROOT_PATH="/opt/airflow/git/dbt-azure-monitor.git/dags/dbt/dbtAzureMonitor" # --> managed airflow path
#PROJECT_ROOT_PATH="/home/gopal/dbt-workspace/jaffle_shop/dags/dbt/jaffle_shop"  --> local development path
# PROJECT_ROOT_PATH=Variable.get("ROOT_PATH")

profile_config = ProfileConfig(
    profile_name="dbtAzureMonitor",
    target_name="dev",
    #If you are using profiles.yml file in git use below profiles_yml_filepath
    # profiles_yml_filepath=f"{PROJECT_ROOT_PATH}/profiles.yml"
    #here we are using Airflow connection to provide profile details
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id = 'databricks_connection' 
    )
)

default_args = {
 'start_date': datetime (2024, 2, 14),
 'retries': 1,
}

# Instantiate your DAG
dag = DAG ('my_first_dag', default_args=default_args, schedule_interval=None)

# Define tasks
def task1():
 print ("Executing Task 1")

def task2():
 print ("Executing Task 2")

task_1 = PythonOperator(
 task_id='task_1',
 python_callable=task1,
 dag=dag,
)
task_2 = PythonOperator(
 task_id='task_2',
 python_callable=task2,
 dag=dag,
)

# Set task dependencies
task_1 >> task_2