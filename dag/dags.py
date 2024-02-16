from pendulum import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from cosmos import DbtDag, LoadMode, RenderConfig, DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos.constants import TestBehavior
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator

PROJECT_ROOT_PATH="/opt/airflow/git/dbt-azure-monitor.git/dags/dbt/dbtAzureMonitor" # --> managed airflow path
#PROJECT_ROOT_PATH="/home/gopal/dbt-workspace/jaffle_shop/dags/dbt/jaffle_shop"  --> local development path
# PROJECT_ROOT_PATH=Variable.get("PROJECT_ROOT_PATH")

profile_config = ProfileConfig(
    profile_name="dbtAzureMonitor",
    target_name="dev",
    #If you are using profiles.yml file in git use below profiles_yml_filepath
    #profiles_yml_filepath=f"{PROJECT_ROOT_PATH}/profiles.yml",
    #here we are using Airflow connection to provide profile details
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id = 'dbtAzureMonitor_databricks' 
    )
)

with DAG(
        dag_id="airlinedemo_dag",
        start_date=datetime(2024, 2, 15),
        schedule_interval="@daily",
) as dag:
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        task_id="dbt_tasks",
        project_config=ProjectConfig(
            dbt_project_path=PROJECT_ROOT_PATH,
            manifest_path=f"{PROJECT_ROOT_PATH}/target/manifest.json",
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # By default cosmos generate dag that execute model and test for that model, if you don't want to use test then pass test_behavior=TestBehavior.NONE
            # test_behavior=TestBehavior.NONE
        ),
    )

    e2 = EmptyOperator(task_id="post_dbt")

    run_this_2 = BashOperator(
        task_id="run_after_loop",
        bash_command="dbtlog --env=databricks --dwhcid=databricks_connection --azmonitorcid=azure_monitor",
        trigger_rule="all_done"
    )

    e1 >> dbt_tg >> e2 >> run_this_2
