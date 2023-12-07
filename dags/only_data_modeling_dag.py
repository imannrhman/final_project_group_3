from airflow.utils.dates import days_ago
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

from utils import constants, arguments

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "public"},
    ),
)


execution_config = ExecutionConfig(
    dbt_executable_path=constants.DBT_EXECUTABLE_PATH
)


only_data_modelling_dag = DbtDag(
    dag_id="only_data_modelling_dag",
    project_config=ProjectConfig("/opt/airflow/dbt/airflowdbt"),
    execution_config=execution_config,
    profile_config=profile_config,
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=arguments.default,
    catchup=False,
    operator_args={ 
         "install_deps": True# install any necessary dependencies before running any dbt command 
     },
    max_active_runs=1
)