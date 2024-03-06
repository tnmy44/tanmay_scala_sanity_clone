from staging_sc_airflow_dag.utils import *

def DBT_1():
    from airflow.operators.bash import BashOperator
    envs = {}
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "/home/airflow/gcs/data":
        envs = {"DBT_PROFILES_DIR" : "/home/airflow/gcs/data"}

    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile_snowflake":
        dbt_props_cmd = " --profile run_profile_snowflake"
        dbt_deps_cmd = " deps --profile run_profile_snowflake"

    return BashOperator(
        task_id = "DBT_1",
        bash_command = f'''{" && ".join(
          ["set -euxo pipefail && tmpDir=`mktemp -d` && git clone https://github.com/abhisheks-prophecy/sql_snowflake_public_parent --branch main_staging --single-branch $tmpDir && cd $tmpDir/",            "dbt" + dbt_deps_cmd,  "dbt seed" + dbt_props_cmd,  "dbt run" + dbt_props_cmd,            "dbt test" + dbt_props_cmd]
        )}''',
        env = envs,
        append_env = True,
        retries = 0
    )
