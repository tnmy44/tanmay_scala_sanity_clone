from staging_airflow_mwaa_scala_dagg_updated_1.utils import *

def Script_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_1", bash_command = "echo \"test test\"", )
