from staging_sc_airflow_dag.utils import *

def Script_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_1", bash_command = "echo \"test\"", )
