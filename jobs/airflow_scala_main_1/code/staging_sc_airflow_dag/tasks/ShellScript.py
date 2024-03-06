from staging_sc_airflow_dag.utils import *

def ShellScript():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "ShellScript", bash_command = "ls -ltr", )
