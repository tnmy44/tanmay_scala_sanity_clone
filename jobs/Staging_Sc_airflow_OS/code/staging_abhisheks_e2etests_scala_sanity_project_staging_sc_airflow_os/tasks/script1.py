from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

def script1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "script1", bash_command = "echo \"sdaf\"", )
