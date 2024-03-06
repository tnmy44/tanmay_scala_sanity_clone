from staging_abhisheks_e2etests_scala_sanity_project_staging_airflow253_scala_composer_job.utils import *

def ShellScript():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "ShellScript", bash_command = "ls -ltr", )
