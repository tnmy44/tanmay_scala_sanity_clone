from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

def Branch_1_1():

    def branchtest():
        return "script1_1"

    from datetime import timedelta
    from airflow.operators.python import BranchPythonOperator

    return BranchPythonOperator(task_id = "Branch_1_1", python_callable = branchtest, )
