from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

def Python_1():

    def pyth():
        return "test_sony_livy_os_airflow_os"

    import json
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(task_id = "Python_1", python_callable = pyth, show_return_value_in_logs = True)
