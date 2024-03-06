from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

@task_wrapper(task_id = "TriggerDag_1")
def TriggerDag_1(ti=None, params=None, **context):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from datetime import timedelta

    return TriggerDagRunOperator(
        task_id = "TriggerDag_1",
        trigger_dag_id = "test_sony_livy_os_airflow_os",
        conf = default,
        reset_dag_run = False, 
        wait_for_completion = False
    )
