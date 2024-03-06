import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from staging_abhisheks_e2etests_scala_sanity_project_staging_airflow253_scala_composer_job.tasks import (
    Branch_1_1,
    DBT_1,
    Email_1_1,
    Email_2,
    ForEachLoop_1_tg,
    Python_1,
    S3FileSensor_1,
    SM_IO_SCALA_BASIC,
    Script_1,
    ShellScript,
    TaskGroup_1_tg,
    TriggerDag_1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "staging_abhisheks_e2etests_Scala_Sanity_Project_Staging_AIRFLOW253_SCALA_COMPOSER_JOB", 
    schedule_interval = "0 0 1 1 *", 
    default_args = {
      "owner": "Prophecy", 
      "retries": 0, 
      "retry_delay": timedelta(minutes = 1.0), 
      "ignore_first_depends_on_past": True, 
      "do_xcom_push": True
    }, 
    start_date = pendulum.today('UTC'), 
    catchup = True, 
    tags = []
) as dag:
    Python_1_op = Python_1()
    ForEachLoop_1_op = ForEachLoop_1_tg.expand(value = Python_1_op.output)
    ShellScript_op = ShellScript()
    DBT_1_op = DBT_1()
    SM_IO_SCALA_BASIC_op = SM_IO_SCALA_BASIC()
    Script_1_op = Script_1()
    S3FileSensor_1_op = S3FileSensor_1()
    Branch_1_1_op = Branch_1_1()
    Email_2_op = Email_2()
    TriggerDag_1_op = TriggerDag_1()
    TaskGroup_1_op = TaskGroup_1_tg()
    Email_1_1_op = Email_1_1()
    SM_IO_SCALA_BASIC_op >> Script_1_op
    Branch_1_1_op >> [Email_1_1_op, Email_2_op]
    Script_1_op >> S3FileSensor_1_op
    Python_1_op >> [ForEachLoop_1_op, TaskGroup_1_op]
    ShellScript_op >> [Branch_1_1_op, DBT_1_op, TriggerDag_1_op]
