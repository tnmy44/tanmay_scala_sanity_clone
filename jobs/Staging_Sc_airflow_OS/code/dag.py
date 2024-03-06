import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.tasks import (
    Branch_1,
    Email_1,
    Python_1,
    Slack_1,
    TaskGroup_1_tg,
    TriggerDag_1,
    script1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "staging_abhisheks_e2etests_Scala_Sanity_Project_Staging_Sc_airflow_OS", 
    schedule_interval = None, 
    default_args = {"owner" : "Prophecy", "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.today('UTC'), 
    catchup = True
) as dag:
    TriggerDag_1_op = TriggerDag_1()
    Python_1_op = Python_1()
    Slack_1_op = Slack_1()
    TaskGroup_1_op = TaskGroup_1_tg()
    Branch_1_op = Branch_1()
    Email_1_op = Email_1()
    script1_op = script1()
    Branch_1_op >> [Email_1_op, script1_op]
