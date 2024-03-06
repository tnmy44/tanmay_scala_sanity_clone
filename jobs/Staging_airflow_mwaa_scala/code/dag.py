import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from staging_airflow_mwaa_scala_dagg_updated_1.tasks import (
    Email_1,
    HTTPSensor_1,
    S3FileSensor_1,
    SCALA_BASIC,
    Script_1,
    Slack_1_1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "Staging_airflow_mwaa_scala_dagg_updated_1", 
    schedule_interval = "0 0 1 9 *", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.today('UTC'), 
    catchup = True, 
    tags = []
) as dag:
    SCALA_BASIC_op = SCALA_BASIC()
    HTTPSensor_1_op = HTTPSensor_1()
    S3FileSensor_1_op = S3FileSensor_1()
    Slack_1_1_op = Slack_1_1()
    Email_1_op = Email_1()
    Script_1_op = Script_1()
    SCALA_BASIC_op >> HTTPSensor_1_op
    Email_1_op >> Script_1_op
