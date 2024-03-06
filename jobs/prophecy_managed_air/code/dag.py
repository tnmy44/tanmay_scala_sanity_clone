import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from ovcu7hplihc7jbzprtndwa_.tasks import (
    Email_1,
    HTTPSensor_1,
    REL_SC_PIP_DEP_MGMT_ALL,
    S3FileSensor_1,
    SCALA_BASIC,
    SM_IO_SCALA_BASIC,
    Slack_1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "OVCU7HPLIHc7JbZPRTnDwA_", 
    schedule_interval = "0 0/1 * * *", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "hhFvJ5E5"}, 
    start_date = pendulum.datetime(2023, 10, 10, tz = "UTC"), 
    end_date = pendulum.datetime(2025, 7, 10, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    SM_IO_SCALA_BASIC_op = SM_IO_SCALA_BASIC()
    HTTPSensor_1_op = HTTPSensor_1()
    S3FileSensor_1_op = S3FileSensor_1()
    SCALA_BASIC_op = SCALA_BASIC()
    REL_SC_PIP_DEP_MGMT_ALL_op = REL_SC_PIP_DEP_MGMT_ALL()
    Email_1_op = Email_1()
    Slack_1_op = Slack_1()
    SCALA_BASIC_op >> REL_SC_PIP_DEP_MGMT_ALL_op
    SM_IO_SCALA_BASIC_op >> HTTPSensor_1_op
    REL_SC_PIP_DEP_MGMT_ALL_op >> Email_1_op
    HTTPSensor_1_op >> S3FileSensor_1_op
    S3FileSensor_1_op >> Slack_1_op
