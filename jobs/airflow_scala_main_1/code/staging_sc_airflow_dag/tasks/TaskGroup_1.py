from airflow.decorators import task_group
from .TaskGroup_1_tasks import *
from staging_sc_airflow_dag.utils import *

@task_group(group_id = "TaskGroup_1", default_args = {})
def TaskGroup_1_tg():
    S3FileSensor_1_1_op = S3FileSensor_1_1()
    HTTPSensor_1_1_op = HTTPSensor_1_1()
    Slack_1_1_op = Slack_1_1()
    HTTPSensor_1_1_op >> S3FileSensor_1_1_op
    S3FileSensor_1_1_op >> Slack_1_1_op
