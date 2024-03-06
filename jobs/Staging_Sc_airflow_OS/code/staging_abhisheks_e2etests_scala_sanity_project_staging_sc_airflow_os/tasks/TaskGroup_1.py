from airflow.decorators import task_group
from .TaskGroup_1_tasks import *
from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

@task_group(group_id = "TaskGroup_1", default_args = {})
def TaskGroup_1_tg():
    Slack_1_1_op = Slack_1_1()
    script1_1_op = script1_1()
    Email_1_1_op = Email_1_1()
    Branch_1_1_op = Branch_1_1()
    Branch_1_1_op >> [Email_1_1_op, script1_1_op]
