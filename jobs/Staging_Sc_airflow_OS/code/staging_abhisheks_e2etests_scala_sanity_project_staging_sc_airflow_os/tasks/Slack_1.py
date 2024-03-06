from staging_abhisheks_e2etests_scala_sanity_project_staging_sc_airflow_os.utils import *

def Slack_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1",
        text = "hey!",
        channel = "sonytest",
        slack_conn_id = "slack_default",
    )
