from staging_airflow_mwaa_scala_dagg_updated_1.utils import *

def Slack_1_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1_1",
        text = "test msg from sanity job",
        channel = "sonytest",
        slack_conn_id = "slack_default",
    )
