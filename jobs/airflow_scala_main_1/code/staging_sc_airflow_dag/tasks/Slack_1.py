from staging_sc_airflow_dag.utils import *

def Slack_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1",
        text = "test msg from sanity job",
        channel = "sonytest",
        slack_conn_id = "slack_default",
    )
