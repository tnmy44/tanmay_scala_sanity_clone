from staging_abhisheks_e2etests_scala_sanity_project_staging_airflow253_scala_composer_job.utils import *

def Slack_1_1():
    from airflow.providers.slack.operators.slack import SlackAPIPostOperator
    from datetime import timedelta

    return SlackAPIPostOperator(
        task_id = "Slack_1_1",
        text = "Python Sanity Job Run",
        channel = "abhyslackpub",
        slack_conn_id = "slack_default",
        retries = 0
    )
